"""The job queue. Many producers, one writer thread.

The inbox holds one condition. A job's future stores its value, then
wakes the waiter. That stays true when the GIL is off.
"""

import inspect
from asyncio import AbstractEventLoop
from asyncio import Future as AsyncFuture
from asyncio import InvalidStateError as AsyncSettled
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future, InvalidStateError
from threading import Condition
from typing import Any

from litewriter.errors import QueryError, StateError


def _async_write() -> QueryError:
    return QueryError(
        "a write function is a plain def, not async def",
        help_text="It runs on the writer thread inside the transaction.",
        example="def add(tx: Tx, body: str) -> int:\n    ...",
    )


class Job:
    """One write. Built on the caller thread. Run on the writer thread."""

    __slots__ = ("args", "fn", "future", "kwargs", "loop")

    def __init__(
        self,
        fn: Callable[..., object],
        args: tuple[object, ...],
        kwargs: dict[str, Any],
        future: Future[Any] | AsyncFuture[Any] | None,
        loop: AbstractEventLoop | None,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.future = future
        self.loop = loop

    def run(self, tx: object) -> object:
        if self.kwargs:
            result = self.fn(tx, *self.args, **self.kwargs)
        elif self.args:
            result = self.fn(tx, *self.args)
        else:
            result = self.fn(tx)
        if inspect.iscoroutine(result):
            result.close()
            raise _async_write()
        return result

    def claim(self) -> bool:
        """False when the caller cancelled before the writer started this.

        A ``concurrent.futures.Future`` moves to RUNNING (a pending
        ``cancel()`` already won). An asyncio future cancelled by its
        task reports here. A push job (no future) always runs.
        """
        future = self.future
        if future is None:
            return True
        if isinstance(future, Future):
            return future.set_running_or_notify_cancel()
        return not future.cancelled()


class Inbox[T = Job | None]:
    """Thread-safe deque. Many producers, one writer thread."""

    __slots__ = ("_closed", "_cond", "_items")

    def __init__(self) -> None:
        self._cond = Condition()
        self._items: deque[T] = deque()
        self._closed = False

    def put(self, item: T) -> None:
        cond = self._cond
        with cond:
            if self._closed:
                raise StateError("the writer is closed")
            self._items.append(item)
            cond.notify()

    def take_batch(self) -> list[T]:
        """Wait for one item, then drain the rest under the same lock."""
        cond = self._cond
        with cond:
            while not self._items:
                cond.wait()
            batch = list(self._items)
            self._items.clear()
            return batch

    def close(self) -> list[T]:
        """Refuse new items. Return the items that never ran."""
        with self._cond:
            self._closed = True
            rest = list(self._items)
            self._items.clear()
            return rest

    def reopen(self) -> None:
        with self._cond:
            self._closed = False


def settle(
    future: Future[Any] | AsyncFuture[Any],
    result: object,
    error: BaseException | None,
) -> None:
    """Deliver one job's outcome. A cancelled waiter is skipped."""
    if future.done():
        return
    try:
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)
    except InvalidStateError, AsyncSettled:
        pass
