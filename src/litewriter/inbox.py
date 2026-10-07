"""Inbox, job, and result slot.

Each of those holds one condition. A slot stores its value, then wakes
the waiter. That stays true when the GIL is off.
"""

from asyncio import AbstractEventLoop
from asyncio import Future as AsyncFuture
from collections import deque
from collections.abc import Callable
from threading import Condition
from time import monotonic
from typing import Any, cast

from litewriter.errors import WriterRuntime


class Slot[R = object]:
    """One result. The value is published under the condition, then the waiter wakes."""

    __slots__ = ("_cond", "_done", "_error", "_value")

    def __init__(self) -> None:
        self._cond = Condition()
        self._value: R | None = None
        self._error: BaseException | None = None
        self._done = False

    def _finish(self, value: R | None, error: BaseException | None) -> None:
        with self._cond:
            if self._done:
                return
            self._value = value
            self._error = error
            self._done = True
            self._cond.notify_all()

    def set_result(self, value: R) -> None:
        self._finish(value, None)

    def set_exception(self, exc: BaseException) -> None:
        self._finish(None, exc)

    def done(self) -> bool:
        with self._cond:
            return self._done

    def result(self, timeout: float | None = None) -> R:
        with self._cond:
            if not self._done:
                if timeout is None:
                    while not self._done:
                        self._cond.wait()
                else:
                    deadline = monotonic() + timeout
                    while not self._done:
                        left = deadline - monotonic()
                        if left <= 0 or not self._cond.wait(left):
                            raise TimeoutError("writer slot timed out")
            if self._error is not None:
                raise self._error
            return cast(R, self._value)


class Job:
    """One write. Built on the caller thread. Run on the writer thread."""

    __slots__ = ("args", "fn", "future", "isolated", "kwargs", "loop")

    def __init__(
        self,
        fn: Callable[..., object],
        args: tuple[object, ...],
        kwargs: dict[str, Any],
        isolated: bool,
        future: Slot[Any] | AsyncFuture[Any] | None,
        loop: AbstractEventLoop | None,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.isolated = isolated
        self.future = future
        self.loop = loop

    def run(self, tx: object) -> object:
        if self.kwargs:
            return self.fn(tx, *self.args, **self.kwargs)
        if self.args:
            return self.fn(tx, *self.args)
        return self.fn(tx)


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
                raise WriterRuntime("the writer is closed")
            self._items.append(item)
            cond.notify()

    def get(self) -> T:
        cond = self._cond
        with cond:
            while not self._items:
                cond.wait()
            return self._items.popleft()

    def take_rest(self) -> list[T]:
        with self._cond:
            if not self._items:
                return []
            rest = list(self._items)
            self._items.clear()
            return rest

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


def enqueue_submit(
    inbox: Inbox[Job | None],
    fn: Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, Any],
    isolated: bool,
) -> Slot[Any]:
    slot: Slot[Any] = Slot()
    inbox.put(Job(fn, args, kwargs, isolated, slot, None))
    return slot


def enqueue_call(
    inbox: Inbox[Job | None],
    fn: Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, Any],
    isolated: bool,
    future: AsyncFuture[Any],
    loop: AbstractEventLoop,
) -> None:
    inbox.put(Job(fn, args, kwargs, isolated, future, loop))


def enqueue_push(
    inbox: Inbox[Job | None],
    fn: Callable[..., object],
    args: tuple[object, ...],
    kwargs: dict[str, Any],
    isolated: bool,
) -> None:
    inbox.put(Job(fn, args, kwargs, isolated, None, None))


def settle(
    future: Slot[Any] | AsyncFuture[Any], result: object, error: BaseException | None
) -> None:
    if future.done():
        return
    if error is not None:
        future.set_exception(error)
    else:
        future.set_result(result)
