"""Wake a query when a commit changes a column it reads.

SQLite names the tables and columns once, when the watch opens. The
writer records inserts, deletes, and updated columns while a watch is
open. After ``COMMIT`` a watch wakes on an insert or delete of its
table, or an update of one of its columns. The watch reads again on
its own loop and yields when the rows differ.
"""

# pyright: reportPrivateUsage=false

import asyncio
from asyncio import AbstractEventLoop
from collections.abc import Callable, Mapping
from threading import Lock
from typing import TYPE_CHECKING, Any, Self, cast

from litewriter.errors import StateError
from litewriter.q import run

if TYPE_CHECKING:
    from litewriter.writer import LiteWriter


class Watch[T]:
    """One live query. Use it as ``async with db.watch(q) as live``.

    ``into`` builds each row the way ``db.query`` does. ``into`` runs
    only when the raw rows change.
    """

    __slots__ = (
        "_bound",
        "_db",
        "_event",
        "_gen",
        "_have",
        "_into",
        "_key",
        "_last",
        "_lock",
        "_loop",
        "_seen",
        "_sql",
        "_stop",
        "columns",
        "tables",
        "wid",
    )

    def __init__(
        self,
        db: "LiteWriter",  # noqa: UP037
        text: str,
        bound: Mapping[str, object] | None,
        into: Callable[..., T],
    ) -> None:
        self._db = db
        self._sql = text
        self._bound = bound
        self._into = into
        try:
            self._key: object = (
                text,
                frozenset(bound.items()) if bound else None,
            )
        except TypeError:
            self._key = None
        self.tables: frozenset[str] = frozenset()
        self.columns: frozenset[tuple[str, str]] = frozenset()
        self.wid = 0
        self._gen = 0
        self._seen = -1
        self._have = False
        self._stop = False
        self._lock = Lock()
        self._loop: AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None
        self._last: tuple[tuple[Any, ...], ...] = ()

    async def __aenter__(self) -> Self:
        if self._event is not None:
            raise StateError("this watch is already open")
        self._db._require_started()
        self.tables, self.columns = self._db._dependencies(self._sql)
        loop = asyncio.get_running_loop()
        self._db._wakes.ensure(loop)
        self._loop = loop
        self._event = asyncio.Event()
        try:
            self._db._track.add(self)
        except BaseException:
            self._loop = None
            self._event = None
            raise
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._db._track.drop(self)
        self.stop()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> list[T]:
        event = self._event
        if event is None:
            raise StateError(
                "open the watch with async with",
                example="async with db.watch(doc) as live:\n    async for rows in live:\n        ...",
            )
        while True:
            with self._lock:
                if self._stop:
                    raise StopAsyncIteration
                gen = self._gen
            if gen != self._seen:
                key = self._key
                rows = (
                    self._read()
                    if key is None
                    else self._db._track.read_shared(key, self._read)
                )
                with self._lock:
                    if self._stop:
                        raise StopAsyncIteration
                    now = self._gen
                if now != gen:
                    continue
                self._seen = gen
                if not self._have or rows != self._last:
                    self._have = True
                    self._last = rows
                    if self._into is tuple:
                        return cast("list[T]", list(rows))
                    return [self._into(*row) for row in rows]
            event.clear()
            with self._lock:
                if self._stop:
                    raise StopAsyncIteration
                if self._gen != self._seen:
                    continue
            await event.wait()

    def advance(self) -> tuple[AbstractEventLoop, Callable[[], None]] | None:
        """Count this commit. The caller sets the event on the returned loop."""
        with self._lock:
            self._gen += 1
            event = self._event
            loop = self._loop
        if event is None or loop is None:
            return None
        return loop, event.set

    def stop(self) -> None:
        """Unblock a waiter. The next read stops."""
        with self._lock:
            self._stop = True
            event = self._event
        if event is not None:
            self._on_loop(event.set)

    def _on_loop(self, fn: Callable[[], None]) -> None:
        """Run ``fn`` on the watch loop. Direct when we are already there.

        ``close`` runs on that loop and then joins the writer. A posted
        wake would sit in the mailbox until the loop runs, which it cannot
        do while ``close`` waits. Set the event here instead.
        """
        loop = self._loop
        if loop is None:
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            fn()
            return
        self._db._wakes.post(loop, fn)

    def _read(self) -> tuple[tuple[Any, ...], ...]:
        """The raw rows. ``into`` builds objects only on a real change."""
        conn = self._db._conn()
        return tuple(run(conn, self._sql, self._bound))
