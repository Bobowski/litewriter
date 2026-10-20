"""One read-only connection per OS thread, plus ``db.snapshot()``.

SQLite connections are not shared. A read does not need the writer
thread. The file must exist. ``seal`` marks the writer closed and
closes every open connection, on any thread. The generation counter
invalidates the thread-local state; the closed connection drops on the
next read.
"""

# pyright: reportPrivateUsage=false

import asyncio
import weakref
from asyncio import AbstractEventLoop
from contextlib import suppress
from pathlib import Path
from threading import Lock, local
from typing import TYPE_CHECKING, cast

import apsw

from litewriter.connect import connect
from litewriter.errors import StateError

if TYPE_CHECKING:
    from litewriter.writer import LiteWriter


def _missing_file() -> StateError:
    return StateError(
        "the database file does not exist",
        example="db = LiteWriter(path, schema=SCHEMA)\ndb.start()",
    )


def _snapshot_span() -> StateError:
    return StateError(
        "db.snapshot() cannot span an await",
        help_text="Read what you need, then leave the block before you await.",
        example=(
            "with db.snapshot():\n"
            "    a = db.query(...)\n"
            "    b = db.query(...)\n"
            "await ..."
        ),
    )


class _ReadState:
    """This thread's read connection and its snapshot bookkeeping."""

    __slots__ = (
        "conn",
        "gen",
        "snap",
        "snap_broken",
        "snap_task",
        "token",
        "txn",
    )

    def __init__(self, conn: apsw.Connection, gen: int) -> None:
        self.conn = conn
        self.gen = gen
        self.txn = False
        self.token = 0
        self.snap = 0
        self.snap_task: asyncio.Task[object] | None = None
        self.snap_broken = False


class ThreadReads:
    """One read-only connection per OS thread.

    SQLite connections are not shared. A read does not need the writer
    thread. ``db.snapshot()`` holds one fixed view until the block
    exits. ``seal`` marks the writer closed and closes every open
    connection, on any thread. The generation counter invalidates the
    thread-local state; the closed connection drops on the next read.
    """

    __slots__ = ("_busy_ms", "_closed", "_conns", "_gen", "_local", "_lock")

    def __init__(self, busy_ms: int) -> None:
        self._local = local()
        self._lock = Lock()
        self._closed = False
        self._busy_ms = busy_ms
        self._gen = 0
        self._conns: weakref.WeakSet[apsw.Connection] = weakref.WeakSet()

    def open(self) -> None:
        with self._lock:
            self._closed = False
            self._gen += 1

    def read(self, path: Path) -> apsw.Connection:
        """This thread's read-only connection. Another thread gets another."""
        st = self._state(path)
        if not st.snap:
            return st.conn
        loop = cast("AbstractEventLoop | None", asyncio._get_running_loop())
        if loop is None:
            return st.conn
        if st.snap_task is not None and (asyncio.current_task() is not st.snap_task):
            if not st.snap_broken:
                self._end_txn(st)
                st.snap_broken = True
            return st.conn
        if st.snap_broken:
            raise _snapshot_span()
        return st.conn

    def _state(self, path: Path) -> _ReadState:
        st = getattr(self._local, "st", None)
        if st is not None and st.gen == self._gen:
            return st
        if st is not None:
            self._local.st = None
            with suppress(apsw.Error):
                st.conn.close()
        with self._lock:
            if self._closed:
                raise StateError("the writer is closed")
            gen = self._gen
        if not path.exists():
            raise _missing_file()
        connection = connect(path, readonly=True, busy_ms=self._busy_ms)
        with self._lock:
            if self._closed:
                connection.close()
                raise StateError("the writer is closed")
            self._conns.add(connection)
        st = _ReadState(connection, gen)
        self._local.st = st
        return st

    def _end_txn(self, st: _ReadState) -> None:
        """COMMIT the read transaction this module opened."""
        with suppress(apsw.Error):
            st.conn.execute("COMMIT")
            st.txn = False

    def _snap_enter(self, path: Path) -> None:
        st = self._state(path)
        st.snap += 1
        if st.snap > 1:
            return
        if st.txn:
            self._end_txn(st)
        st.token += 1
        conn = st.conn
        conn.execute("BEGIN")
        conn.execute("PRAGMA schema_version").fetchall()
        st.txn = True
        loop = cast("AbstractEventLoop | None", asyncio._get_running_loop())
        st.snap_task = asyncio.current_task() if loop is not None else None
        st.snap_broken = False
        if loop is not None:
            loop.call_soon(self._snap_expire, st, st.token)

    def _snap_expire(self, st: _ReadState, token: int) -> None:
        """The snapshot block yielded to the loop: it spanned an await."""
        if st.token != token or not st.snap:
            return
        self._end_txn(st)
        st.snap_broken = True

    def _snap_exit(self, clean: bool) -> None:
        st = getattr(self._local, "st", None)
        if st is None:
            return
        st.snap -= 1
        if st.snap > 0:
            return
        if st.txn:
            self._end_txn(st)
        st.token += 1
        st.snap_task = None
        if clean and st.snap_broken:
            raise _snapshot_span()

    def drop_local(self) -> None:
        """Close this thread's connection. Other threads keep theirs."""
        st = getattr(self._local, "st", None)
        if st is not None:
            self._local.st = None
            with suppress(apsw.Error):
                st.conn.close()

    def seal(self) -> None:
        """Refuse new connections. Close every open one, on any thread."""
        with self._lock:
            self._closed = True
            self._gen += 1
            conns = list(self._conns)
            self._conns.clear()
        for conn in conns:
            with suppress(apsw.Error):
                conn.close(True)
        self.drop_local()


class Snapshot:
    """``with db.snapshot():`` — the block's reads share one fixed view."""

    __slots__ = ("_db",)

    def __init__(self, db: "LiteWriter") -> None:  # noqa: UP037
        self._db = db

    def __enter__(self) -> None:
        self._db._require_reader()
        self._db._reads._snap_enter(self._db.path)

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        self._db._reads._snap_exit(exc is None)
