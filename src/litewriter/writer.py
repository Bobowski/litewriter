"""One file, one writer thread, one read connection per calling thread.

Enqueue ``fn(tx, *args)`` from any thread or asyncio loop. The writer
gathers waiting jobs, runs each in its own savepoint, commits once, then
wakes the waiters. A job that raises is rolled back to its savepoint and
its caller gets the exception after the batch commits.

A read uses this thread's own read-only connection. The writer does not
need to be started. The file must exist.

Waiters on an asyncio loop are woken once per batch. A socketpair
mailbox does that when the loop supports ``add_reader``. Otherwise one
``call_soon_threadsafe`` carries the batch. Matching watches on that
loop are set in that same wake.

A connection stays on the thread that opened it. The inbox and the
watch tables sit under a lock. That stays true when the GIL is off.

``claim`` is an optional lock on a sibling file. It does not change
SQLite's locks. ``close`` drops it after the last commit.
"""

import asyncio
import inspect
import logging
import time
from asyncio import AbstractEventLoop
from asyncio import Future as AsyncFuture
from collections.abc import Callable
from concurrent.futures import Future
from contextlib import AbstractContextManager, suppress
from functools import partial
from pathlib import Path
from threading import Condition, Event, Thread, current_thread, local
from typing import Any, Concatenate, overload

import apsw

from litewriter.build import Statement
from litewriter.claim import Claim, claim_path, validate_timeout
from litewriter.connect import connect
from litewriter.errors import QueryError, StateError, check_seconds
from litewriter.inbox import Inbox, Job, async_write_error, settle
from litewriter.q import as_into, merge, one, rows, run, value
from litewriter.reads import Snapshot, ThreadReads
from litewriter.tx import Tx, ended_error
from litewriter.wake import LoopWakes, running_loop
from litewriter.watch import Tracker, Watch

type OnError = Callable[[BaseException], None]
type _Outcome = tuple[Future[Any] | AsyncFuture[Any], object, BaseException | None]

_local = local()


def _default_error(exc: BaseException) -> None:
    logging.getLogger("litewriter").error("push failed", exc_info=exc)


def _busy_ms(timeout: object) -> int:
    seconds = check_seconds(
        timeout,
        "busy_timeout",
        "LiteWriter(path, busy_timeout=5)",
        help_text="0 returns as soon as the file is busy.",
    )
    millis = round(seconds * 1000)
    if millis > 2_147_483_647:
        raise QueryError(
            "busy_timeout is too large",
            help_text="SQLite stores this timeout in milliseconds.",
            example="LiteWriter(path, busy_timeout=5)",
        )
    return millis


def _no_loop(what: str, example: str = "await db.write(fn, ...)") -> None:
    """Refuse to block a running event loop."""
    if running_loop() is None:
        return
    raise StateError(f"{what} would block the event loop", example=example)


def _finish(work: list[_Outcome]) -> None:
    for future, result, error in work:
        settle(future, result, error)


def _no_wait(what: str) -> None:
    """Refuse to block the writer thread or a running event loop."""
    if getattr(_local, "writer", False):
        raise StateError(
            f"{what} cannot block a writer thread",
            help_text="Inside a write, call the function directly with "
            "this tx. On a loop, await the write.",
            example="def outer(tx: Tx) -> int:\n    return inner(tx)",
        )
    _no_loop(what)


def _write_fn(fn: object) -> None:
    """Refuse a non-function or an async write before it reaches the inbox."""
    if not callable(fn):
        raise QueryError(
            "a write is a function",
            example="def add(tx: Tx) -> int:\n    ...",
        )
    if inspect.iscoroutinefunction(fn):
        raise async_write_error()


class _WriteFuture[R](Future[R]):
    """What ``submit`` returns. ``result()`` refuses to block a loop."""

    def result(self, timeout: float | None = None) -> R:
        if not self.done():
            _no_wait("submit().result()")
        return super().result(timeout)

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if not self.done():
            _no_wait("submit().exception()")
        return super().exception(timeout)


class LiteWriter:
    """Process facade: writer thread + one read connection per OS thread.

    ``schema`` runs once on the write connection inside one transaction
    before ``start`` returns: a string is multi-statement SQL, a callable
    is ``fn(tx)``. ``claim=30`` locks ``{path}-claim`` before starting.
    ``busy_timeout=5`` is the SQLite busy timeout in seconds.

    Bootstrap::

        with LiteWriter(path, schema=SCHEMA, hz=60) as db:
            yield
    """

    __slots__ = (
        "_boot_error",
        "_busy_ms",
        "_claim",
        "_claim_cancel",
        "_claim_timeout",
        "_closing",
        "_committing",
        "_hz",
        "_inbox",
        "_joining",
        "_life",
        "_on_error",
        "_pace",
        "_reads",
        "_ready",
        "_schema",
        "_schema_error",
        "_thread",
        "_track",
        "_wakes",
        "_write",
        "path",
    )

    def __init__(
        self,
        path: str | Path,
        *,
        schema: str | Callable[[Tx], object] | None = None,
        hz: float = 60.0,
        claim: float | None = None,
        busy_timeout: float = 5,
        on_error: OnError | None = None,
    ) -> None:
        if not str(path) or str(path) == ":memory:":
            raise QueryError(
                "litewriter needs a file",
                help_text="Every thread opens its own read connection, "
                "so :memory: cannot work.",
                example='LiteWriter("app.sqlite3")',
            )
        self.path = Path(path)
        self._schema = schema
        self._busy_ms = _busy_ms(busy_timeout)
        self._claim_timeout = None if claim is None else validate_timeout(claim)
        self._claim = Claim(claim_path(self.path))
        self._claim_cancel = Event()
        self._hz = 60.0
        self.hz = hz
        self._on_error = _default_error if on_error is None else on_error
        self._inbox: Inbox[Job | None] = Inbox()
        self._wakes = LoopWakes()
        self._track = Tracker(self._wakes)
        self._committing = False
        self._thread: Thread | None = None
        self._write: apsw.Connection | None = None
        self._reads = ThreadReads(self._busy_ms)
        self._life = Condition()
        self._closing = False
        self._joining = False
        self._pace = Event()
        self._ready = Event()
        self._boot_error: BaseException | None = None
        self._schema_error: BaseException | None = None

    @property
    def hz(self) -> float:
        """LiteWriter ticks per second. ``0`` drains as fast as jobs arrive.

        Safe to change while the writer is running; the next tick uses it.
        """
        return self._hz

    @hz.setter
    def hz(self, value: float) -> None:
        self._hz = check_seconds(
            value, "hz", "LiteWriter(path, hz=60)\nLiteWriter(path, hz=0)  # no floor"
        )

    def start(self) -> None:
        """Take the claim (if ``claim=`` was given) and start the thread.

        On an event loop use ``async with`` instead — a claim wait blocks.
        """
        if self._claim_timeout is not None and not self._claim.held():
            if self._claim_timeout > 0:
                _no_loop(
                    "start() with a claim wait",
                    example="async with LiteWriter(path, claim=30) as db:\n    ...",
                )
            self._claim_cancel.clear()
            self._claim.acquire(self._claim_timeout, self._claim_cancel)
        try:
            self._boot()
        except BaseException:
            self._claim.release()
            raise

    def _boot(self) -> None:
        with self._life:
            while self._joining:
                self._life.wait()
            if self._thread is not None:
                raise StateError("writer is already started")
            self._closing = False
            self._reads.open()
            self._track.open()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._pace.clear()
            self._ready.clear()
            self._boot_error = None
            self._schema_error = None
            self._inbox.reopen()
            self._thread = Thread(target=self._run, name="litewriter", daemon=True)
            self._thread.start()
            while not self._ready.wait(0.1):
                if not self._thread.is_alive():
                    break
            if not self._ready.is_set():
                self._thread.join()
                self._thread = None
                raise StateError("the writer thread exited before it was ready")
            if self._schema_error is not None:
                self._thread.join()
                self._thread = None
                raise StateError("the schema failed") from self._schema_error
            if self._boot_error is not None:
                self._thread.join()
                self._thread = None
                raise StateError("writer failed to open the file") from self._boot_error

    def close(self) -> None:
        """Drain the inbox, COMMIT the last batch, then stop the writer.

        The WAL is folded after that commit. The claim drops after the fold.
        On an event loop close would block it — use ``aclose`` or
        ``async with`` instead.
        """
        thread = self._thread
        if thread is not None and current_thread() is thread:
            raise StateError(
                "close() cannot run inside a write function",
                help_text="The write joins the writer it runs on.",
            )
        _no_loop("close()", example="await db.aclose()")
        self._claim_cancel.set()
        try:
            self._stop()
            self._checkpoint()
        finally:
            self._claim.release()

    def _checkpoint(self) -> None:
        """Fold the WAL once. This thread's reads are already closed."""
        if not self.path.is_file():
            return
        connection = connect(self.path, busy_ms=self._busy_ms)
        try:
            try:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except apsw.BusyError:
                return
        finally:
            connection.close()

    def _stop(self) -> None:
        with self._life:
            while self._joining:
                self._life.wait()
            thread = self._thread
            self._closing = True
            if thread is None:
                self._reads.seal()
                return
            self._joining = True
        try:
            self._track.stop_all()
            self._pace.set()
            self._inbox.put(None)
            thread.join()
            with self._life:
                self._thread = None
            late = [job for job in self._inbox.close() if job is not None]
            if late:
                closed = StateError("the writer closed before this write ran")
                self._wake(late, None, closed)
            for loop, wake in self._wakes.close():
                if running_loop() is loop:
                    wake()
                else:
                    with suppress(RuntimeError):
                        loop.call_soon_threadsafe(wake)
            self._reads.seal()
        finally:
            with self._life:
                self._joining = False
                self._life.notify_all()

    def __enter__(self) -> LiteWriter:
        self.start()
        return self

    async def aclose(self) -> None:
        """``close`` on a worker thread, for a running event loop.

        A cancelled close still finishes: the join moves to a daemon
        thread so the writer drains instead of dying mid-commit.
        """
        try:
            await asyncio.to_thread(self.close)
        except asyncio.CancelledError:
            Thread(target=self.close, daemon=True).start()
            raise

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    async def __aenter__(self) -> LiteWriter:
        """Claim on the loop, then ``start`` off it: starting blocks."""
        if self._claim_timeout is not None and not self._claim.held():
            self._claim_cancel.clear()
            await self._claim.aacquire(self._claim_timeout, self._claim_cancel)
        try:
            await asyncio.to_thread(self.start)
        except BaseException:
            self._claim.release()
            raise
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    def _conn(self) -> apsw.Connection:
        """This thread's read-only connection. Another thread gets another."""
        self._require_reader()
        return self._reads.read(self.path)

    def _cursor(self, q: str | Statement, params: dict[str, object]) -> apsw.Cursor:
        conn = self._conn()
        text, bound = merge(q, params)
        return run(conn, text, bound)

    def _require_reader(self) -> None:
        """Reads run off the writer thread. Inside a write, use ``tx``."""
        if self._thread is current_thread():
            raise StateError(
                "inside a write, read with tx",
                help_text="db.query sees committed data from before this "
                "batch. tx.query sees the batch.",
                example="def add(tx: Tx):\n    return tx.query(...)",
            )

    @overload
    def query(
        self, q: str | Statement, /, **params: object
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def query[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> list[T]: ...
    def query(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> list[Any]:
        """All rows, each built as ``into(*row)``. ``params`` fill ``:name``."""
        into = as_into(into)
        return rows(self._cursor(q, params), into)

    @overload
    def one(
        self, q: str | Statement, /, **params: object
    ) -> tuple[Any, ...] | None: ...
    @overload
    def one[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> T | None: ...
    def one(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> Any:
        """The first row as ``into(*row)``, or None when there is none."""
        into = as_into(into)
        return one(self._cursor(q, params), into)

    def value(self, q: str | Statement, /, **params: object) -> Any:
        """The first column of the first row. No row raises QueryError."""
        return value(self._cursor(q, params))

    @overload
    async def aquery(
        self, q: str | Statement, /, **params: object
    ) -> list[tuple[Any, ...]]: ...
    @overload
    async def aquery[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> list[T]: ...
    async def aquery(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> list[Any]:
        """``query`` on a worker thread — for a read too heavy for the loop."""
        return await asyncio.to_thread(self.query, q, into, **params)

    @overload
    async def aone(
        self, q: str | Statement, /, **params: object
    ) -> tuple[Any, ...] | None: ...
    @overload
    async def aone[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> T | None: ...
    async def aone(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> Any:
        """``one`` on a worker thread — for a read too heavy for the loop."""
        return await asyncio.to_thread(self.one, q, into, **params)

    async def avalue(self, q: str | Statement, /, **params: object) -> Any:
        """``value`` on a worker thread — for a read too heavy for the loop."""
        return await asyncio.to_thread(self.value, q, **params)

    def snapshot(self) -> AbstractContextManager[None]:
        """One fixed read view for the block's reads on this thread.

        The block opens a read transaction, so its reads see the data as
        of entry even when the writer commits in between. On an event
        loop the block cannot contain an ``await``: the view ends and
        the block raises ``StateError``. On a plain thread keep the
        block short — an open snapshot holds back the WAL checkpoint.
        """
        self._require_reader()
        return Snapshot(self._reads, self.path, self._require_reader)

    @overload
    def watch(
        self, q: str | Statement, /, **params: object
    ) -> Watch[tuple[Any, ...]]: ...
    @overload
    def watch[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> Watch[T]: ...
    def watch(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> Watch[Any]:
        """The rows now, then again after each commit that changes them."""
        into = as_into(into)
        text, bound = merge(q, params)
        return Watch(
            self._track,
            text,
            bound,
            into,
            prepare=self._watch_deps,
            conn=self._conn,
        )

    def _watch_deps(
        self, text: str
    ) -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
        self._require_started()
        return self._dependencies(text)

    def _dependencies(
        self, q: str | Statement
    ) -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
        """Tables and columns ``q`` reads.

        An empty column name means the query cares that rows appear or
        disappear, not which column changed. ``count(*)`` is that shape.
        """
        text = q if isinstance(q, str) else q.sql
        tables: set[str] = set()
        columns: set[tuple[str, str]] = set()

        def note(
            op: int, a: str | None, b: str | None, db: str | None, trigger: str | None
        ) -> int:
            if op != apsw.SQLITE_READ or not a or a.startswith("sqlite_"):
                return apsw.SQLITE_OK
            tables.add(a)
            if b:
                columns.add((a, b))
            return apsw.SQLITE_OK

        conn = self._conn()
        conn.authorizer = note
        try:
            conn.execute(text, can_cache=False, explain=2).fetchall()
        except apsw.BindingsError, KeyError:
            pass
        except apsw.Error as exc:
            raise QueryError(
                "the watch query failed to prepare",
                context={"detail": str(exc)},
                example='db.watch(Select("id", from_="t"))',
            ) from exc
        finally:
            conn.authorizer = None
        return frozenset(tables), frozenset(columns)

    def submit[**P, R](
        self,
        fn: Callable[Concatenate[Tx, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        """Enqueue from any thread. Completes after the batch COMMIT.

        ``.result()`` waits on this thread — it refuses to block an
        event loop, so on a loop ``await db.write(fn, ...)`` instead.
        """
        self._require_started()
        _write_fn(fn)
        future: _WriteFuture[R] = _WriteFuture()
        self._inbox.put(Job(fn, args, kwargs, future, None))
        return future

    async def write[**P, R](
        self,
        fn: Callable[Concatenate[Tx, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Await a write on this loop. Woken with the rest of the batch.

        A job that raises is rolled back to its savepoint. The caller
        gets the exception after the batch commits.
        """
        self._require_started()
        if self._thread is current_thread():
            raise StateError(
                "db.write inside a write function",
                help_text="Call the other write function directly with this tx.",
                example="def outer(tx: Tx) -> int:\n    return inner(tx)",
            )
        _write_fn(fn)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise StateError(
                "db.write needs a running event loop",
                help_text="Inside a write, call the other function "
                "directly with this tx. Elsewhere, run a loop.",
            ) from None
        self._wakes.ensure(loop)
        future: AsyncFuture[R] = loop.create_future()
        self._inbox.put(Job(fn, args, kwargs, future, loop))
        return await future

    def push[**P](
        self,
        fn: Callable[Concatenate[Tx, P], object],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        """Fire-and-forget write. Failures go to ``on_error``."""
        self._require_started()
        _write_fn(fn)
        self._inbox.put(Job(fn, args, kwargs, None, None))

    def _require_started(self) -> None:
        with self._life:
            if self._closing:
                raise StateError("the writer is closed")
            if self._thread is None:
                raise StateError(
                    "call start() before writes",
                    example="db = LiteWriter(path)\ndb.start()\nawait db.write(fn, ...)\ndb.close()",
                )

    def _may_commit(self) -> bool:
        """Commit hook: True vetoes. Only the writer may commit a batch."""
        return not self._committing

    def _run(self) -> None:
        _local.writer = True
        try:
            write = connect(self.path, busy_ms=self._busy_ms)
        except BaseException as exc:
            self._boot_error = exc
            self._ready.set()
            return
        write.setcommithook(self._may_commit)
        self._write = write
        try:
            self._run_schema(write)
        except BaseException as exc:
            self._schema_error = exc
            self._ready.set()
            write.close()
            self._write = None
            return
        self._ready.set()
        try:
            while True:
                raw = self._inbox.take_batch()
                stop = False
                batch: list[Job] = []
                for item in raw:
                    if item is None:
                        stop = True
                        continue
                    batch.append(item)
                if not batch:
                    return
                batch = [job for job in batch if job.claim()]
                if not batch:
                    if stop:
                        return
                    continue
                started = time.monotonic()
                try:
                    self._commit(write, batch)
                except BaseException as exc:
                    self._wake(batch, None, exc)
                period = 0.0 if self._hz == 0 else 1.0 / self._hz
                deadline = started + period
                if period > 0 and deadline > time.monotonic():
                    # Await the pace, not the clock: ``_stop`` sets the
                    # event so ``close`` never waits out the floor.
                    self._pace.wait(max(0.0, deadline - time.monotonic()))
                if stop:
                    return
        finally:
            write.close()
            self._write = None

    def _run_schema(self, write: apsw.Connection) -> None:
        """Apply ``schema`` in one transaction before the thread is ready."""
        schema = self._schema
        if schema is None:
            return
        write.execute("BEGIN IMMEDIATE")
        try:
            if isinstance(schema, str):
                for _row in write.execute(schema):
                    pass
            else:
                schema(Tx(write))
            self._committing = True
            try:
                write.execute("COMMIT")
            finally:
                self._committing = False
        except BaseException:
            with suppress(apsw.Error):
                write.execute("ROLLBACK")
            raise

    def _commit(self, write: apsw.Connection, batch: list[Job]) -> None:
        """One transaction. Every job gets a savepoint and an outcome."""
        results: list[tuple[object, BaseException | None]] = []
        self._track.begin()
        write.execute("BEGIN IMMEDIATE")
        try:
            traced = self._track.live()
            before: tuple[int, int] | None = None
            stale = False
            if traced:
                before = self._schema_version(write)
                stale = self._track.check_schema(before)
            tx = Tx(write, self._track if traced else None)
            for index, job in enumerate(batch):
                save = f"j{index}"
                write.execute(f"SAVEPOINT {save}")
                mark = self._track.mark() if traced else None
                try:
                    result = job.run(tx)
                except BaseException as exc:
                    if not write.in_transaction:
                        raise ended_error() from exc
                    if mark is not None:
                        self._track.restore(mark)
                    write.execute(f"ROLLBACK TO {save}")
                    write.execute(f"RELEASE {save}")
                    results.append((None, exc))
                    continue
                if not write.in_transaction:
                    raise ended_error()
                write.execute(f"RELEASE {save}")
                results.append((result, None))
            self._committing = True
            try:
                write.execute("COMMIT")
            finally:
                self._committing = False
        except BaseException as exc:
            with suppress(apsw.Error):
                write.execute("ROLLBACK")
            self._wake(batch, None, exc)
            return
        try:
            changed = False
            if traced and before is not None:
                after = self._schema_version(write)
                self._track.seen(after)
                changed = stale or after != before
            self._track.notify(changed, traced)
        except Exception:
            logging.getLogger("litewriter").exception("watch notify failed")
        self._wake(batch, results, None)

    def _wake(
        self,
        batch: list[Job],
        results: list[tuple[object, BaseException | None]] | None,
        shared_error: BaseException | None,
    ) -> None:
        grouped: dict[AbstractEventLoop, list[_Outcome]] = {}
        for index, job in enumerate(batch):
            if results is not None:
                result, error = results[index]
            else:
                result, error = None, shared_error
            future = job.future
            if future is None:
                if error is not None:
                    with suppress(BaseException):
                        self._on_error(error)
                continue
            loop = job.loop
            if loop is None or loop.is_closed():
                settle(future, result, error)
                continue
            grouped.setdefault(loop, []).append((future, result, error))
        for loop, items in grouped.items():
            self._wakes.post(loop, partial(_finish, items))

    def _schema_version(self, write: apsw.Connection) -> tuple[int, int]:
        """The main and temp schema versions. DDL bumps one of them."""
        main = write.execute("PRAGMA schema_version").fetchone()
        temp = write.execute("PRAGMA temp.schema_version").fetchone()
        return (
            0 if main is None else int(main[0]),
            0 if temp is None else int(temp[0]),
        )
