"""One file, one writer thread, one read connection per calling thread.

Enqueue ``fn(tx, *args)`` from any thread or asyncio loop. The writer
gathers waiting jobs, commits once, then wakes waiters. ``isolated=True``
is the default. A failure undoes that write after the batch commits.
``isolated=False`` rolls the batch back.

A read uses this thread's own read-only connection. The writer does not
need to be started. The file must exist.

Waiters on an asyncio loop are woken once per batch. A socketpair
mailbox does that when the loop supports ``add_reader``. Otherwise one
``call_soon_threadsafe`` carries the batch. Matching watches on that
loop are set in that same wake.

A connection stays on the thread that opened it. The inbox, each
result slot, the reader set, and the counters sit under a lock.
That stays true when the GIL is off.

``claim`` is an optional lock on a sibling file. It does not change
SQLite's locks. ``close`` drops it after the last commit.
"""

import asyncio
import math
import time
from asyncio import AbstractEventLoop
from asyncio import Future as AsyncFuture
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from threading import Condition, Event, Lock, Thread, get_ident, local
from typing import Any, Concatenate

import apsw

from litewriter.claim import Claim, validate_timeout
from litewriter.connect import Bindings, connect
from litewriter.errors import WriterError, WriterRolledBack, WriterRuntime
from litewriter.fn import Isolated, Tx, WriteFn, unwrap
from litewriter.inbox import (
    Inbox,
    Job,
    Slot,
    enqueue_call,
    enqueue_push,
    enqueue_submit,
    settle,
)
from litewriter.q import Clauses, execute, one, rows, sql, value
from litewriter.wake import LoopWakes, deliver
from litewriter.watch import Watch


def _split[**P, R](
    fn: Callable[P, R] | Isolated[P, R], isolated: bool
) -> tuple[Callable[P, R], bool]:
    plain, alone = unwrap(fn)
    return plain, alone or isolated


type AfterCommit = Callable[[tuple[Outcome, ...]], None]
type OnAbort = Callable[[], None]
type OnError = Callable[[BaseException], None]


@dataclass(frozen=True, slots=True)
class Outcome:
    """One job after a successful batch COMMIT (or a recorded isolated error)."""

    ok: bool
    result: object
    error: BaseException | None
    isolated: bool


@dataclass(slots=True)
class Stats:
    """Cheap counters. Filled only when ``profile=True``."""

    jobs: int = 0
    batches: int = 0
    enqueue_ns: int = 0
    fn_ns: int = 0
    commit_ns: int = 0
    wake_ns: int = 0
    sleep_ns: int = 0
    _lock: Lock = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._lock = Lock()

    def add(self, **counts: int) -> None:
        """Add to counters. Safe from the writer and from producer threads."""
        with self._lock:
            for name, count in counts.items():
                setattr(self, name, getattr(self, name) + count)

    def snapshot(self) -> Stats:
        with self._lock:
            return Stats(
                jobs=self.jobs,
                batches=self.batches,
                enqueue_ns=self.enqueue_ns,
                fn_ns=self.fn_ns,
                commit_ns=self.commit_ns,
                wake_ns=self.wake_ns,
                sleep_ns=self.sleep_ns,
            )


def _tick_rate(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise WriterError(
            "hz must be 0 or a positive tick rate",
            example="LiteWriter(path, hz=60)\nLiteWriter(path, hz=0)  # no floor",
        )
    try:
        rate = float(value)
    except OverflowError:
        rate = math.inf
    if not math.isfinite(rate) or rate < 0:
        raise WriterError(
            "hz must be 0 or a positive tick rate",
            example="LiteWriter(path, hz=60)\nLiteWriter(path, hz=0)  # no floor",
        )


def _busy_ms(timeout: object) -> int:
    if isinstance(timeout, bool) or not isinstance(timeout, int | float):
        raise WriterError(
            "busy_timeout is a number of seconds",
            example="LiteWriter(path, busy_timeout=5)",
        )
    try:
        seconds = float(timeout)
    except OverflowError:
        seconds = math.inf
    if not math.isfinite(seconds) or seconds < 0:
        raise WriterError(
            "busy_timeout must be 0 or greater",
            help_text="0 returns as soon as the file is busy.",
            example="LiteWriter(path, busy_timeout=5)",
        )
    millis = round(seconds * 1000)
    if millis > 2_147_483_647:
        raise WriterError(
            "busy_timeout is too large",
            help_text="SQLite stores this timeout in milliseconds.",
            example="LiteWriter(path, busy_timeout=5)",
        )
    return millis


def _missing_file() -> WriterRuntime:
    return WriterRuntime(
        "the database file does not exist",
        example="db.execute_script(SCHEMA)\ndb.query(q)",
    )


class _ThreadReads:
    """One read-only connection per OS thread.

    SQLite connections are not shared. A read does not need the writer
    thread. ``seal`` marks the writer closed. The thread that owns a
    connection is the thread that closes it.
    """

    __slots__ = ("_busy_ms", "_closed", "_local", "_lock")

    def __init__(self, busy_ms: int) -> None:
        self._local = local()
        self._lock = Lock()
        self._closed = False
        self._busy_ms = busy_ms

    def open(self) -> None:
        with self._lock:
            self._closed = False

    def get(self, path: Path) -> apsw.Connection:
        current = getattr(self._local, "conn", None)
        if current is not None and not self._is_closed():
            return current
        if current is not None:
            self._drop(current)
            raise WriterRuntime("the writer is closed")
        if self._is_closed():
            raise WriterRuntime("the writer is closed")
        if not path.exists():
            raise _missing_file()
        connection = connect(path, readonly=True, busy_ms=self._busy_ms)
        with self._lock:
            if self._closed:
                connection.close()
                raise WriterRuntime("the writer is closed")
        self._local.conn = connection
        if self._is_closed():
            self._drop(connection)
            raise WriterRuntime("the writer is closed")
        return connection

    def drop_local(self) -> None:
        """Close this thread's connection. Other threads keep theirs."""
        current = getattr(self._local, "conn", None)
        if current is not None:
            self._drop(current)

    def seal(self) -> None:
        """Refuse new connections. Close the connection on this thread."""
        with self._lock:
            self._closed = True
        self.drop_local()

    def _is_closed(self) -> bool:
        with self._lock:
            return self._closed

    def _drop(self, current: apsw.Connection) -> None:
        self._local.conn = None
        current.close()


def _sleep_until(deadline: float) -> None:
    """Sleep until ``deadline``. One ``sleep`` can return early."""
    for _ in range(8):
        leftover = deadline - time.monotonic()
        if leftover <= 0:
            return
        time.sleep(leftover)


def _bookkeeping(sql: str) -> bool:
    """True for SQL the writer runs around the user's statement."""
    if sql in {"BEGIN IMMEDIATE", "COMMIT", "ROLLBACK", "PRAGMA schema_version"}:
        return True
    if sql.startswith(("SAVEPOINT j", "RELEASE j", "ROLLBACK TO j")):
        return sql.rpartition("j")[2].isdigit()
    return False


class LiteWriter:
    """Process facade: writer thread + one read connection per OS thread.

    A read does not start the writer. The file must already exist.
    ``claim=30`` claims the file before ``start``. The default is no claim.
    ``busy_timeout=5`` is the SQLite busy timeout in seconds.

    Bootstrap::

        with LiteWriter(path, hz=60) as db:
            db.execute_script(SCHEMA)
            yield
    """

    __slots__ = (
        "_after_commit",
        "_armed",
        "_boot_error",
        "_busy_ms",
        "_by_column",
        "_by_table",
        "_changed_columns",
        "_changed_tables",
        "_claim",
        "_claim_timeout",
        "_closing",
        "_effects",
        "_hz",
        "_inbox",
        "_joining",
        "_life",
        "_on_abort",
        "_on_error",
        "_pending_columns",
        "_pending_tables",
        "_profile",
        "_readers",
        "_readers_lock",
        "_reads",
        "_ready",
        "_thread",
        "_wakes",
        "_watch_ids",
        "_watch_lock",
        "_watches",
        "_write",
        "name",
        "path",
        "stats",
    )

    def __init__(
        self,
        path: str | Path,
        *,
        hz: float = 60.0,
        after_commit: AfterCommit | None = None,
        on_abort: OnAbort | None = None,
        on_error: OnError | None = None,
        profile: bool = False,
        name: str = "writer",
        claim: float | None = None,
        busy_timeout: float = 5,
    ) -> None:
        self.path = Path(path)
        self.name = name
        self._busy_ms = _busy_ms(busy_timeout)
        self._claim_timeout = None if claim is None else validate_timeout(claim)
        self._claim = Claim(self.path.with_name(self.path.name + "-claim"))
        self._hz = 60.0
        self.hz = hz
        self._after_commit = after_commit
        self._on_abort = on_abort
        self._on_error = on_error
        self._profile = profile
        self.stats = Stats()
        self._inbox = Inbox()
        self._wakes = LoopWakes()
        self._armed = False
        self._by_table: dict[str, set[int]] = {}
        self._by_column: dict[tuple[str, str], set[int]] = {}
        self._effects: dict[str, tuple[frozenset[str], frozenset[tuple[str, str]]]] = {}
        self._pending_tables: set[str] = set()
        self._pending_columns: set[tuple[str, str]] = set()
        self._changed_tables: set[str] = set()
        self._changed_columns: set[tuple[str, str]] = set()
        self._watch_lock = Lock()
        self._watches: dict[int, Watch] = {}
        self._watch_ids = 0
        self._thread: Thread | None = None
        self._write: apsw.Connection | None = None
        self._reads = _ThreadReads(self._busy_ms)
        self._readers: set[Reader] = set()
        self._readers_lock = Lock()
        self._life = Condition()
        self._closing = False
        self._joining = False
        self._ready = Event()
        self._boot_error: BaseException | None = None

    @property
    def hz(self) -> float:
        """LiteWriter ticks per second. ``0`` drains as fast as jobs arrive.

        Safe to change while the writer is running; the next tick uses it.
        """
        return self._hz

    @hz.setter
    def hz(self, value: float) -> None:
        _tick_rate(value)
        self._hz = value

    def claim(self, timeout: float | None = None) -> None:
        """Block until this process holds the claim.

        The lock file is ``{path}-claim``. It is not a SQLite lock.
        Readers keep working. A process that never claims can still write.

        ``timeout`` is seconds. ``0`` tries once. When the constructor
        ``claim`` is set, that value is the default. The call raises
        ``WriterBusy`` when the wait ends. A second call does nothing
        while this writer holds the lock. ``close`` releases it after
        the last commit. The kernel also releases it when the process dies.
        """
        self._claim.acquire(self._resolve_claim_timeout(timeout))

    async def aclaim(self, timeout: float | None = None) -> None:
        """Claim the file. The wait runs off this loop. See ``claim``."""
        timeout = self._resolve_claim_timeout(timeout)
        taken: list[bool] = []
        loop = asyncio.get_running_loop()
        finished = asyncio.Event()
        box: list[BaseException] = []

        def run() -> None:
            try:
                self._claim.acquire(timeout, taken)
            except BaseException as exc:
                box.append(exc)
            loop.call_soon_threadsafe(finished.set)

        Thread(target=run, name="litewriter-aclaim", daemon=True).start()
        try:
            await finished.wait()
        except asyncio.CancelledError:
            self._claim.abort()
            if taken:
                self._claim.release()
            raise
        if box:
            raise box[0]

    def _resolve_claim_timeout(self, timeout: float | None) -> float:
        if timeout is None:
            timeout = self._claim_timeout
        if timeout is None:
            raise WriterError(
                "claim needs a timeout",
                help_text="Pass timeout in seconds, or set claim= on the writer.",
                example="db.claim(timeout=30)",
            )
        return validate_timeout(timeout)

    def start(self) -> None:
        took = False
        if self._claim_timeout is not None:
            took = self._claim.acquire(self._claim_timeout)
        try:
            self._boot()
        except BaseException:
            if took:
                self._claim.release()
            raise

    def _boot(self) -> None:
        with self._life:
            while self._joining:
                self._life.wait()
            if self._claim_timeout is not None and not self._claim.held():
                raise WriterRuntime(
                    "the writer closed before the claim was kept",
                    help_text="close() releases the claim.",
                )
            if self._thread is not None:
                raise WriterRuntime("writer is already started")
            self._closing = False
            self._reads.open()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._ready.clear()
            self._boot_error = None
            self._inbox.reopen()
            self._thread = Thread(
                target=self._run, name=f"litewriter-{self.name}", daemon=True
            )
            self._thread.start()
            if not self._ready.wait(timeout=5.0):
                self._thread.join(timeout=1.0)
                self._thread = None
                raise WriterRuntime("writer thread did not become ready")
            if self._boot_error is not None:
                self._thread.join(timeout=1.0)
                self._thread = None
                raise WriterRuntime(
                    "writer failed to open the file"
                ) from self._boot_error

    def close(self) -> None:
        """Drain the inbox, COMMIT the last batch, then stop the writer.

        The WAL is folded after that commit. The claim drops after the fold.
        """
        self._claim.abort()
        try:
            self._stop()
            self._checkpoint()
        finally:
            self._claim.release()

    def _checkpoint(self) -> None:
        """Fold the WAL once. Readers on this writer are already closed."""
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
                self._retire_readers()
                return
            self._joining = True
        try:
            self._stop_watches()
            self._inbox.put(None)
            thread.join()
            with self._life:
                self._thread = None
            late = [job for job in self._inbox.close() if job is not None]
            if late:
                closed = WriterRuntime("the writer closed before this write ran")
                self._wake(late, None, closed)
            self._wakes.close()
            self._reads.seal()
            self._retire_readers()
        finally:
            with self._life:
                self._joining = False
                self._life.notify_all()

    def __enter__(self) -> LiteWriter:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    async def __aenter__(self) -> LiteWriter:
        took = False
        if self._claim_timeout is not None and not self._claim.held():
            await self.aclaim(self._claim_timeout)
            took = self._claim.held()
        try:
            self.start()
        except BaseException:
            if took:
                self._claim.release()
            raise
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def reader(self) -> Reader:
        """A new read-only connection for this thread.

        The writer does not need to be started. The file must exist.
        Close the reader on this thread.
        """
        with self._life:
            if self._closing:
                raise WriterRuntime("the writer is closed")
            if not self.path.exists():
                raise _missing_file()
            item = Reader(self, self._busy_ms)
            with self._readers_lock:
                self._readers.add(item)
        return item

    def conn(self) -> apsw.Connection:
        """This thread's read-only connection. Another thread gets another.

        A second call on this thread returns the same connection.
        The writer does not need to be started. The file must exist.
        """
        return self._reads.get(self.path)

    def _drop_reader(self, reader: Reader) -> None:
        with self._readers_lock:
            self._readers.discard(reader)

    def _retire_readers(self) -> None:
        """Close readers opened on this thread. Other threads close their own."""
        with self._readers_lock:
            found = list(self._readers)
        for reader in found:
            if reader.owned_here():
                reader.close()
            else:
                reader._retire()  # pyright: ignore[reportPrivateUsage]

    def execute(self, query: str | Clauses, params: Bindings = ()) -> apsw.Cursor:
        """Run SQL text or a query on this thread's read-only connection."""
        return execute(self.conn(), query, params)

    def query(self, query: str | Clauses, /, **params: object) -> list[tuple[Any, ...]]:
        """All rows. ``params`` fill each ``:name``."""
        return rows(self.conn(), query, params)

    def one(self, query: str | Clauses, /, **params: object) -> tuple[Any, ...] | None:
        """The first row, or None when the query returns nothing."""
        return one(self.conn(), query, params)

    def value(self, query: str | Clauses, /, **params: object) -> Any:
        """The first column of the first row."""
        return value(self.conn(), query, params)

    def watch(self, query: str | Clauses, /, **params: object) -> Watch:
        """The rows now, then again after each commit that changes them."""
        text = query if isinstance(query, str) else sql(query)
        return Watch(self, text, params)

    def tables(self, query: str | Clauses) -> frozenset[str]:
        """The tables ``query`` reads, as SQLite sees them (views resolved)."""
        tables, _columns = self._dependencies(query)
        return tables

    def _dependencies(
        self, query: str | Clauses
    ) -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
        """Tables and columns ``query`` reads.

        An empty column name means the query cares that rows appear or
        disappear, not which column changed. ``count(*)`` is that shape.
        """
        text = query if isinstance(query, str) else sql(query)
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

        conn = self.conn()
        conn.authorizer = note
        try:
            conn.execute(text, can_cache=False, explain=2).fetchall()
        except apsw.BindingsError:
            pass
        finally:
            conn.authorizer = None
        return frozenset(tables), frozenset(columns)

    def execute_script(self, sql: str) -> None:
        """DDL / multi-statement SQL. After start, runs on the writer."""
        with self._life:
            running = self._thread is not None and not self._closing
            busy = self._joining
        if running:
            self.submit(lambda tx: tx.execute(sql)).result()
            return
        if busy:
            raise WriterRuntime("the writer is closed")
        connection = connect(self.path, busy_ms=self._busy_ms)
        try:
            connection.execute(sql)
        finally:
            connection.close()

    def submit[**P, R](
        self,
        fn: WriteFn[P, R],
        *args: P.args,
        isolated: bool = True,  # pyright: ignore[reportGeneralTypeIssues]
        **kwargs: P.kwargs,
    ) -> Slot[R]:
        """Enqueue from any thread. Completes after the batch COMMIT.

        ``isolated`` defaults to True and sets a savepoint. A failure
        undoes that write. ``isolated=False`` skips the savepoint.
        A failure undoes the batch. ``.result()`` waits on this thread.
        On an asyncio loop, use ``call``.
        """
        self._require_started()
        plain, alone = _split(fn, isolated)
        if self._profile:
            t0 = time.perf_counter_ns()
            slot = enqueue_submit(self._inbox, plain, args, kwargs, alone)
            self.stats.add(enqueue_ns=time.perf_counter_ns() - t0)
            return slot
        return enqueue_submit(self._inbox, plain, args, kwargs, alone)

    async def call[**P, R](
        self,
        fn: WriteFn[P, R],
        *args: P.args,
        isolated: bool = True,  # pyright: ignore[reportGeneralTypeIssues]
        **kwargs: P.kwargs,
    ) -> R:
        """Await a write on this loop. Woken with the rest of the batch.

        ``isolated`` defaults to True and sets a savepoint. A failure
        undoes that write. ``isolated=False`` skips the savepoint.
        A failure undoes the batch.
        """
        self._require_started()
        plain, alone = _split(fn, isolated)
        loop = asyncio.get_running_loop()
        self._wakes.ensure(loop)
        future: AsyncFuture[R] = loop.create_future()
        if self._profile:
            t0 = time.perf_counter_ns()
            enqueue_call(self._inbox, plain, args, kwargs, alone, future, loop)
            self.stats.add(enqueue_ns=time.perf_counter_ns() - t0)
        else:
            enqueue_call(self._inbox, plain, args, kwargs, alone, future, loop)
        return await future

    def push[**P](
        self,
        fn: WriteFn[P, object],
        *args: P.args,
        isolated: bool = True,  # pyright: ignore[reportGeneralTypeIssues]
        **kwargs: P.kwargs,
    ) -> None:
        """Fire-and-forget write. Failures go to ``on_error`` after COMMIT.

        ``isolated`` defaults to True and sets a savepoint. A failure
        undoes that write. ``isolated=False`` skips the savepoint.
        A failure undoes the batch.
        """
        self._require_started()
        plain, alone = _split(fn, isolated)
        if self._profile:
            t0 = time.perf_counter_ns()
            enqueue_push(self._inbox, plain, args, kwargs, alone)
            self.stats.add(enqueue_ns=time.perf_counter_ns() - t0)
            return
        enqueue_push(self._inbox, plain, args, kwargs, alone)

    async def offload[**P, R](
        self,
        fn: Callable[Concatenate[apsw.Connection, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Rare fat read. A new read-only connection on a worker thread.

        The writer does not need to be started. The file must exist.
        The worker closes that connection before it returns.
        """
        with self._life:
            if self._closing:
                raise WriterRuntime("the writer is closed")
            if not self.path.exists():
                raise _missing_file()

        def run() -> R:
            connection = connect(self.path, readonly=True, busy_ms=self._busy_ms)
            try:
                return fn(connection, *args, **kwargs)
            finally:
                connection.close()

        return await asyncio.to_thread(run)

    def _require_started(self) -> None:
        with self._life:
            if self._closing or self._thread is None:
                raise WriterRuntime(
                    "call start() before writes",
                    example="db = LiteWriter(path)\ndb.start()\nawait db.call(fn, ...)\ndb.close()",
                )

    def _run(self) -> None:
        try:
            write = connect(self.path, busy_ms=self._busy_ms)
        except BaseException as exc:
            self._boot_error = exc
            self._ready.set()
            return
        self._write = write
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
                started = time.monotonic()
                self._commit(write, batch)
                if self._profile:
                    self.stats.add(batches=1, jobs=len(batch))
                period = 0.0 if self._hz == 0 else 1.0 / self._hz
                deadline = started + period
                if period > 0 and deadline > time.monotonic():
                    if self._profile:
                        s0 = time.perf_counter_ns()
                        _sleep_until(deadline)
                        self.stats.add(sleep_ns=time.perf_counter_ns() - s0)
                    else:
                        _sleep_until(deadline)
                if stop:
                    return
        finally:
            write.close()
            self._write = None

    def _commit(self, write: apsw.Connection, batch: list[Job]) -> None:
        tx = Tx(write)
        outcomes: list[Outcome] = []
        t0 = time.perf_counter_ns() if self._profile else 0
        fn_ns = 0
        self._changed_tables.clear()
        self._changed_columns.clear()
        traced = self._sync_watch(write)
        schema_before = self._schema_version(write)
        self._statement(write, "BEGIN IMMEDIATE")
        try:
            for index, job in enumerate(batch):
                if job.isolated:
                    save = f"j{index}"
                    self._statement(write, f"SAVEPOINT {save}")
                    kept_tables = set(self._changed_tables)
                    kept_columns = set(self._changed_columns)
                    try:
                        self._pending_tables.clear()
                        self._pending_columns.clear()
                        if self._profile:
                            f0 = time.perf_counter_ns()
                            result = job.run(tx)
                            fn_ns += time.perf_counter_ns() - f0
                        else:
                            result = job.run(tx)
                    except BaseException as exc:
                        self._changed_tables.clear()
                        self._changed_tables.update(kept_tables)
                        self._changed_columns.clear()
                        self._changed_columns.update(kept_columns)
                        self._statement(write, f"ROLLBACK TO {save}")
                        self._statement(write, f"RELEASE {save}")
                        outcomes.append(
                            Outcome(
                                ok=False,
                                result=None,
                                error=exc,
                                isolated=True,
                            )
                        )
                        continue
                    self._statement(write, f"RELEASE {save}")
                    outcomes.append(
                        Outcome(ok=True, result=result, error=None, isolated=True)
                    )
                    continue
                try:
                    self._pending_tables.clear()
                    self._pending_columns.clear()
                    if self._profile:
                        f0 = time.perf_counter_ns()
                        result = job.run(tx)
                        fn_ns += time.perf_counter_ns() - f0
                    else:
                        result = job.run(tx)
                except BaseException as exc:
                    self._statement(write, "ROLLBACK")
                    self._abort()
                    self._resolve_rolled_back(batch, index, exc)
                    return
                outcomes.append(
                    Outcome(ok=True, result=result, error=None, isolated=False)
                )
            self._statement(write, "COMMIT")
        except BaseException as exc:
            with suppress(apsw.Error):
                self._statement(write, "ROLLBACK")
            self._abort()
            self._wake(batch, None, exc)
            return
        self._notify(self._schema_version(write) != schema_before, traced=traced)
        if self._profile:
            self.stats.add(fn_ns=fn_ns, commit_ns=time.perf_counter_ns() - t0 - fn_ns)

        if self._after_commit is not None:
            try:
                self._after_commit(tuple(outcomes))
            except BaseException as exc:
                if self._on_error is not None:
                    self._on_error(exc)
        self._wake(batch, outcomes, None)

    def _abort(self) -> None:
        if self._on_abort is None:
            return
        try:
            self._on_abort()
        except BaseException as exc:
            if self._on_error is not None:
                with suppress(BaseException):
                    self._on_error(exc)

    def _resolve_rolled_back(
        self,
        batch: list[Job],
        failed_at: int,
        exc: BaseException,
    ) -> None:
        errors: list[BaseException] = []
        for index in range(len(batch)):
            if index == failed_at:
                errors.append(exc)
            elif index < failed_at:
                errors.append(
                    WriterRolledBack(
                        "batch rolled back after a non-isolated write failed",
                        context={"failed_index": failed_at},
                    )
                )
            else:
                errors.append(
                    WriterRolledBack(
                        "batch rolled back; this write never ran",
                        context={"failed_index": failed_at},
                    )
                )
        self._wake(batch, None, None, errors)

    def _wake(
        self,
        batch: list[Job],
        outcomes: list[Outcome] | None,
        shared_error: BaseException | None,
        errors: list[BaseException] | None = None,
    ) -> None:
        t0 = time.perf_counter_ns() if self._profile else 0
        grouped: dict[
            AbstractEventLoop, list[tuple[Job, object, BaseException | None]]
        ] = {}
        for index, job in enumerate(batch):
            if outcomes is not None:
                result, error = outcomes[index].result, outcomes[index].error
            elif errors is not None:
                result, error = None, errors[index]
            else:
                result, error = None, shared_error
            if job.future is None:
                if error is not None and self._on_error is not None:
                    with suppress(BaseException):
                        self._on_error(error)
                continue
            loop = job.loop
            if loop is None or loop.is_closed():
                settle(job.future, result, error)
                continue
            grouped.setdefault(loop, []).append((job, result, error))
        for loop, items in grouped.items():
            captured = items

            def run(
                work: list[tuple[Job, object, BaseException | None]] = captured,
            ) -> None:
                for job, result, error in work:
                    if job.future is not None:
                        settle(job.future, result, error)

            self._wakes.post(loop, run)
        if self._profile:
            self.stats.add(wake_ns=time.perf_counter_ns() - t0)

    def _schema_version(self, write: apsw.Connection) -> int:
        self._pending_tables.clear()
        self._pending_columns.clear()
        row = write.execute("PRAGMA schema_version").fetchone()
        return 0 if row is None else int(row[0])

    def _statement(self, write: apsw.Connection, text: str) -> None:
        """Run writer SQL. Drop any half-built column note first."""
        self._pending_tables.clear()
        self._pending_columns.clear()
        write.execute(text)

    def _sync_watch(self, write: apsw.Connection) -> bool:
        """Arm the write trace only while a watch is open. Return that flag."""
        with self._watch_lock:
            live = bool(self._watches)
        if live and not self._armed:
            write.authorizer = self._auth_write
            write.setexectrace(self._trace)
            self._armed = True
        elif not live and self._armed:
            write.authorizer = None
            write.setexectrace(None)
            self._armed = False
        return self._armed

    def _auth_write(
        self,
        op: int,
        a: str | None,
        b: str | None,
        _db: str | None,
        _trigger: str | None,
    ) -> int:
        """Record the table or the column this statement writes."""
        if not a or a.startswith("sqlite_"):
            return apsw.SQLITE_OK
        if op == apsw.SQLITE_INSERT or op == apsw.SQLITE_DELETE:
            self._pending_tables.add(a)
        elif op == apsw.SQLITE_UPDATE and b:
            self._pending_columns.add((a, b))
        return apsw.SQLITE_OK

    def _trace(self, _cursor: object, sql: str, _bindings: object) -> bool:
        """Apply the cached effect of ``sql``. The query itself stays unread.

        ``BEGIN``, ``COMMIT``, ``SAVEPOINT``, ``RELEASE``, and
        ``PRAGMA schema_version`` add no user table. Skip them.
        """
        if _bookkeeping(sql):
            self._pending_tables.clear()
            self._pending_columns.clear()
            return True
        if self._pending_tables or self._pending_columns:
            tables = frozenset(self._pending_tables)
            columns = frozenset(self._pending_columns)
            self._pending_tables.clear()
            self._pending_columns.clear()
            self._effects[sql] = (tables, columns)
        else:
            found = self._effects.get(sql)
            if found is None:
                # The authorizer has not run yet. Do not remember "no change".
                return True
            tables, columns = found
        if tables:
            self._changed_tables.update(tables)
        if columns:
            self._changed_columns.update(columns)
        return True

    def _notify(self, schema_changed: bool, *, traced: bool) -> None:
        with self._watch_lock:
            if schema_changed:
                self._effects.clear()
            if not self._watches:
                return
            if schema_changed or not traced:
                found = list(self._watches.values())
            else:
                ids: set[int] = set()
                for table in self._changed_tables:
                    hit = self._by_table.get(table)
                    if hit:
                        ids.update(hit)
                for column in self._changed_columns:
                    hit = self._by_column.get(column)
                    if hit:
                        ids.update(hit)
                found = [self._watches[wid] for wid in ids if wid in self._watches]
        notes: list[tuple[AbstractEventLoop, Callable[[], None]]] = []
        for watch in found:
            armed = watch.advance()
            if armed is not None:
                notes.append(armed)
        if notes:
            deliver(self._wakes, notes)

    def _add_watch(self, watch: Watch) -> None:
        with self._watch_lock:
            self._watch_ids += 1
            watch.wid = self._watch_ids
            self._watches[watch.wid] = watch
            for table in watch.tables:
                self._by_table.setdefault(table, set()).add(watch.wid)
            for column in watch.columns:
                self._by_column.setdefault(column, set()).add(watch.wid)

    def _drop_watch(self, watch: Watch) -> None:
        with self._watch_lock:
            self._watches.pop(watch.wid, None)
            for table in watch.tables:
                bucket = self._by_table.get(table)
                if bucket is not None:
                    bucket.discard(watch.wid)
                    if not bucket:
                        del self._by_table[table]
            for column in watch.columns:
                bucket = self._by_column.get(column)
                if bucket is not None:
                    bucket.discard(watch.wid)
                    if not bucket:
                        del self._by_column[column]

    def _stop_watches(self) -> None:
        with self._watch_lock:
            found = list(self._watches.values())
            self._watches.clear()
            self._by_table.clear()
            self._by_column.clear()
        for watch in found:
            watch.stop()


class Reader:
    """One read-only connection.

    Open as many as you need. Use each one from the thread that opened it.
    Close it on that same thread. A read does not enter the writer queue.
    """

    __slots__ = ("_conn", "_db", "_lock", "_owner", "_retired")

    def __init__(self, db: LiteWriter, busy_ms: int) -> None:
        self._db = db
        self._owner = get_ident()
        self._lock = Lock()
        self._retired = False
        self._conn: apsw.Connection | None = connect(
            db.path, readonly=True, busy_ms=busy_ms
        )

    def execute(self, query: str | Clauses, params: Bindings = ()) -> apsw.Cursor:
        """Run SQL text or a query on this connection."""
        return execute(self._live(), query, params)

    def query(self, query: str | Clauses, /, **params: object) -> list[tuple[Any, ...]]:
        """All rows. ``params`` fill each ``:name``."""
        return rows(self._live(), query, params)

    def one(self, query: str | Clauses, /, **params: object) -> tuple[Any, ...] | None:
        """The first row, or None when the query returns nothing."""
        return one(self._live(), query, params)

    def value(self, query: str | Clauses, /, **params: object) -> Any:
        """The first column of the first row."""
        return value(self._live(), query, params)

    def close(self) -> None:
        """Close this connection. A second close does nothing."""
        self._check_owner()
        with self._lock:
            conn = self._conn
            self._conn = None
            self._retired = True
        self._db._drop_reader(self)  # pyright: ignore[reportPrivateUsage]
        if conn is not None:
            conn.close()

    def owned_here(self) -> bool:
        """True when this thread opened the reader."""
        return get_ident() == self._owner

    def _retire(self) -> None:
        """The writer closed on another thread. This thread closes the connection."""
        with self._lock:
            self._retired = True

    def _check_owner(self) -> None:
        if not self.owned_here():
            raise WriterRuntime(
                "use this reader from the thread that opened it",
                example="with db.reader() as reader:\n    reader.query(q)",
            )

    def _live(self) -> apsw.Connection:
        self._check_owner()
        with self._lock:
            retired = self._retired
            conn = self._conn
        if retired or conn is None:
            self.close()
            raise WriterRuntime("this reader is closed")
        return conn

    def __enter__(self) -> Reader:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
