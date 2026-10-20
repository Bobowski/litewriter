import asyncio
import threading
from asyncio import AbstractEventLoop
from collections.abc import Callable
from pathlib import Path
from threading import Thread
from typing import cast

import apsw
import pytest

from conftest import SCHEMA, insert
from litewriter import LiteError, LiteWriter, QueryError, Select, StateError, Tx


def insert_kw(tx: Tx, *, body: str) -> int:
    return insert(tx, body)


def test_close_is_idempotent(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    db.close()
    db.close()


def test_context_manager_commits_then_stops(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    with LiteWriter(path, hz=0, schema=SCHEMA) as db:
        row_id = db.submit(insert, "kept").result(timeout=1.0)
        assert row_id == 1
    check = LiteWriter(path, hz=0)
    check.start()
    assert check.query("SELECT body FROM t WHERE id = 1") == [("kept",)]
    check.close()


def test_start_twice(db: LiteWriter) -> None:
    with pytest.raises(StateError, match="already started"):
        db.start()


def test_kwargs_and_push_then_read(db: LiteWriter) -> None:
    row_id = db.submit(insert_kw, body="kw").result(timeout=1.0)
    assert db.query("SELECT body FROM t WHERE id = :id", id=row_id) == [("kw",)]


def test_write_functions_may_have_any_kwarg_names(db: LiteWriter) -> None:
    def both(tx: Tx, *, body: str, into: int) -> int:
        tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)
        return into

    assert db.submit(both, body="x", into=7).result(timeout=1.0) == 7


def test_close_drains_pending(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0, schema=SCHEMA)
    db.start()
    futures = [db.submit(insert, str(i)) for i in range(20)]
    db.close()
    ids = [f.result(timeout=1.0) for f in futures]
    assert len(ids) == 20
    check = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    check.start()
    assert check.value("SELECT count(*) FROM t") == 20
    check.close()


def test_calling_thread_cannot_write(db: LiteWriter) -> None:
    with pytest.raises(apsw.ReadOnlyError):
        db.query("INSERT INTO t(body) VALUES ('no')")


def test_execute_is_gone(db: LiteWriter) -> None:
    assert not hasattr(LiteWriter, "execute")


def test_read_of_a_missing_file_fails(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "missing.sqlite3", hz=0)
    with pytest.raises(StateError, match="does not exist"):
        db.query("SELECT 1")


def test_reads_before_start(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    made = LiteWriter(path, hz=0, schema=SCHEMA)
    made.start()
    made.close()
    writer = LiteWriter(path, hz=0)
    assert writer.value("SELECT count(*) FROM t") == 0
    writer.close()


def test_reads_work_right_after_start(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0, schema=SCHEMA)
    db.start()
    assert db.value("SELECT count(*) FROM t") == 0
    db.close()


def test_read_connections_are_per_thread(db: LiteWriter) -> None:
    seen: list[int] = []

    def look() -> None:
        seen.append(id(db._conn()))

    look()
    thread = Thread(target=look)
    thread.start()
    thread.join(timeout=1.0)
    assert len(seen) == 2
    assert seen[0] != seen[1]


def test_into_builds_rows(db: LiteWriter) -> None:
    db.submit(insert, "x").result(timeout=1.0)
    rows = db.query("SELECT id, body FROM t", lambda i, b: f"{i}:{b}")
    assert rows == ["1:x"]
    one = db.one("SELECT body FROM t", lambda b: b.upper())
    assert one == "X"


def test_a_param_named_into(db: LiteWriter) -> None:
    db.submit(insert, "x").result(timeout=1.0)
    rows = db.query("SELECT id, body FROM t WHERE id = :into", into=1)
    assert rows == [(1, "x")]
    assert db.one("SELECT body FROM t WHERE id = :into", into=1) == ("x",)


def test_a_query_param_may_be_an_underscore_name(db: LiteWriter) -> None:
    assert db.query("SELECT :_1 AS x", _1=5) == [(5,)]
    with pytest.raises(QueryError, match="reserved"):
        db.query(Select("id", from_="t"), _1=5)


def test_a_callable_into_as_a_keyword_raises(db: LiteWriter) -> None:
    db.submit(insert, "x").result(timeout=1.0)
    with pytest.raises(QueryError, match="into is positional"):
        db.query("SELECT id, body FROM t", into=tuple)
    with pytest.raises(QueryError, match="into is positional"):
        db.one("SELECT id, body FROM t", into=tuple)
    with pytest.raises(QueryError, match="into is positional"):
        db.value("SELECT id FROM t", into=int)

    def work(tx: Tx) -> None:
        tx.query("SELECT id FROM t", into=tuple)

    with pytest.raises(QueryError, match="into is positional"):
        db.submit(work).result(timeout=1.0)

    def other(tx: Tx) -> None:
        tx.execute("SELECT 1", into=tuple)

    with pytest.raises(QueryError, match="into is positional"):
        db.submit(other).result(timeout=1.0)


def test_lite_error_carries_context() -> None:
    exc = QueryError("nope", context={"a": 1}, help_text="try this", example="x()")
    text = str(exc)
    assert "nope" in text
    assert "a=1" in text
    assert "try this" in text
    assert "x()" in text
    assert isinstance(exc, LiteError)
    assert isinstance(exc, ValueError)
    assert isinstance(StateError("x"), RuntimeError)


def test_a_loop_without_add_reader_wakes_once() -> None:
    from litewriter.wake import LoopWakes

    ran: list[str] = []
    queued: list[Callable[[], None]] = []

    class Bare:
        def add_reader(self, _fd: int, _callback: object) -> None:
            raise NotImplementedError

        def is_closed(self) -> bool:
            return False

        def call_soon_threadsafe(self, fn: Callable[[], None]) -> None:
            queued.append(fn)

    loop = cast(AbstractEventLoop, Bare())
    wakes = LoopWakes()
    wakes.ensure(loop)
    wakes.post(loop, lambda: ran.append("a"))
    wakes.post(loop, lambda: ran.append("b"))
    assert len(queued) == 2
    queued.pop()()
    assert ran == ["a", "b"]
    wakes.close()


async def test_no_posted_wake_is_lost() -> None:
    """5000 posts from a thread while the loop runs: every wake fires.

    Regression: _fire used to take the pending list before draining the
    socket, so a post between the two steps lost its byte and stranded.
    """
    from litewriter.wake import LoopWakes

    n = 5_000
    loop = asyncio.get_running_loop()
    wakes = LoopWakes()
    wakes.ensure(loop)
    ran = 0
    done = asyncio.Event()

    def tick() -> None:
        nonlocal ran
        ran += 1
        if ran == n:
            done.set()

    def produce() -> None:
        for _ in range(n):
            wakes.post(loop, tick)

    thread = Thread(target=produce)
    thread.start()
    await asyncio.wait_for(done.wait(), 10)
    thread.join(timeout=2)
    wakes.close()
    assert ran == n


def test_uri_escaped_names_read_their_own_file(tmp_path: Path) -> None:
    """`a%20b.sqlite3` is a different file from `a b.sqlite3`."""
    space = tmp_path / "a b.sqlite3"
    escaped = tmp_path / "a%20b.sqlite3"
    with LiteWriter(space, hz=0, schema=SCHEMA) as db:
        db.submit(insert, "space").result(timeout=1.0)
    with LiteWriter(escaped, hz=0, schema=SCHEMA) as db:
        db.submit(insert, "escaped").result(timeout=1.0)
    check = LiteWriter(escaped, hz=0)
    check.start()
    try:
        assert check.value("SELECT body FROM t") == "escaped"
    finally:
        check.close()


def test_a_missing_parent_is_created(tmp_path: Path) -> None:
    path = tmp_path / "deep" / "nest" / "t.sqlite3"
    with LiteWriter(path, hz=0, schema=SCHEMA) as db:
        db.submit(insert, "nested").result(timeout=1.0)
        assert db.value("SELECT body FROM t") == "nested"


@pytest.mark.parametrize(
    "name", ["a b.sqlite3", "a#b.sqlite3", "a?b.sqlite3", "zażółć.sqlite3"]
)
def test_unusual_file_names(tmp_path: Path, name: str) -> None:
    path = tmp_path / name
    with LiteWriter(path, hz=0, schema=SCHEMA) as db:
        db.submit(insert, name).result(timeout=1.0)
    check = LiteWriter(path, hz=0)
    check.start()
    try:
        assert check.value("SELECT body FROM t") == name
    finally:
        check.close()


async def test_a_read_row_does_not_pin_the_snapshot(db: LiteWriter) -> None:
    await db.write(insert, "a")
    await db.write(insert, "b")
    assert db.one("SELECT id FROM t") == (1,)
    await db.write(insert, "c")
    assert db.value("SELECT count(*) FROM t") == 3
    async with db.watch("SELECT count(*) FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [(3,)]
        await db.write(insert, "d")
        assert await asyncio.wait_for(anext(it), 1) == [(4,)]


async def test_sockets_of_dead_loops_close(db: LiteWriter) -> None:
    import gc
    import warnings

    def once() -> None:
        asyncio.run(db.write(insert, "x"))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        threads = [Thread(target=once) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        await db.write(insert, "y")
        gc.collect()
    assert not [
        warning
        for warning in caught
        if issubclass(warning.category, ResourceWarning)
        and "socket" in str(warning.message)
    ]
    assert len(db._wakes._loops) <= 1
    assert db.value("SELECT count(*) FROM t") == 6


async def test_async_with_starts_and_stops_the_writer(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    async with LiteWriter(path, hz=0, claim=1, schema=SCHEMA) as db:
        await db.write(insert, "x")
    assert not any(t.name == "litewriter" for t in threading.enumerate())
    other = LiteWriter(path, claim=0)
    other.start()
    await asyncio.to_thread(other.close)


def test_memory_and_empty_paths_are_rejected() -> None:
    with pytest.raises(QueryError, match="needs a file"):
        LiteWriter(":memory:")
    with pytest.raises(QueryError, match="needs a file"):
        LiteWriter("")


async def test_no_read_transaction_while_the_loop_is_idle(
    db: LiteWriter,
) -> None:
    await db.write(insert, "x")
    for _ in range(5):
        db.value("SELECT count(*) FROM t")
    with db.snapshot():
        db.value("SELECT count(*) FROM t")
    await asyncio.sleep(0.05)
    check = apsw.Connection(str(db.path))
    try:
        row = check.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        check.close()
    assert row is not None
    assert row[0] == 0


def test_a_snapshot_holds_a_fixed_view(db: LiteWriter) -> None:
    db.submit(insert, "a").result(timeout=2)
    with db.snapshot():
        assert db.value("SELECT count(*) FROM t") == 1
        db.submit(insert, "b").result(timeout=2)
        assert db.value("SELECT count(*) FROM t") == 1
        with db.snapshot():
            assert db.value("SELECT count(*) FROM t") == 1
    assert db.value("SELECT count(*) FROM t") == 2


async def test_a_snapshot_cannot_span_an_await(db: LiteWriter) -> None:
    await db.write(insert, "x")

    async def read_then_yield() -> None:
        with db.snapshot():
            db.value("SELECT count(*) FROM t")
            await asyncio.sleep(0)

    with pytest.raises(StateError, match="span an await"):
        await read_then_yield()
    await asyncio.sleep(0.05)
    check = apsw.Connection(str(db.path))
    try:
        row = check.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        check.close()
    assert row is not None
    assert row[0] == 0


async def test_another_task_reads_while_the_snapshot_owner_awaits(
    db: LiteWriter,
) -> None:
    await db.write(insert, "a")
    opened = asyncio.Event()
    go = asyncio.Event()
    seen: list[int] = []

    async def owner() -> None:
        with db.snapshot():
            assert db.value("SELECT count(*) FROM t") == 1
            opened.set()
            await go.wait()
            db.value("SELECT count(*) FROM t")

    async def other() -> None:
        await opened.wait()
        await asyncio.to_thread(lambda: db.submit(insert, "b").result(timeout=2))
        seen.append(cast("int", db.value("SELECT count(*) FROM t")))
        go.set()

    task = asyncio.create_task(owner())
    other_task = asyncio.create_task(other())
    await other_task
    with pytest.raises(StateError, match="span an await"):
        await task
    assert seen == [2]
    assert db.value("SELECT count(*) FROM t") == 2


def test_a_snapshot_after_close_raises(db: LiteWriter) -> None:
    db.close()
    with pytest.raises(StateError, match="closed"), db.snapshot():
        pass


async def test_an_into_that_reads(db: LiteWriter) -> None:
    await db.write(insert, "a")
    seen: list[int] = []

    def look(row_id: int, body: str) -> tuple[int, str]:
        seen.append(cast("int", db.value("SELECT count(*) FROM t")))
        return row_id, body

    assert db.query("SELECT id, body FROM t", look) == [(1, "a")]
    assert seen == [1]
