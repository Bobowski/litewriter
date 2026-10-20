import asyncio
import logging
import threading
import time
from collections.abc import Callable
from itertools import pairwise
from pathlib import Path
from typing import cast

import apsw
import pytest

from conftest import SCHEMA, boom, insert, one_batch
from litewriter import LiteWriter, QueryError, StateError, Tx
from litewriter.watch import Tracker


def test_hz(tmp_path: Path) -> None:
    paced = LiteWriter(tmp_path / "a.sqlite3", hz=50)
    assert paced.hz == 50
    paced.hz = 0
    assert paced.hz == 0
    with pytest.raises(QueryError, match="hz"):
        paced.hz = -1
    with pytest.raises(QueryError, match="hz"):
        LiteWriter(tmp_path / "b.sqlite3", hz=-5)


def test_start_required(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    with pytest.raises(StateError, match="start"):
        writer.submit(insert, "x")
    with pytest.raises(StateError, match="start"):
        writer.push(insert, "x")
    writer.close()


async def test_write_needs_start(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    with pytest.raises(StateError, match="start"):
        await writer.write(insert, "x")
    await asyncio.to_thread(writer.close)


async def test_write_returns_after_commit(db: LiteWriter) -> None:
    row_id = await db.write(insert, "hi")
    assert db.query("SELECT body FROM t WHERE id = :id", id=row_id) == [("hi",)]


async def test_a_failing_job_rolls_back_and_siblings_commit(db: LiteWriter) -> None:
    """B fails; A and C are on disk before B's await raises."""
    async with one_batch(db):
        fa = asyncio.create_task(db.write(insert, "A"))
        fb = asyncio.create_task(db.write(boom, "nope"))
        fc = asyncio.create_task(db.write(insert, "C"))

    a = await fa
    c = await fc
    with pytest.raises(ValueError, match="nope"):
        await fb

    bodies = [row[0] for row in db.query("SELECT body FROM t ORDER BY id")]
    assert bodies == ["A", "C"]
    assert a != c


async def test_a_failure_inside_a_job_keeps_its_own_writes_out(
    db: LiteWriter,
) -> None:
    def bad(tx: Tx) -> None:
        tx.execute("INSERT INTO t(body) VALUES ('gone')")
        raise ValueError("halfway")

    async with one_batch(db):
        fa = asyncio.create_task(db.write(insert, "kept"))
        fb = asyncio.create_task(db.write(bad))

    await fa
    with pytest.raises(ValueError, match="halfway"):
        await fb
    assert db.query("SELECT body FROM t") == [("kept",)]


def test_push_errors_after_commit(tmp_path: Path) -> None:
    errors: list[BaseException] = []

    def on_error(exc: BaseException) -> None:
        errors.append(exc)

    writer = LiteWriter(tmp_path / "p.sqlite3", hz=0, schema=SCHEMA, on_error=on_error)
    writer.start()
    writer.push(insert, "ok")
    writer.push(boom, "late")
    # A submit after the pushes commits no earlier than them.
    writer.submit(insert, "fine").result(timeout=2)
    count = writer.value("SELECT count(*) FROM t")
    writer.close()
    assert count == 2
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "late" in str(errors[0])


def test_push_failure_logs_by_default(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    writer = LiteWriter(tmp_path / "p.sqlite3", hz=0, schema=SCHEMA)
    writer.start()
    with caplog.at_level(logging.ERROR, logger="litewriter"):
        writer.push(boom, "kaput")
        writer.submit(insert, "done").result(timeout=2)
    writer.close()
    assert any(
        record.name == "litewriter"
        and record.message == "push failed"
        and record.exc_info
        and isinstance(record.exc_info[1], ValueError)
        for record in caplog.records
    )


def test_submit_from_two_threads(db: LiteWriter) -> None:
    ids: list[int] = []
    lock = threading.Lock()

    def worker(label: str) -> None:
        row_id = db.submit(insert, label).result(timeout=1.0)
        with lock:
            ids.append(row_id)

    threads = [
        threading.Thread(target=worker, args=("t1",)),
        threading.Thread(target=worker, args=("t2",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1.0)
    assert len(ids) == 2
    assert db.value("SELECT count(*) FROM t") == 2


async def test_submit_result_refuses_to_block_the_loop(db: LiteWriter) -> None:
    async with one_batch(db):
        future = db.submit(insert, "held")
        with pytest.raises(StateError, match="event loop"):
            future.result()
    assert await asyncio.to_thread(future.result, 2) > 0


async def test_a_done_future_returns_on_the_loop(db: LiteWriter) -> None:
    future = db.submit(insert, "settled")
    await asyncio.to_thread(future.result, 2)
    assert future.result() > 0


def test_result_blocks_a_plain_thread(db: LiteWriter) -> None:
    box: list[int] = []

    def wait() -> None:
        box.append(db.submit(insert, "x").result(timeout=2))

    thread = threading.Thread(target=wait)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(box) == 1


async def test_two_loops(db: LiteWriter) -> None:
    results: list[int] = []

    def run_other() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results.append(loop.run_until_complete(db.write(insert, "loop-b")))
        finally:
            loop.close()

    thread = threading.Thread(target=run_other)
    thread.start()
    here = await db.write(insert, "loop-a")
    thread.join(timeout=1.0)
    assert here > 0
    assert len(results) == 1
    assert db.value("SELECT count(*) FROM t") == 2


def test_reads_on_other_threads_see_commits(db: LiteWriter) -> None:
    db.submit(insert, "a").result(timeout=1.0)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def work() -> None:
        try:
            barrier.wait(timeout=2)
            for _ in range(20):
                assert db.query("SELECT body FROM t ORDER BY id")[0] == ("a",)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []


def test_reads_and_writes_from_many_threads(db: LiteWriter) -> None:
    errors: list[BaseException] = []

    def write_rows(label: str) -> None:
        try:
            for i in range(40):
                db.submit(insert, f"{label}-{i}").result()
        except BaseException as exc:
            errors.append(exc)

    def read_rows() -> None:
        try:
            for _ in range(40):
                assert db.value("SELECT count(*) FROM t") >= 0
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write_rows, args=(str(n),)) for n in range(4)]
    threads.extend(threading.Thread(target=read_rows) for _ in range(4))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert db.value("SELECT count(*) FROM t") == 160


def test_other_thread_read_after_seal(db: LiteWriter) -> None:
    started = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def work() -> None:
        try:
            assert db.value("SELECT 1") == 1
            started.set()
            assert release.wait(timeout=2)
            db.value("SELECT 1")
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=work)
    thread.start()
    assert started.wait(timeout=2)
    db.close()
    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], StateError)
    assert "closed" in str(errors[0])


def test_reads_fail_after_close(db: LiteWriter) -> None:
    db.close()
    with pytest.raises(StateError, match="closed"):
        db.query("SELECT 1")


def test_writes_fail_after_close(db: LiteWriter) -> None:
    db.close()
    with pytest.raises(StateError, match="closed"):
        db.submit(insert, "x")


def test_busy_timeout_is_seconds(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "busy.sqlite3", busy_timeout=2)
    db.start()
    assert db.value("PRAGMA busy_timeout") == 2000
    db.close()


def test_busy_timeout_rejects_a_negative(tmp_path: Path) -> None:
    with pytest.raises(QueryError, match="busy_timeout"):
        LiteWriter(tmp_path / "busy.sqlite3", busy_timeout=-1)


def test_close_folds_the_wal(tmp_path: Path) -> None:
    path = tmp_path / "fold.sqlite3"
    db = LiteWriter(path, schema=SCHEMA)
    db.start()
    db.submit(insert, "kept").result(timeout=2)
    db.close()
    wal = path.with_name(path.name + "-wal")
    assert not wal.exists() or wal.stat().st_size == 0
    reader = LiteWriter(path)
    assert reader.value("SELECT body FROM t") == "kept"
    reader.close()


def test_close_folds_a_wal_written_before_start(tmp_path: Path) -> None:
    path = tmp_path / "early.sqlite3"
    db = LiteWriter(path, schema=SCHEMA)
    db.close()
    wal = path.with_name(path.name + "-wal")
    assert not wal.exists() or wal.stat().st_size == 0


def test_schema_string_creates_the_tables(tmp_path: Path) -> None:
    path = tmp_path / "s.sqlite3"
    db = LiteWriter(
        path,
        schema="CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER);",
    )
    db.start()
    names = [
        row[0]
        for row in db.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    ]
    db.close()
    assert names == ["a", "b"]
    assert path.exists()


def test_schema_callable_gets_a_tx(tmp_path: Path) -> None:
    def make(tx: Tx) -> None:
        tx.execute("CREATE TABLE w(id INTEGER PRIMARY KEY, x TEXT)")
        tx.execute("INSERT INTO w(x) VALUES ('seed')")

    db = LiteWriter(tmp_path / "s.sqlite3", schema=make)
    db.start()
    assert db.value("SELECT x FROM w") == "seed"
    db.close()


def test_a_failing_schema_stays_stopped(tmp_path: Path) -> None:
    path = tmp_path / "s.sqlite3"
    db = LiteWriter(path, schema="CREATE TABLE broken(", claim=1)
    with pytest.raises(StateError, match="schema failed"):
        db.start()
    with pytest.raises(StateError, match="start"):
        db.submit(insert, "x")
    # The claim was released: another writer can take it.
    other = LiteWriter(path, claim=0, schema=SCHEMA)
    other.start()
    other.close()


def test_hz_caps_commit_rate(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "hz.sqlite3", hz=50, schema=SCHEMA)
    writer.start()
    # The first commit opens the file. Later commits show the floor.
    writer.submit(insert, "warm").result(timeout=2.0)
    ticks: list[float] = []

    def ticked(tx: Tx, body: str) -> int:
        ticks.append(time.monotonic())
        return insert(tx, body)

    for i in range(4):
        writer.submit(ticked, str(i)).result(timeout=2.0)
    writer.close()
    gaps = [b - a for a, b in pairwise(ticks)]
    # 50 Hz leaves about 20 ms between batch starts. A slow file
    # system adds time, so assert the lower bound only.
    assert min(gaps) >= 0.015


def test_the_writer_thread_is_named(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "n.sqlite3", schema=SCHEMA)
    db.start()
    try:
        names = [t.name for t in threading.enumerate()]
        assert "litewriter" in names
    finally:
        db.close()


def test_underscore_kwargs_reach_the_write_function(db: LiteWriter) -> None:
    def flag(_tx: Tx, _flag: bool = False) -> bool:
        return _flag

    assert db.submit(flag, _flag=True).result(timeout=1.0) is True


async def test_underscore_kwargs_reach_an_async_write(db: LiteWriter) -> None:
    def flag(_tx: Tx, _flag: bool = False) -> bool:
        return _flag

    assert await db.write(flag, _flag=True) is True


async def test_gather_many_writes(db: LiteWriter) -> None:
    ids = await asyncio.gather(*(db.write(insert, str(i)) for i in range(16)))
    assert len(set(ids)) == 16
    assert db.value("SELECT count(*) FROM t") == 16


def commit_now(tx: Tx) -> None:
    tx.execute("COMMIT")


def end_now(tx: Tx) -> None:
    tx.execute("END")


def rollback_then_write(tx: Tx) -> None:
    tx.execute("ROLLBACK")
    tx.execute("INSERT INTO t(body) VALUES ('sneak')")


@pytest.mark.parametrize("job", [commit_now, end_now, rollback_then_write])
async def test_a_write_cannot_end_the_transaction(
    db: LiteWriter, job: Callable[[Tx], None]
) -> None:
    async with one_batch(db):
        first = asyncio.create_task(db.write(insert, "a"))
        middle = asyncio.create_task(db.write(job))
        last = asyncio.create_task(db.write(insert, "b"))
    for task in (first, middle, last):
        with pytest.raises(StateError, match="ended the transaction"):
            await task
    assert db.value("SELECT count(*) FROM t") == 0
    await db.write(insert, "after")
    assert db.value("SELECT count(*) FROM t") == 1


async def test_a_schema_version_failure_fails_only_that_batch(
    db: LiteWriter, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    real = LiteWriter._schema_version

    def once(self: LiteWriter, write: apsw.Connection) -> tuple[int, int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("schema_version broke")
        return real(self, write)

    monkeypatch.setattr(LiteWriter, "_schema_version", once)
    async with db.watch("SELECT count(*) FROM t"):
        with pytest.raises(RuntimeError, match="schema_version broke"):
            await db.write(insert, "x")
        await db.write(insert, "y")
    assert db.value("SELECT count(*) FROM t") == 1


async def test_an_untraced_batch_skips_the_schema_check(
    db: LiteWriter, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    real = LiteWriter._schema_version

    def counted(self: LiteWriter, write: apsw.Connection) -> tuple[int, int]:
        nonlocal calls
        calls += 1
        return real(self, write)

    monkeypatch.setattr(LiteWriter, "_schema_version", counted)
    await db.write(insert, "x")
    assert calls == 0
    async with db.watch("SELECT count(*) FROM t"):
        await db.write(insert, "y")
        assert calls == 2
    await db.write(insert, "z")
    assert calls == 2


async def test_a_watch_notify_failure_still_settles(
    db: LiteWriter,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def broke(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("notify broke")

    monkeypatch.setattr(Tracker, "notify", broke)
    with caplog.at_level(logging.ERROR, logger="litewriter"):
        row_id = await db.write(insert, "x")
    assert row_id > 0
    assert any(
        record.name == "litewriter" and "watch" in record.message
        for record in caplog.records
    )


async def test_a_write_cancelled_before_it_runs_is_skipped(
    db: LiteWriter,
) -> None:
    async with one_batch(db):
        task = asyncio.create_task(db.write(insert, "555"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert db.value("SELECT count(*) FROM t WHERE body = '555'") == 0


def test_a_submit_cancelled_while_queued_never_runs(db: LiteWriter) -> None:
    entered = threading.Event()
    release = threading.Event()

    def hold(_tx: Tx) -> None:
        entered.set()
        release.wait(timeout=2.0)

    blocker = db.submit(hold)
    assert entered.wait(timeout=2.0)
    queued = db.submit(insert, "777")
    assert queued.cancel()
    release.set()
    blocker.result(timeout=2.0)
    assert db.value("SELECT count(*) FROM t WHERE body = '777'") == 0
    ran = db.submit(insert, "ok")
    assert ran.result(timeout=2.0) > 0
    assert not ran.cancel()


async def test_submit_exception_refuses_to_block_the_loop(
    db: LiteWriter,
) -> None:
    async with one_batch(db):
        future = db.submit(insert, "held")
        with pytest.raises(StateError, match="event loop"):
            future.exception()
    assert await asyncio.to_thread(future.result, 2.0) > 0


def test_a_schema_string_runs_every_statement(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "s.sqlite3", schema="SELECT 1; CREATE TABLE z(a)")
    db.start()
    assert db.value("SELECT count(*) FROM z") == 0
    db.close()


def test_close_closes_idle_reads_on_other_threads(tmp_path: Path) -> None:
    path = tmp_path / "r.sqlite3"
    db = LiteWriter(
        path,
        hz=0,
        schema="CREATE TABLE IF NOT EXISTS t"
        "(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT",
    )
    db.start()
    db.submit(insert, "one").result(timeout=2.0)
    ready = threading.Event()
    hold = threading.Event()
    seen: list[int] = []

    def helper() -> None:
        seen.append(db.value("SELECT count(*) FROM t"))
        ready.set()
        hold.wait(timeout=5.0)
        seen.append(db.value("SELECT count(*) FROM t"))

    thread = threading.Thread(target=helper)
    thread.start()
    assert ready.wait(timeout=2.0)
    db.close()
    wal = path.with_name(path.name + "-wal")
    assert not wal.exists() or wal.stat().st_size == 0
    db.start()
    db.submit(insert, "two").result(timeout=2.0)
    hold.set()
    thread.join(timeout=5.0)
    assert seen == [1, 2]
    db.close()


def test_close_inside_a_write_function_fails(db: LiteWriter) -> None:
    def closer(_tx: Tx) -> None:
        db.close()

    with pytest.raises(StateError, match="inside a write"):
        db.submit(closer).result(timeout=2.0)
    db.submit(insert, "x").result(timeout=2.0)
    assert db.value("SELECT count(*) FROM t") == 1


def test_submit_result_inside_a_write_raises(db: LiteWriter) -> None:
    def inner(_tx: Tx) -> int:
        return 1

    def outer(_tx: Tx) -> int:
        return db.submit(inner).result(timeout=2)

    with pytest.raises(StateError, match="block a writer thread"):
        db.submit(outer).result(timeout=2)
    db.submit(insert, "ok").result(timeout=2)
    assert db.value("SELECT count(*) FROM t") == 1


def test_submit_exception_inside_a_write_raises(db: LiteWriter) -> None:
    def outer(_tx: Tx) -> None:
        db.submit(insert, "x").exception(timeout=2)

    with pytest.raises(StateError, match="block a writer thread"):
        db.submit(outer).result(timeout=2)


async def test_an_async_write_function_raises(db: LiteWriter) -> None:
    async def add(tx: Tx) -> int:
        return tx.value("SELECT 1")

    with pytest.raises(QueryError, match="async def"):
        await db.write(add)
    with pytest.raises(QueryError, match="async def"):
        db.submit(add)
    with pytest.raises(QueryError, match="async def"):
        db.push(add)
    await db.write(insert, "x")
    assert db.value("SELECT count(*) FROM t") == 1


async def test_a_write_returning_a_coroutine_fails_only_that_caller(
    db: LiteWriter,
) -> None:
    import gc
    import warnings

    async def coro() -> int:
        return 1

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        async with one_batch(db):
            good = asyncio.create_task(db.write(insert, "kept"))
            bad = asyncio.create_task(db.write(lambda _tx: coro()))
        await good
        with pytest.raises(QueryError, match="async def"):
            await bad
        gc.collect()
    assert db.value("SELECT count(*) FROM t") == 1
    assert not [
        warning for warning in caught if issubclass(warning.category, RuntimeWarning)
    ]


def test_a_slow_schema_still_starts(tmp_path: Path) -> None:
    """A schema slower than the old 5 s deadline still starts once."""

    def slow(tx: Tx) -> None:
        tx.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY)")
        time.sleep(5.5)

    db = LiteWriter(tmp_path / "slow.sqlite3", schema=slow, hz=0)
    db.start()
    assert sum(t.name == "litewriter" for t in threading.enumerate()) == 1

    def add(tx: Tx) -> None:
        tx.execute("INSERT INTO t VALUES (1)")

    db.submit(add).result(timeout=2.0)
    assert db.value("SELECT count(*) FROM t") == 1
    db.close()
    assert not any(t.name == "litewriter" for t in threading.enumerate())


def test_start_fails_when_the_writer_thread_dies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(LiteWriter, "_run", lambda _self: None)
    db = LiteWriter(tmp_path / "t.sqlite3")
    with pytest.raises(StateError, match="exited before it was ready"):
        db.start()
    assert db._thread is None
    db.close()


def test_db_reads_inside_a_write_raise(db: LiteWriter) -> None:
    def peek(_tx: Tx) -> None:
        db.query("SELECT count(*) FROM t")

    with pytest.raises(StateError, match="read with tx"):
        db.submit(peek).result(timeout=2)


def test_db_value_inside_a_write_raises(db: LiteWriter) -> None:
    def peek(_tx: Tx) -> None:
        db.value("SELECT 1")

    with pytest.raises(StateError, match="read with tx"):
        db.submit(peek).result(timeout=2)


def test_db_snapshot_inside_a_write_raises(db: LiteWriter) -> None:
    def peek(_tx: Tx) -> None:
        with db.snapshot():
            pass

    with pytest.raises(StateError, match="read with tx"):
        db.submit(peek).result(timeout=2)


def test_submit_rejects_a_non_function(db: LiteWriter) -> None:
    with pytest.raises(QueryError, match="a write is a function"):
        db.submit(cast("Callable[[Tx], None]", 42))


def test_into_must_be_callable(db: LiteWriter) -> None:
    with pytest.raises(QueryError, match="into builds a row"):
        db.query("SELECT 1", cast("Callable[..., tuple]", 5))


def test_tx_stays_on_the_writer_thread(db: LiteWriter) -> None:
    kept: list[Tx] = []

    def grab(tx: Tx) -> None:
        kept.append(tx)

    db.submit(grab).result(timeout=2)
    with pytest.raises(StateError, match="writer thread"):
        kept[0].execute("SELECT 1")
    with pytest.raises(StateError, match="writer thread"):
        kept[0].last_insert_rowid()


def test_close_does_not_wait_out_the_pace(tmp_path: Path) -> None:
    slow = LiteWriter(tmp_path / "slow.sqlite3", hz=1, schema=SCHEMA)
    slow.start()
    slow.submit(insert, "x").result(timeout=2)
    started = time.monotonic()
    slow.close()
    assert time.monotonic() - started < 0.5


async def test_close_on_a_loop_raises(db: LiteWriter) -> None:
    with pytest.raises(StateError, match="block the event loop"):
        db.close()


async def test_aclose_closes_on_a_worker_thread(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "t.sqlite3", schema="CREATE TABLE t (v INTEGER);")
    writer.start()
    await writer.aclose()
    with pytest.raises(StateError):
        await writer.write(lambda tx: tx.execute("INSERT INTO t VALUES (1)"))


async def test_start_with_a_claim_wait_refuses_the_loop(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "t.sqlite3", claim=30)
    with pytest.raises(StateError, match="block the event loop"):
        writer.start()
