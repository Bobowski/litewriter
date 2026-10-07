import asyncio
import threading
import time
from itertools import pairwise
from pathlib import Path

import pytest

from conftest import SCHEMA, one_batch
from litewriter import (
    LiteWriter,
    Outcome,
    WriterError,
    WriterRolledBack,
    WriterRuntime,
    isolated,
)


def insert(tx, body: str) -> int:
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))
    return int(tx.last_insert_rowid())


def boom(_tx, message: str) -> None:
    raise ValueError(message)


def test_hz(tmp_path: Path) -> None:
    paced = LiteWriter(tmp_path / "a.sqlite3", hz=50)
    assert paced.hz == 50
    paced.hz = 0
    assert paced.hz == 0
    with pytest.raises(WriterError, match="hz"):
        paced.hz = -1
    with pytest.raises(WriterError, match="hz"):
        LiteWriter(tmp_path / "b.sqlite3", hz=-5)


def test_start_required(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    with pytest.raises(WriterRuntime, match="start"):
        writer.submit(insert, "x")
    writer.close()


async def test_call_returns_after_commit(db: LiteWriter) -> None:
    row_id = await db.call(insert, "hi")
    row = db.execute("SELECT body FROM t WHERE id = ?", (row_id,)).fetchone()
    assert row == ("hi",)


async def test_isolated_error_after_commit_sees_siblings(db: LiteWriter) -> None:
    """B fails; A and C are on disk before B's await raises."""
    seen: list[int] = []

    def after(outcomes: tuple[Outcome, ...]) -> None:
        n = db.execute("SELECT count(*) FROM t").fetchone()
        assert n is not None
        seen.append(int(n[0]))

    async with one_batch(db):
        db._after_commit = after
        fa = asyncio.create_task(db.call(isolated(insert), "A"))
        fb = asyncio.create_task(db.call(isolated(boom), "nope"))
        fc = asyncio.create_task(db.call(isolated(insert), "C"))

    a = await fa
    c = await fc
    with pytest.raises(ValueError, match="nope"):
        await fb

    assert seen == [0, 2]
    bodies = [row[0] for row in db.execute("SELECT body FROM t ORDER BY id").fetchall()]
    assert bodies == ["A", "C"]
    assert a != c


async def test_nonisolated_failure_rolls_back_batch(db: LiteWriter) -> None:
    async with one_batch(db):
        fa = asyncio.create_task(db.call(insert, "A", isolated=False))
        fb = asyncio.create_task(db.call(boom, "bad", isolated=False))
        fc = asyncio.create_task(db.call(insert, "C", isolated=False))

    with pytest.raises(WriterRolledBack):
        await fa
    with pytest.raises(ValueError, match="bad"):
        await fb
    with pytest.raises(WriterRolledBack):
        await fc

    count = db.execute("SELECT count(*) FROM t").fetchone()
    assert count == (0,)


async def test_push_errors_after_commit(tmp_path: Path) -> None:
    errors: list[BaseException] = []
    committed = threading.Event()

    def on_error(exc: BaseException) -> None:
        errors.append(exc)

    def after(_outcomes: tuple[Outcome, ...]) -> None:
        committed.set()

    writer = LiteWriter(
        tmp_path / "p.sqlite3",
        hz=0,
        on_error=on_error,
        after_commit=after,
    )
    writer.execute_script(SCHEMA)
    writer.start()
    writer.push(isolated(insert), "ok")
    writer.push(isolated(boom), "late")
    assert committed.wait(timeout=1.0)
    for _ in range(50):
        if errors:
            break
        time.sleep(0.01)
    count = writer.execute("SELECT count(*) FROM t").fetchone()
    writer.close()
    assert any(isinstance(e, ValueError) and "late" in str(e) for e in errors)
    assert count == (1,)


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
    count = db.execute("SELECT count(*) FROM t").fetchone()
    assert count == (2,)


async def test_two_loops(db: LiteWriter) -> None:
    results: list[int] = []

    def run_other() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results.append(loop.run_until_complete(db.call(insert, "loop-b")))
        finally:
            loop.close()

    thread = threading.Thread(target=run_other)
    thread.start()
    here = await db.call(insert, "loop-a")
    thread.join(timeout=1.0)
    assert here > 0
    assert len(results) == 1
    count = db.execute("SELECT count(*) FROM t").fetchone()
    assert count == (2,)


async def test_default_keeps_the_siblings(db: LiteWriter) -> None:
    async with one_batch(db):
        fa = asyncio.create_task(db.call(insert, "A"))
        fb = asyncio.create_task(db.call(boom, "nope"))
        fc = asyncio.create_task(db.call(insert, "C"))

    a = await fa
    c = await fc
    with pytest.raises(ValueError, match="nope"):
        await fb

    bodies = [row[0] for row in db.execute("SELECT body FROM t ORDER BY id")]
    assert bodies == ["A", "C"]
    assert a != c


def test_many_readers_see_a_commit(db: LiteWriter) -> None:
    db.submit(insert, "hi").result()
    readers = [db.reader() for _ in range(4)]
    try:
        for reader in readers:
            assert reader.query("SELECT body FROM t") == [("hi",)]
        with pytest.raises(Exception, match=r"readonly|READONLY|read-only"):
            readers[0].execute("INSERT INTO t(body) VALUES ('no')")
    finally:
        for reader in readers:
            reader.close()
    with pytest.raises(WriterRuntime, match="closed"):
        readers[0].query("SELECT body FROM t")


def test_readers_run_on_two_threads(db: LiteWriter) -> None:
    db.submit(insert, "a").result()
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def work() -> None:
        try:
            with db.reader() as reader:
                barrier.wait(timeout=2)
                for _ in range(20):
                    assert reader.query("SELECT body FROM t ORDER BY id")[0] == ("a",)
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
                row = db.execute("SELECT count(*) FROM t").fetchone()
                assert row is not None
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write_rows, args=(str(n),)) for n in range(4)]
    threads.extend(threading.Thread(target=read_rows) for _ in range(4))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    count = db.execute("SELECT count(*) FROM t").fetchone()
    assert count == (160,)


def test_reader_rejects_another_thread(db: LiteWriter) -> None:
    reader = db.reader()
    errors: list[BaseException] = []

    def work() -> None:
        try:
            reader.query("SELECT 1")
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=work)
    thread.start()
    thread.join()
    assert len(errors) == 1
    assert isinstance(errors[0], WriterRuntime)
    assert "thread" in str(errors[0])
    assert reader.query("SELECT 1") == [(1,)]
    reader.close()


def test_other_thread_closes_its_own_read_after_seal(db: LiteWriter) -> None:
    started = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def work() -> None:
        try:
            assert db.execute("SELECT 1").fetchone() == (1,)
            started.set()
            assert release.wait(timeout=2)
            db.execute("SELECT 1").fetchone()
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
    assert isinstance(errors[0], WriterRuntime)
    assert "closed" in str(errors[0])


def test_close_closes_open_readers(db: LiteWriter) -> None:
    reader = db.reader()
    db.close()
    with pytest.raises(WriterRuntime, match="closed"):
        reader.query("SELECT 1")


async def test_offload_readonly(db: LiteWriter) -> None:
    await db.call(insert, "x")

    def count_rows(tx) -> int:
        row = tx.execute("SELECT count(*) FROM t").fetchone()
        assert row is not None
        return int(row[0])

    assert await db.offload(count_rows) == 1

    def write_should_fail(tx) -> None:
        tx.execute("INSERT INTO t(body) VALUES (?)", ("no",))

    with pytest.raises(Exception, match=r"readonly|READONLY|read-only"):
        await db.offload(write_should_fail)


def test_hz_caps_commit_rate(tmp_path: Path) -> None:
    ticks: list[float] = []

    def after(_outcomes: tuple[Outcome, ...]) -> None:
        ticks.append(time.monotonic())

    writer = LiteWriter(tmp_path / "hz.sqlite3", hz=50, after_commit=after)
    writer.execute_script(SCHEMA)
    writer.start()
    # The first commit opens the file. Later commits show the floor.
    writer.submit(insert, "warm").result(timeout=2.0)
    ticks.clear()
    for i in range(4):
        writer.submit(insert, str(i)).result(timeout=2.0)
    writer.close()
    assert len(ticks) == 4
    gaps = [b - a for a, b in pairwise(ticks)]
    # 50 Hz leaves about 20 ms from the start of one commit to the next.
    assert min(gaps) >= 0.012
