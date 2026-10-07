import asyncio
from pathlib import Path
from threading import Thread

import apsw
import pytest

from conftest import SCHEMA
from litewriter import LiteWriter, Tx, WriterError, WriterRuntime


def insert(tx: Tx, body: str) -> int:
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))
    return int(tx.last_insert_rowid())


def insert_kw(tx: Tx, *, body: str) -> int:
    return insert(tx, body)


def test_close_is_idempotent(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    db.close()
    db.close()


def test_context_manager_commits_then_stops(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    with LiteWriter(path, hz=0) as db:
        db.execute_script(SCHEMA)
        row_id = db.submit(insert, "kept").result(timeout=1.0)
        assert row_id == 1
    check = LiteWriter(path, hz=0)
    check.start()
    assert check.execute("SELECT body FROM t WHERE id = 1").fetchone() == ("kept",)
    check.close()


def test_start_twice(db: LiteWriter) -> None:
    with pytest.raises(WriterRuntime, match="already started"):
        db.start()


def test_execute_script_before_start(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    db.execute_script(SCHEMA)
    db.start()
    row_id = db.submit(insert, "x").result(timeout=1.0)
    assert db.execute("SELECT body FROM t WHERE id = ?", (row_id,)).fetchone() == ("x",)
    db.close()


def test_kwargs_and_push_then_read(db: LiteWriter) -> None:
    row_id = db.submit(insert_kw, body="kw").result(timeout=1.0)
    assert db.execute("SELECT body FROM t WHERE id = ?", (row_id,)).fetchone() == (
        "kw",
    )


def test_close_drains_pending(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    db.execute_script(SCHEMA)
    db.start()
    futures = [db.submit(insert, str(i)) for i in range(20)]
    db.close()
    ids = [f.result(timeout=1.0) for f in futures]
    assert len(ids) == 20
    check = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    check.start()
    count = check.execute("SELECT count(*) FROM t").fetchone()
    check.close()
    assert count == (20,)


def test_execute_script_runs_every_statement(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    db.start()
    db.execute_script("CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER);")
    names = [
        row[0]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    ]
    db.close()
    assert names == ["a", "b"]


def test_calling_thread_cannot_write(db: LiteWriter) -> None:
    with pytest.raises(apsw.ReadOnlyError):
        db.execute("INSERT INTO t(body) VALUES ('no')")


def test_read_of_a_missing_file_fails(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "missing.sqlite3", hz=0)
    with pytest.raises(WriterRuntime, match="does not exist"):
        db.query("SELECT 1")
    with pytest.raises(WriterRuntime, match="does not exist"):
        db.reader()


def test_reads_before_start(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    writer = LiteWriter(path, hz=0)
    writer.execute_script(SCHEMA)
    assert writer.execute("SELECT count(*) FROM t").fetchone() == (0,)
    seen: list[int] = []

    def look() -> None:
        seen.append(id(writer.conn()))
        with writer.reader() as reader:
            assert reader.query("SELECT count(*) FROM t") == [(0,)]

    look()
    thread = Thread(target=look)
    thread.start()
    thread.join(timeout=2)
    assert len(seen) == 2
    assert seen[0] != seen[1]
    writer.close()


def test_each_file_has_its_own_read_connection(tmp_path: Path) -> None:
    one = LiteWriter(tmp_path / "a.sqlite3", hz=0)
    two = LiteWriter(tmp_path / "b.sqlite3", hz=0)
    one.execute_script(SCHEMA)
    two.execute_script("CREATE TABLE u(id INTEGER PRIMARY KEY)")
    assert one.conn() is not two.conn()
    assert one.execute("SELECT count(*) FROM t").fetchone() == (0,)
    assert two.execute("SELECT count(*) FROM u").fetchone() == (0,)
    one.close()
    two.close()


async def test_offload_before_start(tmp_path: Path) -> None:
    writer = LiteWriter(tmp_path / "t.sqlite3", hz=0)
    writer.execute_script(SCHEMA)

    def count(tx: apsw.Connection) -> int:
        row = tx.execute("SELECT count(*) FROM t").fetchone()
        assert row is not None
        return int(row[0])

    assert await writer.offload(count) == 0
    writer.close()


def test_readers_are_per_thread(db: LiteWriter) -> None:
    seen: list[int] = []

    def look() -> None:
        seen.append(id(db.conn()))

    look()
    thread = Thread(target=look)
    thread.start()
    thread.join(timeout=1.0)
    assert len(seen) == 2
    assert seen[0] != seen[1]


def test_profile_counts_jobs_and_batches(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=0, profile=True)
    db.execute_script(SCHEMA)
    db.start()
    futures = [db.submit(insert, str(i)) for i in range(32)]
    for future in futures:
        future.result(timeout=1.0)
    db.close()
    snap = db.stats.snapshot()
    assert snap.jobs == 32
    assert snap.batches >= 1
    assert snap.enqueue_ns > 0
    assert snap.fn_ns > 0
    assert snap.commit_ns > 0


def test_live_hz_change(tmp_path: Path) -> None:
    db = LiteWriter(tmp_path / "t.sqlite3", hz=60)
    db.hz = 40
    assert db.hz == 40
    with pytest.raises(WriterError):
        db.hz = -0.1


async def test_gather_many_calls(db: LiteWriter) -> None:
    ids = await asyncio.gather(*(db.call(insert, str(i)) for i in range(16)))
    assert len(set(ids)) == 16
    count = db.execute("SELECT count(*) FROM t").fetchone()
    assert count == (16,)


async def test_after_commit_error_still_returns(tmp_path: Path) -> None:
    def boom(_outcomes: object) -> None:
        raise RuntimeError("hook")

    db = LiteWriter(tmp_path / "t.sqlite3", hz=0, after_commit=boom)
    db.execute_script(SCHEMA)
    db.start()
    row_id = await db.call(insert, "ok")
    assert row_id > 0
    db.close()
