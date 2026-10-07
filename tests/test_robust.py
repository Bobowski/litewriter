"""Several processes, a killed writer, many threads, and large batches."""

import asyncio
import subprocess
import sys
from pathlib import Path
from threading import Thread

import apsw

from litewriter import LiteWriter, Tx

SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"
SRC = str(Path(__file__).resolve().parent.parent / "src")

WRITER = """
import sys
from litewriter import LiteWriter

path, tag, count = sys.argv[1], sys.argv[2], int(sys.argv[3])

def add(tx, body):
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))

with LiteWriter(path, hz=0) as db:
    for i in range(count):
        db.submit(add, f"{tag}-{i}").result()
        print("ok", flush=True)
"""


def add(tx: Tx, body: str) -> None:
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))


def _spawn(path: Path, tag: str, count: int) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", WRITER, str(path), tag, str(count)],
        env={"PYTHONPATH": SRC, "PATH": ""},
        stdout=subprocess.PIPE,
        text=True,
    )


def _count(path: Path) -> int:
    conn = apsw.Connection(str(path))
    try:
        row = conn.execute("SELECT count(*) FROM t").fetchone()
        assert row is not None
        return int(row[0])
    finally:
        conn.close()


def test_three_processes_write_one_file(tmp_path: Path) -> None:
    path = tmp_path / "shared.sqlite3"
    with LiteWriter(path, hz=0) as db:
        db.execute_script(SCHEMA)
    procs = [_spawn(path, tag, 200) for tag in ("a", "b", "c")]
    for proc in procs:
        proc.communicate(timeout=60)
        assert proc.returncode == 0
    assert _count(path) == 600


def test_acknowledged_writes_survive_a_killed_process(tmp_path: Path) -> None:
    path = tmp_path / "crash.sqlite3"
    with LiteWriter(path, hz=0) as db:
        db.execute_script(SCHEMA)
    proc = _spawn(path, "k", 100_000)
    assert proc.stdout is not None
    acknowledged = 0
    for line in proc.stdout:
        if line.strip() == "ok":
            acknowledged += 1
        if acknowledged >= 100:
            proc.kill()
            break
    for line in proc.stdout:
        if line.strip() == "ok":
            acknowledged += 1
    proc.wait(timeout=10)
    conn = apsw.Connection(str(path))
    try:
        assert list(conn.execute("PRAGMA integrity_check")) == [("ok",)]
    finally:
        conn.close()
    assert _count(path) >= acknowledged


def test_many_threads_submit(tmp_path: Path) -> None:
    path = tmp_path / "threads.sqlite3"
    with LiteWriter(path, hz=0) as db:
        db.execute_script(SCHEMA)

        def work(tag: int) -> None:
            for i in range(250):
                db.submit(add, f"{tag}-{i}").result(timeout=30)

        threads = [Thread(target=work, args=(tag,)) for tag in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        rows = db.query("SELECT count(*), count(DISTINCT body) FROM t")
    assert rows == [(2000, 2000)]


async def test_a_large_batch_commits_once_and_in_order(tmp_path: Path) -> None:
    path = tmp_path / "big.sqlite3"
    async with LiteWriter(path, hz=0) as db:
        db.execute_script(SCHEMA)
        for i in range(20_000):
            db.push(add, str(i))
        await db.call(add, "last")
        first = db.query("SELECT body FROM t ORDER BY id LIMIT 3")
        total = db.query("SELECT count(*) FROM t")
    assert first == [("0",), ("1",), ("2",)]
    assert total == [(20_001,)]


async def test_close_while_calls_are_waiting(tmp_path: Path) -> None:
    path = tmp_path / "late.sqlite3"
    db = LiteWriter(path, hz=0)
    db.start()
    db.execute_script(SCHEMA)
    tasks = [asyncio.create_task(db.call(add, str(i))) for i in range(200)]
    await asyncio.sleep(0)
    db.close()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    done = [item for item in outcomes if not isinstance(item, BaseException)]
    assert _count(path) == len(done)
