"""One process can claim the database file."""

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path
from threading import Thread

import pytest

from litewriter import LiteWriter, WriterBusy, WriterError, WriterRuntime

SRC = str(Path(__file__).resolve().parent.parent / "src")
SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"

HOLDER = """
import sys
import time
from litewriter import LiteWriter

db = LiteWriter(sys.argv[1])
db.claim(timeout=2)
print("ok", flush=True)
time.sleep(30)
"""


def _child_env() -> dict[str, str]:
    env = {"PYTHONPATH": SRC, "PATH": ""}
    if sys.platform == "win32":
        env["PATH"] = os.environ.get("PATH", "")
        root = os.environ.get("SYSTEMROOT", "")
        if root:
            env["SYSTEMROOT"] = root
    return env


def _claim_file(path: Path) -> Path:
    return path.with_name(path.name + "-claim")


def test_claim_is_optional(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    db = LiteWriter(path, hz=0)
    db.start()
    db.close()
    assert not _claim_file(path).exists()


def test_claim_without_constructor_timeout(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    db = LiteWriter(path)
    with pytest.raises(WriterError, match="timeout"):
        db.claim()
    db.claim(timeout=0)
    other = LiteWriter(path)
    with pytest.raises(WriterBusy, match="holds the claim") as caught:
        other.claim(timeout=0)
    assert caught.value.context["holder"] == os.getpid()
    db.claim(timeout=0)
    db.close()
    other.claim(timeout=0)
    other.close()


def test_negative_timeout_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    with pytest.raises(WriterError, match="0 or greater"):
        LiteWriter(path, claim=-1)
    db = LiteWriter(path)
    with pytest.raises(WriterError, match="0 or greater"):
        db.claim(timeout=-1)


def test_timeout_raises_and_drops_the_wait(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path)
    holder.claim(timeout=0)
    other = LiteWriter(path)
    started = time.monotonic()
    with pytest.raises(WriterBusy):
        other.claim(timeout=0.25)
    elapsed = time.monotonic() - started
    assert elapsed >= 0.1
    assert elapsed < 2
    holder.close()
    other.claim(timeout=1)
    other.close()


def test_close_wakes_a_waiter(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path)
    holder.claim(timeout=0)
    other = LiteWriter(path)
    box: list[BaseException] = []

    def wait() -> None:
        try:
            other.claim(timeout=5)
        except BaseException as exc:
            box.append(exc)

    thread = Thread(target=wait)
    thread.start()
    time.sleep(0.05)
    started = time.monotonic()
    holder.close()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert box == []
    assert time.monotonic() - started < 0.5
    other.close()


def test_start_claims_when_asked(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    db = LiteWriter(path, hz=0, claim=1)
    db.start()
    with pytest.raises(WriterRuntime, match="already started"):
        db.start()
    other = LiteWriter(path)
    with pytest.raises(WriterBusy):
        other.claim(timeout=0)
    db.close()
    other.claim(timeout=0)
    other.close()


def test_failed_start_releases_the_claim(tmp_path: Path) -> None:
    path = tmp_path / "not-a-file.sqlite3"
    path.mkdir()
    db = LiteWriter(path, claim=1)
    with pytest.raises(WriterRuntime, match="open the file"):
        db.start()
    other = LiteWriter(path)
    other.claim(timeout=0)
    other.close()


def test_readers_work_while_the_claim_is_held(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"

    def add(tx, body: str) -> None:
        tx.execute("INSERT INTO t(body) VALUES (?)", (body,))

    with LiteWriter(path, hz=0, claim=1) as db:
        db.execute_script(SCHEMA)
        db.submit(add, "a").result()
        reader = LiteWriter(path, hz=0)
        assert reader.value("SELECT count(*) FROM t") == 1
        with pytest.raises(WriterBusy):
            LiteWriter(path).claim(timeout=0)


def test_async_with_claims_and_releases(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"

    async def run() -> None:
        async with LiteWriter(path, hz=0, claim=1) as db:
            db.execute_script(SCHEMA)
            with pytest.raises(WriterBusy):
                LiteWriter(path).claim(timeout=0)
        follower = LiteWriter(path)
        follower.claim(timeout=0)
        follower.close()

    asyncio.run(run())


def test_aclaim_cancel_does_not_keep_the_lock(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path)
    holder.claim(timeout=0)

    async def run() -> None:
        waiter = LiteWriter(path)
        task = asyncio.create_task(waiter.aclaim(timeout=5))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    holder.close()
    follower = LiteWriter(path)
    follower.claim(timeout=1)
    follower.close()


def test_a_killed_holder_drops_the_claim(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    proc = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(path)],
        stdout=subprocess.PIPE,
        text=True,
        env=_child_env(),
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "ok"
        proc.kill()
        proc.wait(timeout=5)
        follower = LiteWriter(path)
        follower.claim(timeout=2)
        follower.close()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
