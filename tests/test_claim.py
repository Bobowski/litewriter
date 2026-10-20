"""One process can claim the database file."""

import asyncio
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from threading import Thread

import pytest

import litewriter.claim as claim_mod
from litewriter import ClaimBusy, LiteWriter, QueryError, StateError, Tx
from litewriter.claim import Claim

SRC = str(Path(__file__).resolve().parent.parent / "src")
SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"

HOLDER = """
import sys
import time
from litewriter import LiteWriter

db = LiteWriter(sys.argv[1], claim=2)
db.start()
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


def test_claim_reports_the_holder_pid(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path, claim=0)
    holder.start()
    other = LiteWriter(path, claim=0)
    with pytest.raises(ClaimBusy, match="holds the claim") as caught:
        other.start()
    assert caught.value.context["holder"] == os.getpid()
    holder.close()
    other.start()
    other.close()


def test_timeout_zero_tries_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path, claim=0)
    holder.start()
    tries = 0
    real = claim_mod._try_lock

    def counting(fd: int) -> bool:
        nonlocal tries
        tries += 1
        return real(fd)

    monkeypatch.setattr(claim_mod, "_try_lock", counting)
    with pytest.raises(ClaimBusy):
        LiteWriter(path, claim=0).start()
    assert tries == 1
    holder.close()


def test_a_timed_out_claim_leaves_no_thread(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path, claim=0)
    holder.start()
    before = threading.active_count()
    with pytest.raises(ClaimBusy):
        LiteWriter(path, claim=0.05).start()
    time.sleep(0.05)
    assert threading.active_count() == before
    holder.close()


def test_a_waiting_claim_ends_with_state_error_on_close(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path, claim=0)
    holder.start()
    waiter = LiteWriter(path, claim=30)
    box: list[BaseException] = []

    def wait() -> None:
        try:
            waiter.start()
        except BaseException as exc:
            box.append(exc)

    thread = Thread(target=wait)
    thread.start()
    time.sleep(0.05)
    waiter.close()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(box) == 1
    assert isinstance(box[0], StateError)
    holder.close()


def test_async_claim_cancellation_releases_the_fd(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path, claim=0)
    holder.start()
    waiter = LiteWriter(path, claim=30)

    async def run() -> None:
        task = asyncio.create_task(waiter.__aenter__())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert waiter._claim._fd is None
    holder.close()
    follower = LiteWriter(path, claim=1, schema=SCHEMA)
    follower.start()
    follower.close()


def test_a_negative_timeout_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(QueryError, match="0 or greater"):
        LiteWriter(tmp_path / "t.sqlite3", claim=-1)


def test_claim_close_then_start_again(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    db = LiteWriter(path, claim=0, schema=SCHEMA)
    db.start()
    db.close()
    other = LiteWriter(path, claim=0)
    other.start()
    other.close()


def test_failed_start_releases_the_claim(tmp_path: Path) -> None:
    path = tmp_path / "not-a-file.sqlite3"
    path.mkdir()
    db = LiteWriter(path, claim=1)
    with pytest.raises(StateError, match="open the file"):
        db.start()
    # The claim file lock is free even though start failed.
    probe = Claim(path.with_name(path.name + "-claim"))
    assert probe.acquire(0, threading.Event())
    probe.release()


def test_failed_schema_releases_the_claim(tmp_path: Path) -> None:
    path = tmp_path / "s.sqlite3"
    db = LiteWriter(path, claim=1, schema="CREATE TABLE bad(")
    with pytest.raises(StateError, match="schema failed"):
        db.start()
    other = LiteWriter(path, claim=0, schema=SCHEMA)
    other.start()
    other.close()


def test_readers_work_while_the_claim_is_held(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"

    def add(tx: Tx, body: str) -> None:
        tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)

    with LiteWriter(path, hz=0, claim=1, schema=SCHEMA) as db:
        db.submit(add, "a").result()
        reader = LiteWriter(path, hz=0)
        assert reader.value("SELECT count(*) FROM t") == 1
        with pytest.raises(ClaimBusy):
            LiteWriter(path, claim=0).start()


def test_async_with_claims_and_releases(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"

    async def run() -> None:
        async with LiteWriter(path, hz=0, claim=1, schema=SCHEMA):
            with pytest.raises(ClaimBusy):
                LiteWriter(path, claim=0).start()
        follower = LiteWriter(path, claim=0)
        follower.start()
        await asyncio.to_thread(follower.close)

    asyncio.run(run())


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
        follower = LiteWriter(path, claim=2)
        follower.start()
        follower.close()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_claim_holds_across_release_and_reacquire(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    claim = Claim(path.with_name(path.name + "-claim"))
    cancel = threading.Event()
    assert claim.acquire(0, cancel)
    assert claim.held()
    other = Claim(path.with_name(path.name + "-claim"))
    with pytest.raises(ClaimBusy):
        other.acquire(0, cancel)
    claim.release()
    assert not claim.held()
    assert other.acquire(0, cancel)
    other.release()


def test_an_async_claim_wait_ends_on_close(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3"
    holder = LiteWriter(path, claim=0)
    holder.start()
    waiter = LiteWriter(path, claim=30)

    async def run() -> None:
        task = asyncio.create_task(waiter.__aenter__())
        await asyncio.sleep(0.05)
        await asyncio.to_thread(waiter.close)
        with pytest.raises(StateError, match="closed"):
            await asyncio.wait_for(task, 2)

    asyncio.run(run())
    holder.close()


def test_a_second_async_acquire_returns_false(tmp_path: Path) -> None:
    path = tmp_path / "t.sqlite3-claim"
    holder = Claim(path)
    assert holder.acquire(0, threading.Event())
    contender = Claim(path)

    async def run() -> None:
        first = asyncio.create_task(contender.aacquire(0.2, threading.Event()))
        await asyncio.sleep(0)
        assert await contender.aacquire(0, threading.Event()) is False
        with pytest.raises(ClaimBusy):
            await first

    asyncio.run(run())
    holder.release()
