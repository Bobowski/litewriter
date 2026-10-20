import asyncio
import threading
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from litewriter import LiteWriter, Tx

SCHEMA = """
CREATE TABLE t(
    id INTEGER PRIMARY KEY,
    body TEXT NOT NULL
) STRICT
"""


def insert(tx: Tx, body: str) -> int:
    tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)
    return tx.last_insert_rowid()


def boom(_tx: Tx, message: str) -> None:
    raise ValueError(message)


@asynccontextmanager
async def one_batch(db: LiteWriter):
    """Hold the writer so jobs queued in the block share one commit."""
    holding = threading.Event()
    release = threading.Event()

    def hold(_tx: object) -> None:
        holding.set()
        release.wait(timeout=2)

    def wait_hold() -> None:
        db.submit(hold).result(timeout=2)

    waiter = asyncio.create_task(asyncio.to_thread(wait_hold))
    assert await asyncio.to_thread(holding.wait, 2)
    try:
        yield
        await asyncio.sleep(0)
    finally:
        release.set()
        await waiter


@pytest.fixture
def db(tmp_path: Path) -> Iterator[LiteWriter]:
    writer = LiteWriter(tmp_path / "t.sqlite3", hz=0, schema=SCHEMA)
    writer.start()
    yield writer
    writer.close()
