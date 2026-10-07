import asyncio
import threading
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from litewriter import LiteWriter

SCHEMA = """
CREATE TABLE t(
    id INTEGER PRIMARY KEY,
    body TEXT NOT NULL
) STRICT
"""


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
    writer = LiteWriter(tmp_path / "t.sqlite3", hz=0, name="test")
    writer.start()
    writer.execute_script(SCHEMA)
    yield writer
    writer.close()
