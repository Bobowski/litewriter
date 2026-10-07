import asyncio

import pytest

from litewriter import LiteWriter, WriterRuntime
from litewriter.watch import Watch


def insert(tx, body: str) -> None:
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))


def insert_u(tx, body: str) -> None:
    tx.execute("INSERT INTO u(body) VALUES (?)", (body,))


def rename(tx, body: str) -> None:
    tx.execute("UPDATE t SET body = ?", (body,))


PAGE = {"select": ["id", "body"], "from": "t", "order_by": ["id"]}
IDS = {"select": ["id"], "from": "t", "order_by": ["id"]}


async def test_two_watches_wake_from_one_commit(db: LiteWriter) -> None:
    async with db.watch(PAGE) as page, db.watch(IDS) as ids:
        rows = aiter(page)
        keys = aiter(ids)
        assert await anext(rows) == []
        assert await anext(keys) == []
        await db.call(insert, "hi")
        assert await anext(rows) == [(1, "hi")]
        assert await anext(keys) == [(1,)]


async def test_a_repeated_insert_wakes_the_watch(db: LiteWriter) -> None:
    await db.call(insert, "hi")
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == [(1, "hi")]
        await db.call(insert, "yo")
        assert await anext(it) == [(1, "hi"), (2, "yo")]


async def test_a_repeated_update_wakes_the_watch(db: LiteWriter) -> None:
    await db.call(insert, "hi")
    await db.call(rename, "a")
    async with db.watch("SELECT body FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [("a",)]
        await db.call(rename, "b")
        assert await anext(it) == [("b",)]


async def test_watch_yields_now_then_the_new_rows(db: LiteWriter) -> None:
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.call(insert, "hi")
        assert await anext(it) == [(1, "hi")]


async def test_raw_sql_through_a_view_wakes(db: LiteWriter) -> None:
    db.execute_script(
        "CREATE VIEW longest AS SELECT body FROM t ORDER BY length(body) DESC"
    )
    async with db.watch("SELECT body FROM longest WHERE body LIKE :p", p="%a%") as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.call(insert, "abc")
        assert await anext(it) == [("abc",)]


async def test_other_table_does_not_wake(db: LiteWriter) -> None:
    db.execute_script("CREATE TABLE u(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.call(insert_u, "x")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)


async def test_two_writes_yield_the_newest_once(db: LiteWriter) -> None:
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.call(insert, "a")
        await db.call(insert, "b")
        assert await anext(it) == [(1, "a"), (2, "b")]
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)


async def test_same_rows_do_not_yield_again(db: LiteWriter) -> None:
    async with db.watch(IDS) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.call(insert, "a")
        assert await anext(it) == [(1,)]
        await db.call(rename, "b")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)


async def test_rolled_back_write_does_not_wake(db: LiteWriter) -> None:
    def boom(tx) -> None:
        tx.execute("INSERT INTO t(body) VALUES ('no')")
        raise RuntimeError("nope")

    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        with pytest.raises(RuntimeError, match="nope"):
            await db.call(boom)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)
        assert db.execute("SELECT count(*) FROM t").fetchone() == (0,)


async def test_close_stops_the_watch(db: LiteWriter) -> None:
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        waiting = asyncio.create_task(anext(it))
        await asyncio.sleep(0)
        db.close()
        with pytest.raises(StopAsyncIteration):
            await waiting


def set_seen(tx, seen: int) -> None:
    tx.execute("UPDATE t SET seen = ? WHERE id = 1", (seen,))


def set_body(tx, body: str) -> None:
    tx.execute("UPDATE t SET body = ? WHERE id = 1", (body,))


async def test_other_column_does_not_reread(db: LiteWriter) -> None:
    db.execute_script("ALTER TABLE t ADD COLUMN seen INTEGER NOT NULL DEFAULT 0")
    await db.call(insert, "hi")
    reads = 0
    original = Watch._read

    def counted(self: Watch):
        nonlocal reads
        reads += 1
        return original(self)

    Watch._read = counted  # type: ignore[method-assign]
    try:
        async with db.watch("SELECT body FROM t WHERE id = 1") as live:
            it = aiter(live)
            assert await anext(it) == [("hi",)]
            assert reads == 1
            waiter = asyncio.create_task(anext(it))
            await asyncio.sleep(0)
            await db.call(set_seen, 4)
            await asyncio.sleep(0)
            assert reads == 1
            assert not waiter.done()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
    finally:
        Watch._read = original  # type: ignore[method-assign]


async def test_watched_column_wakes(db: LiteWriter) -> None:
    db.execute_script("ALTER TABLE t ADD COLUMN seen INTEGER NOT NULL DEFAULT 0")
    await db.call(insert, "hi")
    async with db.watch("SELECT body FROM t WHERE id = 1") as live:
        it = aiter(live)
        assert await anext(it) == [("hi",)]
        await db.call(set_body, "yo")
        assert await anext(it) == [("yo",)]


async def test_count_ignores_an_update_and_sees_an_insert(db: LiteWriter) -> None:
    reads = 0
    original = Watch._read

    def counted(self: Watch):
        nonlocal reads
        reads += 1
        return original(self)

    Watch._read = counted  # type: ignore[method-assign]
    try:
        async with db.watch("SELECT count(*) FROM t") as live:
            it = aiter(live)
            assert await anext(it) == [(0,)]
            assert reads == 1
            waiter = asyncio.create_task(anext(it))
            await asyncio.sleep(0)
            await db.call(rename, "x")
            await asyncio.sleep(0)
            assert reads == 1
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            await db.call(insert, "hi")
            assert await anext(it) == [(1,)]
            assert reads == 2
    finally:
        Watch._read = original  # type: ignore[method-assign]


def test_watch_outside_with_raises(db: LiteWriter) -> None:
    live = db.watch(PAGE)

    async def pull() -> None:
        await anext(live)

    with pytest.raises(WriterRuntime, match="async with"):
        asyncio.run(pull())
