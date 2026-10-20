import asyncio

import apsw
import pytest

from conftest import insert
from litewriter import LiteWriter, QueryError, Select, StateError, Tx, col, param
from litewriter.watch import Watch


def insert_u(tx: Tx, body: str) -> None:
    tx.execute("INSERT INTO u(body) VALUES (:body)", body=body)


def rename(tx: Tx, body: str) -> None:
    tx.execute("UPDATE t SET body = :body", body=body)


PAGE = Select("id", "body", from_="t", order_by="id")
IDS = Select("id", from_="t", order_by="id")


async def test_two_watches_wake_from_one_commit(db: LiteWriter) -> None:
    async with db.watch(PAGE) as page, db.watch(IDS) as ids:
        rows = aiter(page)
        keys = aiter(ids)
        assert await anext(rows) == []
        assert await anext(keys) == []
        await db.write(insert, "hi")
        assert await anext(rows) == [(1, "hi")]
        assert await anext(keys) == [(1,)]


async def test_three_watches_on_one_table_wake_together(
    db: LiteWriter,
) -> None:
    q = "SELECT count(*) FROM t"
    async with (
        db.watch(q) as first,
        db.watch(q) as second,
        db.watch(q) as third,
    ):
        streams = [aiter(first), aiter(second), aiter(third)]
        for stream in streams:
            assert await anext(stream) == [(0,)]
        await db.write(insert, "hi")
        for stream in streams:
            assert await anext(stream) == [(1,)]


def setup_extras(tx: Tx) -> None:
    tx.execute(
        "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
        "CREATE TABLE child("
        "id INTEGER PRIMARY KEY,"
        "pid INT REFERENCES parent(id) ON DELETE CASCADE"
        ");"
        "CREATE TABLE u(id INTEGER PRIMARY KEY, email TEXT UNIQUE, n INT);"
        "CREATE TABLE log(msg TEXT);"
        "CREATE TRIGGER tr AFTER UPDATE OF body ON t"
        " BEGIN INSERT INTO log(msg) VALUES ('x'); END;"
        "INSERT INTO parent VALUES (1);"
        "INSERT INTO child VALUES (1, 1);"
        "INSERT INTO u VALUES (1, 'a@x', 0);"
        "INSERT INTO t(body) VALUES ('a')"
    )


async def test_a_trigger_write_wakes_the_touched_table(
    db: LiteWriter,
) -> None:
    await db.write(setup_extras)
    async with db.watch("SELECT count(*) FROM log") as live:
        it = aiter(live)
        assert await anext(it) == [(0,)]

        def touch(tx: Tx) -> None:
            tx.execute("UPDATE t SET body = 'b'")

        await db.write(touch)
        assert await anext(it) == [(1,)]


async def test_a_cascade_delete_wakes_the_child_watch(
    db: LiteWriter,
) -> None:
    await db.write(setup_extras)
    async with db.watch("SELECT count(*) FROM child") as live:
        it = aiter(live)
        assert await anext(it) == [(1,)]

        def touch(tx: Tx) -> None:
            tx.execute("DELETE FROM parent WHERE id = 1")

        await db.write(touch)
        assert await anext(it) == [(0,)]


async def test_an_upsert_wakes_the_updated_column(db: LiteWriter) -> None:
    await db.write(setup_extras)
    async with db.watch("SELECT n FROM u") as live:
        it = aiter(live)
        assert await anext(it) == [(0,)]

        def touch(tx: Tx) -> None:
            tx.execute(
                "INSERT INTO u(email, n) VALUES ('a@x', 3)"
                " ON CONFLICT(email) DO UPDATE SET n = excluded.n"
            )

        await db.write(touch)
        assert await anext(it) == [(3,)]


async def test_executemany_wakes_the_watch(db: LiteWriter) -> None:
    async with db.watch("SELECT count(*) FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [(0,)]

        def touch(tx: Tx) -> None:
            tx.executemany(
                "INSERT INTO t(body) VALUES (:body)",
                [{"body": "a"}, {"body": "b"}],
            )

        await db.write(touch)
        assert await anext(it) == [(2,)]


async def test_a_multi_statement_write_wakes_every_touched_watch(
    db: LiteWriter,
) -> None:
    await db.write(setup_extras)
    async with (
        db.watch("SELECT n FROM u") as u_live,
        db.watch("SELECT count(*) FROM log") as log_live,
    ):
        u_it = aiter(u_live)
        log_it = aiter(log_live)
        assert await anext(u_it) == [(0,)]
        assert await anext(log_it) == [(0,)]

        def touch(tx: Tx) -> None:
            tx.execute(
                "UPDATE u SET n = :n WHERE id = 1;INSERT INTO log(msg) VALUES (:m)",
                n=7,
                m="x",
            )

        await db.write(touch)
        assert await anext(u_it) == [(7,)]
        assert await anext(log_it) == [(1,)]


async def test_an_unpreparable_write_wakes_every_watch(
    db: LiteWriter,
) -> None:
    await db.write(insert, "a")
    async with db.watch("SELECT count(*) FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [(1,)]

        def create_and_fill(tx: Tx) -> None:
            tx.execute("CREATE TABLE z(a); INSERT INTO z VALUES (1)")

        await db.write(create_and_fill)
        assert db._track._unknown
        assert live._gen == 1

        def other(tx: Tx) -> None:
            tx.execute("INSERT INTO z VALUES (2)")

        await db.write(other)
        assert not db._track._unknown
        assert live._gen == 1


async def test_a_write_through_value_is_tracked(db: LiteWriter) -> None:
    await db.write(insert, "a")
    async with db.watch("SELECT body FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [("a",)]

        def touch(tx: Tx) -> int:
            return tx.value("UPDATE t SET body = 'b' RETURNING id")

        assert await db.write(touch) == 1
        assert await anext(it) == [("b",)]


async def test_the_effect_cache_is_capped(db: LiteWriter) -> None:
    def many(tx: Tx) -> None:
        for i in range(1_100):
            tx.execute(f"INSERT INTO t(body) VALUES ('x{i}')")

    async with db.watch("SELECT count(*) FROM t"):
        await db.write(many)
    assert len(db._track._effects) <= 1024


async def test_a_repeated_insert_wakes_the_watch(db: LiteWriter) -> None:
    await db.write(insert, "hi")
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == [(1, "hi")]
        await db.write(insert, "yo")
        assert await anext(it) == [(1, "hi"), (2, "yo")]


async def test_a_repeated_update_wakes_the_watch(db: LiteWriter) -> None:
    await db.write(insert, "hi")
    await db.write(rename, "a")
    async with db.watch("SELECT body FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [("a",)]
        await db.write(rename, "b")
        assert await anext(it) == [("b",)]


async def test_watch_yields_now_then_the_new_rows(db: LiteWriter) -> None:
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(insert, "hi")
        assert await anext(it) == [(1, "hi")]


async def test_watch_into_builds_rows(db: LiteWriter) -> None:
    async with db.watch(IDS, lambda id_: id_ * 10) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(insert, "hi")
        assert await anext(it) == [10]


async def test_watch_params_and_auto_binds(db: LiteWriter) -> None:
    await db.write(insert, "a")
    q = Select("body", from_="t", where=col("body") == "a")
    async with db.watch(q) as live:
        it = aiter(live)
        assert await anext(it) == [("a",)]
        await db.write(rename, "b")
        assert await anext(it) == []


async def test_a_param_named_into(db: LiteWriter) -> None:
    await db.write(insert, "a")
    async with db.watch("SELECT body FROM t WHERE body = :into", into=9) as live:
        it = aiter(live)
        assert await anext(it) == []


async def test_raw_sql_through_a_view_wakes(db: LiteWriter) -> None:
    def make(tx: Tx) -> None:
        tx.execute(
            "CREATE VIEW longest AS SELECT body FROM t ORDER BY length(body) DESC"
        )

    await db.write(make)
    async with db.watch("SELECT body FROM longest WHERE body LIKE :p", p="%a%") as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(insert, "abc")
        assert await anext(it) == [("abc",)]


async def test_other_table_does_not_wake(db: LiteWriter) -> None:
    def make(tx: Tx) -> None:
        tx.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")

    await db.write(make)
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(insert_u, "x")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)


async def test_two_writes_yield_the_newest_once(db: LiteWriter) -> None:
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(insert, "a")
        await db.write(insert, "b")
        assert await anext(it) == [(1, "a"), (2, "b")]
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)


async def test_same_rows_do_not_yield_again(db: LiteWriter) -> None:
    async with db.watch(IDS) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(insert, "a")
        assert await anext(it) == [(1,)]
        await db.write(rename, "b")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)


async def test_rolled_back_write_does_not_wake(db: LiteWriter) -> None:
    def boom(tx: Tx) -> None:
        tx.execute("INSERT INTO t(body) VALUES ('no')")
        raise RuntimeError("nope")

    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        with pytest.raises(RuntimeError, match="nope"):
            await db.write(boom)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.05)
        assert db.value("SELECT count(*) FROM t") == 0


async def test_close_stops_the_watch(db: LiteWriter) -> None:
    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        waiting = asyncio.create_task(anext(it))
        await asyncio.sleep(0)
        await asyncio.to_thread(db.close)
        with pytest.raises(StopAsyncIteration):
            await waiting


def set_seen(tx: Tx, seen: int) -> None:
    tx.execute("UPDATE t SET seen = :seen WHERE id = 1", seen=seen)


def set_body(tx: Tx, body: str) -> None:
    tx.execute("UPDATE t SET body = :body WHERE id = 1", body=body)


async def test_other_column_does_not_reread(db: LiteWriter) -> None:
    def alter(tx: Tx) -> None:
        tx.execute("ALTER TABLE t ADD COLUMN seen INTEGER NOT NULL DEFAULT 0")

    await db.write(alter)
    await db.write(insert, "hi")
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
            await db.write(set_seen, 4)
            await asyncio.sleep(0)
            assert reads == 1
            assert not waiter.done()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
    finally:
        Watch._read = original  # type: ignore[method-assign]


async def test_watched_column_wakes(db: LiteWriter) -> None:
    def alter(tx: Tx) -> None:
        tx.execute("ALTER TABLE t ADD COLUMN seen INTEGER NOT NULL DEFAULT 0")

    await db.write(alter)
    await db.write(insert, "hi")
    async with db.watch("SELECT body FROM t WHERE id = 1") as live:
        it = aiter(live)
        assert await anext(it) == [("hi",)]
        await db.write(set_body, "yo")
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
            await db.write(rename, "x")
            await asyncio.sleep(0)
            assert reads == 1
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            await db.write(insert, "hi")
            assert await anext(it) == [(1,)]
            assert reads == 2
    finally:
        Watch._read = original  # type: ignore[method-assign]


def test_watch_outside_with_raises(db: LiteWriter) -> None:
    live = db.watch(PAGE)

    async def pull() -> None:
        await anext(live)

    with pytest.raises(StateError, match="async with"):
        asyncio.run(pull())


async def test_watch_reports_tables_and_columns(db: LiteWriter) -> None:
    live = db.watch(Select("id", "body", from_="t", where=col("id") > param("x")), x=0)
    async with live:
        assert live.tables == frozenset({"t"})
        assert ("t", "id") in live.columns


class Plain:
    def __init__(self, *a: object) -> None:
        self.a = a


async def test_watch_compares_raw_rows_not_built_objects(db: LiteWriter) -> None:
    """An equal-update wakes no watch even when into hides __eq__."""
    await db.write(insert, "x")

    def same(tx: Tx) -> None:
        tx.execute("UPDATE t SET body = body")

    async with db.watch("SELECT id, body FROM t", Plain) as live:
        it = aiter(live)
        first = await anext(it)
        assert isinstance(first[0], Plain)
        await db.write(same)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(it), 0.3)
        await db.write(rename, "y")
        rows = await asyncio.wait_for(anext(it), 1)
        assert isinstance(rows[0], Plain)
        assert rows[0].a == (1, "y")


async def test_a_watch_opened_after_stop_raises(
    db: LiteWriter, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = LiteWriter._dependencies

    def stopped(
        self: LiteWriter, q: str
    ) -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
        found = real(self, q)
        self._track.stop_all()
        return found

    monkeypatch.setattr(LiteWriter, "_dependencies", stopped)
    with pytest.raises(StateError, match="closed"):
        async with db.watch("SELECT count(*) FROM t"):
            pass
    assert not db._track._watches


def test_watch_into_as_a_keyword_raises(db: LiteWriter) -> None:
    with pytest.raises(QueryError, match="into is positional"):
        db.watch("SELECT id FROM t", into=tuple)


def setup_parent_log(tx: Tx) -> None:
    tx.execute(
        "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);"
        "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)"
    )


def ins_parent(tx: Tx, name: str) -> None:
    tx.execute("INSERT INTO parent(name) VALUES (:name)", name=name)


async def test_a_temp_trigger_created_in_a_job_wakes_the_watch(
    db: LiteWriter,
) -> None:
    """A TEMP trigger makes the cached effect of an INSERT stale."""
    await db.write(setup_parent_log)
    async with db.watch("SELECT count(*) FROM log") as live:
        it = aiter(live)
        assert await anext(it) == [(0,)]
        await db.write(ins_parent, "a")

        def trigger(tx: Tx) -> None:
            tx.execute(
                "CREATE TEMP TRIGGER tt AFTER INSERT ON parent"
                " BEGIN INSERT INTO log(msg) VALUES ('t'); END"
            )

        await db.write(trigger)
        await db.write(ins_parent, "b")
        assert await asyncio.wait_for(anext(it), 2) == [(1,)]


async def test_a_trigger_from_another_connection_wakes_the_watch(
    db: LiteWriter,
) -> None:
    """A trigger added by another connection bumps the schema version."""
    await db.write(setup_parent_log)
    async with db.watch("SELECT count(*) FROM log") as live:
        it = aiter(live)
        assert await anext(it) == [(0,)]
        await db.write(ins_parent, "a")
        other = apsw.Connection(str(db.path))
        try:
            other.execute(
                "CREATE TRIGGER tt AFTER INSERT ON parent"
                " BEGIN INSERT INTO log(msg) VALUES ('t'); END"
            )
        finally:
            other.close()
        await db.write(ins_parent, "b")
        assert await asyncio.wait_for(anext(it), 2) == [(1,)]


async def test_a_rowid_update_wakes_the_watch(db: LiteWriter) -> None:
    await db.write(insert, "a")

    def bump(tx: Tx) -> None:
        tx.execute("UPDATE t SET rowid = rowid + 1 WHERE id = 1")

    async with db.watch("SELECT rowid FROM t") as live:
        it = aiter(live)
        assert await anext(it) == [(1,)]
        await db.write(bump)
        assert await asyncio.wait_for(anext(it), 2) == [(2,)]


async def test_a_rowid_update_without_an_alias_wakes(db: LiteWriter) -> None:
    def setup(tx: Tx) -> None:
        tx.execute("CREATE TABLE t2(x TEXT)")
        tx.execute("INSERT INTO t2(x) VALUES ('a')")

    await db.write(setup)

    def bump(tx: Tx) -> None:
        tx.execute("UPDATE t2 SET rowid = rowid + 1")

    async with db.watch("SELECT rowid FROM t2") as live:
        it = aiter(live)
        assert await anext(it) == [(1,)]
        await db.write(bump)
        assert await asyncio.wait_for(anext(it), 2) == [(2,)]


async def test_identical_watches_share_the_reread(db: LiteWriter) -> None:
    reads = 0
    original = Watch._read

    def counted(self: Watch):
        nonlocal reads
        reads += 1
        return original(self)

    Watch._read = counted  # type: ignore[method-assign]
    try:
        async with db.watch(PAGE) as a, db.watch(PAGE) as b:
            assert await anext(aiter(a)) == []
            assert await anext(aiter(b)) == []
            reads = 0
            await db.write(insert, "x")
            results = await asyncio.gather(anext(aiter(a)), anext(aiter(b)))
            assert results == [[(1, "x")], [(1, "x")]]
            assert reads == 1
    finally:
        Watch._read = original  # type: ignore[method-assign]


async def test_an_attach_in_a_write_keeps_watches_working(db: LiteWriter) -> None:
    def attach(tx: Tx) -> None:
        tx.execute("ATTACH ':memory:' AS scratch")

    async with db.watch(PAGE) as live:
        it = aiter(live)
        assert await anext(it) == []
        await db.write(attach)
        await db.write(insert, "x")
        assert await asyncio.wait_for(anext(it), 2) == [(1, "x")]


async def test_a_watch_on_a_missing_table_is_a_query_error(db: LiteWriter) -> None:
    with pytest.raises(QueryError, match="prepare"):
        async with db.watch("SELECT * FROM nope"):
            pass


async def test_a_watch_opens_once(db: LiteWriter) -> None:
    watch = db.watch(PAGE)
    async with watch:
        with pytest.raises(StateError, match="already open"):
            await watch.__aenter__()


async def test_a_missing_param_with_a_watch_is_a_query_error(
    db: LiteWriter,
) -> None:
    async with db.watch(PAGE):

        def bad(tx: Tx) -> None:
            tx.execute("INSERT INTO t(body) VALUES (:nope)")

        with pytest.raises(QueryError, match="parameters"):
            await db.write(bad)
