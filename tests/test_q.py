"""The renderer: exact .sql text and the auto-bound .params."""

import re
import typing
from typing import cast

import apsw
import pytest

from litewriter import (
    BuiltinName,
    Delete,
    Insert,
    LiteWriter,
    QueryError,
    Select,
    Tx,
    Update,
    case,
    col,
    fn,
    lit,
    param,
    raw,
)

PAGE = Select(
    "m.id",
    "m.body",
    fn("lower", col("u.name")).as_("author"),
    from_="messages",
    join=("left", "users", col("m.author") == col("u.id")),
    where=col("m.room") == param("room"),
    order_by=col("m.at").desc(),
    limit=50,
)


def test_reads_like_hand_written_sql() -> None:
    assert PAGE.sql == (
        "SELECT m.id, m.body, lower(u.name) AS author"
        " FROM messages"
        " LEFT JOIN users ON m.author = u.id"
        " WHERE m.room = :room"
        " ORDER BY m.at DESC LIMIT :_1"
    )
    assert PAGE.params == {"_1": 50}


def test_auto_bound_values() -> None:
    q = Select("id", from_="t", where=col("room") == "open")
    assert q.sql == "SELECT id FROM t WHERE room = :_1"
    assert q.params == {"_1": "open"}
    # A copy: mutating it cannot corrupt the cached render.
    q.params["_1"] = "mutated"
    assert q.params == {"_1": "open"}


def test_the_same_shape_renders_the_same_sql() -> None:
    a = Select("id", from_="t", where=col("room") == "open")
    b = Select("id", from_="t", where=col("room") == "closed")
    assert a.sql == b.sql
    assert a.params != b.params


def test_binding_renumbers_across_where() -> None:
    q = Select("id", from_="t", where=col("a") == "x").where(col("b") == "y")
    assert q.sql == "SELECT id FROM t WHERE a = :_1 AND b = :_2"
    assert q.params == {"_1": "x", "_2": "y"}


def test_binding_renumbers_through_a_subquery() -> None:
    inner = Select("id", from_="rooms", where=col("kind") == "open")
    q = Select("id", from_="t", where=(col("room").in_(inner)) & (col("a") == "x"))
    assert q.sql == (
        "SELECT id FROM t WHERE "
        "room IN (SELECT id FROM rooms WHERE kind = :_1) AND a = :_2"
    )
    assert q.params == {"_1": "open", "_2": "x"}


def test_binding_renumbers_through_a_union() -> None:
    a = Select("id", from_="t", where=col("room") == "a")
    b = Select("id", from_="u", where=col("room") == "b")
    q = a.union_all(b)
    assert q.sql == (
        "SELECT id FROM t WHERE room = :_1 UNION ALL SELECT id FROM u WHERE room = :_2"
    )
    assert q.params == {"_1": "a", "_2": "b"}


def test_and_or_not_render_with_precedence() -> None:
    cond = (col("a") == 1) | (col("b") == 2)
    q = Select("id", from_="t", where=cond & (col("c") == 3))
    assert q.sql == "SELECT id FROM t WHERE (a = :_1 OR b = :_2) AND c = :_3"
    neg = Select("id", from_="t", where=~(col("a") == 1) & (col("b") == 2))
    assert neg.sql == "SELECT id FROM t WHERE NOT a = :_1 AND b = :_2"
    both = Select("id", from_="t", where=~((col("a") == 1) & (col("b") == 2)))
    assert both.sql == "SELECT id FROM t WHERE NOT (a = :_1 AND b = :_2)"


def test_and_or_flatten() -> None:
    cond = (col("a") == 1) & (col("b") == 2) & (col("c") == 3)
    assert Select("id", from_="t", where=cond).sql == (
        "SELECT id FROM t WHERE a = :_1 AND b = :_2 AND c = :_3"
    )


def test_where_takes_a_list_as_and() -> None:
    q = Select(
        "id",
        from_="t",
        where=[col("room") == param("room"), col("at") > 0],
        order_by=[col("at").desc(), "id"],
        group_by=["room", "id"],
    )
    assert q.sql == (
        "SELECT id FROM t WHERE room = :room AND at > :_1"
        " GROUP BY room, id ORDER BY at DESC, id"
    )


def test_literals() -> None:
    q = Select(lit("it's"), 1, -2, 0.5, True, None)
    assert q.sql == "SELECT 'it''s', 1, -2, 0.5, TRUE, NULL"


def test_keywords_are_quoted() -> None:
    q = Select("t.order", "name", from_="t", order_by="order")
    assert q.sql == 'SELECT t."order", name FROM t ORDER BY "order"'


def test_forms() -> None:
    rooms = Select(1, from_="rooms", where=col("rooms.id") == col("t.room"))
    q = Select(
        fn("count"),
        fn("count", col("author")),
        col("n").cast("integer"),
        case((col("status") == lit("open"), 1), else_=0),
        col("name").collate("nocase"),
        from_="t",
        where=[
            col("status").in_(["open", "done"]),
            col("at").between(param("lo"), param("hi")),
            col("deleted_at").is_not(None),
            rooms.exists(),
        ],
        group_by="status",
        having=fn("count") > 1,
        order_by=[col("at").desc("last"), raw("random()")],
        offset=10,
    )
    assert q.sql == (
        "SELECT count(), count(author), CAST(n AS INTEGER),"
        " CASE WHEN status = 'open' THEN :_1 ELSE :_2 END, name COLLATE NOCASE"
        " FROM t"
        " WHERE status IN (:_3, :_4) AND at BETWEEN :lo AND :hi"
        " AND deleted_at IS NOT NULL"
        " AND EXISTS (SELECT 1 FROM rooms WHERE rooms.id = t.room)"
        " GROUP BY status HAVING count() > :_5"
        " ORDER BY at DESC NULLS LAST, random() LIMIT -1 OFFSET :_6"
    )
    assert q.params == {
        "_1": 1,
        "_2": 0,
        "_3": "open",
        "_4": "done",
        "_5": 1,
        "_6": 10,
    }


def test_in_with_a_select_and_not_in() -> None:
    inner = Select("id", from_="rooms")
    q = Select("id", from_="t", where=col("room").not_in(inner))
    assert q.sql == "SELECT id FROM t WHERE room NOT IN (SELECT id FROM rooms)"
    empty = Select("id", from_="t", where=col("room").in_([]))
    assert empty.sql == "SELECT id FROM t WHERE room IN ()"


def test_not_exists() -> None:
    inner = Select(1, from_="rooms", where=col("rooms.id") == col("t.room"))
    q = Select("id", from_="t", where=~inner.exists())
    assert q.sql == (
        "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM rooms"
        " WHERE rooms.id = t.room)"
    )


def test_writes() -> None:
    added = Insert(
        "messages",
        values={"room": param("room"), "body": "hi"},
        on_conflict=["id"],
        do_update={"body": raw("excluded.body")},
        returning="id",
    )
    assert added.sql == (
        "INSERT INTO messages (room, body) VALUES (:room, :_1)"
        " ON CONFLICT (id) DO UPDATE SET body = excluded.body RETURNING id"
    )
    assert added.params == {"_1": "hi"}

    chained = (
        Insert("messages")
        .values({"room": param("room"), "body": "hi"})
        .on_conflict("id")
        .do_update(body=raw("excluded.body"))
        .returning("id")
    )
    assert chained.sql == added.sql

    changed = Update("t", set={"body": "b"}, where=col("id") == param("id"))
    assert changed.sql == "UPDATE t SET body = :_1 WHERE id = :id"
    assert changed.params == {"_1": "b"}

    removed = Delete("t", where=col("at") < param("before"), returning="id")
    assert removed.sql == "DELETE FROM t WHERE at < :before RETURNING id"

    distinct = Select("id", distinct=True, from_="t")
    assert distinct.sql == "SELECT DISTINCT id FROM t"


def test_many_values_rows() -> None:
    q = Insert("t", values=[{"body": "a"}, {"body": "b"}])
    assert q.sql == "INSERT INTO t (body) VALUES (:_1), (:_2)"
    assert q.params == {"_1": "a", "_2": "b"}


def test_insert_from_select() -> None:
    q = Insert(
        "archive",
        columns=["id"],
        select=Select("id", from_="t"),
        on_conflict=[],
        do_nothing=True,
    )
    assert q.sql == (
        "INSERT INTO archive (id) SELECT id FROM t WHERE TRUE ON CONFLICT DO NOTHING"
    )


def test_with_and_compounds() -> None:
    recent = Select("id", from_="messages", where=col("at") > param("since"))
    q = (
        Select("id", from_="recent")
        .union_all(Select("value", from_=raw("json_each(:ids)")))
        .with_("recent", recent)
        .limit(5)
    )
    assert q.sql == (
        "WITH recent AS (SELECT id FROM messages WHERE at > :since)"
        " SELECT id FROM recent UNION ALL SELECT value FROM json_each(:ids)"
        " LIMIT :_1"
    )
    done = (
        Select("id", from_="a")
        .union(Select("id", from_="b"))
        .intersect(Select("id", from_="c"))
    )
    assert done.sql == (
        "SELECT id FROM a UNION SELECT id FROM b INTERSECT SELECT id FROM c"
    )


def test_compound_tail_goes_on_the_compound() -> None:
    q = Select("id", from_="a").union(Select("id", from_="b")).order_by("id").limit(2)
    assert q.sql == "SELECT id FROM a UNION SELECT id FROM b ORDER BY id LIMIT :_1"
    with pytest.raises(QueryError, match="member"):
        _ = Select("id", from_="a", limit=1).union(Select("id", from_="b")).sql


def test_a_select_nests() -> None:
    inner = Select("id", "body", from_="t", where=col("id") > 0)
    mid = Select("id", from_=inner.as_("i"), limit=5)
    outer = Select(
        "m.id",
        Select(fn("count", raw("*")), from_="t").as_("n"),
        from_=mid.as_("m"),
        where=col("m.id").in_(Select("id", from_="t")),
    ).join("left", Select("id", from_="t").as_("s"), col("m.id") == col("s.id"))
    assert outer.sql == (
        "SELECT m.id, (SELECT count(*) FROM t) AS n"
        " FROM (SELECT id FROM (SELECT id, body FROM t WHERE id > :_1) AS i"
        " LIMIT :_2) AS m"
        " LEFT JOIN (SELECT id FROM t) AS s ON m.id = s.id"
        " WHERE m.id IN (SELECT id FROM t)"
    )


def test_arbitrary_function_and_concat() -> None:
    q = Select(
        fn("json_extract", col("body"), lit("$.n")),
        fn("my_fn", 1, 2.5),
        fn("count", raw("*")),
        from_="t",
    )
    assert q.sql == (
        "SELECT json_extract(body, '$.n'), my_fn(:_1, :_2), count(*) FROM t"
    )
    assert q.params == {"_1": 1, "_2": 2.5}
    assert fn("count").data[1] == "count"
    glued = Select(col("a").concat(col("b")), from_="t")
    assert glued.sql == "SELECT a || b FROM t"
    deep = Select(col("doc").op("->>", "key"), from_="t")
    assert deep.sql == "SELECT doc ->> :_1 FROM t"
    assert deep.params == {"_1": "key"}


def test_none_is_null_in_value_rows() -> None:
    q = Insert("t", values={"a": None, "b": "x"})
    assert q.sql == "INSERT INTO t (a, b) VALUES (NULL, :_1)"


def test_a_comparison_with_none_raises() -> None:
    with pytest.raises(QueryError, match="None"):
        col("deleted_at").__eq__(None)
    with pytest.raises(QueryError, match="None"):
        col("n").__lt__(None)
    assert Select(col("deleted_at").is_(None)).sql == "SELECT deleted_at IS NULL"
    assert Select(None).sql == "SELECT NULL"


def test_an_expression_has_no_truth_value() -> None:
    with pytest.raises(TypeError, match="no truth value"):
        bool(col("id") == 1)


def test_magic_strings_in_name_positions_raise() -> None:
    with pytest.raises(QueryError, match="lit"):
        _ = Select("'open").sql
    with pytest.raises(QueryError, match="param"):
        _ = Select("id", from_=":t").sql
    with pytest.raises(QueryError, match="name"):
        _ = Select("it's").sql


def test_param_rejects_a_underscore_name() -> None:
    with pytest.raises(QueryError, match="reserved"):
        param("_1")


def test_rejects() -> None:
    with pytest.raises(QueryError, match="finite"):
        _ = Select(float("nan")).sql
    with pytest.raises(QueryError, match="join needs from"):
        _ = Select("id", join=("inner", "u", col("a") == col("b"))).sql
    with pytest.raises(QueryError, match="needs"):
        _ = Insert("t").sql
    with pytest.raises(QueryError, match="do_nothing or do_update"):
        _ = Insert("t", values={"a": 1}, do_nothing=True).sql
    with pytest.raises(QueryError, match="same keys"):
        _ = Insert("t", values=[{"a": 1}, {"b": 2}]).sql


def test_query_runs(db: LiteWriter) -> None:
    db.submit(lambda tx: tx.execute("INSERT INTO t(body) VALUES ('hi')")).result()
    found = db.query(
        Select("body", from_="t", where=col("body") == param("body")),
        body="hi",
    )
    assert found == [("hi",)]
    db.submit(lambda tx: tx.execute("INSERT INTO t(body) VALUES ('a'), ('b')")).result()
    inner = Select("id", "body", from_="t", where=col("body") != lit("hi"))
    found = db.query(Select("s.body", from_=inner.as_("s"), order_by="s.id"))
    assert found == [("a",), ("b",)]


def test_value_needs_a_row(db: LiteWriter) -> None:
    with pytest.raises(QueryError, match="no row"):
        db.value(Select("body", from_="t", where=col("id") == 1))


def test_missing_parameter(db: LiteWriter) -> None:
    with pytest.raises(QueryError, match="parameters do not match"):
        db.query(Select("id", from_="t", where=col("id") == param("id")))


def test_underscore_names_are_allowed(db: LiteWriter) -> None:
    assert param("_x").data == ("par", "_x")
    db.submit(lambda tx: tx.execute("INSERT INTO t(body) VALUES ('hi')")).result()
    assert db.query("SELECT body FROM t WHERE body = :_x", _x="hi") == [("hi",)]
    assert db.query("SELECT :_1 AS x", _1="x") == [("x",)]
    with pytest.raises(QueryError, match="reserved"):
        db.query(Select("id", from_="t"), _1="x")


def test_raw_rejects_an_auto_bind() -> None:
    with pytest.raises(QueryError, match="param"):
        raw("b = :_1")
    with pytest.raises(QueryError, match="param"):
        raw("x = @_2")
    with pytest.raises(QueryError, match="param"):
        raw("x = $_3")
    assert raw("x = :room").data == ("raw", "x = :room")


def test_fn_name_must_be_a_name() -> None:
    with pytest.raises(QueryError, match="name"):
        fn("x() FROM t; DROP TABLE t; --")
    with pytest.raises(QueryError, match="name"):
        fn(cast("str", 1))
    assert fn("lower", col("x")).data[1] == "lower"


def test_fn_renders_any_name() -> None:
    q = Select(fn("substr", col("name"), 1, 3), from_="t")
    assert q.sql == "SELECT substr(name, :_1, :_2) FROM t"
    assert q.params == {"_1": 1, "_2": 3}
    assert fn("count").data == ("fnc", "count", ())
    assert fn("my_registered_fn", 1).data == ("fnc", "my_registered_fn", (1,))


def test_builtin_name_covers_sqlite() -> None:
    names = typing.get_args(BuiltinName.__value__)
    assert len(names) == 136
    assert {"lower", "json_extract", "row_number"} <= set(names)
    assert all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", n) for n in names)
    conn = apsw.Connection(":memory:")
    try:
        real = {row[0] for row in conn.execute("PRAGMA function_list")}
    finally:
        conn.close()
    assert set(names) <= real


def test_every_python_value_binds() -> None:
    q = Update("t", set={"x": 5}, where=col("id") == 7)
    assert q.sql == "UPDATE t SET x = :_1 WHERE id = :_2"
    assert q.params == {"_1": 5, "_2": 7}
    # Numbers are bound, so the text is stable for the statement cache.
    other = Update("t", set={"x": 9}, where=col("id") == 3)
    assert other.sql == q.sql


def test_limit_offset_and_in_run(db: LiteWriter) -> None:
    db.submit(
        lambda tx: tx.execute("INSERT INTO t(body) VALUES ('a'), ('b'), ('c'), ('d')")
    ).result()
    paged = Select("id", from_="t", order_by="id").limit(10).offset(2)
    assert [row[0] for row in db.query(paged)] == [3, 4]
    listed = Select("id", from_="t", where=col("id").in_([1, 2, 3]), order_by="id")
    assert [row[0] for row in db.query(listed)] == [1, 2, 3]


def test_insert_from_select_runs(db: LiteWriter) -> None:
    db.submit(lambda tx: tx.execute("INSERT INTO t(body) VALUES ('a'), ('b')")).result()
    again = Insert(
        "t",
        columns=["id", "body"],
        select=Select("id", "body", from_="t"),
        on_conflict=["id"],
        do_nothing=True,
    )
    assert "WHERE TRUE" in again.sql
    db.submit(lambda tx: tx.execute(again)).result()
    assert db.value("SELECT count(*) FROM t") == 2


def test_a_bool_binds_and_round_trips(db: LiteWriter) -> None:
    db.submit(
        lambda tx: tx.execute(
            "CREATE TABLE flags(id INTEGER PRIMARY KEY, flag INTEGER)"
        )
    ).result()
    added = Insert("flags", values={"flag": True})
    assert added.params == {"_1": True}
    db.submit(lambda tx: tx.execute(added)).result()
    assert db.value("SELECT flag FROM flags") == 1
    found = Select("id", from_="flags", where=col("flag") == True)  # noqa: E712
    assert [row[0] for row in db.query(found)] == [1]


def test_a_built_insert_runs_many_times(db: LiteWriter) -> None:
    add = Insert("t", columns=["body"], returning="id")

    def write(tx: Tx) -> int:
        tx.executemany(add, [{"body": "a"}, {"body": "b"}])
        return tx.value(add, body="c")

    assert db.submit(write).result() == 3


def test_a_built_update_runs(db: LiteWriter) -> None:
    db.submit(lambda tx: tx.execute("INSERT INTO t(body) VALUES ('a'), ('b')")).result()
    move = Update("t", set=["body"], where=col("id") == param("id"))
    db.submit(lambda tx: tx.execute(move, body="x", id=2)).result()
    assert db.value("SELECT body FROM t WHERE id = 2") == "x"


def test_a_built_upsert_runs(db: LiteWriter) -> None:
    db.submit(
        lambda tx: tx.execute("CREATE TABLE u(email TEXT PRIMARY KEY, n INT)")
    ).result()
    add = Insert("u", columns=["email", "n"], on_conflict="email", do_update=["n"])

    def write(tx: Tx) -> None:
        tx.execute(add, email="a", n=1)
        tx.execute(add, email="a", n=2)

    db.submit(write).result()
    assert db.value("SELECT n FROM u WHERE email = 'a'") == 2
