from collections.abc import Sequence
from typing import cast

import pytest

from litewriter import (
    Delete,
    Insert,
    LiteWriter,
    QueryError,
    Select,
    Update,
    col,
    lit,
    param,
)
from litewriter.build import Compound, JoinSpec, _Join
from litewriter.expr import Expr


def test_constructor_and_chained_methods_agree() -> None:
    page = Select(
        "m.id",
        "m.body",
        from_="messages",
        join=("left", "users", col("m.author") == col("u.id")),
        where=col("m.room") == param("room"),
        order_by=col("m.at").desc(),
        limit=50,
    )
    chained = (
        Select("m.id", "m.body")
        .from_("messages")
        .join("left", "users", col("m.author") == col("u.id"))
        .where(col("m.room") == param("room"))
        .order_by(col("m.at").desc())
        .limit(50)
    )
    assert page.sql == chained.sql


def test_methods_return_a_new_statement() -> None:
    page = Select("id", from_="t", limit=50)
    mine = page.where(col("author") == param("me")).limit(20)
    assert page.sql == "SELECT id FROM t LIMIT :_1"
    assert page.params == {"_1": 50}
    assert mine.sql == "SELECT id FROM t WHERE author = :me LIMIT :_1"
    assert mine.params == {"_1": 20}


def test_inputs_are_snapshotted() -> None:
    row: dict[str, str | int] = {"body": "b"}
    added = Insert("t", values=row)
    row["room"] = 1
    assert added.sql == "INSERT INTO t (body) VALUES (:_1)"
    items = ["id"]
    q = Select(*items, from_="t")
    items.append("evil")
    assert q.sql == "SELECT id FROM t"


def test_joins() -> None:
    q = (
        Select("a.id", from_="a")
        .join("left", "b", col("a.id") == col("b.id"))
        .join("cross", "c")
    )
    assert q.sql == ("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id CROSS JOIN c")
    both = Select(
        "a.id",
        from_="a",
        join=[
            ("inner", "b", col("a.id") == col("b.a")),
            ("cross", "c"),
        ],
    )
    assert both.sql == ("SELECT a.id FROM a JOIN b ON a.id = b.a CROSS JOIN c")


def test_join_kind_is_checked() -> None:
    bad = cast(_Join, "sideways")
    with pytest.raises(QueryError, match="join kind"):
        Select("id", from_="t").join(bad, "u", col("a") == col("b"))
    with pytest.raises(QueryError, match="join"):
        _ = Select("id", from_="t", join=cast("JoinSpec", ("inner", "u"))).sql


def test_distinct_and_group_by() -> None:
    q = Select("room", distinct=True, from_="t", group_by="room")
    assert q.sql == "SELECT DISTINCT room FROM t GROUP BY room"


def test_update_and_delete() -> None:
    changed = (
        Update("t", set={"body": "b"})
        .set(done=1)
        .where(col("id") == param("id"))
        .returning("id")
    )
    assert changed.sql == "UPDATE t SET done = :_1 WHERE id = :id RETURNING id"
    assert changed.params == {"_1": 1}
    removed = Delete("t", where=col("at") < param("before"), returning="id")
    assert removed.sql == "DELETE FROM t WHERE at < :before RETURNING id"
    chained = Delete("t").where(col("id") == 1).returning("id", "body")
    assert chained.sql == "DELETE FROM t WHERE id = :_1 RETURNING id, body"


def test_update_can_join() -> None:
    q = Update(
        "t",
        set={"body": "x"},
        from_="u",
        join=("inner", "v", col("v.id") == col("t.v")),
    )
    assert q.sql == "UPDATE t SET body = :_1 FROM u JOIN v ON v.id = t.v"


def test_insert_columns() -> None:
    q = Insert("t", columns=["a", "b"], values={"a": 1, "b": "x"})
    assert q.sql == "INSERT INTO t (a, b) VALUES (:_1, :_2)"
    assert q.params == {"_1": 1, "_2": "x"}
    with pytest.raises(QueryError, match="differ"):
        _ = Insert("t", columns=["a"], values={"b": 1}).sql


def test_select_can_be_replaced() -> None:
    q = Select("a", distinct=True, from_="t").select("b", "c")
    assert q.sql == "SELECT b, c FROM t"


def test_with_recursive() -> None:
    walk = Select("id", from_="t")
    q = Select("id", from_="walk", with_={"walk": walk}, recursive=True)
    assert q.sql == "WITH RECURSIVE walk AS (SELECT id FROM t) SELECT id FROM walk"
    other = Select("id", from_="w").with_recursive("w", walk)
    assert other.sql.startswith("WITH RECURSIVE")
    both = Select("id", from_="t")
    with pytest.raises(QueryError, match="not both"):
        both.with_("a", walk).with_recursive("b", walk)


def test_statement_repr_is_the_sql() -> None:
    q = Select("id", from_="t")
    assert "SELECT id FROM t" in repr(q)


def test_compound_is_private_but_chainable() -> None:
    q = Select("id", from_="a").union(Select("id", from_="b"))
    assert isinstance(q, Compound)
    assert not hasattr(__import__("litewriter"), "Compound")
    more = q.union_all(Select("id", from_="c")).except_(Select("id", from_="d"))
    assert more.sql == (
        "SELECT id FROM a UNION SELECT id FROM b"
        " UNION ALL SELECT id FROM c EXCEPT SELECT id FROM d"
    )


def test_insert_needs_values_or_select() -> None:
    with pytest.raises(QueryError, match="values or select"):
        Insert("t", values={"a": 1}, select=Select("a", from_="u"))
    with pytest.raises(QueryError, match="needs"):
        _ = Insert("t").sql


def test_values_must_be_mappings() -> None:
    with pytest.raises(QueryError, match="dict"):
        Insert("t", values=cast("list", [[1, 2]]))
    with pytest.raises(QueryError, match="non-empty"):
        Insert("t", values=[])


def test_query_runs(db: LiteWriter) -> None:
    db.submit(
        lambda tx: tx.execute("INSERT INTO t (body) VALUES ('hi'), ('a'), ('b')")
    ).result()
    inner = Select("id", "body", from_="t", where=col("body") != lit("hi"))
    found = db.query(Select("s.body", from_=inner.as_("s"), order_by="s.id"))
    assert found == [("a",), ("b",)]


def test_an_insert_from_names() -> None:
    add = Insert("users", columns=["name", "age"], returning="id")
    assert add.sql == (
        "INSERT INTO users (name, age) VALUES (:name, :age) RETURNING id"
    )
    assert add.params == {}
    chained = Insert("users").columns("name", "age").returning("id")
    assert chained.sql == add.sql


def test_an_update_from_names() -> None:
    move = Update("t", set=["name", "age"], where=col("id") == param("id"))
    assert move.sql == "UPDATE t SET name = :name, age = :age WHERE id = :id"
    assert move.params == {}
    one = Update("t", set="name")
    assert one.sql == "UPDATE t SET name = :name"
    mixed = Update("t", set={"a": 1}).set("name", age=5)
    assert mixed.sql == "UPDATE t SET name = :name, age = :_1"
    assert mixed.params == {"_1": 5}


def test_an_upsert_from_names() -> None:
    add = Insert("u", columns=["email", "n"], on_conflict="email", do_update=["n"])
    assert add.sql == (
        "INSERT INTO u (email, n) VALUES (:email, :n)"
        " ON CONFLICT (email) DO UPDATE SET n = excluded.n"
    )
    assert add.params == {}
    chained = Insert("u").columns("email", "n").on_conflict("email").do_update("n")
    assert chained.sql == add.sql


def test_a_compiled_statement_is_shared() -> None:
    q = Update("t", set=["body"], where=col("id") == param("id"))
    assert q.sql is q.sql
    assert q._compiled() is q._compiled()
    leaked = q.params
    leaked["bogus"] = 1
    assert q._compiled()[1] == {}


def test_name_shorthands_are_checked() -> None:
    with pytest.raises(QueryError, match="name"):
        Insert("t", columns=["_1"])
    with pytest.raises(QueryError, match="name"):
        Update("t", set=cast("Sequence[str]", [1]))
    with pytest.raises(QueryError, match="name"):
        Update("t", set={"a": 1}).set("_2")
    with pytest.raises(QueryError, match="name"):
        Insert("t", columns=["email"]).on_conflict("email").do_update("_3")


def test_a_none_condition_raises() -> None:
    with pytest.raises(QueryError, match="not None"):
        Select("id", from_="t", where=None)
    with pytest.raises(QueryError, match="not None"):
        Select("id", from_="t", having=None)
    with pytest.raises(QueryError, match="not None"):
        Select("id", from_="t", where=[col("id") == 1, cast("Expr", None)])
    with pytest.raises(QueryError, match="not None"):
        Select("id", from_="t").where(cast("Expr", None))
    with pytest.raises(QueryError, match="not None"):
        Select("id", from_="t").having(cast("Expr", None))
    with pytest.raises(QueryError, match="not None"):
        Update("t", set={"a": 1}, where=[cast("Expr", None)])
    with pytest.raises(QueryError, match="not None"):
        Delete("t").where(cast("Expr", None))
