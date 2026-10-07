from collections.abc import Mapping

import pytest

from litewriter import (
    Delete,
    Insert,
    LiteWriter,
    Replace,
    Select,
    UnionAll,
    Update,
    WriterError,
    col,
    exists,
    lit,
    param,
    sql,
)


def test_page_matches_hand_written_sql() -> None:
    page = Select(
        "m.id",
        "m.body",
        ("as", ("lower", "u.name"), "author"),
        from_=("as", "messages", "m"),
        join=("left", ("as", "users", "u"), ("=", "m.author", "u.id")),
        where=("=", "m.room", ":room"),
        order_by=("desc", "m.at"),
        limit=50,
    )
    chained = (
        Select("m.id", "m.body", ("as", ("lower", "u.name"), "author"))
        .from_(("as", "messages", "m"))
        .left_join(("as", "users", "u"), ("=", "m.author", "u.id"))
        .where(("=", "m.room", ":room"))
        .order_by(("desc", "m.at"))
        .limit(50)
    )
    assert (
        sql(page)
        == sql(chained)
        == (
            "SELECT m.id, m.body, lower(u.name) AS author"
            " FROM messages AS m"
            " LEFT JOIN users AS u ON m.author = u.id"
            " WHERE m.room = :room"
            " ORDER BY m.at DESC LIMIT 50"
        )
    )


def test_later_edits_do_not_change_the_query() -> None:
    call = ["lower", "u.name"]
    row: dict[str, object] = {"body": ":body"}
    page = Select(call, from_="t")
    added = Insert("t", values=row)
    call.append("x")
    row["room"] = 1
    assert sql(page) == "SELECT lower(u.name) FROM t"
    assert sql(added) == "INSERT INTO t (body) VALUES (:body)"


def test_a_list_of_conditions_is_and() -> None:
    page = Select(
        "id",
        from_="t",
        where=[("=", "room", ":room"), (">", "at", 0)],
        order_by=[("desc", "at"), "id"],
        group_by=["room", "id"],
    )
    assert sql(page) == (
        "SELECT id FROM t WHERE room = :room AND at > 0"
        " GROUP BY room, id ORDER BY at DESC, id"
    )


def test_where_returns_a_new_query() -> None:
    page = Select("id", from_="t", limit=50)
    mine = page.where(("=", "author", ":me")).limit(20)
    assert sql(page) == "SELECT id FROM t LIMIT 50"
    assert sql(mine) == "SELECT id FROM t WHERE author = :me LIMIT 20"


def test_a_call_means_the_same_in_every_clause() -> None:
    q = Select(("count", "author")).from_("t").where(("lower", "name"))
    assert sql(q) == "SELECT count(author) FROM t WHERE lower(name)"


def test_an_alias_means_the_same_in_every_clause() -> None:
    q = Select(("as", ("count", "author"), "n")).from_(("as", "t", "x"))
    assert sql(q) == "SELECT count(author) AS n FROM t AS x"
    deleted = Delete("t").returning(("as", "id", "gone"))
    assert sql(deleted) == "DELETE FROM t RETURNING id AS gone"


def test_a_literal_form_quotes_text() -> None:
    q = Select(("lit", "it's")).from_("t")
    assert sql(q) == "SELECT 'it''s' FROM t"


def test_a_list_is_the_same_call() -> None:
    tupled = Select(("lower", "u.name")).from_("t")
    listed = Select(["lower", "u.name"]).from_("t")
    assert sql(tupled) == sql(listed) == "SELECT lower(u.name) FROM t"


def test_forms() -> None:
    rooms = Select(1).from_("rooms").where(("=", "rooms.id", "t.room"))
    q = (
        Select(
            ("count", "*"),
            ("count", ("distinct", "author")),
            ("cast", "n", "integer"),
            ("case", ("=", "status", "'open'"), 1, 0),
            ("collate", "name", "nocase"),
        )
        .from_("t")
        .where(
            ("in", "status", "'open'", "'done'"),
            ("between", "at", ":lo", ":hi"),
            ("is not", "deleted_at", None),
            ("exists", rooms),
        )
        .group_by("status")
        .having((">", ("count", "*"), 1))
        .order_by(("desc", "at", "nulls last"), ("raw", "random()"))
        .offset(10)
    )
    assert sql(q) == (
        "SELECT count(*), count(DISTINCT author), CAST(n AS INTEGER),"
        " CASE WHEN status = 'open' THEN 1 ELSE 0 END, name COLLATE NOCASE"
        " FROM t"
        " WHERE status IN ('open', 'done') AND at BETWEEN :lo AND :hi"
        " AND deleted_at IS NOT NULL"
        " AND EXISTS (SELECT 1 FROM rooms WHERE rooms.id = t.room)"
        " GROUP BY status HAVING count(*) > 1"
        " ORDER BY at DESC NULLS LAST, random() LIMIT -1 OFFSET 10"
    )


def test_and_or_not() -> None:
    cond = (
        "or",
        ("and", ("=", "a", 1), ("=", "b", 2)),
        ("not", (">", "c", ("+", "d", 1))),
    )
    assert sql(Select("*").from_("t").where(cond)) == (
        "SELECT * FROM t WHERE a = 1 AND b = 2 OR NOT c > d + 1"
    )


def test_arbitrary_function() -> None:
    q = Select(("json_extract", "body", "'$.n'"), ("my_fn", 1, 2.5)).from_("t")
    assert sql(q) == "SELECT json_extract(body, '$.n'), my_fn(1, 2.5) FROM t"


def test_writes() -> None:
    added = Insert(
        "messages",
        values={"room": ":room", "body": ":body"},
        on_conflict=["id"],
        do_update={"body": "excluded.body"},
        returning="id",
    )
    chained = (
        Insert("messages")
        .values({"room": ":room", "body": ":body"})
        .on_conflict("id")
        .do_update(body="excluded.body")
        .returning("id")
    )
    assert (
        sql(added)
        == sql(chained)
        == (
            "INSERT INTO messages (room, body) VALUES (:room, :body)"
            " ON CONFLICT (id) DO UPDATE SET body = excluded.body RETURNING id"
        )
    )
    changed = Update("t", set={"body": ":body"}, where=("=", "id", ":id"))
    assert sql(changed) == "UPDATE t SET body = :body WHERE id = :id"
    removed = Delete("t", where=("<", "at", ":before"), returning="id")
    assert sql(removed) == "DELETE FROM t WHERE at < :before RETURNING id"
    replaced = Replace("t", values={"id": 1, "body": ":body"})
    assert sql(replaced) == "REPLACE INTO t (id, body) VALUES (1, :body)"
    distinct = Select("id", distinct=True, from_="t")
    assert sql(distinct) == "SELECT DISTINCT id FROM t"


def test_union_and_table_function() -> None:
    recent = Select("id", from_="messages", where=(">", "at", ":since"))
    q = UnionAll(
        Select("id", from_="recent"),
        Select("value", from_=("json_each", ":ids")),
        with_={"recent": recent},
        limit=5,
    )
    chained = (
        UnionAll(
            Select("id", from_="recent"),
            Select("value", from_=("json_each", ":ids")),
        )
        .with_("recent", recent)
        .limit(5)
    )
    assert (
        sql(q)
        == sql(chained)
        == (
            "WITH recent AS (SELECT id FROM messages WHERE at > :since)"
            " SELECT id FROM recent UNION ALL SELECT value FROM json_each(:ids) LIMIT 5"
        )
    )


def test_a_select_nests() -> None:
    inner = Select("id", "body", from_="t", where=(">", "id", 0))
    mid = Select("id", from_=("as", inner, "i"), limit=5)
    outer = Select(
        "m.id",
        ("as", Select(("count", "*"), from_="t"), "n"),
        from_=("as", mid, "m"),
        where=("in", "m.id", Select("id", from_="t")),
    ).left_join(("as", Select("id", from_="t"), "s"), ("=", "m.id", "s.id"))
    assert sql(outer) == (
        "SELECT m.id, (SELECT count(*) FROM t) AS n"
        " FROM (SELECT id FROM (SELECT id, body FROM t WHERE id > 0) AS i LIMIT 5) AS m"
        " LEFT JOIN (SELECT id FROM t) AS s ON m.id = s.id"
        " WHERE m.id IN (SELECT id FROM t)"
    )


def test_a_limited_arm_is_a_nested_select() -> None:
    page = UnionAll(
        Select("body", from_="t"),
        Select("body", from_=Select("body", from_="t", where=("=", "id", 1), limit=1)),
    )
    assert sql(page) == (
        "SELECT body FROM t"
        " UNION ALL SELECT body FROM (SELECT body FROM t WHERE id = 1 LIMIT 1)"
    )


def test_expressions_match_the_tuples() -> None:
    inner = Select("id", "body", from_="t", where=col("id") > 0)
    page = Select(
        col("m.id"),
        col("body").as_("text"),
        (col("n") + 1) * 2,
        from_=inner.as_("m"),
        where=[
            col("m.room") == param("room"),
            col("body").like(param("q")),
            col("id").in_(Select("id", from_="rooms")),
            col("deleted_at").is_(None),
        ],
        order_by=col("m.at").desc("nulls last"),
    )
    assert sql(page) == (
        "SELECT m.id, body AS text, (n + 1) * 2"
        " FROM (SELECT id, body FROM t WHERE id > 0) AS m"
        " WHERE m.room = :room AND body LIKE :q"
        " AND id IN (SELECT id FROM rooms)"
        " AND deleted_at IS NULL"
        " ORDER BY m.at DESC NULLS LAST"
    )


def test_literals_and_reflected_operators() -> None:
    assert sql(Select(lit("it's"))) == "SELECT 'it''s'"
    assert sql(Select(1 + col("n"))) == "SELECT 1 + n"
    assert sql(Select(2 - col("n"))) == "SELECT 2 - n"
    assert sql(Select(col("flags") & 1)) == "SELECT flags & 1"
    assert (
        sql(Select(exists(Select(1, from_="t")))) == "SELECT EXISTS (SELECT 1 FROM t)"
    )


def test_an_expression_has_no_truth_value() -> None:
    with pytest.raises(TypeError):
        bool(col("id") == 1)


def _defines(cls: type, name: str) -> bool:
    for base in cls.__mro__:
        if name in base.__dict__:
            return base not in {Mapping, object}
    return False


def test_each_statement_has_its_own_methods() -> None:
    assert _defines(Select, "from_")
    assert _defines(Select, "group_by")
    assert not _defines(Select, "values")
    assert _defines(Insert, "values")
    assert _defines(Replace, "do_nothing")
    assert not _defines(Insert, "from_")
    assert _defines(Update, "set")
    assert _defines(Update, "from_")
    assert not _defines(Update, "group_by")
    assert _defines(Delete, "where")
    assert not _defines(Delete, "from_")
    assert _defines(UnionAll, "limit")
    assert not _defines(UnionAll, "where")
    assert _defines(Delete, "as_")


def test_query_runs(db: LiteWriter) -> None:
    db.execute_script("INSERT INTO t (body) VALUES ('hi')")
    found = db.query(
        Select("body").from_("t").where(("=", "body", ":body")),
        body="hi",
    )
    assert found == [("hi",)]
    db.execute_script("INSERT INTO t (body) VALUES ('a'), ('b')")
    inner = Select("id", "body", from_="t", where=("!=", "body", "'hi'"))
    found = db.query(Select("s.body", from_=("as", inner, "s"), order_by="s.id"))
    assert found == [("a",), ("b",)]


def test_compound_needs_two_queries() -> None:
    with pytest.raises(WriterError, match="two"):
        UnionAll(Select("id"))
