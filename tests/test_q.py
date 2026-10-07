import apsw
import pytest

from litewriter import Insert, LiteWriter, Select, Tx, WriterError, sql, where

PAGE = {
    "select": ["m.id", "m.body", ["as", ["lower", "u.name"], "author"]],
    "from": ["as", "messages", "m"],
    "join": [["left", ["as", "users", "u"], ["=", "m.author", "u.id"]]],
    "where": ["=", "m.room", ":room"],
    "order_by": [["desc", "m.at"]],
    "limit": 50,
}


def test_reads_like_hand_written_sql() -> None:
    assert sql(PAGE) == (
        "SELECT m.id, m.body, lower(u.name) AS author"
        " FROM messages AS m"
        " LEFT JOIN users AS u ON m.author = u.id"
        " WHERE m.room = :room"
        " ORDER BY m.at DESC LIMIT 50"
    )


def test_a_pair_is_not_an_alias() -> None:
    assert sql({"select": [["count", "author"]], "from": "t"}) == (
        "SELECT count(author) FROM t"
    )


def test_any_function_is_just_a_name() -> None:
    q = {"select": [["json_extract", "body", "'$.n'"], ["my_fn", 1, 2.5]], "from": "t"}
    assert sql(q) == "SELECT json_extract(body, '$.n'), my_fn(1, 2.5) FROM t"


def test_key_order_does_not_matter() -> None:
    assert sql({"limit": 1, "from": "t", "select": ["*"]}) == "SELECT * FROM t LIMIT 1"


def test_literals() -> None:
    q = {"select": ["'it''s'", 1, -2, 0.5, True, None, b"\x01\xff"]}
    assert sql(q) == "SELECT 'it''s', 1, -2, 0.5, TRUE, NULL, X'01ff'"


def test_parentheses_only_where_needed() -> None:
    cond = ["and", ["or", ["=", "a", 1], ["=", "b", 2]], [">", "c", ["+", "d", 1]]]
    assert sql({"select": ["*"], "from": "t", "where": cond}) == (
        "SELECT * FROM t WHERE (a = 1 OR b = 2) AND c > d + 1"
    )
    minus = {"select": [["-", "a", ["-", "b", "c"]], ["*", ["+", "a", "b"], 2]]}
    assert sql(minus) == "SELECT a - (b - c), (a + b) * 2"


def test_keywords_are_quoted() -> None:
    q = {"select": ["t.order", "name"], "from": "t", "order_by": ["order"]}
    assert sql(q) == 'SELECT t."order", name FROM t ORDER BY "order"'


def test_where_adds_a_filter() -> None:
    base = {"select": ["id"], "from": "t"}
    one = where(base, ["=", "room", ":room"])
    two = where(one, [">", "at", 0], ["not", ["=", "kind", "'note'"]])
    assert "where" not in base
    assert one["where"] == ["=", "room", ":room"]
    assert sql(two) == (
        "SELECT id FROM t WHERE room = :room AND at > 0 AND NOT kind = 'note'"
    )


def test_forms() -> None:
    q = {
        "select": [
            ["count", "*"],
            ["count", ["distinct", "author"]],
            ["cast", "n", "integer"],
            ["case", ["=", "status", "'open'"], 1, 0],
            ["collate", "name", "nocase"],
        ],
        "from": "t",
        "where": [
            "and",
            ["in", "status", "'open'", "'done'"],
            ["between", "at", ":lo", ":hi"],
            ["is not", "deleted_at", None],
            [
                "exists",
                {"select": [1], "from": "rooms", "where": ["=", "rooms.id", "t.room"]},
            ],
        ],
        "group_by": ["status"],
        "having": [">", ["count", "*"], 1],
        "order_by": [["desc", "at", "nulls last"], ["raw", "random()"]],
        "offset": 10,
    }
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


def test_with_union_and_table_functions() -> None:
    q = {
        "with": {
            "recent": {
                "select": ["id"],
                "from": "messages",
                "where": [">", "at", ":since"],
            }
        },
        "union_all": [
            {"select": ["id"], "from": "recent"},
            {"select": ["value"], "from": ["json_each", ":ids"]},
        ],
        "limit": 5,
    }
    assert sql(q) == (
        "WITH recent AS (SELECT id FROM messages WHERE at > :since)"
        " SELECT id FROM recent UNION ALL SELECT value FROM json_each(:ids) LIMIT 5"
    )


def test_writes() -> None:
    assert sql(
        {
            "insert_into": "messages",
            "values": [{"room": ":room", "body": ":body"}],
            "on_conflict": ["id"],
            "do_update_set": {"body": "excluded.body"},
            "returning": ["id"],
        }
    ) == (
        "INSERT INTO messages (room, body) VALUES (:room, :body)"
        " ON CONFLICT (id) DO UPDATE SET body = excluded.body RETURNING id"
    )
    assert (
        sql(
            {
                "insert_into": "archive",
                "columns": ["id"],
                "select": ["id"],
                "from": "t",
                "on_conflict": [],
                "do_nothing": True,
            }
        )
        == "INSERT INTO archive (id) SELECT id FROM t WHERE TRUE ON CONFLICT DO NOTHING"
    )
    assert sql(
        {"update": "t", "set": {"body": ":body"}, "where": ["=", "id", ":id"]}
    ) == ("UPDATE t SET body = :body WHERE id = :id")
    assert sql(
        {"delete_from": "t", "where": ["<", "at", ":before"], "returning": ["id"]}
    ) == ("DELETE FROM t WHERE at < :before RETURNING id")


@pytest.mark.parametrize(
    ("query", "match"),
    [
        ({"select": [["lower(", "x"]]}, "function name"),
        ({"select": ["id"], "from": "t", "find": "?m"}, "unknown query key"),
        (
            {"select": ["id"], "join": [["inner", "u", ["=", "a", "b"]]]},
            "join needs from",
        ),
        ({"select": ["id"], "from": "t", "set": {"a": 1}}, "not part of a select"),
        ({"select": ["it's"]}, "not a name"),
        ({"select": ["'open"]}, "string literal"),
        ({"select": [":1x"]}, "parameter"),
        ({"select": ["id"], "from": "t", "order_by": ["desc", "at"]}, "own list"),
        ({"select": ["id"], "join": [["u", ["=", "a", "b"]]], "from": "t"}, "kind"),
        ({"select": [float("nan")]}, "finite"),
        ({"insert_into": "t"}, "values or select"),
        ({"from": "t"}, "needs select"),
    ],
)
def test_rejects(query: dict[str, object], match: str) -> None:
    with pytest.raises(WriterError, match=match):
        sql(query)


def _double(*values: apsw.SQLiteValue) -> apsw.SQLiteValue:
    number = values[0]
    assert isinstance(number, int)
    return number * 2


def test_registered_function_runs(db: LiteWriter) -> None:
    db.conn().create_scalar_function("double", _double)
    assert db.query({"select": [["double", ":n"]]}, n=21) == [(42,)]


def add(tx: Tx, body: str) -> int:
    found = tx.value(
        Insert("t", values={"body": ":body"}, returning="id"),
        body=body,
    )
    return int(found)


def test_write_and_read_with_the_same_builder(db: LiteWriter) -> None:
    row = db.submit(add, "Ada").result(timeout=1.0)
    q = {
        "select": ["id", ["lower", "body"]],
        "from": "t",
        "where": ["=", "body", ":body"],
    }
    assert db.query(q, body="Ada") == [(row, "ada")]
    assert db.query("SELECT body FROM t WHERE id = :id", id=row) == [("Ada",)]


def test_value_needs_a_row(db: LiteWriter) -> None:
    with pytest.raises(WriterError, match="no row"):
        db.value(Select("body", from_="t", where=("=", "id", 1)))


def test_missing_parameter(db: LiteWriter) -> None:
    with pytest.raises(WriterError, match="missing parameter"):
        db.query({"select": ["id"], "from": "t", "where": ["=", "id", ":id"]})


def test_tables_resolve_views(db: LiteWriter) -> None:
    db.execute_script(
        "CREATE TABLE u(id INTEGER PRIMARY KEY, name TEXT);"
        "CREATE VIEW named AS SELECT t.id, u.name FROM t JOIN u ON u.id = t.id"
    )
    assert db.tables(
        {"select": ["*"], "from": "named", "where": ["=", "id", ":id"]}
    ) >= {"t", "u"}
