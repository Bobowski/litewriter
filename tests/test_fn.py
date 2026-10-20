from collections.abc import Iterable, Mapping
from typing import cast

import pytest

from litewriter import Insert, LiteWriter, QueryError, Select, Tx, col


def insert_row(tx: Tx, body: str) -> int:
    tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)
    return tx.last_insert_rowid()


def update_row(tx: Tx, row_id: int, body: str) -> None:
    tx.execute("UPDATE t SET body = :body WHERE id = :id", body=body, id=row_id)


def read_then_write(tx: Tx, row_id: int, suffix: str) -> str:
    row = tx.one("SELECT body FROM t WHERE id = :id", id=row_id)
    assert row is not None
    body = f"{row[0]}{suffix}"
    tx.execute("UPDATE t SET body = :body WHERE id = :id", body=body, id=row_id)
    return body


def insert_many(tx: Tx, rows: list[str]) -> int:
    tx.executemany(
        "INSERT INTO t(body) VALUES (:body)",
        ({"body": row} for row in rows),
    )
    return tx.last_insert_rowid()


def test_insert_update_and_read_then_write(db: LiteWriter) -> None:
    row_id = db.submit(insert_row, "hi").result(timeout=1.0)
    db.submit(update_row, row_id, "hello").result(timeout=1.0)
    body = db.submit(read_then_write, row_id, "!").result(timeout=1.0)
    assert body == "hello!"
    assert db.query("SELECT body FROM t WHERE id = :id", id=row_id) == [("hello!",)]


def test_insert_many(db: LiteWriter) -> None:
    last = db.submit(insert_many, ["a", "b", "c"]).result(timeout=1.0)
    assert db.value("SELECT count(*) FROM t") == 3
    assert last == 3


def test_tx_sees_the_uncommitted_batch(db: LiteWriter) -> None:
    def add_and_count(tx: Tx) -> int:
        tx.execute("INSERT INTO t(body) VALUES ('x')")
        return int(tx.value("SELECT count(*) FROM t"))

    db.submit(insert_row, "a").result(timeout=1.0)
    assert db.submit(add_and_count).result(timeout=1.0) == 2


def test_tx_query_one_value(db: LiteWriter) -> None:
    def work(tx: Tx) -> tuple[object, object, object]:
        tx.execute("INSERT INTO t(body) VALUES ('a'), ('b')")
        rows = tx.query(Select("id", "body", from_="t", order_by="id"))
        first = tx.one(Select("body", from_="t", where=col("body") == "b"))
        val = tx.value("SELECT count(*) FROM t")
        return rows, first, val

    rows, first, val = db.submit(work).result(timeout=1.0)
    assert rows == [(1, "a"), (2, "b")]
    assert first == ("b",)
    assert val == 2


def test_tx_query_into(db: LiteWriter) -> None:
    def work(tx: Tx) -> list[str]:
        tx.execute("INSERT INTO t(body) VALUES ('a')")
        return tx.query(Select("body", from_="t"), lambda body: f"[{body}]")

    assert db.submit(work).result(timeout=1.0) == ["[a]"]


def test_tx_value_no_row(db: LiteWriter) -> None:

    def work(tx: Tx) -> None:
        tx.value(Select("id", from_="t", where=col("id") == 99))

    with pytest.raises(QueryError, match="no row"):
        db.submit(work).result(timeout=1.0)


def test_insert_statement_binds(db: LiteWriter) -> None:
    def work(tx: Tx, body: str) -> int:
        return int(tx.value(Insert("t", values={"body": body}, returning="id")))

    row_id = db.submit(work, "bound").result(timeout=1.0)
    assert db.query("SELECT body FROM t WHERE id = :id", id=row_id) == [("bound",)]


def test_tx_execute_returns_none_and_runs_every_statement(
    db: LiteWriter,
) -> None:
    def work(tx: Tx) -> None:
        out = tx.execute(
            "INSERT INTO t(body) VALUES ('a') RETURNING id;"
            " INSERT INTO t(body) VALUES ('b')"
        )
        assert out is None

    db.submit(work).result(timeout=1.0)
    assert db.value("SELECT count(*) FROM t") == 2


def test_executemany_allows_underscore_names_but_not_auto_binds(
    db: LiteWriter,
) -> None:
    def work(tx: Tx) -> None:
        tx.executemany("INSERT INTO t(body) VALUES (:_x)", [{"_x": "a"}, {"_x": "b"}])
        tx.executemany("INSERT INTO t(body) VALUES (:_1)", [{"_1": "c"}])

    db.submit(work).result(timeout=1.0)
    assert db.value("SELECT count(*) FROM t") == 3

    def bad(tx: Tx) -> None:
        tx.executemany(Insert("t", columns=["body"]), [{"body": "a", "_1": "b"}])

    with pytest.raises(QueryError, match="auto-bind"):
        db.submit(bad).result(timeout=1.0)
    assert db.value("SELECT count(*) FROM t") == 3


def test_executemany_needs_named_parameters(db: LiteWriter) -> None:
    literal = Insert("t", values={"body": "fixed"})

    def write(tx: Tx) -> None:
        tx.executemany(literal, [{"body": "a"}, {"body": "b"}])

    with pytest.raises(QueryError, match="named parameters"):
        db.submit(write).result(timeout=2)
    assert db.value("SELECT count(*) FROM t") == 0


def test_executemany_rejects_a_key_that_is_not_a_parameter(db: LiteWriter) -> None:
    def write(tx: Tx) -> None:
        tx.executemany(
            "INSERT INTO t(body) VALUES (:body)", [{"body": "a", "extra": 1}]
        )

    with pytest.raises(QueryError, match="not a parameter"):
        db.submit(write).result(timeout=2)
    assert db.value("SELECT count(*) FROM t") == 0


def test_executemany_mixes_a_literal_and_a_named_parameter(db: LiteWriter) -> None:
    from litewriter import param

    stmt = Insert("t", values={"id": param("id"), "body": "lit"})

    def write(tx: Tx) -> None:
        tx.executemany(stmt, [{"id": 5}, {"id": 6}])

    db.submit(write).result(timeout=2)
    assert db.query("SELECT id, body FROM t ORDER BY id") == [(5, "lit"), (6, "lit")]


def test_executemany_rejects_a_non_dict_row(db: LiteWriter) -> None:
    def write(tx: Tx) -> None:
        tx.executemany(
            "INSERT INTO t(body) VALUES (:body)",
            cast("Iterable[Mapping[str, object]]", ["a"]),
        )

    with pytest.raises(QueryError, match="a dict"):
        db.submit(write).result(timeout=2)


def test_insert_default_values(db: LiteWriter) -> None:
    def write(tx: Tx) -> None:
        tx.execute("CREATE TABLE d(x INTEGER DEFAULT 7)")
        tx.execute(Insert("d", values={}))

    db.submit(write).result(timeout=2)
    assert db.query("SELECT x FROM d") == [(7,)]
