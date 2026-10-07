from litewriter import LiteWriter, Tx


def insert_row(sql: Tx, body: str) -> int:
    sql.execute("INSERT INTO t(body) VALUES (?)", (body,))
    return int(sql.last_insert_rowid())


def update_row(sql: Tx, row_id: int, body: str) -> None:
    sql.execute("UPDATE t SET body = ? WHERE id = ?", (body, row_id))


def read_then_write(sql: Tx, row_id: int, suffix: str) -> str:
    row = sql.execute("SELECT body FROM t WHERE id = ?", (row_id,)).fetchone()
    assert row is not None
    body = f"{row[0]}{suffix}"
    sql.execute("UPDATE t SET body = ? WHERE id = ?", (body, row_id))
    return body


def insert_many(sql: Tx, rows: list[str]) -> int:
    sql.executemany("INSERT INTO t(body) VALUES (?)", ((row,) for row in rows))
    return int(sql.last_insert_rowid())


def test_insert_update_and_read_then_write(db: LiteWriter) -> None:
    row_id = db.submit(insert_row, "hi").result(timeout=1.0)
    db.submit(update_row, row_id, "hello").result(timeout=1.0)
    body = db.submit(read_then_write, row_id, "!").result(timeout=1.0)
    assert body == "hello!"
    assert db.execute("SELECT body FROM t WHERE id = ?", (row_id,)).fetchone() == (
        "hello!",
    )


def test_insert_many(db: LiteWriter) -> None:
    last = db.submit(insert_many, ["a", "b", "c"]).result(timeout=1.0)
    count = db.execute("SELECT count(*) FROM t").fetchone()
    assert count == (3,)
    assert last == 3
