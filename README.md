# Litewriter

```text
pip install litewriter
```

Source: https://github.com/Bobowski/litewriter

The index name is `litewriter`. The import is `litewriter`.
The class is `LiteWriter`. It is SQLite only.

One SQLite file. One writer thread. Writes that are waiting share one
`COMMIT`. Each OS thread reads on its own connection. A read does not
wait on that writer. A watch reads again when a commit changes a column
it reads.

```python
from litewriter import LiteWriter
```

## A query

A query is a statement. The constructor takes the clauses.
`col` is a name. `lit` is a string literal. `param` is a parameter.
`sql(q)` returns one line of SQL. `query` returns a list of tuples.

```python
from litewriter import Select, col, param, sql

page = Select(
    col("m.id"),
    col("m.body"),
    ("as", ("lower", "u.name"), "author"),
    from_=col("messages").as_("m"),
    join=("left", col("users").as_("u"), col("m.author") == col("u.id")),
    where=col("m.room") == param("room"),
    order_by=col("m.at").desc(),
    limit=50,
)
print(sql(page))

mine = page.where(col("m.author") == param("me")).limit(20)
print(sql(mine))
print(sql(page))

listed = Select(
    "id",
    from_="t",
    where=[col("room") == param("room"), col("at") > 0],
)
print(sql(listed))

inner = Select("id", from_="t", where=col("ok") == 1)
print(sql(Select(col("s.id"), from_=inner.as_("s"))))
```

```text
SELECT m.id, m.body, lower(u.name) AS author FROM messages AS m LEFT JOIN users AS u ON m.author = u.id WHERE m.room = :room ORDER BY m.at DESC LIMIT 50
SELECT m.id, m.body, lower(u.name) AS author FROM messages AS m LEFT JOIN users AS u ON m.author = u.id WHERE m.room = :room AND m.author = :me ORDER BY m.at DESC LIMIT 20
SELECT m.id, m.body, lower(u.name) AS author FROM messages AS m LEFT JOIN users AS u ON m.author = u.id WHERE m.room = :room ORDER BY m.at DESC LIMIT 50
SELECT id FROM t WHERE room = :room AND at > 0
SELECT s.id FROM (SELECT id FROM t WHERE ok = 1) AS s
```

The third line is the first line again. `page.where(...)` returns a new
statement. `page` stays as it was. `.where(a, b)` joins those arguments
with `AND`. A list is one argument. `.where([a, b])` does not compile.

In a constructor, a list of conditions is `AND`. A list that starts with
a name is a call. `["=", "room", ":room"]` is `room = :room`.
`("count", "*")` is `count(*)`. The string `count(*)` is not a name.

`&` and `|` are bitwise. An expression has no truth value. The words
`and` and `or` raise `TypeError`. Put conditions in a list in the
constructor. That list is `AND`.

`from_` and `with_` keep the underscore. `from` and `with` are Python
keywords.

A `Select` is a subquery. `from_=inner.as_("s")` is
`FROM (SELECT ...) AS s`. The same form works in a join, in `where`,
and in the select list.

A tuple is a call. `("lower", "name")` is `lower(name)`.
`("as", expr, "name")` is the same alias as `.as_("name")`.
`("lit", "open")` is the same literal as `lit("open")`.

`Select` has the select methods. `Insert` has the insert methods.
`Update` has `set`, `from_`, and `where`. `Delete` has `where`.
A union has `order_by` and `limit`. The constructor is the whole
statement. A method adds one clause. A dict of clauses still renders.

## Values

A value becomes SQL:

| Value | SQL |
| --- | --- |
| `col("u.name")`, `col("*")`, `col("u.*")` | a name (a keyword such as `order` is quoted) |
| `lit("open")` or `"'open'"` or `("lit", "open")` | a string literal |
| `param("room")` or `":room"` | a parameter |
| `col("room") == param("room")` | `room = :room` |
| `Select("id", from_="t").as_("s")` | `(SELECT id FROM t) AS s` |
| `1`, `2.5`, `True`, `None`, `b"\x01"` | `1`, `2.5`, `TRUE`, `NULL`, `X'01'` |
| `("lower", "u.name")` or `["lower", "u.name"]` | a call: `lower(u.name)` |
| `("as", "messages", "m")` | `messages AS m` |
| `("as", ("lower", "u.name"), "author")` | `lower(u.name) AS author` |
| `{"select": ["id"], "from": "t"}` | `(SELECT id FROM t)` |

A call name that is not an operator is a function. LiteWriter does not
keep a list of functions. `json_extract`, `datetime`, and a function
that you register on the connection all work the same way.

A value from outside goes in as a parameter (`:name`), never as text.

## Calls

The call name is not case sensitive.

| Call | SQL |
| --- | --- |
| `["=", a, b]`, also `!=` `<` `<=` `>` `>=` `is` `is not` `like` `glob` `regexp` `match` | `a = b` |
| `["and", a, b, c]`, `["or", ...]` | `a AND b AND c` |
| `["+", a, b]`, also `-` `*` `/` `%` `\|\|` `->` `->>` `&` `\|` `<<` `>>` | `a + b` |
| `["not", a]` | `NOT a` |
| `["in", a, x, y]`, `["in", a, query]`, `not in` | `a IN (x, y)` |
| `["between", a, lo, hi]`, `not between` | `a BETWEEN lo AND hi` |
| `["exists", query]`, `not exists` | `EXISTS (...)` |
| `["case", when, then, ..., else]` | `CASE WHEN ... END` |
| `["as", a, "name"]` | `a AS name` |
| `["cast", a, "integer"]` | `CAST(a AS INTEGER)` |
| `["collate", a, "nocase"]` | `a COLLATE NOCASE` |
| `["desc", a]`, `["asc", a, "nulls last"]` | `a DESC` |
| `["distinct", a]` | `DISTINCT a` |
| `["raw", "any SQL"]` | the text as it is |

Parentheses come out only where SQL needs them.
`["and", ["or", a, b], c]` is `(a OR b) AND c`.

## Clauses

The key order in the dict does not matter. The output uses SQL order.

| Statement | Keys |
| --- | --- |
| select | `with`, `with_recursive`, `select` or `select_distinct`, `from`, `join`, `where`, `group_by`, `having`, `order_by`, `limit`, `offset` |
| compound | `union`, `union_all`, `intersect`, or `except` (a list of queries), `order_by`, `limit`, `offset` |
| insert | `insert_into` or `replace_into`, `columns`, `values` or the select keys, `on_conflict`, `do_nothing`, `do_update_set`, `returning` |
| update | `update`, `set`, `from`, `join`, `where`, `returning` |
| delete | `delete_from`, `where`, `returning` |

- `select`, `group_by`, `order_by`, `returning`, and `columns` are lists.
  A call sits in its own list: `"select": [["count", "*"]]`.
- `from` is a table, `("as", "table", "t")`, a table function
  (`("json_each", ":ids")`), or a subquery.
- `join` is `(kind, source, on)`. A list of those is many joins.
  `kind` is `inner`, `left`, `right`, `full`, or `cross`.
  A cross join is `("cross", source)`.
- `with` is a dict of name to query.
- `values` is one row (a dict) or many rows. A list of lists needs
  `columns`.
- `set` and `do_update_set` are a dict of column to value.
- `on_conflict` is a list of columns. `[]` means any conflict.
  `do_update` needs those column names.

An unknown key raises `WriterError`. A key that does not belong on that
statement raises `WriterError`. A bad name or a missing parameter does
too. An insert may contain the select keys. That form is
`INSERT INTO t SELECT ...`.

## The file

This program replaces `example.sqlite3` in the current directory.
`async with` starts the writer and closes it at the end. `hz=0` commits
as soon as the job arrives. The default is 60.

```python
import asyncio
from pathlib import Path

from litewriter import Insert, LiteWriter, Select, Tx, col, param

page = Select(
    col("m.id"),
    col("m.body"),
    ("as", ("lower", "u.name"), "author"),
    from_=col("messages").as_("m"),
    join=("left", col("users").as_("u"), col("m.author") == col("u.id")),
    where=col("m.room") == param("room"),
    order_by=col("m.at").desc(),
    limit=50,
)
mine = page.where(col("m.author") == param("me")).limit(20)

SCHEMA = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    room INTEGER NOT NULL,
    author INTEGER REFERENCES users (id),
    body TEXT NOT NULL,
    at INTEGER NOT NULL
);
"""


def add_user(tx: Tx, name: str) -> int:
    return tx.value(
        Insert("users", values={"name": ":name"}, returning="id"),
        name=name,
    )


def send(tx: Tx, room: int, body: str, author: int, at: int) -> int:
    return tx.value(
        Insert(
            "messages",
            values={
                "room": ":room",
                "author": ":author",
                "body": ":body",
                "at": ":at",
            },
            returning="id",
        ),
        room=room,
        author=author,
        body=body,
        at=at,
    )


async def main() -> None:
    path = Path("example.sqlite3")
    path.unlink(missing_ok=True)
    path.with_name(path.name + "-wal").unlink(missing_ok=True)
    path.with_name(path.name + "-shm").unlink(missing_ok=True)
    async with LiteWriter(path, hz=0) as db:
        db.execute_script(SCHEMA)
        user = await db.call(add_user, "Ada")
        msg = await db.call(send, 8, "hi", user, 1)
        print("msg", msg)
        print("page", db.query(page, room=8))
        print("mine", db.query(mine, room=8, me=user))
        print("empty", db.one(page, room=9))
        print("count", db.value(Select(("count", "*"), from_="messages")))
        row = db.execute(
            "SELECT name FROM users WHERE id = :id",
            {"id": user},
        ).fetchone()
        print("name", tuple(row))
        print("tables", sorted(db.tables(page)))
        with db.reader() as reader:
            print("reader", reader.query(page, room=8))
        async with db.watch(page, room=8) as live:
            found = aiter(live)
            print("now", await anext(found))
            await db.call(send, 8, "next", user, 2)
            print("later", await anext(found))


asyncio.run(main())
```

```text
msg 1
page [(1, 'hi', 'ada')]
mine [(1, 'hi', 'ada')]
empty None
count 1
name ('Ada',)
tables ['messages', 'users']
reader [(1, 'hi', 'ada')]
now [(1, 'hi', 'ada')]
later [(2, 'next', 'ada'), (1, 'hi', 'ada')]
```

The writer calls `fn(tx, *args, **kwargs)` on the writer thread, inside
`BEGIN IMMEDIATE`. You do not `COMMIT`. You do not keep `tx`. You do not
block. `tx.value` is the first column of the first row. `tx.one` is the
first row. `tx.query` is every row. `tx.execute` runs SQL you already
have. A sequence fills `?`. A mapping fills `:name`.

- `await db.call(fn, ...)` waits on this asyncio loop. The caller thread stays free.
- `db.push(fn, ...)` puts the write on the queue. It does not wait. Errors go to `on_error`.
- `db.submit(fn, ...)` returns a slot. `.result()` waits on the caller thread.
  On an asyncio loop, use `call`.
- The result comes after the batch `COMMIT`.
- `hz=60` is the default. After a commit, the writer sleeps the rest of that 1/60 s.
  When the inbox is empty, the writer parks. `hz=0` commits as fast as
  jobs arrive. You can change `db.hz` while it runs.
- `isolated=True` is the default. It sets a SAVEPOINT for that write.
  A failure undoes only that write. The error comes back after the rest
  of the batch commits. `isolated(fn)` does the same thing.
- `isolated=False` shares the batch. A failure rolls back the whole batch.
  The other writes in that batch get `WriterRolledBack`.
- `db.close()` drains the inbox, commits the last batch, and stops. A
  write that arrives too late fails with `WriterRuntime`.
- `submit` returns `Slot[R]`, and `call` returns `R`, where `R` is the
  return type of `fn`. Pyright checks the arguments against `fn`.

## Reads

| Call | Returns |
| --- | --- |
| `db.reader()` | a new read-only connection. Open as many as you need |
| `db.query(q, **params)` | all rows, a list of tuples |
| `db.one(q, **params)` | the first row, or None |
| `db.value(q, **params)` | the first column of the first row. No row raises `WriterError` |
| `db.execute(q, params)` | an APSW cursor. `params` is a mapping or a sequence |
| `db.watch(q, **params)` | live rows. The writer must be started |
| `db.tables(q)` | the tables `q` reads |
| `db.offload(fn)` | `fn(conn)` on a worker thread, for a rare large read |

`q` is SQL text or a query. A dict of clauses is still a query.
`query`, `one`, and `value` take keyword arguments.
`execute` takes the bindings as one argument.

A read does not enter the writer queue. The writer does not need to be
started. The file must exist. A missing file raises `WriterRuntime`.
`db.query` uses one read-only connection for this OS thread. Another
thread opens its own connection. `db.reader()` opens one more.
Use that reader from the thread that opened it. Close it on that thread.
`db.close()` closes a reader opened on this thread. A reader on another
thread closes on its next use.

## Watch

The first read is the rows now. A later read comes after a commit that
changed them. `async for rows in live` keeps going. The program above
stops after the second read. A slow reader gets the newest rows, not a
queue of old ones.

The commit does not run the query. SQLite names the columns the query
reads, once, when the watch opens. The writer records inserts, deletes,
and updated columns only while a watch is open. `BEGIN`, `COMMIT`,
`SAVEPOINT`, `RELEASE`, `ROLLBACK`, and `PRAGMA schema_version` are the
writer's own SQL. They add no user table.

An insert or a delete wakes every watch of that table. An update wakes
a watch when the updated column is one the query reads. `count(*)`
wakes on insert and delete only. After `COMMIT`, every matching watch
on one asyncio loop is set in one wake.

A write to `messages` does not wake a watch of `users`. An update of
`users.seen` does not wake `SELECT name FROM users`. The watch reads
again on this thread's read connection. Equal rows do not yield. A
rolled-back batch does not wake anyone. A schema change wakes every
watch. A view, a CTE, and raw SQL text all work, because SQLite
resolves them to base columns.

## Install

CPython 3.14. The package is pure Python. `pip install litewriter` also
installs APSW. A `v*` tag on the package repository publishes it.

```text
pip install litewriter
```

From a checkout of this package:

```text
uv sync
uv run pytest
```

## Threads

`submit`, `call`, and `push` are safe from any OS thread. The writer
owns the write connection. Each OS thread reads on its own connection.
A `reader()` stays on the thread that opened it. Jobs sit on a deque
and a Condition. After `COMMIT` the writer wakes each asyncio loop once
per batch through a socketpair mailbox. Matching watches on that loop
are set in that same wake. The mailbox is made on the loop's own thread
(the first `call` or `watch` there), because `add_reader` is not
thread-safe.

Each connection has a 64 MiB page cache and maps up to 1 GiB of the file
(`litewriter.connect.CACHE_KIB`, `MMAP_BYTES`).
