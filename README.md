# litewriter

SQLite for asyncio apps: one writer thread, group commit, live queries.

```text
pip install litewriter    # CPython 3.14+, depends on APSW
```

## The idea

SQLite allows one writer at a time. litewriter builds on that rule instead of fighting it:

- **One writer thread** owns the write connection. Every write goes through it.
- **Group commit.** Writes that arrive together share one transaction and one `COMMIT`.
- **Reads never queue.** Each thread reads on its own read-only connection (WAL mode).
- **Live queries.** `db.watch(q)` reads `q` again only when a commit touched a column that `q` reads.

## Quick start

```python
import asyncio
import tempfile
from pathlib import Path

from litewriter import LiteWriter, Tx

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    room INTEGER NOT NULL,
    body TEXT NOT NULL
);
"""
ROOM = "SELECT body FROM messages WHERE room = :room ORDER BY id"


def send(tx: Tx, room: int, body: str) -> int:
    return tx.value(
        "INSERT INTO messages (room, body) VALUES (:room, :body) RETURNING id",
        room=room,
        body=body,
    )


async def main() -> None:
    path = Path(tempfile.mkdtemp()) / "chat.sqlite3"
    async with LiteWriter(path, schema=SCHEMA) as db:
        print(await db.write(send, 1, "hi"))

        async with db.watch(ROOM, room=1) as live:
            async for rows in live:
                print(rows)
                if len(rows) >= 3:
                    break
                db.push(send, 1, f"msg {len(rows) + 1}")


asyncio.run(main())
```

```text
1
[('hi',)]
[('hi',), ('msg 2',)]
[('hi',), ('msg 2',), ('msg 3',)]
```

## Writes are functions

A write is a plain function. Its first argument is `tx`:

```python
def rename(tx: Tx, user_id: int, name: str) -> None:
    tx.execute("UPDATE users SET name = :name WHERE id = :id", name=name, id=user_id)
```

The writer calls it on the writer thread, inside `BEGIN IMMEDIATE`. `tx.execute` returns nothing: run `tx.value("SELECT changes()")` for a changed-row count. You never commit. Do not run `BEGIN`, `COMMIT`, `END`, or `ROLLBACK` inside a write: the batch fails with `StateError`. Keep the function short: no network, no sleep, no hashing. It holds the whole batch.

There are three ways to send a write:

| Call | Waits | You get | Use it from |
| --- | --- | --- | --- |
| `await db.write(fn, *args)` | on the event loop | the return value, or the exception | async code |
| `db.submit(fn, *args)` | no | a Future; `.result()` blocks this thread | threads, scripts, tests |
| `db.push(fn, *args)` | no | nothing; errors go to `on_error` | logs, counters, fire and forget |

A result arrives after the batch commits. A returned value is on disk.

On an event loop, use `await db.write`. A `.result()` there would block every request until the commit, so it raises `StateError` instead.

An `async def` write function raises `QueryError`: the writer thread cannot await it. `submit().result()` inside a write raises `StateError`: the writer would wait on itself — call the other write function directly with the same `tx`. `close()` inside a write raises `StateError` for the same reason.

On an event loop, `close()` raises `StateError`: it would block the loop while it drains. Leave through `async with`, or call `await db.aclose()` — both close on a worker thread, and a cancelled `aclose` still finishes the drain on a daemon thread.

**Failure.** Each write runs in its own savepoint. When the function raises, only that write is undone. The rest of the batch commits. The exception goes back to that caller.

**Cancel.** A write cancelled before the writer starts it does not run. A write cancelled after that still commits.

**Pacing.** With `hz=60` (the default) the writer waits out the rest of 1/60 s after each commit, so the next batch is larger. A write waits at most one period. An idle writer sleeps and does not spin. `hz=0` commits as soon as work arrives. Awaited in sequence, each write waits one period: 30 `await db.write` calls at `hz=60` take about half a second. Put related changes in one write function, or `asyncio.gather` them.

## Reads

```python
db.query(q, **params)   # every row, a list of tuples
db.one(q, **params)     # the first row, or None
db.value(q, **params)   # the first column of the first row; no row raises
```

`q` is SQL text or a builder statement. `:name` in the SQL takes the keyword argument `name`. On a builder statement the names `_1`, `_2`, … are reserved for the values the builder binds; plain SQL text may use `:_1`. A read runs on the calling thread. On an event loop a read holds the loop for its duration, so keep reads short and indexed. A heavy read (an export, a report over every row) awaits `db.aquery`/`aone`/`avalue`, which run the read on a worker thread. A read does not need the writer to be started, but the file must exist.

Inside a write, reads go through `tx`: `tx.query`, `tx.one`, and `tx.value` see the uncommitted batch. The `db` read calls and `db.snapshot()` raise `StateError` there — `db` reads committed data, so inside a write they would see the world before the batch began.

A second positional argument builds each row. A dataclass or a named tuple fits:

```python
@dataclass(frozen=True)
class Message:
    id: int
    body: str

rows = db.query("SELECT id, body FROM messages WHERE room = :room", Message, room=1)
```

### `db.snapshot()`

`with db.snapshot():` gives the block's reads on this thread one fixed view: they see the data as of entry, even when the writer commits in between. Nested blocks join the outer view.

```python
with db.snapshot():
    head = db.one("SELECT ...")
    rows = db.query("SELECT ...")   # head and rows agree
```

On an event loop the block cannot contain an `await`: the view ends, the block raises `StateError`, and other tasks keep reading fresh data. On a plain thread there is no such limit, but keep the block short — an open snapshot holds back the WAL checkpoint. After `close()`, entering raises `StateError` like a read does.

## Watch

```python
async with db.watch(q, room=8) as live:
    async for rows in live:
        render(rows)
```

- The first item is the rows now. The next item comes after a commit that changed them.
- When the watch opens, SQLite reports the tables and columns that `q` reads. During a commit, the writer records the tables and columns it writes. A match reads `q` again.
- An insert or a delete wakes every watch on that table. An update wakes a watch only when it reads the updated column.
- Equal rows do not yield. A slow consumer gets the newest rows, not a backlog. The raw rows are compared first; the `into` objects are built only when they change.
- Only commits through this `LiteWriter` wake a watch. A write from another process does not.
- `close()` ends every watch. A watch does not yield the rows of the final batch.
- Two open watches on the same query and parameters share one re-read per commit.
- `ATTACH` or `DETACH` in a write wakes every open watch: their reach is not tracked.
- Joins, views, CTEs, and raw SQL text all work, because SQLite resolves them to base columns.
- Watches cost nothing while none is open: the writer records changes only while a watch exists.
- `db.watch(q, Message, room=8)` builds rows the same way as `db.query`.

## Query builder

The builder is optional. SQL text works everywhere. Use the builder when you compose one query into several.

```python
from litewriter import Select, col, fn, param

page = Select(
    col("m.id"),
    col("m.body"),
    fn("lower", col("u.name")).as_("author"),
    from_=col("messages").as_("m"),
    join=("left", col("users").as_("u"), col("m.author") == col("u.id")),
    where=col("m.room") == param("room"),
    order_by=col("m.at").desc(),
    limit=50,
)
mine = page.where(col("m.author") == param("me")).limit(20)
print(mine.sql)
```

```text
SELECT m.id, m.body, lower(u.name) AS author FROM messages AS m LEFT JOIN users AS u ON m.author = u.id WHERE m.room = :room AND m.author = :me ORDER BY m.at DESC LIMIT :_1
```

### Build once, run many

A statement compiles its SQL on first use and keeps it. Later runs only bind parameters, so a built statement costs the same as SQL text. Build it at module level: a new statement per call compiles again each time.

```python
from litewriter import Insert, Update, col, param

ADD = Insert("messages", columns=["room", "body"], returning="id")
MOVE = Update("messages", set=["room"], where=col("id") == param("id"))

print(ADD.sql)
print(MOVE.sql)
```

```text
INSERT INTO messages (room, body) VALUES (:room, :body) RETURNING id
UPDATE messages SET room = :room WHERE id = :id
```

Inside a write, parameters fill the named slots:

```python
new_id = tx.value(ADD, room=1, body="hi")
tx.execute(MOVE, room=2, id=7)
tx.executemany(ADD, [{"room": 1, "body": "a"}, {"room": 1, "body": "b"}])
```

`tx.executemany` needs named parameters — `columns=[...]` or `param()` on a builder, `:name` in SQL text. Every row key must be one of those names, so a row can never be ignored. A statement that is all literal values has no parameter for a row to fill; it raises `QueryError` — run it once with `tx.execute`.

A name list is a column and a parameter in one word. Put every changing value in `param()` or a name list, so the text stays the same.

In a value position every Python value binds as a parameter; nothing is pasted into the SQL, so two statements that differ only in values share one cached statement:

```python
from litewriter import Insert

add = Insert("users", values={"name": "Ada", "age": 36}, returning="id")
print(add.sql, add.params)
```

```text
INSERT INTO users (name, age) VALUES (:_1, :_2) RETURNING id {'_1': 'Ada', '_2': 36}
```

- A statement is immutable. `.where()` returns a new statement and adds its condition with `AND`.
- A string in the select list, `from_`, `order_by`, or a column key is a name. A string anywhere else is data, bound as `:_1`, `:_2`, …. In a value position every Python value binds; `None` is `NULL`; `lit()` and `raw()` are the only inline text. In a name position a number, bytes, or a bool renders as a SQL literal — `order_by=1` renders `ORDER BY 1`, which keeps its SQLite meaning.
- `col("u.name")` is a name in a value position. `param("room")` is `:room`, filled when you run the query. `lit("open")` is the inline `'open'`. `raw("random()")` is verbatim SQL.
- Operators build expressions: `==`, `<`, `+`, `.in_()`, `.like()`, `.between()`, `.is_(None)`, `.desc()`.
- Python cannot overload `and` and `or`. `&` is `AND`, `|` is `OR`, `~` is `NOT`: `(a | b) & ~c`.
- `fn` is every SQLite function: `fn("lower", col("name"))` is `lower(name)`, and so is a function you register.
- The editor suggests SQLite's built-in names inside `fn("` — a registered function's name works the same way.
- A `Select` nests anywhere: `from_=inner.as_("s")`, `col("id").in_(inner)`, `inner.exists()`.
- `Insert`, `Update`, and `Delete` follow the same pattern. `.union()`, `.union_all()`, `.intersect()`, and `.except_()` chain two selects.
- Name lists make the build-once shorthands: `Insert("t", columns=["a", "b"])` binds `:a` and `:b`, `set=["a"]` binds `:a`, and `do_update=["a"]` sets `a = excluded.a`. The `.set(...)` and `.do_update(...)` methods mix positional names with keyword assigns.
- `col("x") == None` raises, because `= NULL` never matches. Write `col("x").is_(None)`.

## Lifecycle

```python
async with LiteWriter(path, schema=SCHEMA, claim=30) as db:
    ...
```

`async with` starts the writer. Start and close run off the event loop. The exit drains the queue, commits the last batch, and folds the WAL. `:memory:` is not supported: every thread opens its own read connection, and one file is required.

`schema=` runs once at start, in one transaction, before the first write. It is SQL text, or a function `fn(tx)` when a migration needs to look before it changes something.

`claim=30` takes an exclusive lock on `{path}-claim` before it starts. It waits up to 30 s, then raises `ClaimBusy`. Use it so two copies of an app (the old one and the new one during a deploy) never write one file. Close releases the lock. The kernel releases it when the process dies.

Every connection uses WAL, `synchronous=NORMAL`, `foreign_keys=ON`, a 64 MiB page cache, and a 1 GiB memory map.

## Errors

| Error | Meaning |
| --- | --- |
| `LiteError` | the base of the three below |
| `QueryError` | a bad call: a missing parameter, a bad builder input, `into` that cannot build a row, an async write function, or no row for `value()` |
| `StateError` | the right call at the wrong time: not started, closed, a blocked loop, or a `db` read on the writer thread |
| `ClaimBusy` | another process holds the claim |

SQLite errors pass through as `apsw.Error` subclasses — `apsw.ConstraintError` from a write, `apsw.SQLError` for bad SQL. Catch them from `apsw`.

## Development

```text
uv sync
uv run pytest
```
