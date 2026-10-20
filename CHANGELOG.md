# Changelog

## 0.3.0

Breaking changes:

- `WriterError` is `QueryError`. `WriterRuntime` is `StateError`.
  `WriterBusy` is `ClaimBusy`. `WriterRolledBack` is gone: every job
  always runs in its own savepoint.
- `await db.call(fn, ...)` is `await db.write(fn, ...)`. `db.submit`
  returns a `concurrent.futures.Future`; calling `.result()` on a
  running event loop raises `StateError`. The `Slot` class is gone.
- The `isolated=` kwarg is gone — savepoints are unconditional. The
  `Isolated` marker and `@isolated` decorator are gone.
- `LiteWriter(...)` takes `schema=` (SQL text or `fn(tx)`) and
  `on_error=`. `after_commit`, `on_abort`, `profile`, `name`, and
  `Stats` are gone, and so are `db.stats`, `db.execute_script`,
  `db.claim`, `db.aclaim`, `db.reader`, `db.conn`, `db.offload`, and
  `db.tables`.
- `db.execute` is gone. Reads are `db.query`/`one`/`value`. On a write
  connection `Tx.execute` and `Tx.executemany` return `None` and run
  every statement in the SQL text to completion.
- `db.query`/`one`/`value` and `watch` take an optional positional
  `into` row builder (default `tuple`) and named `**params` only.
  Positional `?` bindings and the `execute(q, params)` dict form are
  gone.
- Statements are immutable objects, not Mappings. `.sql` and `.params`
  replace `sql(q)` and the clause dict; `sql()` and `where()` are gone.
  `Union`, `UnionAll`, `Intersect`, and `Except` are `.union()`,
  `.union_all()`, `.intersect()`, and `.except_()`.
- Every Python value in a value position is auto-bound as `:_1`,
  `:_2`, ... (stored in `.params`) — `str`, `bytes`, `int`, `float`,
  and `bool`; `None` renders `NULL`. `lit()` and `raw()` are the only
  inline text. The tuple-call `("lower", "x")` form is
  `fn("lower", col("x"))`; `"'open'"` and `":room"` magic strings raise
  `QueryError`. `exists()`/`not_exists()` are `inner.exists()` and
  `~inner.exists()`.
- `fn` is a function: `fn("lower", col("x"))`; the attribute form
  `fn.lower(...)` is gone.
- `&`, `|`, and `~` now build `AND`, `OR`, and `NOT`. The bitwise
  operators moved to `expr.op("&", ...)`; unary `+` is gone.
- `inner_join`/`left_join`/`right_join`/`full_join`/`cross_join` are
  `.join(kind, source, on)`.
- `Insert` `values` takes a dict or a list of dicts; list-of-lists
  rows are gone.
- The reserved parameter names are now exactly `_1`, `_2`, ... (`_`
  followed by digits), and only on builder statements. `param("_x")`
  and a `**params` key like `_x` work; `param("_1")`, a `:_1` reference
  inside `raw()`, and a `**params` or `executemany` key like `_1` on a
  builder statement raise `QueryError`. Plain SQL text may name a
  parameter `:_1`. Write-function kwargs are plain Python kwargs and
  are unaffected.
- `into=` passed as a keyword raises `QueryError`: `into` is
  positional. A positional `into` that is not callable raises
  `QueryError` too.
- Reads belong to the reader threads. `db.query`, `db.one`,
  `db.value`, and `db.snapshot` raise `StateError` on the writer
  thread: inside a write, `tx` is the read. `db.write` and
  `submit().result()` raise `StateError` on any writer thread.
- `tx` stays on the writer thread: using a stored `Tx` from another
  thread raises `StateError`, and any `tx` call after the batch ends
  raises `StateError`.
- `tx.executemany` needs named parameters: `columns=[...]` or
  `param()` on a builder, `:name` in SQL text. Every row key must be
  a parameter of the statement. A statement that is all literal
  values raises `QueryError`; run it once with `tx.execute`.
- `Insert("t", values={})` renders `INSERT INTO t DEFAULT VALUES`.
  `.values()` with no rows raises `QueryError`.
- `close()` on a running event loop raises `StateError`: leave with
  `async with` or `await db.aclose()`, which closes on a worker thread.
  `close()` no longer waits out the `hz` floor.
- `start()` on an event loop refuses a `claim` wait (`claim=0` still
  tries once).
- `submit`/`write`/`push` reject a non-callable or an `async def`
  right at the call.
- An `async def` write function raises `QueryError` at `write`,
  `submit`, or `push`. A write function that returns a coroutine rolls
  back to its savepoint and that caller gets `QueryError`.
- `submit().result()` or `.exception()` inside a write function raises
  `StateError`: the writer would wait on itself.
- `where(None)` and `having(None)` raise `QueryError` (also inside a
  condition list).
- `LiteWriter(":memory:")` raises `QueryError`: every thread opens its
  own read connection, and one file is required.
- `Reader` and `Outcome` are no longer public.

Builder:

- A statement compiles its SQL on first use and keeps it; later runs
  only bind parameters. Build once and reuse.
- Name lists shorten the build-once statements: `Insert` `columns=`
  alone renders `VALUES (:name, ...)`; `Update` `set=` accepts column
  names that bind `:name`; `do_update=` accepts column names that
  render `name = excluded.name`. `.set(...)` and `.do_update(...)`
  mix positional names with keyword assigns.
- `fn("name", ...)` suggests SQLite's built-in names in the editor;
  a registered function's name works the same.

Reads:

- `db.aquery`/`aone`/`avalue` are the async twins of the sync reads:
  they run the read on a worker thread, for a read too heavy for the
  loop. Short indexed reads stay on the loop with `db.query`/`one`/`value`.
- `db.snapshot()` opens a `with` block whose reads on this thread share
  one fixed view. On an event loop the block cannot contain an `await`
  (`StateError`); on a plain thread it holds the view across commits.
- The public read path is shorter: `db.query`/`one`/`value` bind
  parameters and fetch rows inline.

Fixes:

- Watch tracking no longer re-prepares every write: the writer
  classifies each statement once with `EXPLAIN` under a temporary
  authorizer and caches the answer by SQL text. A write that cannot be
  classified wakes every watch.

- A read-only connection builds its URI with `Path.as_uri()`, so a
  file named `a%20b.sqlite3`, `a b.sqlite3`, `a#b.sqlite3`, or
  `a?b.sqlite3` reads its own bytes.
- A write function that runs `COMMIT`, `END`, or `ROLLBACK` fails the
  whole batch with `StateError`; nothing from that batch reaches disk.
- A write cancelled before the writer starts it does not run. A write
  cancelled after that still commits.
- `submit().exception()` has the same event-loop guard as
  `submit().result()`.
- The writer survives an internal error during a batch: the batch's
  callers get the error and later writes still work.
- The claim is a polling lock, not a thread: `timeout=0` tries once, a
  wait cancelled by `close()` raises `StateError` (sync and async), and
  `ClaimBusy` reports the holder pid when it is readable (POSIX and
  Windows).
- `submit().result()` from a plain thread and for a done future works
  everywhere, including inside an event loop thread.
- Two watches on one loop no longer crash the writer thread, and a
  close drains the last wakes instead of losing them.
- A watch compares raw rows before building `into` objects, and only
  commits through the same `LiteWriter` wake it.
- The socket pair of a dead event loop closes; `close()` closes the
  idle read connection on every thread.
- `async with` runs `start` and `close` off the event loop.
- A watch opened while `close()` runs raises `StateError` instead of
  waiting forever.
- `start()` waits for the writer without a deadline: a slow schema no
  longer fails `start()` or leaves a second writer thread behind, and
  a writer thread that exits before it is ready makes `start()` raise
  `StateError`.
- A schema change made outside a batch — a trigger created by another
  connection, or a TEMP trigger created by a job — clears the
  write-effect cache and wakes every watch.
- A batch with no watch open skips the schema-version checks, so
  watches cost nothing while none is open.
- `UPDATE t SET rowid = ...` now wakes a watch that reads `rowid`,
  `oid`, or `_rowid_`: the authorizer reports the write as `ROWID`
  but the read as the INTEGER PRIMARY KEY name, so the writer maps
  it back.
- A write that cannot be classified — a missing parameter, or an
  `ATTACH`/`DETACH` — wakes every watch instead of leaking `KeyError`
  or tracking too little.
- `db.watch` on a table that does not exist raises `QueryError`,
  not `apsw.SQLError`.
- Two open watches on the same query and parameters share one
  re-read per commit.
- A second `Claim.aacquire` on one claim returns `False` instead of
  racing the first.
- A `Watch` opens once; a second `async with` on it raises
  `StateError`.

## 0.2.0

- `claim` waits on `{path}-claim` so one process can take the file during a switch.
- `close` folds the WAL once, then drops the claim.
- `busy_timeout` is seconds. The default is 5.
- `Replace` is gone. A comparison with `None` raises `WriterError`.

## 0.1.1

- Windows loops have no `add_reader`. One callback still carries the batch.
- The writer sleeps until the commit floor. One `sleep` can return early.

## 0.1.0

- One SQLite file. One writer thread. Reads stay on the calling thread.
- `isolated=True` is the default. A failed write undoes only that write.
- `Select`, `Insert`, `Update`, and `Delete`. `col`, `lit`, `param`, and `.as_()` build expressions.
- `db.watch` yields again when a commit changes a column the query reads.
