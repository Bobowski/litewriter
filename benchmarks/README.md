# Litewriter benches

```text
cd shared/litewriter
uv run --package litewriter python benchmarks/compare.py
uv run --package litewriter python benchmarks/writer.py
uv run --package litewriter python benchmarks/watch.py
```

`compare.py` — writes and PK reads vs naive APSW (`BEGIN` + one
`INSERT` + `COMMIT`). Every job gets its own savepoint; there is no
faster non-isolated mode to compare against.
`writer.py` — group drain under load, the hz floor, submit vs push.
`watch.py` — a watched write against the same write with no watch, and
against one poll of the query.
