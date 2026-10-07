# Litewriter benches

```text
cd shared/litewriter
uv run --package litewriter python benchmarks/compare.py
uv run --package litewriter python benchmarks/phases.py
uv run --package litewriter python benchmarks/writer.py
uv run --package litewriter python benchmarks/watch.py
```

`compare.py` — writes and PK reads vs naive APSW (`BEGIN` + one
`INSERT` + `COMMIT`). Group-commit runs pass `isolated=False`.
`phases.py` — Python hop vs user fn vs BEGIN/COMMIT, noop vs insert,
inbox/slot micro, then cProfile. These runs pass `isolated=False`.
`writer.py` — savepoint vs shared batch, group drain, hz floor, call vs push.
`watch.py` — a watched write against the same write with no watch, and
against one poll of the query.
