#!/usr/bin/env python3
"""Naive APSW vs litewriter. Same PRAGMAs, same INSERT / SELECT."""

import time
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory

import apsw

from litewriter import LiteWriter, Tx
from litewriter.connect import connect

N = 8_000
SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"


def insert_row(tx: Tx, body: str) -> int:
    tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)
    return int(tx.last_insert_rowid())


def report(name: str, n: int, elapsed: float) -> None:
    print(f"{name:42s}  {1e6 * elapsed / n:8.1f} µs   {n / elapsed:10.0f} /s")


def best(run: Callable[[], float]) -> float:
    """One warm-up pass, then the best of five."""
    run()
    return min(run() for _ in range(5))


def naive_write(path: Path, n: int) -> float:
    conn = connect(path)
    conn.execute(SCHEMA)
    t0 = time.perf_counter()
    for i in range(n):
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO t(body) VALUES (:body)", {"body": f"n-{i}"})
        conn.execute("COMMIT")
    elapsed = time.perf_counter() - t0
    conn.close()
    return elapsed


def naive_read(path: Path, n: int) -> float:
    conn = connect(path)
    t0 = time.perf_counter()
    for i in range(n):
        conn.execute(
            "SELECT body FROM t WHERE id = :id", {"id": 1 + i % 1000}
        ).fetchone()
    elapsed = time.perf_counter() - t0
    conn.close()
    return elapsed


def writer_write(path: Path, n: int, *, wait: bool) -> float:
    db = LiteWriter(path, hz=0, schema=SCHEMA)
    db.start()
    t0 = time.perf_counter()
    if wait:
        futures = [db.submit(insert_row, f"s-{i}") for i in range(n)]
        for future in futures:
            future.result()
    else:
        for i in range(n):
            db.push(insert_row, f"s-{i}")
        db.submit(insert_row, "tail").result()
        n += 1
    elapsed = time.perf_counter() - t0
    db.close()
    return elapsed


def writer_read(path: Path, n: int) -> float:
    db = LiteWriter(path, hz=0)
    db.start()
    t0 = time.perf_counter()
    for i in range(n):
        db.one("SELECT body FROM t WHERE id = :id", id=1 + i % 1000)
    elapsed = time.perf_counter() - t0
    db.close()
    return elapsed


def writer_read_snapshot(path: Path, n: int, per_snapshot: int) -> float:
    """Reads on a plain thread inside ``with db.snapshot():`` blocks."""
    db = LiteWriter(path, hz=0)
    db.start()
    left = n
    t0 = time.perf_counter()
    while left:
        with db.snapshot():
            for _ in range(min(per_snapshot, left)):
                db.one("SELECT body FROM t WHERE id = :id", id=1 + left % 1000)
                left -= 1
    elapsed = time.perf_counter() - t0
    db.close()
    return elapsed


def main() -> None:
    print(f"n={N}  APSW {apsw.apswversion()}  SQLite {apsw.sqlitelibversion()}")
    with TemporaryDirectory(prefix="litewriter-cmp-") as raw:
        root = Path(raw)
        e = naive_write(root / "naive.sqlite3", N)
        report("naive  INSERT+COMMIT (baseline)", N, e)
        e = writer_write(root / "group.sqlite3", N, wait=True)
        report("writer  submit group (in flight)", N, e)
        e = writer_write(root / "push.sqlite3", N, wait=False)
        report("writer  push (no Future)", N, e)

        db = LiteWriter(root / "seq.sqlite3", hz=0, schema=SCHEMA)
        db.start()
        t0 = time.perf_counter()
        for i in range(N):
            db.submit(insert_row, f"q-{i}").result()
        e = time.perf_counter() - t0
        db.close()
        report("writer  submit sequential", N, e)

        seed = root / "read.sqlite3"
        naive_write(seed, 1_000)
        e = best(lambda: naive_read(seed, N))
        report("naive  PK SELECT (baseline)", N, e)
        e = best(lambda: writer_read(seed, N))
        report("writer  PK SELECT (thread-local)", N, e)
        e = best(lambda: writer_read_snapshot(seed, N, 10))
        report("writer  PK SELECT, 10 reads in db.snapshot()", N, e)


if __name__ == "__main__":
    main()
