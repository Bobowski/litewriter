#!/usr/bin/env python3
"""Naive APSW vs litewriter. Same PRAGMAs, same INSERT / SELECT."""

import time
from pathlib import Path
from tempfile import TemporaryDirectory

import apsw

from litewriter import LiteWriter, Tx
from litewriter.connect import connect

N = 8_000
SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"


def insert_row(sql: Tx, body: str) -> int:
    sql.execute("INSERT INTO t(body) VALUES (?)", (body,))
    return int(sql.last_insert_rowid())


def report(name: str, n: int, elapsed: float) -> None:
    print(f"{name:42s}  {1e6 * elapsed / n:8.1f} µs   {n / elapsed:10.0f} /s")


def naive_write(path: Path, n: int) -> float:
    conn = connect(path)
    conn.execute(SCHEMA)
    t0 = time.perf_counter()
    for i in range(n):
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO t(body) VALUES (?)", (f"n-{i}",))
        conn.execute("COMMIT")
    elapsed = time.perf_counter() - t0
    conn.close()
    return elapsed


def naive_read(path: Path, n: int) -> float:
    conn = connect(path)
    t0 = time.perf_counter()
    for i in range(n):
        conn.execute("SELECT body FROM t WHERE id = ?", (1 + i % 1000,)).fetchone()
    elapsed = time.perf_counter() - t0
    conn.close()
    return elapsed


def writer_write(path: Path, n: int, *, wait: bool, alone: bool) -> float:
    db = LiteWriter(path, hz=0)
    db.execute_script(SCHEMA)
    db.start()
    t0 = time.perf_counter()
    if wait:
        futures = [db.submit(insert_row, f"s-{i}", isolated=alone) for i in range(n)]
        for future in futures:
            future.result()
    else:
        for i in range(n):
            db.push(insert_row, f"s-{i}", isolated=False)
        db.submit(insert_row, "tail", isolated=False).result()
        n += 1
    elapsed = time.perf_counter() - t0
    db.close()
    return elapsed


def writer_read(path: Path, n: int) -> float:
    db = LiteWriter(path, hz=0)
    db.start()
    t0 = time.perf_counter()
    for i in range(n):
        db.execute("SELECT body FROM t WHERE id = ?", (1 + i % 1000,)).fetchone()
    elapsed = time.perf_counter() - t0
    db.close()
    return elapsed


def main() -> None:
    print(f"n={N}  APSW {apsw.apswversion()}  SQLite {apsw.sqlitelibversion()}")
    with TemporaryDirectory(prefix="litewriter-cmp-") as raw:
        root = Path(raw)
        e = naive_write(root / "naive.sqlite3", N)
        report("naive  INSERT+COMMIT (baseline)", N, e)
        e = writer_write(root / "group.sqlite3", N, wait=True, alone=False)
        report("writer  submit group (in flight)", N, e)
        e = writer_write(root / "iso.sqlite3", N, wait=True, alone=True)
        report("writer  submit group + SAVEPOINT", N, e)
        e = writer_write(root / "push.sqlite3", N, wait=False, alone=False)
        report("writer  push (no Future)", N, e)

        db = LiteWriter(root / "seq.sqlite3", hz=0)
        db.execute_script(SCHEMA)
        db.start()
        t0 = time.perf_counter()
        for i in range(N):
            db.submit(insert_row, f"q-{i}", isolated=False).result()
        e = time.perf_counter() - t0
        db.close()
        report("writer  submit sequential", N, e)

        seed = root / "read.sqlite3"
        naive_write(seed, 1_000)
        e = naive_read(seed, N)
        report("naive  PK SELECT (baseline)", N, e)
        e = writer_read(seed, N)
        report("writer  PK SELECT (thread-local)", N, e)


if __name__ == "__main__":
    main()
