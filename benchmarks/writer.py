#!/usr/bin/env python3
"""LiteWriter shapes: group drain, hz floor, submit vs push."""

import time
from pathlib import Path
from tempfile import TemporaryDirectory

from litewriter import LiteWriter, Tx

N = 4_000
SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"


def insert(tx: Tx, body: str) -> int:
    tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)
    return int(tx.last_insert_rowid())


def report(name: str, n: int, elapsed: float) -> None:
    print(f"{name:36s}  {1e6 * elapsed / n:8.1f} µs/job   {n / elapsed:10.0f} /s")


def run(path: Path, *, hz: float, wait: bool, n: int) -> float:
    db = LiteWriter(path, hz=hz, schema=SCHEMA)
    db.start()
    t0 = time.perf_counter()
    if wait:
        futures = [db.submit(insert, str(i)) for i in range(n)]
        for future in futures:
            future.result()
    else:
        for i in range(n):
            db.push(insert, str(i))
        # last waited insert so we know the queue drained
        db.submit(insert, "tail").result()
        n += 1
    elapsed = time.perf_counter() - t0
    db.close()
    return elapsed


def main() -> None:
    with TemporaryDirectory(prefix="litewriter-bench-") as raw:
        root = Path(raw)
        e = run(root / "group.sqlite3", hz=0, wait=True, n=N)
        report("submit hz=0  (group under load)", N, e)
        e = run(root / "push.sqlite3", hz=0, wait=False, n=N)
        report("push   hz=0  (no Future)", N, e)
        e = run(root / "hz.sqlite3", hz=60, wait=True, n=240)
        report("submit hz=60 (240 jobs)", 240, e)

        db = LiteWriter(root / "seq.sqlite3", hz=0, schema=SCHEMA)
        db.start()
        t0 = time.perf_counter()
        for i in range(N):
            db.submit(insert, str(i)).result()
        elapsed = time.perf_counter() - t0
        db.close()
        report("submit hz=0  sequential await", N, elapsed)


if __name__ == "__main__":
    main()
