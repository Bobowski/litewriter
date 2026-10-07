#!/usr/bin/env python3
"""Where a shared-batch write spends time: Python hop vs SQLite vs user fn.

These runs pass ``isolated=False``. The library default is a savepoint.

Also dumps cProfile for the submit path, and micro-benches the inbox
and the result slot against ``concurrent.futures.Future``.
"""

import cProfile
import pstats
import time
from concurrent.futures import Future as ThreadFuture
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from litewriter import LiteWriter, Tx
from litewriter.inbox import Inbox, Slot

N = 8_000
HOP_N = 80_000
SCHEMA = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL) STRICT"


def noop(_tx: Tx) -> None:
    return None


def insert_row(sql: Tx, body: str) -> int:
    sql.execute("INSERT INTO t(body) VALUES (?)", (body,))
    return int(sql.last_insert_rowid())


def pct(part: int, total: int) -> str:
    if total <= 0:
        return "  n/a"
    return f"{100.0 * part / total:5.1f}%"


def report_row(name: str, ns: int, n: int, total: int) -> None:
    print(f"{name:<22}  {ns / n / 1e3:8.2f}  {pct(ns, total)}")


def run_profiled(path: Path, n: int, fn, *args: object) -> None:
    db = LiteWriter(path, hz=0, profile=True)
    db.execute_script(SCHEMA)
    db.start()
    t0 = time.perf_counter()
    futures = [db.submit(fn, *args, isolated=False) for _ in range(n)]
    for future in futures:
        future.result()
    wall = time.perf_counter() - t0
    db.close()
    s = db.stats.snapshot()
    python_ns = s.enqueue_ns + s.wake_ns + s.sleep_ns
    sqlite_ns = s.fn_ns + s.commit_ns
    accounted = python_ns + sqlite_ns
    print(f"jobs={s.jobs}  batches={s.batches}  wall={wall * 1e3:.1f} ms")
    print(f"{'phase':<22}  {'µs/job':>8}  {'share':>7}")
    report_row("enqueue (Python hop)", s.enqueue_ns, n, accounted)
    report_row("user fn (APSW)", s.fn_ns, n, accounted)
    report_row("BEGIN+COMMIT", s.commit_ns, n, accounted)
    report_row("wake (Python hop)", s.wake_ns, n, accounted)
    report_row("sleep", s.sleep_ns, n, accounted)
    report_row("Python hop (sum)", python_ns, n, accounted)
    report_row("SQLite (fn+txn)", sqlite_ns, n, accounted)
    report_row("accounted", accounted, n, accounted)


def run_inside_fn(path: Path, n: int) -> None:
    sql_ns = 0
    py_ns = 0

    def split_insert(tx: Tx, body: str) -> int:
        nonlocal sql_ns, py_ns
        t0 = time.perf_counter_ns()
        tx.execute("INSERT INTO t(body) VALUES (?)", (body,))
        sql_ns += time.perf_counter_ns() - t0
        t1 = time.perf_counter_ns()
        row_id = int(tx.last_insert_rowid())
        py_ns += time.perf_counter_ns() - t1
        return row_id

    db = LiteWriter(path, hz=0)
    db.execute_script(SCHEMA)
    db.start()
    futures = [db.submit(split_insert, str(i), isolated=False) for i in range(n)]
    for future in futures:
        future.result()
    db.close()
    print(f"{'inside insert_row':<22}  {'µs/job':>8}")
    print(f"{'  APSW execute':<22}  {sql_ns / n / 1e3:8.2f}")
    print(f"{'  last_insert+int':<22}  {py_ns / n / 1e3:8.2f}")


def run_cprofile(path: Path, n: int) -> None:
    db = LiteWriter(path, hz=0)
    db.execute_script(SCHEMA)
    db.start()
    profiler = cProfile.Profile()
    profiler.enable()
    futures = [db.submit(insert_row, str(i), isolated=False) for i in range(n)]
    for future in futures:
        future.result()
    profiler.disable()
    db.close()
    out = StringIO()
    stats = pstats.Stats(profiler, stream=out)
    stats.sort_stats("cumulative")
    stats.print_stats(18)
    print(out.getvalue())


def bench_inbox(cls, n: int) -> float:
    box = cls()
    t0 = time.perf_counter()
    for i in range(n):
        box.put(i)
    first = box.get()
    rest = box.take_rest()
    elapsed = time.perf_counter() - t0
    assert first == 0
    assert len(rest) == n - 1
    return elapsed


def bench_slot(n: int) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        slot = Slot()
        slot.set_result(i)
        assert slot.result() == i
    return time.perf_counter() - t0


def bench_future(n: int) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        future: ThreadFuture[int] = ThreadFuture()
        future.set_result(i)
        assert future.result() == i
    return time.perf_counter() - t0


def report_hop(name: str, n: int, elapsed: float) -> None:
    print(f"{name:42s}  {1e6 * elapsed / n:8.1f} µs   {n / elapsed:10.0f} /s")


def main() -> None:
    with TemporaryDirectory(prefix="litewriter-prof-") as raw:
        root = Path(raw)
        print("== phase timers: insert_row ==")
        run_profiled(root / "insert.sqlite3", N, insert_row, "x")
        print()
        print("== phase timers: noop (Python + empty txn) ==")
        run_profiled(root / "noop.sqlite3", N, noop)
        print()
        print("== inside the write function ==")
        run_inside_fn(root / "split.sqlite3", N)
        print()
        print("== hop micro (no SQLite) ==")
        report_hop("Inbox put+drain", HOP_N, bench_inbox(Inbox, HOP_N))
        report_hop("concurrent.futures.Future", HOP_N, bench_future(HOP_N))
        report_hop("Slot set+result", HOP_N, bench_slot(HOP_N))
        print()
        print("== cProfile submit + result (top 18) ==")
        run_cprofile(root / "c.sqlite3", N)


if __name__ == "__main__":
    main()
