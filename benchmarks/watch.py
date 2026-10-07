#!/usr/bin/env python3
"""Watch cost against a plain poll of the same query.

A watch is doing too much when a poll of that query is cheaper.
The writer must stay close to a run with no watch at all.
"""

import asyncio
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from litewriter.writer import LiteWriter
from litewriter.watch import Watch

N = 2_000
OTHERS = 32

SCHEMA = """
CREATE TABLE users(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    seen INTEGER NOT NULL
) STRICT;
CREATE TABLE mail(
    id INTEGER PRIMARY KEY,
    body TEXT NOT NULL
) STRICT;
INSERT INTO users(name, seen) VALUES ('Ada', 0);
"""

NAME = "SELECT name FROM users WHERE id = 1"
SEEN = "SELECT seen FROM users WHERE id = 1"


def insert_mail(tx, body: str) -> None:
    tx.execute("INSERT INTO mail(body) VALUES (?)", (body,))


def touch_seen(tx, seen: int) -> None:
    tx.execute("UPDATE users SET seen = ? WHERE id = 1", (seen,))


def touch_name(tx, name: str) -> None:
    tx.execute("UPDATE users SET name = ? WHERE id = 1", (name,))


def open_db(path: Path) -> LiteWriter:
    db = LiteWriter(path, hz=0)
    db.start()
    db.execute_script(SCHEMA)
    return db


def us(elapsed: float, n: int) -> str:
    return f"{1e6 * elapsed / n:8.2f} µs"


async def writes(db: LiteWriter, fn, n: int) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        await db.call(fn, i)
    return time.perf_counter() - t0


async def once(path: Path, sample) -> tuple[float, int]:
    reads = {"n": 0}
    original = Watch._read

    def counted(self: Watch):
        reads["n"] += 1
        return original(self)

    Watch._read = counted  # type: ignore[method-assign]
    try:
        db = open_db(path)
        try:
            elapsed = await sample(db, reads)
        finally:
            db.close()
    finally:
        Watch._read = original  # type: ignore[method-assign]
    return elapsed, reads["n"]


async def median(root: Path, name: str, sample) -> tuple[float, int]:
    elapsed = 0.0
    rereads = 0
    times: list[float] = []
    for index in range(3):
        elapsed, rereads = await once(root / f"{name}-{index}.sqlite3", sample)
        times.append(elapsed)
    times.sort()
    return times[1], rereads


async def bench() -> None:
    with TemporaryDirectory(prefix="litewriter-watch-") as raw:
        root = Path(raw)
        await once(
            root / "warm.sqlite3", lambda db, _reads: writes(db, insert_mail, 200)
        )

        async def bare(db: LiteWriter, _reads: dict[str, int]) -> float:
            return await writes(db, insert_mail, N)

        async def foreign(db: LiteWriter, _reads: dict[str, int]) -> float:
            lives = [db.watch(NAME) for _ in range(OTHERS)]
            entered = [await item.__aenter__() for item in lives]
            try:
                return await writes(db, insert_mail, N)
            finally:
                for item in entered:
                    await item.__aexit__(None, None, None)

        async def other_column(db: LiteWriter, reads: dict[str, int]) -> float:
            async with db.watch(NAME) as live:
                it = aiter(live)
                assert await anext(it) == [("Ada",)]
                reads["n"] = 0

                async def parked() -> None:
                    await anext(it)

                waiter = asyncio.create_task(parked())
                await asyncio.sleep(0)
                elapsed = await writes(db, touch_seen, N)
                await asyncio.sleep(0)
                waiter.cancel()
                return elapsed

        async def pull_each(db: LiteWriter, reads: dict[str, int]) -> float:
            async with db.watch(NAME) as live:
                it = aiter(live)
                assert await anext(it) == [("Ada",)]
                reads["n"] = 0
                t0 = time.perf_counter()
                for i in range(N):
                    await db.call(touch_name, f"Ada {i}")
                    await anext(it)
                return time.perf_counter() - t0

        async def burst(db: LiteWriter, reads: dict[str, int]) -> float:
            async with db.watch(NAME) as live:
                it = aiter(live)
                assert await anext(it) == [("Ada",)]
                reads["n"] = 0
                t0 = time.perf_counter()
                for i in range(N):
                    db.push(touch_name, f"Ada {i}")
                await db.call(touch_name, "Ada end")
                elapsed = time.perf_counter() - t0
                await anext(it)
                return elapsed

        async def poll(db: LiteWriter, _reads: dict[str, int]) -> float:
            t0 = time.perf_counter()
            for _ in range(N):
                db.query(NAME)
            return time.perf_counter() - t0

        bare_s, _ = await median(root, "bare", bare)
        foreign_s, foreign_reads = await median(root, "foreign", foreign)
        other_s, other_reads = await median(root, "other", other_column)
        each_s, each_reads = await median(root, "each", pull_each)
        burst_s, burst_reads = await median(root, "burst", burst)
        poll_s, _ = await median(root, "poll", poll)

        print(f"write mail, no watch                 {us(bare_s, N)}")
        print(
            f"write mail, {OTHERS} watches on name       "
            f"{us(foreign_s, N)}  rereads {foreign_reads}"
        )
        print(
            f"update seen, name watch is waiting  {us(other_s, N)}  "
            f"rereads {other_reads}"
        )
        print(
            f"update name, pull every commit      {us(each_s, N)}  rereads {each_reads}"
        )
        print(
            f"burst name updates, one pull        {us(burst_s, N + 1)}  "
            f"rereads {burst_reads}"
        )
        print(f"poll the name query                 {us(poll_s, N)}")
        print(
            f"tax for {OTHERS} watches on another table  "
            f"{1e6 * (foreign_s - bare_s) / N:8.2f} µs/write"
        )
        print(f"one poll, for scale                 {1e6 * poll_s / N:8.2f} µs")


if __name__ == "__main__":
    asyncio.run(bench())
