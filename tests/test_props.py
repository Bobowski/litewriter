"""Properties that cover a wide part of the library.

SQL trees must match SQLite, including the builder. A batch must match
the isolation rule, and the default is a savepoint. Each thread reads
on its own connection, including before the writer starts.
"""

import operator
import re
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, BrokenBarrierError, Event, Lock, Thread

import apsw
import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from conftest import SCHEMA
from litewriter import (
    Delete,
    Insert,
    LiteWriter,
    Select,
    Tx,
    UnionAll,
    Update,
    WriterRolledBack,
    sql,
)

_FAST = settings(max_examples=60, deadline=None)
_DB = settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
_BIN = ("+", "-", "*", "&", "|", "=", "!=", "<", "<=", ">", ">=")
_ARITH = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "&": operator.and_,
    "|": operator.or_,
}
_CMP = {
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}


def _trees(column: str | None) -> st.SearchStrategy[object]:
    leaf: st.SearchStrategy[object] = st.integers(-6, 6)
    if column is not None:
        leaf = st.one_of(leaf, st.just(column))

    def extend(inner: st.SearchStrategy[object]) -> st.SearchStrategy[object]:
        pair = st.tuples(st.sampled_from(_BIN), inner, inner)
        unary = st.one_of(
            st.tuples(st.just("-"), inner),
            st.tuples(st.just("+"), inner),
            st.tuples(st.just("not"), inner),
            st.tuples(st.just("abs"), inner),
            st.tuples(st.just("cast"), inner, st.just("integer")),
            st.tuples(st.just("collate"), inner, st.just("binary")),
            st.tuples(st.just("coalesce"), st.none(), inner),
        )
        triple = st.one_of(
            st.tuples(st.just("and"), inner, inner),
            st.tuples(st.just("or"), inner, inner),
            st.tuples(st.just("min"), inner, inner),
            st.tuples(st.just("max"), inner, inner),
            st.tuples(st.sampled_from(("between", "not between")), inner, inner, inner),
            st.tuples(st.just("iif"), inner, inner, inner),
            st.tuples(st.just("case"), inner, inner, inner),
        )
        member = st.builds(
            _membership,
            st.booleans(),
            inner,
            st.lists(inner, min_size=1, max_size=3),
        )
        return st.one_of(pair, unary, triple, member)

    return st.recursive(leaf, extend, max_leaves=6)


def _membership(
    negated: bool, value: object, options: list[object]
) -> tuple[object, ...]:
    name = "not in" if negated else "in"
    return (name, value, *options)


def _eval(tree: object, env: Mapping[str, int] | None = None) -> int:
    if type(tree) is int:
        return tree
    if type(tree) is str:
        assert env is not None
        return env[tree]
    if not isinstance(tree, tuple) or not tree or type(tree[0]) is not str:
        raise AssertionError(tree)
    op = tree[0]
    args = tree[1:]
    if op == "cast":
        return int(_eval(args[0], env))
    if op == "collate":
        return _eval(args[0], env)
    if op == "coalesce":
        for arg in args:
            if arg is not None:
                return _eval(arg, env)
        raise AssertionError(tree)
    if op == "case":
        pairs = args[:-1] if len(args) % 2 else args
        for index in range(0, len(pairs), 2):
            if _eval(pairs[index], env):
                return _eval(pairs[index + 1], env)
        return _eval(args[-1], env)
    vals = [_eval(arg, env) for arg in args]
    if op in {"+", "-"} and len(vals) == 1:
        return vals[0] if op == "+" else -vals[0]
    if op == "not":
        return int(not vals[0])
    if op == "abs":
        return abs(vals[0])
    if op == "min":
        return min(vals)
    if op == "max":
        return max(vals)
    if op == "iif":
        return vals[1] if vals[0] else vals[2]
    if op == "and":
        return int(all(vals))
    if op == "or":
        return int(any(vals))
    if op == "between":
        return int(vals[1] <= vals[0] <= vals[2])
    if op == "not between":
        return int(not (vals[1] <= vals[0] <= vals[2]))
    if op == "in":
        return int(vals[0] in vals[1:])
    if op == "not in":
        return int(vals[0] not in vals[1:])
    if op in _ARITH:
        return _ARITH[op](vals[0], vals[1])
    return int(_CMP[op](vals[0], vals[1]))


@given(tree=_trees(None))
@_FAST
def test_sql_tree_matches_sqlite(tree: object) -> None:
    conn = apsw.Connection(":memory:")
    text = sql({"select": [tree]})
    row = conn.execute(text).fetchone()
    conn.close()
    assert row == (_eval(tree),)


@given(
    rows=st.lists(st.integers(-5, 5), max_size=5),
    tags=st.dictionaries(
        st.integers(-5, 5),
        st.text(alphabet="abc", min_size=1, max_size=3),
        max_size=4,
    ),
    tree=_trees("t.n"),
    extra=st.one_of(st.none(), _trees("t.n")),
    desc=st.booleans(),
    limit=st.integers(0, 4),
    offset=st.integers(0, 4),
)
@_FAST
def test_builder_query_matches_the_rows(
    rows: list[int],
    tags: dict[int, str],
    tree: object,
    extra: object,
    desc: bool,
    limit: int,
    offset: int,
) -> None:
    conn = apsw.Connection(":memory:")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
    conn.execute("CREATE TABLE u(n INTEGER PRIMARY KEY, tag TEXT NOT NULL)")
    for number in rows:
        conn.execute("INSERT INTO t(n) VALUES (?)", (number,))
    for number, tag in tags.items():
        conn.execute("INSERT INTO u(n, tag) VALUES (?, ?)", (number, tag))
    query = Select(
        "t.id",
        "t.n",
        "u.tag",
        from_=("as", "t", "t"),
        join=("left", ("as", "u", "u"), ("=", "t.n", "u.n")),
        where=tree,
        order_by=("desc" if desc else "asc", "t.id"),
        limit=limit,
        offset=offset,
    )
    if extra is not None:
        query = query.where(extra)
    got = list(conn.execute(sql(query)))
    conn.close()
    matched: list[tuple[int, int, str | None]] = []
    for index, number in enumerate(rows, start=1):
        env = {"t.n": number}
        if _eval(tree, env) and (extra is None or _eval(extra, env)):
            matched.append((index, number, tags.get(number)))
    matched.sort(key=lambda item: item[0], reverse=desc)
    assert got == matched[offset : offset + limit]


_TEXT = st.text(alphabet="abcde' ", min_size=0, max_size=8)


@given(bodies=st.lists(_TEXT, min_size=1, max_size=4))
@_FAST
def test_write_statements_match_sqlite(bodies: list[str]) -> None:
    conn = apsw.Connection(":memory:")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
    inserted = Insert(
        "t",
        values=[{"body": ("lit", body)} for body in bodies],
        returning="body",
    )
    assert [row[0] for row in conn.execute(sql(inserted))] == bodies
    changed = Update(
        "t",
        set={"body": ("lit", bodies[0] + "z")},
        where=("=", "id", 1),
        returning="body",
    )
    assert list(conn.execute(sql(changed))) == [(bodies[0] + "z",)]
    removed = Delete("t", where=("=", "id", 1), returning="id")
    assert list(conn.execute(sql(removed))) == [(1,)]
    rest = bodies[1:]
    assert [row[0] for row in conn.execute("SELECT body FROM t ORDER BY id")] == rest
    if rest:
        both = UnionAll(Select("body", from_="t"), Select("body", from_="t"))
        got = [row[0] for row in conn.execute(sql(both))]
        assert got == rest + rest
        kept = conn.execute("SELECT id, body FROM t ORDER BY id").fetchone()
        assert kept is not None
        again = Insert(
            "t",
            values={"id": kept[0], "body": ("lit", "nope")},
            on_conflict=["id"],
            do_nothing=True,
        )
        conn.execute(sql(again))
        assert [
            row[0] for row in conn.execute("SELECT body FROM t ORDER BY id")
        ] == rest
    conn.close()


def _write(tx: Tx, body: str, fail: bool) -> None:
    if fail:
        raise ValueError(body)
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))


def _alone(flag: bool | None) -> bool:
    return True if flag is None else flag


def _batch_expect(
    jobs: list[tuple[str, bool, bool | None]],
) -> tuple[list[str], list[str]]:
    boom: int | None = None
    for index, (_body, fail, flag) in enumerate(jobs):
        if fail and not _alone(flag):
            boom = index
            break
    if boom is not None:
        kinds = ["rollback"] * len(jobs)
        kinds[boom] = "value"
        return [], kinds
    disk = [body for body, fail, _flag in jobs if not fail]
    kinds = ["value" if fail else "ok" for _body, fail, _flag in jobs]
    return disk, kinds


@example([("a", False, None), ("b", True, None), ("c", False, None)])
@example([("a", False, True), ("b", True, False)])
@given(
    jobs=st.lists(
        st.tuples(
            st.text(alphabet="abcdefghij", min_size=1, max_size=4),
            st.booleans(),
            st.one_of(st.none(), st.booleans()),
        ),
        min_size=1,
        max_size=6,
        unique_by=lambda item: item[0],
    )
)
@_DB
def test_a_batch_matches_the_isolation_rule(
    jobs: list[tuple[str, bool, bool | None]],
) -> None:
    disk, kinds = _batch_expect(jobs)
    with TemporaryDirectory() as raw:
        db = LiteWriter(Path(raw) / "t.sqlite3", hz=0)
        db.execute_script(SCHEMA)
        db.start()
        try:
            holding = Event()
            release = Event()

            def hold(_tx: object) -> None:
                holding.set()
                release.wait(timeout=2)

            def wait() -> None:
                db.submit(hold).result(timeout=2)

            waiter = Thread(target=wait)
            waiter.start()
            assert holding.wait(timeout=2)
            slots = []
            for body, fail, flag in jobs:
                if flag is None:
                    slots.append(db.submit(_write, body, fail))
                else:
                    slots.append(db.submit(_write, body, fail, isolated=flag))
            release.set()
            waiter.join(timeout=2)
            assert not waiter.is_alive()
            for slot, kind, (body, _fail, _flag) in zip(
                slots, kinds, jobs, strict=True
            ):
                if kind == "ok":
                    assert slot.result(timeout=2) is None
                elif kind == "value":
                    with pytest.raises(ValueError, match=f"^{re.escape(body)}$"):
                        slot.result(timeout=2)
                else:
                    with pytest.raises(WriterRolledBack):
                        slot.result(timeout=2)
            got = [row[0] for row in db.execute("SELECT body FROM t ORDER BY id")]
            assert got == disk
        finally:
            release.set()
            db.close()


def _probe(db: LiteWriter, expect: list[tuple[str, ...]], threads: int) -> None:
    found: list[tuple[int, int]] = []
    errors: list[BaseException] = []
    lock = Lock()
    barrier = Barrier(threads)

    def work() -> None:
        reader = None
        try:
            first = db.conn()
            assert db.conn() is first
            reader = db.reader()
            owned = reader._conn  # pyright: ignore[reportPrivateUsage]
            assert owned is not None
            assert owned is not first
            assert reader.query("SELECT body FROM t ORDER BY id") == expect
            with pytest.raises(apsw.ReadOnlyError):
                reader.execute("INSERT INTO t(body) VALUES ('no')")
            with lock:
                found.append((id(first), id(owned)))
            barrier.wait(timeout=2)
            with pytest.raises(apsw.ReadOnlyError):
                db.execute("INSERT INTO t(body) VALUES ('no')")
            assert list(db.execute("SELECT body FROM t ORDER BY id")) == expect
        except BaseException as exc:
            errors.append(exc)
            with suppress(BrokenBarrierError):
                barrier.abort()
        finally:
            if reader is not None:
                reader.close()

    workers = [Thread(target=work) for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2)
    assert errors == []
    assert all(not worker.is_alive() for worker in workers)
    assert len(found) == threads
    conns = [item[0] for item in found]
    readers = [item[1] for item in found]
    assert len(set(conns)) == threads
    assert len(set(readers)) == threads
    assert set(conns).isdisjoint(readers)


def _insert(tx: Tx, body: str) -> None:
    tx.execute("INSERT INTO t(body) VALUES (?)", (body,))


@example(["alpha", "beta"], 2)
@given(
    bodies=st.lists(
        st.text(alphabet="abcdefghij", min_size=1, max_size=6),
        min_size=1,
        max_size=6,
        unique=True,
    ),
    threads=st.integers(2, 4),
)
@_DB
def test_each_thread_has_its_own_read_connection(
    bodies: list[str], threads: int
) -> None:
    expect = [(body,) for body in bodies]
    with TemporaryDirectory() as raw:
        path = Path(raw) / "t.sqlite3"
        running = LiteWriter(path, hz=0)
        running.execute_script(SCHEMA)
        running.start()
        try:
            for body in bodies:
                running.submit(_insert, body).result(timeout=2)
            _probe(running, expect, threads)
        finally:
            running.close()
        idle = LiteWriter(path, hz=0)
        try:
            _probe(idle, expect, threads)
        finally:
            idle.close()
