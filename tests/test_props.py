"""Properties that cover a wide part of the library.

SQL trees must match SQLite, including the builder. A batch must match
the isolation rule, and the default is a savepoint. Each thread reads
on its own connection, including before the writer starts. Literals,
names, null tests, timeouts, and a closed file do too.
"""

import operator
import re
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, BrokenBarrierError, Event, Lock, Thread
from typing import cast

import apsw
import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from conftest import SCHEMA
from litewriter import (
    Delete,
    Expr,
    Insert,
    LiteWriter,
    Select,
    Tx,
    UnionAll,
    Update,
    WriterError,
    WriterRolledBack,
    col,
    lit,
    sql,
)
from litewriter.claim import validate_timeout

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


_Built = tuple[object, Callable[[int], int]]
_EXPR_OPS = ("+", "-", "*", "&", "|", "=", "!=", "<", "<=", ">", ">=")
_CMP_METHODS = ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__")
_OP = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "&": operator.and_,
    "|": operator.or_,
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}
_KEYWORDS = frozenset(word.upper() for word in apsw.keywords)
_KEYS = frozenset(
    {
        "with",
        "with_recursive",
        "select",
        "select_distinct",
        "from",
        "join",
        "where",
        "group_by",
        "having",
        "order_by",
        "limit",
        "offset",
        "union",
        "union_all",
        "intersect",
        "except",
        "insert_into",
        "columns",
        "values",
        "on_conflict",
        "do_nothing",
        "do_update_set",
        "returning",
        "update",
        "set",
        "delete_from",
    }
)
_CHARS = st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00")
_NAME = st.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,12}", fullmatch=True)
_SECONDS = st.one_of(
    st.integers(0, 30),
    st.floats(0, 30, allow_nan=False, allow_infinity=False),
)
_BAD_NUMBER = st.one_of(
    st.just(True),
    st.just(False),
    st.just(float("nan")),
    st.just(float("inf")),
    st.just(float("-inf")),
    st.just(10**400),
    st.none(),
    st.text(alphabet="abc", max_size=4),
    st.integers(max_value=-1),
    st.floats(max_value=-1e-9, allow_nan=False, allow_infinity=False),
)


def _const(value: int) -> _Built:
    def ev(_n: int) -> int:
        return value

    return value, ev


def _column() -> _Built:
    return col("n"), lambda n: n


def _apply(name: str, left: _Built, right: _Built) -> _Built:
    fn = _OP[name]
    lexpr, lfn = left
    rexpr, rfn = right
    built = fn(lexpr, rexpr)

    def ev(n: int) -> int:
        return int(fn(lfn(n), rfn(n)))

    return built, ev


def _unary(name: str, child: _Built) -> _Built:
    expr, fn = child
    if name == "neg":
        built: object = -expr if isinstance(expr, Expr) else ("-", expr)

        def ev(n: int) -> int:
            return -fn(n)

        return built, ev
    built = +expr if isinstance(expr, Expr) else ("+", expr)

    def ev_pos(n: int) -> int:
        return fn(n)

    return built, ev_pos


def _expr_trees() -> st.SearchStrategy[_Built]:
    leaf: st.SearchStrategy[_Built] = st.one_of(
        st.integers(-6, 6).map(_const),
        st.just(_column()),
    )

    def extend(inner: st.SearchStrategy[_Built]) -> st.SearchStrategy[_Built]:
        return st.one_of(
            st.builds(_apply, st.sampled_from(_EXPR_OPS), inner, inner),
            st.builds(_unary, st.sampled_from(("neg", "pos")), inner),
        )

    return st.recursive(leaf, extend, max_leaves=5)


def _as_expr(built: _Built) -> tuple[Expr, Callable[[int], int]]:
    expr, fn = built
    if isinstance(expr, Expr):
        return expr, fn
    return col("n") - col("n") + expr, fn


def _memory() -> apsw.Connection:
    conn = apsw.Connection(":memory:")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
    return conn


@given(built=_expr_trees(), n=st.integers(-6, 6))
@_FAST
def test_an_expression_matches_sqlite(built: _Built, n: int) -> None:
    expr, fn = _as_expr(built)
    conn = _memory()
    try:
        conn.execute("INSERT INTO t(n) VALUES (?)", (n,))
        row = conn.execute(sql(Select(expr, from_="t"))).fetchone()
    finally:
        conn.close()
    assert row == (fn(n),)
    for method in _CMP_METHODS:
        with pytest.raises(WriterError, match="None"):
            getattr(expr, method)(None)
    assert "IS NULL" in sql(Select(expr.is_(None)))
    assert "IS NOT NULL" in sql(Select(expr.is_not(None)))


@given(
    values=st.lists(st.one_of(st.none(), st.integers(-5, 5)), max_size=8),
    probe=st.integers(-5, 5),
)
@_FAST
def test_null_tests_match_the_rows(values: list[int | None], probe: int) -> None:
    conn = _memory()
    try:
        for value in values:
            conn.execute("INSERT INTO t(n) VALUES (?)", (value,))
        missing = [
            index for index, value in enumerate(values, start=1) if value is None
        ]
        present = [
            index for index, value in enumerate(values, start=1) if value is not None
        ]
        zeros = [index for index, value in enumerate(values, start=1) if value == probe]
        assert list(
            conn.execute(sql(Select("id", from_="t", where=col("n").is_(None))))
        ) == [(index,) for index in missing]
        assert list(
            conn.execute(sql(Select("id", from_="t", where=col("n").is_not(None))))
        ) == [(index,) for index in present]
        assert (
            list(conn.execute(sql(Select("id", from_="t", where=("=", "n", None)))))
            == []
        )
        assert list(
            conn.execute(sql(Select("id", from_="t", where=col("n") == probe)))
        ) == [(index,) for index in zeros]
    finally:
        conn.close()


@given(
    text=st.text(alphabet=_CHARS, max_size=24),
    blob=st.binary(max_size=16),
    flag=st.booleans(),
    number=st.integers(-(10**6), 10**6),
    point=st.floats(allow_nan=False, allow_infinity=False, width=64),
)
@_FAST
def test_literals_round_trip(
    text: str, blob: bytes, flag: bool, number: int, point: float
) -> None:
    conn = apsw.Connection(":memory:")
    try:
        row = conn.execute(
            sql(Select(lit(text), blob, flag, number, point, None))
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row == (text, blob, int(flag), number, point, None)


@given(
    prefix=st.text(alphabet=_CHARS, max_size=8),
    suffix=st.text(alphabet=_CHARS, max_size=8),
)
@_FAST
def test_a_nul_in_a_literal_raises(prefix: str, suffix: str) -> None:
    with pytest.raises(WriterError, match="NUL"):
        sql(Select(lit(prefix + "\x00" + suffix)))


@given(name=_NAME)
@_FAST
def test_a_name_round_trips(name: str) -> None:
    conn = _memory()
    try:
        conn.execute(f'CREATE TABLE u("{name}" INTEGER NOT NULL)')
        conn.execute(f'INSERT INTO u("{name}") VALUES (7)')
        text = sql(Select(col(name), from_="u"))
        if name.upper() in _KEYWORDS:
            assert f'"{name}"' in text
        assert conn.execute(text).fetchone() == (7,)
    finally:
        conn.close()


@example("replace_into")
@given(
    key=st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=16).filter(
        lambda key: key not in _KEYS
    )
)
@_FAST
def test_an_unknown_key_does_not_render(key: str) -> None:
    with pytest.raises(WriterError, match="unknown"):
        sql({"insert_into": "t", "values": [{"body": "a"}], key: 1})


@given(
    original=st.lists(_TEXT, min_size=1, max_size=4),
    replacement=st.lists(_TEXT, min_size=1, max_size=4),
)
@_FAST
def test_do_update_matches_the_rows(
    original: list[str], replacement: list[str]
) -> None:
    conn = _memory()
    try:
        conn.execute(
            sql(Insert("t", values=[{"body": ("lit", body)} for body in original]))
        )
        payload = [
            {"id": index, "body": ("lit", body)}
            for index, body in enumerate(replacement, start=1)
        ]
        conn.execute(
            sql(
                Insert(
                    "t",
                    values=payload,
                    on_conflict=("id",),
                    do_update={"body": "excluded.body"},
                )
            )
        )
        expect = list(original)
        for index, body in enumerate(replacement):
            if index < len(expect):
                expect[index] = body
            else:
                expect.append(body)
        got = [row[0] for row in conn.execute("SELECT body FROM t ORDER BY id")]
        assert got == expect
    finally:
        conn.close()


def _pragma(tx: Tx) -> int:
    return int(tx.value("PRAGMA busy_timeout"))


@given(seconds=_SECONDS)
@_DB
def test_busy_timeout_reaches_every_connection(seconds: float) -> None:
    expect = round(float(seconds) * 1000)
    with TemporaryDirectory() as raw:
        db = LiteWriter(Path(raw) / "t.sqlite3", hz=0, busy_timeout=seconds)
        db.execute_script(SCHEMA)
        db.start()
        try:
            assert db.value("PRAGMA busy_timeout") == expect
            assert db.submit(_pragma).result(timeout=2) == expect
            with db.reader() as reader:
                assert reader.value("PRAGMA busy_timeout") == expect
        finally:
            db.close()


@given(
    bad=st.one_of(
        _BAD_NUMBER,
        st.integers(min_value=2_147_484),
        st.floats(
            min_value=2_147_484, max_value=1e18, allow_nan=False, allow_infinity=False
        ),
    )
)
@_FAST
def test_busy_timeout_rejects_a_bad_value(bad: object) -> None:
    with pytest.raises(WriterError, match="busy_timeout"):
        LiteWriter("unused.sqlite3", busy_timeout=cast(float, bad))


@given(timeout=_SECONDS)
@_FAST
def test_a_claim_timeout_is_a_count_of_seconds(timeout: float) -> None:
    assert validate_timeout(timeout) == float(timeout)


@given(bad=_BAD_NUMBER)
@_FAST
def test_a_claim_timeout_rejects_a_bad_value(bad: object) -> None:
    with pytest.raises(WriterError, match="claim timeout"):
        validate_timeout(bad)


@given(rate=_SECONDS)
@_FAST
def test_hz_stores_a_finite_rate(rate: float) -> None:
    db = LiteWriter("unused.sqlite3", hz=rate)
    try:
        assert db.hz == rate
    finally:
        db.close()


@given(bad=_BAD_NUMBER)
@_FAST
def test_hz_rejects_a_bad_rate(bad: object) -> None:
    with pytest.raises(WriterError, match="hz"):
        LiteWriter("unused.sqlite3", hz=cast(float, bad))


@given(bodies=st.lists(_TEXT, max_size=5))
@_DB
def test_close_keeps_the_rows_and_folds_the_wal(bodies: list[str]) -> None:
    with TemporaryDirectory() as raw:
        path = Path(raw) / "t.sqlite3"
        db = LiteWriter(path, hz=0)
        db.execute_script(SCHEMA)
        db.start()
        try:
            for body in bodies:
                db.submit(_insert, body).result(timeout=2)
        finally:
            db.close()
        wal = path.with_name(path.name + "-wal")
        assert not wal.exists() or wal.stat().st_size == 0
        conn = apsw.Connection(str(path))
        try:
            got = [row[0] for row in conn.execute("SELECT body FROM t ORDER BY id")]
        finally:
            conn.close()
        assert got == bodies
