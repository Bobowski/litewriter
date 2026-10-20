"""Properties that cover a wide part of the library.

Expression trees must match SQLite. A batch must match the isolation
rule — every job gets its own savepoint. Each thread reads on its own
connection. Literals, names, timeouts, and a closed file do too.
"""

import operator
import re
from collections.abc import Callable
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
    Insert,
    LiteWriter,
    QueryError,
    Select,
    Tx,
    Update,
    col,
    lit,
    param,
    raw,
)
from litewriter.claim import validate_timeout
from litewriter.expr import Expr

_FAST = settings(max_examples=60, deadline=None)
_DB = settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

_Built = tuple[Expr, Callable[[int], int]]


def _const(value: int) -> _Built:
    def ev(_n: int) -> int:
        return value

    return raw(str(value)), ev


def _column() -> _Built:
    return col("n"), lambda n: n


def _bit(op: str, left: _Built, right: _Built) -> _Built:
    lexpr, lfn = left
    rexpr, rfn = right
    built = lexpr.op(op, rexpr)
    fn = operator.and_ if op == "&" else operator.or_

    def ev(n: int) -> int:
        return fn(lfn(n), rfn(n))

    return built, ev


def _apply(name: str, left: _Built, right: _Built) -> _Built:
    lexpr, lfn = left
    rexpr, rfn = right
    if name == "and":
        built = lexpr & rexpr

        def ev(n: int) -> int:
            return int(bool(lfn(n)) and bool(rfn(n)))

        return built, ev
    if name == "or":
        built = lexpr | rexpr

        def ev(n: int) -> int:
            return int(bool(lfn(n)) or bool(rfn(n)))

        return built, ev
    fn = _OP[name]
    built = fn(lexpr, rexpr)

    def ev(n: int) -> int:
        return int(fn(lfn(n), rfn(n)))

    return built, ev


def _negate(child: _Built) -> _Built:
    expr, fn = child
    built = -expr

    def ev(n: int) -> int:
        return -fn(n)

    return built, ev


def _not(child: _Built) -> _Built:
    expr, fn = child
    built = ~expr

    def ev(n: int) -> int:
        return int(not fn(n))

    return built, ev


def _call(name: str, parts: list[_Built]) -> _Built:
    exprs = [expr for expr, _fn in parts]
    fns = [fn for _expr, fn in parts]

    def evs(n: int) -> list[int]:
        return [fn(n) for fn in fns]

    if name == "min":
        return _fnc("min", exprs), lambda n: min(evs(n))
    if name == "max":
        return _fnc("max", exprs), lambda n: max(evs(n))
    if name == "abs":
        return _fnc("abs", exprs[:1]), lambda n: abs(evs(n)[0])
    if name == "iif":
        return _fnc("iif", exprs[:3]), lambda n: evs(n)[1] if evs(n)[0] else evs(n)[2]
    if name == "coalesce":
        return _fnc("coalesce", exprs), lambda n: evs(n)[0]
    raise AssertionError(name)


def _fnc(name: str, args: list[Expr]) -> Expr:
    from litewriter import fn

    return fn(name, *args)


def _between(negated: bool, value: _Built, low: _Built, high: _Built) -> _Built:
    expr, vfn = value
    lo, lfn = low
    hi, hfn = high
    built = expr.not_between(lo, hi) if negated else expr.between(lo, hi)

    def ev(n: int) -> int:
        inside = lfn(n) <= vfn(n) <= hfn(n)
        return int(not inside) if negated else int(inside)

    return built, ev


def _expr_trees() -> st.SearchStrategy[_Built]:
    leaf: st.SearchStrategy[_Built] = st.one_of(
        st.integers(-6, 6).map(_const),
        st.just(_column()),
    )

    def extend(inner: st.SearchStrategy[_Built]) -> st.SearchStrategy[_Built]:
        return st.one_of(
            st.builds(
                _apply,
                st.sampled_from(
                    ("+", "-", "*", "and", "or", "=", "!=", "<", "<=", ">", ">=")
                ),
                inner,
                inner,
            ),
            st.builds(_bit, st.sampled_from(("&", "|")), inner, inner),
            st.builds(_negate, inner),
            st.builds(_not, inner),
            st.builds(_between, st.booleans(), inner, inner, inner),
            st.builds(
                lambda name, a, b: _call(name, [a, b]),
                st.sampled_from(("min", "max", "coalesce")),
                inner,
                inner,
            ),
            st.builds(lambda a: _call("abs", [a]), inner),
            st.builds(lambda a, b, c: _call("iif", [a, b, c]), inner, inner, inner),
        )

    return st.recursive(leaf, extend, max_leaves=5)


_OP = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}
_CMP_METHODS = ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__")
_KEYWORDS = frozenset(word.upper() for word in apsw.keywords)
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


def _memory() -> apsw.Connection:
    conn = apsw.Connection(":memory:")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
    return conn


@example(_apply("=", _const(0), _negate(_const(0))), 0)
@example(_apply("*", _const(2), _negate(_const(0))), 0)
@example(_apply("*", _const(0), _negate(_const(0))), 0)
@example(_apply("+", _const(1), _negate(_const(2))), 3)
@given(built=_expr_trees(), n=st.integers(-6, 6))
@_FAST
def test_an_expression_matches_sqlite(built: _Built, n: int) -> None:
    expr, fn = built
    conn = _memory()
    try:
        conn.execute("INSERT INTO t(n) VALUES (:n)", {"n": n})
        q = Select(expr, from_="t")
        row = conn.execute(q.sql, q.params).fetchone()
    finally:
        conn.close()
    assert row == (fn(n),)
    for method in _CMP_METHODS:
        with pytest.raises(QueryError, match="None"):
            getattr(expr, method)(None)
    assert "IS NULL" in Select(expr.is_(None)).sql
    assert "IS NOT NULL" in Select(expr.is_not(None)).sql


@given(
    values=st.lists(st.one_of(st.none(), st.integers(-5, 5)), max_size=8),
    probe=st.integers(-5, 5),
)
@_FAST
def test_null_tests_match_the_rows(values: list[int | None], probe: int) -> None:
    conn = _memory()
    try:
        for value in values:
            conn.execute("INSERT INTO t(n) VALUES (:n)", {"n": value})
        missing = [
            index for index, value in enumerate(values, start=1) if value is None
        ]
        present = [
            index for index, value in enumerate(values, start=1) if value is not None
        ]
        zeros = [index for index, value in enumerate(values, start=1) if value == probe]

        def ids(where: Expr) -> list[tuple[int, ...]]:
            q = Select("id", from_="t", where=where)
            return [tuple(map(int, row)) for row in conn.execute(q.sql, q.params)]

        assert ids(col("n").is_(None)) == [(i,) for i in missing]
        assert ids(col("n").is_not(None)) == [(i,) for i in present]
        assert ids(col("n") == probe) == [(i,) for i in zeros]
    finally:
        conn.close()


@given(
    rows=st.lists(st.integers(-5, 5), max_size=5),
    tags=st.dictionaries(
        st.integers(-5, 5),
        st.text(alphabet="abc", min_size=1, max_size=3),
        max_size=4,
    ),
    tree=_expr_trees(),
    desc=st.booleans(),
    limit=st.integers(0, 4),
    offset=st.integers(0, 4),
)
@_FAST
def test_builder_query_matches_the_rows(
    rows: list[int],
    tags: dict[int, str],
    tree: _Built,
    desc: bool,
    limit: int,
    offset: int,
) -> None:
    expr, eval_tree = tree
    # The column leaf in the tree is "n"; alias t so t.n matches.
    expr = _swap(expr)
    conn = apsw.Connection(":memory:")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
    conn.execute("CREATE TABLE u(n INTEGER PRIMARY KEY, tag TEXT NOT NULL)")
    for number in rows:
        conn.execute("INSERT INTO t(n) VALUES (:n)", {"n": number})
    for number, tag in tags.items():
        conn.execute(
            "INSERT INTO u(n, tag) VALUES (:n, :tag)", {"n": number, "tag": tag}
        )
    query = Select(
        "t.id",
        "t.n",
        "u.tag",
        from_="t",
        join=("left", "u", col("t.n") == col("u.n")),
        where=expr,
        order_by=col("t.id").desc() if desc else col("t.id").asc(),
        limit=limit,
        offset=offset,
    )
    got = list(conn.execute(query.sql, query.params))
    conn.close()
    matched: list[tuple[int, int, str | None]] = []
    for index, number in enumerate(rows, start=1):
        if eval_tree(number):
            matched.append((index, number, tags.get(number)))
    matched.sort(key=lambda item: item[0], reverse=desc)
    assert got == matched[offset : offset + limit]


def _swap(expr: Expr) -> Expr:
    """Rewrite the column leaf n -> t.n so it reads the joined table."""
    data = expr.data
    if data[0] == "col" and data[1] == "n":
        return col("t.n")
    changed = tuple(_swap_item(item) for item in data)
    return Expr(cast("tuple[object, ...]", changed))


def _swap_item(item: object) -> object:
    if isinstance(item, Expr):
        return _swap(item)
    if isinstance(item, tuple | list):
        return type(item)(_swap_item(x) for x in item)
    return item


_TEXT = st.text(alphabet="abcde' ", min_size=0, max_size=8)


@given(bodies=st.lists(_TEXT, min_size=1, max_size=4))
@_FAST
def test_write_statements_match_sqlite(bodies: list[str]) -> None:
    conn = apsw.Connection(":memory:")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
    inserted = Insert("t", values=[{"body": body} for body in bodies])
    conn.execute(inserted.sql, inserted.params)
    assert [row[0] for row in conn.execute("SELECT body FROM t")] == bodies
    changed = Update(
        "t",
        set={"body": bodies[0] + "z"},
        where=col("id") == 1,
        returning="body",
    )
    assert list(conn.execute(changed.sql, changed.params)) == [(bodies[0] + "z",)]
    removed = Delete("t", where=col("id") == 1, returning="id")
    assert list(conn.execute(removed.sql, removed.params)) == [(1,)]
    rest = bodies[1:]
    assert [row[0] for row in conn.execute("SELECT body FROM t ORDER BY id")] == rest
    if rest:
        both = Select("body", from_="t").union_all(Select("body", from_="t"))
        got = [row[0] for row in conn.execute(both.sql, both.params)]
        assert got == rest + rest
        kept = conn.execute("SELECT id, body FROM t ORDER BY id").fetchone()
        assert kept is not None
        again = Insert(
            "t",
            values={"id": kept[0], "body": "nope"},
            on_conflict=["id"],
            do_nothing=True,
        )
        conn.execute(again.sql, again.params)
        assert [
            row[0] for row in conn.execute("SELECT body FROM t ORDER BY id")
        ] == rest
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
        q = Select(lit(text), param("blob"), flag, number, point, None)
        row = conn.execute(q.sql, {"blob": blob, **q.params}).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row == (text, blob, int(flag), number, point, None)


@given(text=st.text(alphabet=_CHARS, max_size=24))
@_FAST
def test_bound_strings_round_trip(text: str) -> None:
    conn = apsw.Connection(":memory:")
    try:
        q = Select(col("x") == text, from_="t")
        # The bound value is in params; select it back instead.
        probe = Select(param("v"))
        row = conn.execute(probe.sql, {"v": text}).fetchone()
    finally:
        conn.close()
    assert q.params == {"_1": text}
    assert row == (text,)


@given(
    prefix=st.text(alphabet=_CHARS, max_size=8),
    suffix=st.text(alphabet=_CHARS, max_size=8),
)
@_FAST
def test_a_nul_in_a_literal_raises(prefix: str, suffix: str) -> None:
    with pytest.raises(QueryError, match="NUL"):
        _ = Select(lit(prefix + "\x00" + suffix)).sql


@given(name=_NAME)
@_FAST
def test_a_name_round_trips(name: str) -> None:
    conn = _memory()
    try:
        conn.execute(f'CREATE TABLE u("{name}" INTEGER NOT NULL)')
        conn.execute(f'INSERT INTO u("{name}") VALUES (7)')
        q = Select(col(name), from_="u")
        if name.upper() in _KEYWORDS:
            assert f'"{name}"' in q.sql
        assert conn.execute(q.sql, q.params).fetchone() == (7,)
    finally:
        conn.close()


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
        inserted = Insert("t", values=[{"body": body} for body in original])
        conn.execute(inserted.sql, inserted.params)
        payload = [
            {"id": index, "body": body}
            for index, body in enumerate(replacement, start=1)
        ]
        again = Insert(
            "t",
            values=payload,
            on_conflict=["id"],
            do_update={"body": raw("excluded.body")},
        )
        conn.execute(again.sql, again.params)
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


@given(
    a=st.text(alphabet="xyz", min_size=1, max_size=4),
    b=st.text(alphabet="xyz", min_size=1, max_size=4),
)
@_FAST
def test_bound_params_never_collide(a: str, b: str) -> None:
    q = Select("id", from_="t", where=col("x") == a).where(col("y") == b)
    assert q.sql == "SELECT id FROM t WHERE x = :_1 AND y = :_2"
    assert q.params == {"_1": a, "_2": b}


def _write(tx: Tx, body: str, fail: bool) -> None:
    if fail:
        raise ValueError(body)
    tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)


@example([("a", False), ("b", True), ("c", False)])
@given(
    jobs=st.lists(
        st.tuples(
            st.text(alphabet="abcdefghij", min_size=1, max_size=4),
            st.booleans(),
        ),
        min_size=1,
        max_size=6,
        unique_by=lambda item: item[0],
    )
)
@_DB
def test_a_batch_matches_the_isolation_rule(
    jobs: list[tuple[str, bool]],
) -> None:
    """Every job has its own savepoint: failures drop only themselves."""
    disk = [body for body, fail in jobs if not fail]
    with TemporaryDirectory() as raw:
        db = LiteWriter(Path(raw) / "t.sqlite3", hz=0, schema=SCHEMA)
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
            futures = [db.submit(_write, body, fail) for body, fail in jobs]
            release.set()
            waiter.join(timeout=2)
            assert not waiter.is_alive()
            for future, (body, fail) in zip(futures, jobs, strict=True):
                if fail:
                    with pytest.raises(ValueError, match=f"^{re.escape(body)}$"):
                        future.result(timeout=2)
                else:
                    assert future.result(timeout=2) is None
            got = [row[0] for row in db.query("SELECT body FROM t ORDER BY id")]
            assert got == disk
        finally:
            release.set()
            db.close()


def _insert(tx: Tx, body: str) -> None:
    tx.execute("INSERT INTO t(body) VALUES (:body)", body=body)


def _probe(db: LiteWriter, expect: list[tuple[str, ...]], threads: int) -> None:
    found: list[int] = []
    errors: list[BaseException] = []
    lock = Lock()
    barrier = Barrier(threads)

    def work() -> None:
        try:
            conn = db._conn()
            assert db._conn() is conn
            with lock:
                found.append(id(conn))
            barrier.wait(timeout=2)
            with pytest.raises(apsw.ReadOnlyError):
                db.query("INSERT INTO t(body) VALUES ('no')")
            assert db.query("SELECT body FROM t ORDER BY id") == expect
        except BaseException as exc:
            errors.append(exc)
            with suppress(BrokenBarrierError):
                barrier.abort()

    workers = [Thread(target=work) for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2)
    assert errors == []
    assert all(not worker.is_alive() for worker in workers)
    assert len(found) == threads
    assert len(set(found)) == threads


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
        running = LiteWriter(path, hz=0, schema=SCHEMA)
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


def _pragma(tx: Tx) -> int:
    return int(tx.value("PRAGMA busy_timeout"))


@given(seconds=_SECONDS)
@_DB
def test_busy_timeout_reaches_every_connection(seconds: float) -> None:
    expect = round(float(seconds) * 1000)
    with TemporaryDirectory() as raw:
        db = LiteWriter(Path(raw) / "t.sqlite3", hz=0, busy_timeout=seconds)
        db.start()
        try:
            assert db.value("PRAGMA busy_timeout") == expect
            assert db.submit(_pragma).result(timeout=2) == expect
            seen: list[int] = []

            def look() -> None:
                seen.append(db.value("PRAGMA busy_timeout"))

            thread = Thread(target=look)
            thread.start()
            thread.join(timeout=2)
            assert seen == [expect]
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
    with pytest.raises(QueryError, match="busy_timeout"):
        LiteWriter("unused.sqlite3", busy_timeout=cast(float, bad))


@given(timeout=_SECONDS)
@_FAST
def test_a_claim_timeout_is_a_count_of_seconds(timeout: float) -> None:
    assert validate_timeout(timeout) == float(timeout)


@given(bad=_BAD_NUMBER)
@_FAST
def test_a_claim_timeout_rejects_a_bad_value(bad: object) -> None:
    with pytest.raises(QueryError, match="claim timeout"):
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
    with pytest.raises(QueryError, match="hz"):
        LiteWriter("unused.sqlite3", hz=cast(float, bad))


@given(bodies=st.lists(_TEXT, max_size=5))
@_DB
def test_close_keeps_the_rows_and_folds_the_wal(bodies: list[str]) -> None:
    with TemporaryDirectory() as raw:
        path = Path(raw) / "t.sqlite3"
        db = LiteWriter(path, hz=0, schema=SCHEMA)
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
