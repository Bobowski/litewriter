"""Statement objects for SQLite.

``Select``, ``Insert``, ``Update``, and ``Delete`` take the clauses in
the constructor. A method adds one clause and returns a new statement;
the original is untouched. ``.sql`` is the rendered text and ``.params``
the auto-bound values.

In a name position a ``str`` is an identifier: ``Select("m.id")`` is
``SELECT m.id``. In a value position a ``str`` is bound data:
``col("room") == "open"`` renders ``room = :_1`` with
``{"_1": "open"}``. Use ``param("room")`` for a named ``:room``,
``lit("open")`` for an inline ``'open'``, ``col`` for a name.

A ``Select`` nests anywhere: ``from_=inner.as_("s")``,
``col("id").in_(inner)``, ``inner.exists()``.
"""

from collections.abc import Iterable, Mapping, Sequence
from enum import Enum, auto
from typing import Any, Literal, Self, TypeIs

from litewriter.errors import QueryError
from litewriter.expr import (
    Expr,
    Name,
    Params,
    Stmt,
    Value,
    is_iter,
    is_params,
    is_seq,
    param_name,
)
from litewriter.q import render

type JoinKind = Literal["inner", "left", "right", "full", "cross"]
type Cond = Expr | bool
"""One condition: an expression or a Python bool."""
type JoinSpec = (
    tuple[Literal["inner", "left", "right", "full"], Name, Value]
    | tuple[Literal["cross"], Name]
)
"""One join clause, with or without ``ON``."""
_JOINS = frozenset({"inner", "left", "right", "full", "cross"})
_SELECTABLE = frozenset({"select", "compound"})


class _Unset(Enum):
    """No ``where``/``having`` argument was given."""

    UNSET = auto()


_UNSET = _Unset.UNSET


def _none_cond() -> QueryError:
    return QueryError(
        "a condition is an expression, not None",
        help_text="To skip a filter, do not add it.",
    )


class Statement(Stmt):
    """Base of the four statements. Immutable: ``.sql`` and ``.params``
    are computed once and cached. Each subclass holds clause data."""

    __slots__ = ("_cache", "_data")

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data
        self._cache: tuple[str, Params] | None = None

    def clauses(self) -> dict[str, Any]:
        return self._data

    def compiled(self) -> tuple[str, Params]:
        """The rendered text and auto-bound params: compiled once, no copy."""
        cached = self._cache
        if cached is None:
            cached = render(self._data)
            self._cache = cached
        return cached

    @property
    def sql(self) -> str:
        """The SQL text. Bound values render as ``:_1``, ``:_2``, ..."""
        return self.compiled()[0]

    @property
    def params(self) -> dict[str, Any]:
        """The auto-bound values behind ``:_1``, ``:_2``, ... A copy."""
        return dict(self.compiled()[1])

    def as_(self, name: str) -> Expr:
        """This statement as a subquery with an alias: ``(...) AS name``."""
        return Expr(("as", self, name))

    def _new(self, **changes: object) -> Self:
        fresh = object.__new__(type(self))
        fresh._data = {**self._data, **changes}
        fresh._cache = None
        return fresh

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.sql})"


class _Ctes(Statement):
    """``WITH`` and ``WITH RECURSIVE``."""

    __slots__ = ()

    def with_(self, name: str, query: Stmt) -> Self:
        return self._cte(name, query, recursive=False)

    def with_recursive(self, name: str, query: Stmt) -> Self:
        return self._cte(name, query, recursive=True)

    def _cte(self, name: str, query: object, *, recursive: bool) -> Self:
        if self._data.get("recursive") != recursive and self._data.get("with"):
            raise QueryError("use with or with_recursive, not both")
        if not isinstance(query, Stmt):
            raise QueryError(
                "a CTE body is a statement",
                context={"query": query},
                example='db.with_("recent", Select("id", from_="t"))',
            )
        ctes = dict(self._data.get("with") or {})
        ctes[name] = query
        return self._new(**{"with": ctes, "recursive": recursive})


class _Where(Statement):
    """``WHERE``."""

    __slots__ = ()

    def where(self, *conds: Cond) -> Self:
        """AND each condition onto ``WHERE``. The old statement stays."""
        rows: list[object] = list(self._data.get("where") or [])
        rows.extend(conds)
        for cond in rows:
            if cond is None:
                raise _none_cond()
        return self._new(where=rows)


class _From(_Where):
    """``FROM`` and ``JOIN``."""

    __slots__ = ()

    def from_(self, source: Name) -> Self:
        """``FROM source``. A statement or ``source.as_("name")`` nests."""
        return self._new(**{"from": source})

    def join(self, kind: JoinKind, source: Name, on: Value = None) -> Self:
        """Append one join. ``kind`` is inner, left, right, full, or cross."""
        if kind not in _JOINS:
            raise QueryError(
                "a join kind is inner, left, right, full, or cross",
                context={"kind": kind},
            )
        rows = list(self._data.get("join") or [])
        rows.append((kind, source, on))
        return self._new(join=rows)


class _Tail(Statement):
    """``ORDER BY``, ``LIMIT``, and ``OFFSET``."""

    __slots__ = ()

    def order_by(self, *items: Name) -> Self:
        rows = list(self._data.get("order_by") or [])
        rows.extend(items)
        return self._new(order_by=rows)

    def limit(self, count: int | Expr) -> Self:
        return self._new(limit=count)

    def offset(self, count: int | Expr) -> Self:
        return self._new(offset=count)


class _Returning(Statement):
    """``RETURNING``."""

    __slots__ = ()

    def returning(self, *items: Name) -> Self:
        return self._new(returning=list(items))


class Select(_From, _Tail, _Ctes):
    """One SELECT. The constructor takes the clauses it already knows."""

    __slots__ = ()

    def __init__(
        self,
        *items: Value,
        distinct: bool = False,
        from_: Name | None = None,
        join: JoinSpec | list[JoinSpec] | tuple[JoinSpec, ...] | None = None,
        where: Cond | list[Cond] | tuple[Cond, ...] | _Unset = _UNSET,
        group_by: Name | list[Name] | tuple[Name, ...] | None = None,
        having: Cond | list[Cond] | tuple[Cond, ...] | _Unset = _UNSET,
        order_by: Name | list[Name] | tuple[Name, ...] | None = None,
        limit: int | Expr | None = None,
        offset: int | Expr | None = None,
        with_: Mapping[str, Statement] | None = None,
        recursive: bool = False,
    ) -> None:
        if not items:
            raise QueryError(
                "Select needs an expression", example='Select("id", from_="t")'
            )
        data: dict[str, Any] = {
            "kind": "select",
            "distinct": bool(distinct),
            "items": list(items),
        }
        data.update(_ctes(with_, recursive))
        if from_ is not None:
            data["from"] = from_
        if join is not None:
            data["join"] = _joins(join)
        if not isinstance(where, _Unset):
            data["where"] = _conds(where)
        if group_by is not None:
            data["group_by"] = _many(group_by)
        if not isinstance(having, _Unset):
            data["having"] = _conds(having)
        if order_by is not None:
            data["order_by"] = _many(order_by)
        if limit is not None:
            data["limit"] = limit
        if offset is not None:
            data["offset"] = offset
        super().__init__(data)

    def select(self, *items: Value) -> Self:
        """Replace the select list. ``distinct`` resets."""
        if not items:
            raise QueryError(
                "Select needs an expression", example='Select("id", from_="t")'
            )
        return self._new(items=list(items), distinct=False)

    def group_by(self, *items: Name) -> Self:
        rows = list(self._data.get("group_by") or [])
        rows.extend(items)
        return self._new(group_by=rows)

    def having(self, *conds: Cond) -> Self:
        rows: list[object] = list(self._data.get("having") or [])
        rows.extend(conds)
        for cond in rows:
            if cond is None:
                raise _none_cond()
        return self._new(having=rows)

    def union(self, other: Select | Compound) -> Compound:
        return Compound(self, [("union", _member_stmt(other))])

    def union_all(self, other: Select | Compound) -> Compound:
        return Compound(self, [("union_all", _member_stmt(other))])

    def intersect(self, other: Select | Compound) -> Compound:
        return Compound(self, [("intersect", _member_stmt(other))])

    def except_(self, other: Select | Compound) -> Compound:
        return Compound(self, [("except", _member_stmt(other))])

    def exists(self) -> Expr:
        """``EXISTS (this select)``. ``~q.exists()`` is ``NOT EXISTS``."""
        return Expr(("ext", self, False))


class Compound(_Tail, _Ctes):
    """Two or more selects joined by UNION, INTERSECT, or EXCEPT.

    Built by ``Select.union(other)`` and friends; each method appends one
    member. ``.order_by``/``.limit``/``.offset`` go on the compound.
    """

    __slots__ = ()

    def __init__(self, first: Statement, ops: list[tuple[str, Statement]]) -> None:
        super().__init__({"kind": "compound", "first": first, "ops": list(ops)})

    def union(self, other: Select | Compound) -> Compound:
        return self._more("union", other)

    def union_all(self, other: Select | Compound) -> Compound:
        return self._more("union_all", other)

    def intersect(self, other: Select | Compound) -> Compound:
        return self._more("intersect", other)

    def except_(self, other: Select | Compound) -> Compound:
        return self._more("except", other)

    def _more(self, key: str, other: Statement) -> Compound:
        ops = list(self._data["ops"])
        ops.append((key, _member_stmt(other)))
        return self._new(ops=ops)


class Insert(_Returning, _Ctes):
    """One INSERT. ``values`` maps columns to bound values. ``columns``
    alone renders ``VALUES (:name, ...)``, a parameter per column."""

    __slots__ = ()

    def __init__(
        self,
        table: str | Expr,
        *,
        columns: str | Sequence[str] | None = None,
        values: Mapping[str, Value] | Sequence[Mapping[str, Value]] | None = None,
        select: Select | Compound | None = None,
        on_conflict: str | Sequence[str] | None = None,
        do_nothing: bool = False,
        do_update: Mapping[str, Value] | str | Sequence[str] | None = None,
        returning: Name | list[Name] | tuple[Name, ...] | None = None,
        with_: Mapping[str, Statement] | None = None,
        recursive: bool = False,
    ) -> None:
        if values is not None and select is not None:
            raise QueryError(
                "an insert needs values or select (one of them)",
                example='Insert("t", values={"body": "hi"})',
            )
        data: dict[str, Any] = {"kind": "insert", "table": table}
        data.update(_ctes(with_, recursive))
        if columns is not None:
            names = _names(columns, "columns")
            if values is None and select is None:
                # ``VALUES (:name, ...)``: each column doubles as a parameter.
                for name in names:
                    param_name(name)
            data["columns"] = names
        if values is not None:
            data["values"] = _valrows(values)
        if select is not None:
            member = _select_stmt(select)
            sel = member.clauses()
            if sel.get("with"):
                if "with" in data:
                    raise QueryError(
                        "the select and the insert both set with_",
                        example='Insert("t", select=Select(...), with_={...})',
                    )
                data["with"] = dict(sel["with"])
                data["recursive"] = bool(sel.get("recursive"))
            data["select"] = member
        if on_conflict is not None:
            data["on_conflict"] = _names(on_conflict, "on_conflict")
        if do_nothing:
            data["do_nothing"] = True
        if do_update is not None:
            data["do_update"] = _assigns(do_update, "do_update", excluded=True)
        if returning is not None:
            data["returning"] = _many(returning)
        super().__init__(data)

    def columns(self, *names: str) -> Self:
        return self._new(columns=list(names))

    def values(self, *rows: Mapping[str, Value]) -> Self:
        """``VALUES`` rows, each a dict of column to value."""
        if "select" in self._data:
            raise QueryError(
                "an insert needs values or select (one of them)",
            )
        if not rows:
            raise QueryError(
                "values needs at least one row",
                example='.values({"body": "hi"})',
            )
        return self._new(values=[_rowdict(row) for row in rows])

    def on_conflict(self, *columns: str) -> Self:
        """``ON CONFLICT``. No columns means any conflict."""
        return self._new(on_conflict=list(columns))

    def do_nothing(self) -> Self:
        return self._new(do_nothing=True)

    def do_update(self, *columns: str, **assigns: Value) -> Self:
        """``DO UPDATE SET``. Names set ``col = excluded.col``."""
        return self._new(do_update=_pairs(columns, assigns, excluded=True))


class Update(_From, _Returning, _Ctes):
    """One UPDATE. ``set`` maps a column to a value, or lists column
    names to bind as ``:name``."""

    __slots__ = ()

    def __init__(
        self,
        table: str | Expr,
        *,
        set: Mapping[str, Value] | str | Sequence[str],
        from_: Name | None = None,
        join: JoinSpec | list[JoinSpec] | tuple[JoinSpec, ...] | None = None,
        where: Cond | list[Cond] | tuple[Cond, ...] | _Unset = _UNSET,
        returning: Name | list[Name] | tuple[Name, ...] | None = None,
        with_: Mapping[str, Statement] | None = None,
        recursive: bool = False,
    ) -> None:
        data: dict[str, Any] = {
            "kind": "update",
            "table": table,
            "set": _assigns(set, "set", excluded=False),
        }
        data.update(_ctes(with_, recursive))
        if from_ is not None:
            data["from"] = from_
        if join is not None:
            data["join"] = _joins(join)
        if not isinstance(where, _Unset):
            data["where"] = _conds(where)
        if returning is not None:
            data["returning"] = _many(returning)
        super().__init__(data)

    def set(self, *columns: str, **assigns: Value) -> Self:
        """``SET`` for an update. Names bind ``:name``; kwargs keep the value."""
        return self._new(set=_pairs(columns, assigns, excluded=False))


class Delete(_Where, _Returning, _Ctes):
    """One DELETE."""

    __slots__ = ()

    def __init__(
        self,
        table: str | Expr,
        *,
        where: Cond | list[Cond] | tuple[Cond, ...] | _Unset = _UNSET,
        returning: Name | list[Name] | tuple[Name, ...] | None = None,
        with_: Mapping[str, Statement] | None = None,
        recursive: bool = False,
    ) -> None:
        data: dict[str, Any] = {"kind": "delete", "table": table}
        data.update(_ctes(with_, recursive))
        if not isinstance(where, _Unset):
            data["where"] = _conds(where)
        if returning is not None:
            data["returning"] = _many(returning)
        super().__init__(data)


def _rowdict(row: object) -> dict[str, object]:
    if not is_params(row):
        raise QueryError(
            "a values row is a dict",
            context={"row": row},
            example='.values({"body": "hi"})',
        )
    return {str(key): value for key, value in row.items()}


def _conds(raw: object) -> list[object]:
    """One condition, or a list of conditions joined by AND."""
    items: list[object]
    if is_seq(raw):
        if not raw:
            raise QueryError(
                "a condition list is non-empty",
                example='where=col("id") == 1',
            )
        items = list(raw)
    else:
        items = [raw]
    for item in items:
        if item is None:
            raise _none_cond()
    return items


def _many(raw: Name | list[Name] | tuple[Name, ...]) -> list[Name]:
    """A list is many items. Any other value is one item."""
    if isinstance(raw, list | tuple):
        return list(raw)
    return [raw]


def _names(raw: object, key: str) -> list[str]:
    if isinstance(raw, str):
        return [raw]
    if not is_iter(raw):
        raise QueryError(
            f"{key} is a list of names",
            example=f'{key}=["id"]',
        )
    return [str(name) for name in raw]


def _one_join(raw: object) -> TypeIs[JoinSpec]:
    if not is_seq(raw) or not raw:
        return False
    first = raw[0]
    return isinstance(first, str) and first in _JOINS


def _joins(raw: object) -> list[tuple[object, object, object]]:
    """``join`` is one (kind, source, on) tuple or a list of them."""
    if _one_join(raw):
        return [_join_item(raw)]
    if not is_seq(raw):
        raise QueryError(
            "join is (kind, source, on) or a list of joins",
            example='join=("left", "users", col("m.author") == col("u.id"))',
        )
    return [_join_item(item) for item in raw]


def _join_item(item: object) -> tuple[object, object, object]:
    if not is_seq(item) or not item:
        raise QueryError(
            "join is (kind, source, on) or a list of joins",
            context={"join": item},
            example='join=("left", "users", col("m.author") == col("u.id"))',
        )
    if item[0] == "cross":
        if len(item) != 2:
            raise QueryError(
                'a cross join is ("cross", source)', context={"join": item}
            )
        return ("cross", item[1], None)
    if len(item) != 3:
        raise QueryError(
            f'a {item[0]} join is ("{item[0]}", source, on)', context={"join": item}
        )
    return (item[0], item[1], item[2])


def _assigns(
    raw: Mapping[str, Value] | str | Sequence[str], what: str, *, excluded: bool
) -> dict[str, Value]:
    """``set``/``do_update``: a dict, or column names bound as ``:name``."""
    if isinstance(raw, Mapping):
        return {str(key): value for key, value in raw.items()}
    return _pairs(_names(raw, what), {}, excluded=excluded)


def _pairs(
    columns: Iterable[str], assigns: Mapping[str, Value], *, excluded: bool
) -> dict[str, Value]:
    """``.set``/``.do_update`` args: positional names, then kwarg assigns."""
    pairs: dict[str, Value] = {}
    for name in columns:
        checked = param_name(name)
        pairs[checked] = (
            Expr(("col", f"excluded.{checked}")) if excluded else Expr(("par", checked))
        )
    pairs.update(assigns)
    return pairs


def _valrows(
    raw: Mapping[str, Value] | Sequence[Mapping[str, Value]],
) -> list[dict[str, object]]:
    """``values`` is one dict or a list of dicts, snapshotted.

    An empty dict means ``INSERT INTO t DEFAULT VALUES``.
    """
    if isinstance(raw, Mapping):
        return [{str(key): value for key, value in raw.items()}]
    if isinstance(raw, list | tuple):
        rows: list[dict[str, object]] = []
        for row in raw:
            rows.append(_rowdict(row))
        if not rows:
            raise QueryError(
                "values is a non-empty list of rows",
                example='values=[{"body": "hi"}]',
            )
        return rows
    raise QueryError(
        "values is a dict or a list of dicts",
        context={"values": raw},
        example='values={"body": "hi"}',
    )


def _ctes(raw: object, recursive: bool) -> dict[str, Any]:
    if raw is None:
        if recursive:
            raise QueryError(
                "recursive needs with_",
                example='with_={"recent": Select("id", from_="t")}, recursive=True',
            )
        return {}
    if isinstance(raw, Stmt) or not is_params(raw) or not raw:
        raise QueryError(
            "with_ is a dict of name to query",
            example='with_={"recent": Select("id", from_="messages")}',
        )
    ctes: dict[str, Stmt] = {}
    for name, body in raw.items():
        if not isinstance(body, Stmt):
            raise QueryError(
                "a CTE body is a statement",
                context={"name": name, "query": body},
                example='with_={"recent": Select("id", from_="t")}',
            )
        ctes[str(name)] = body
    return {"with": ctes, "recursive": bool(recursive)}


def _member_stmt(raw: object) -> Statement:
    if not isinstance(raw, Statement) or (raw.clauses().get("kind") not in _SELECTABLE):
        raise QueryError(
            "a compound member is a Select",
            example='Select("id", from_="a").union(Select("id", from_="b"))',
        )
    return raw


def _select_stmt(raw: object) -> Stmt:
    if not isinstance(raw, Stmt) or raw.clauses().get("kind") not in _SELECTABLE:
        raise QueryError(
            "select is a Select",
            example='select=Select("id", from_="t")',
        )
    return raw
