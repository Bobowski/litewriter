"""Expressions for the query builder.

``col("m.room")`` is a name. ``param("room")`` is ``:room``, bound at the
call. ``lit("open")`` is the inline literal ``'open'``. ``raw(sql)``
is verbatim SQL. ``fn("lower", col("x"))`` is ``lower(x)``.
``case((cond, value), ..., else_=v)`` is a ``CASE``.

In a value position a Python value is data, not SQL: the renderer binds
``str``, ``bytes``, ``int``, ``float``, and ``bool`` as ``:_1``,
``:_2``, ... ``None`` renders ``NULL``. An ``Expr`` is used as is.

Combine conditions with ``&`` (AND), ``|`` (OR), ``~`` (NOT). An
expression has no truth value.
"""

import re
from collections.abc import Iterable, Mapping
from typing import Any, Literal, TypeIs

from litewriter.errors import QueryError

NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
RESERVED = re.compile(r"_[0-9]+")
"""The auto-bind names: ``_1``, ``_2``, ... are reserved."""
_AUTO_BIND = re.compile(r"[:@$]_[0-9]+\b")
"""A reference to an auto-bind, e.g. ``:_1``, inside raw SQL."""

type Params = Mapping[str, Any]
"""Named values bound to a query; apsw checks each value."""


def is_params(raw: object) -> TypeIs[Params]:
    return isinstance(raw, Mapping)


def is_iter(raw: object) -> TypeIs[Iterable[object]]:
    return isinstance(raw, Iterable)


def is_seq(raw: object) -> TypeIs[list[object] | tuple[object, ...]]:
    return isinstance(raw, list | tuple)


class Stmt:
    """Base of built statements (``Select``, ``Insert``, ...).

    A nominal class, not a Protocol: ``isinstance`` against it runs on
    every query render, and a Protocol check is measurably slower.
    """

    __slots__ = ()

    @property
    def sql(self) -> str:
        raise NotImplementedError

    @property
    def params(self) -> dict[str, Any]:
        raise NotImplementedError

    def clauses(self) -> dict[str, Any]:
        raise NotImplementedError

    def compiled(self) -> tuple[str, Params]:
        raise NotImplementedError


type Value = Expr | Stmt | str | bytes | int | float | bool | None
"""Anything a value position accepts."""

type Name = Expr | Stmt | str
"""Anything a name position accepts."""


# Binary operators accepted by ``Expr.op``. The value is the SQL text.
_OPS: dict[str, str] = {
    "or": "OR",
    "and": "AND",
    "=": "=",
    "==": "==",
    "!=": "!=",
    "<>": "<>",
    "is": "IS",
    "is not": "IS NOT",
    "is distinct from": "IS DISTINCT FROM",
    "is not distinct from": "IS NOT DISTINCT FROM",
    "like": "LIKE",
    "not like": "NOT LIKE",
    "glob": "GLOB",
    "not glob": "NOT GLOB",
    "regexp": "REGEXP",
    "match": "MATCH",
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
    "&": "&",
    "|": "|",
    "<<": "<<",
    ">>": ">>",
    "+": "+",
    "-": "-",
    "*": "*",
    "/": "/",
    "%": "%",
    "||": "||",
    "->": "->",
    "->>": "->>",
}


class Expr:
    """One SQL expression. Build with ``col``, ``param``, ``lit``, ``raw``,
    ``fn``, and ``case``, or from operators and methods on an ``Expr``."""

    __slots__ = ("data",)

    def __init__(self, data: tuple[Any, ...]) -> None:
        self.data = data

    def as_(self, name: str) -> Expr:
        """``expr AS name``."""
        return Expr(("as", self, name))

    def in_(self, values: Iterable[Value] | Stmt) -> Expr:
        """``expr IN (...)``. One iterable of values or one Select."""
        if isinstance(values, Stmt):
            return Expr(("in", self, values, False))
        return Expr(("in", self, _items(values, "in_"), False))

    def not_in(self, values: Iterable[Value] | Stmt) -> Expr:
        """``expr NOT IN (...)``. One iterable of values or one Select."""
        if isinstance(values, Stmt):
            return Expr(("in", self, values, True))
        return Expr(("in", self, _items(values, "not_in"), True))

    def like(self, pattern: Value) -> Expr:
        return _bin("LIKE", self, pattern)

    def not_like(self, pattern: Value) -> Expr:
        return _bin("NOT LIKE", self, pattern)

    def glob(self, pattern: Value) -> Expr:
        return _bin("GLOB", self, pattern)

    def is_(self, other: Value) -> Expr:
        return _bin("IS", self, other)

    def is_not(self, other: Value) -> Expr:
        return _bin("IS NOT", self, other)

    def between(self, low: Value, high: Value) -> Expr:
        """``expr BETWEEN low AND high``."""
        return Expr(("btw", self, low, high, False))

    def not_between(self, low: Value, high: Value) -> Expr:
        """``expr NOT BETWEEN low AND high``."""
        return Expr(("btw", self, low, high, True))

    def asc(self, nulls: str | None = None) -> Expr:
        return _order("ASC", self, nulls)

    def desc(self, nulls: str | None = None) -> Expr:
        return _order("DESC", self, nulls)

    def cast(self, type_name: str) -> Expr:
        """``CAST(expr AS type)``."""
        return Expr(("cast", self, _cast_name(type_name).upper()))

    def collate(self, name: str) -> Expr:
        """``expr COLLATE name``."""
        return Expr(("coll", self, _simple_name(name, "collation").upper()))

    def concat(self, other: Value) -> Expr:
        """``expr || other``."""
        return _bin("||", self, other)

    def op(self, operator: str, other: Value) -> Expr:
        """Any binary operator, e.g. ``col("doc").op("->>", "key")``."""
        return _bin(_op_sql(operator), self, other)

    def __invert__(self) -> Expr:
        """``NOT expr``."""
        return Expr(("not", self))

    def __and__(self, other: object) -> Expr:
        """``expr AND other``. Chains flatten: ``a & b & c`` is one AND."""
        return Expr(("and", _sides(self, other, "and")))

    def __rand__(self, other: object) -> Expr:
        return Expr(("and", _sides(other, self, "and")))

    def __or__(self, other: object) -> Expr:
        """``expr OR other``. Chains flatten: ``a | b | c`` is one OR."""
        return Expr(("or", _sides(self, other, "or")))

    def __ror__(self, other: object) -> Expr:
        return Expr(("or", _sides(other, self, "or")))

    def __neg__(self) -> Expr:
        return Expr(("neg", self))

    def __bool__(self) -> bool:
        raise TypeError(
            "an expression has no truth value. Use & for AND, | for OR, ~ for NOT."
        )

    def __eq__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _cmp("=", self, other)

    def __ne__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _cmp("!=", self, other)

    def __lt__(self, other: object) -> Expr:
        return _cmp("<", self, other)

    def __le__(self, other: object) -> Expr:
        return _cmp("<=", self, other)

    def __gt__(self, other: object) -> Expr:
        return _cmp(">", self, other)

    def __ge__(self, other: object) -> Expr:
        return _cmp(">=", self, other)

    def __add__(self, other: Value) -> Expr:
        return _bin("+", self, other)

    def __radd__(self, other: Value) -> Expr:
        return _bin("+", other, self)

    def __sub__(self, other: Value) -> Expr:
        return _bin("-", self, other)

    def __rsub__(self, other: Value) -> Expr:
        return _bin("-", other, self)

    def __mul__(self, other: Value) -> Expr:
        return _bin("*", self, other)

    def __rmul__(self, other: Value) -> Expr:
        return _bin("*", other, self)

    def __truediv__(self, other: Value) -> Expr:
        return _bin("/", self, other)

    def __rtruediv__(self, other: Value) -> Expr:
        return _bin("/", other, self)

    def __mod__(self, other: Value) -> Expr:
        return _bin("%", self, other)

    def __rmod__(self, other: Value) -> Expr:
        return _bin("%", other, self)

    def __repr__(self) -> str:
        return f"Expr({self.data!r})"


def col(name: str) -> Expr:
    """A name. ``col("m.room")`` is ``m.room``. ``col("*")`` is ``*``."""
    return Expr(("col", name))


def param(name: str) -> Expr:
    """A named parameter. ``param("room")`` renders ``:room``."""
    return Expr(("par", param_name(name)))


def lit(text: str) -> Expr:
    """An inline string literal. ``lit("open")`` renders ``'open'``."""
    return Expr(("lit", text))


def raw(sql: str) -> Expr:
    """Verbatim SQL text. ``raw("random()")`` renders ``random()``."""
    _no_auto_bind(sql)
    return Expr(("raw", sql))


def _no_auto_bind(sql: object) -> None:
    if isinstance(sql, str) and _AUTO_BIND.search(sql):
        raise QueryError(
            "raw() cannot reference an auto-bind name",
            context={"sql": sql},
            help_text="Use param() for a named value.",
        )


def case(*pairs: tuple[Value, Value], else_: Value = None) -> Expr:
    """``CASE WHEN cond THEN value ... ELSE else_ END``.

    Each ``pair`` is ``(condition, value)``. No ``else_`` means no ELSE.
    """
    if not pairs:
        raise QueryError(
            "case needs at least one (condition, value) pair",
            example='case((col("s") == "open", 1), else_=0)',
        )
    cooked = [_branch(pair) for pair in pairs]
    return Expr(("case", tuple(cooked), else_))


def _branch(pair: object) -> tuple[object, object]:
    if not is_seq(pair) or len(pair) != 2:
        raise QueryError(
            "a case branch is a (condition, value) pair",
            context={"branch": pair},
        )
    return pair[0], pair[1]


type BuiltinName = Literal[
    "abs",
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "bm25",
    "ceil",
    "ceiling",
    "changes",
    "char",
    "coalesce",
    "concat",
    "concat_ws",
    "cos",
    "cosh",
    "count",
    "cume_dist",
    "current_date",
    "current_time",
    "current_timestamp",
    "date",
    "datetime",
    "degrees",
    "dense_rank",
    "exp",
    "first_value",
    "floor",
    "format",
    "group_concat",
    "hex",
    "highlight",
    "if",
    "ifnull",
    "iif",
    "instr",
    "json",
    "json_array",
    "json_array_insert",
    "json_array_length",
    "json_error_position",
    "json_extract",
    "json_group_array",
    "json_group_object",
    "json_insert",
    "json_object",
    "json_patch",
    "json_pretty",
    "json_quote",
    "json_remove",
    "json_replace",
    "json_set",
    "json_type",
    "json_valid",
    "jsonb",
    "jsonb_array",
    "jsonb_array_insert",
    "jsonb_extract",
    "jsonb_group_array",
    "jsonb_group_object",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_patch",
    "jsonb_remove",
    "jsonb_replace",
    "jsonb_set",
    "julianday",
    "lag",
    "last_insert_rowid",
    "last_value",
    "lead",
    "length",
    "likelihood",
    "likely",
    "ln",
    "log",
    "log10",
    "log2",
    "lower",
    "ltrim",
    "max",
    "median",
    "min",
    "mod",
    "nth_value",
    "ntile",
    "nullif",
    "octet_length",
    "percent_rank",
    "percentile",
    "percentile_cont",
    "percentile_disc",
    "pi",
    "pow",
    "power",
    "printf",
    "quote",
    "radians",
    "random",
    "randomblob",
    "rank",
    "replace",
    "round",
    "row_number",
    "rtrim",
    "sign",
    "sin",
    "sinh",
    "snippet",
    "sqrt",
    "strftime",
    "string_agg",
    "substr",
    "substring",
    "subtype",
    "sum",
    "tan",
    "tanh",
    "time",
    "timediff",
    "total",
    "total_changes",
    "trim",
    "trunc",
    "typeof",
    "unhex",
    "unicode",
    "unistr",
    "unistr_quote",
    "unixepoch",
    "unlikely",
    "upper",
    "zeroblob",
]
"""The SQLite built-in function names, for editor completion. Any other
name (a registered function) is still accepted."""


def fn(name: BuiltinName | str, /, *args: Value) -> Expr:
    """``fn("lower", col("x"))`` is ``lower(x)``. Any SQLite function,
    built-in or registered, by name. ``fn("count")`` is ``count()``."""
    return Expr(("fnc", _fn_name(name), args))


def _fn_name(name: object) -> str:
    if not isinstance(name, str) or not NAME.fullmatch(name):
        raise QueryError(
            "a function name is a name",
            context={"name": name},
            example='fn("lower", col("x"))',
        )
    return name


def _items(values: object, method: str) -> tuple[object, ...]:
    if isinstance(values, str | bytes) or not is_iter(values):
        raise QueryError(
            f"{method} takes one iterable or one Select",
            context={"values": values},
            example='col("status").in_(["open", "done"])',
        )
    return tuple(values)


def _sides(left: object, right: object, tag: str) -> tuple[object, ...]:
    """Flatten ``a & b & c`` into one AND node. AND/OR are associative."""
    items: list[object] = []
    for side in (left, right):
        if isinstance(side, Expr) and side.data[0] == tag:
            items.extend(side.data[1])
            continue
        items.append(side)
    return tuple(items)


def _bin(op: str, left: object, right: object) -> Expr:
    return Expr(("bin", op, left, right))


def _cmp(op: str, left: Expr, right: object) -> Expr:
    if right is None:
        raise QueryError(
            "a comparison with None never matches",
            help_text="= NULL is unknown. IS NULL matches a missing value.",
            example='col("deleted_at").is_(None)',
        )
    return _bin(op, left, right)


def _order(direction: str, expr: Expr, nulls: str | None) -> Expr:
    return Expr(("ord", direction, expr, _nulls_tail(nulls)))


def _nulls_tail(nulls: object) -> str | None:
    if nulls is None:
        return None
    if not isinstance(nulls, str) or nulls.lower() not in _NULLS:
        raise QueryError(
            'nulls is "first" or "last"',
            context={"nulls": nulls},
            example='col("name").asc(nulls="last")',
        )
    return "NULLS " + nulls.upper()


def _cast_name(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_ ]*", value
    ):
        raise QueryError(
            "a cast type is a word", context={"type": value}, example='"integer"'
        )
    return value


def _simple_name(value: object, what: str) -> str:
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise QueryError(
            f"a {what} is a name", context={what: value}, example='"nocase"'
        )
    return value


def param_name(name: object) -> str:
    if (
        not isinstance(name, str)
        or not NAME.fullmatch(name)
        or RESERVED.fullmatch(name)
    ):
        raise QueryError(
            "a parameter is a name",
            context={"name": name},
            help_text="Names _1, _2, ... are reserved for bound values.",
            example='param("room")',
        )
    return name


def _op_sql(operator: object) -> str:
    sql = _OPS.get(operator.lower()) if isinstance(operator, str) else None
    if sql is None:
        raise QueryError(
            "not a binary operator",
            context={"operator": operator},
            help_text=f"Operators are {', '.join(_OPS)}.",
        )
    return sql


_NULLS = frozenset({"first", "last"})
