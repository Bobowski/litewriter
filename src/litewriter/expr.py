"""Column, literal, and parameter expressions.

``col``, ``lit``, and ``param`` build the same values as the strings and
tuples. Operators do too. ``col("room") == param("room")`` is
``("=", "room", ":room")``.

An expression has no truth value. Combine conditions in a list.
That list is AND.
"""

import re
from collections.abc import Mapping

from litewriter.errors import WriterError

_PARAM = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Expr:
    """One SQL expression."""

    __slots__ = ("data",)

    def __init__(self, data: object) -> None:
        self.data = data

    def as_(self, name: str) -> Expr:
        """``expr AS name``."""
        return Expr(("as", _leaf(self), name))

    def in_(self, *values: object) -> Expr:
        """``expr IN (...)``. One statement is a subquery."""
        if len(values) == 1 and _is_query(values[0]):
            return Expr(("in", _leaf(self), values[0]))
        if not values:
            raise WriterError(
                "in_ needs a value or a query",
                example='col("status").in_(lit("open"), lit("done"))',
            )
        return Expr(("in", _leaf(self), *(_leaf(value) for value in values)))

    def not_in(self, *values: object) -> Expr:
        """``expr NOT IN (...)``. One statement is a subquery."""
        if len(values) == 1 and _is_query(values[0]):
            return Expr(("not in", _leaf(self), values[0]))
        if not values:
            raise WriterError(
                "not_in needs a value or a query",
                example='col("status").not_in(lit("open"))',
            )
        return Expr(("not in", _leaf(self), *(_leaf(value) for value in values)))

    def like(self, pattern: object) -> Expr:
        return _op("like", self, pattern)

    def not_like(self, pattern: object) -> Expr:
        return _op("not like", self, pattern)

    def glob(self, pattern: object) -> Expr:
        return _op("glob", self, pattern)

    def is_(self, other: object) -> Expr:
        return _op("is", self, other)

    def is_not(self, other: object) -> Expr:
        return _op("is not", self, other)

    def between(self, low: object, high: object) -> Expr:
        return Expr(("between", _leaf(self), _leaf(low), _leaf(high)))

    def not_between(self, low: object, high: object) -> Expr:
        return Expr(("not between", _leaf(self), _leaf(low), _leaf(high)))

    def asc(self, nulls: str | None = None) -> Expr:
        return _order("asc", self, nulls)

    def desc(self, nulls: str | None = None) -> Expr:
        return _order("desc", self, nulls)

    def __bool__(self) -> bool:
        raise TypeError(
            "an expression has no truth value. Put conditions in a list. The list is AND."
        )

    def __eq__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _compare("=", self, other)

    def __ne__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _compare("!=", self, other)

    def __lt__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _compare("<", self, other)

    def __le__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _compare("<=", self, other)

    def __gt__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _compare(">", self, other)

    def __ge__(self, other: object) -> Expr:  # pyright: ignore[reportIncompatibleMethodOverride]
        return _compare(">=", self, other)

    def __add__(self, other: object) -> Expr:
        return _op("+", self, other)

    def __radd__(self, other: object) -> Expr:
        return _op("+", other, self)

    def __sub__(self, other: object) -> Expr:
        return _op("-", self, other)

    def __rsub__(self, other: object) -> Expr:
        return _op("-", other, self)

    def __mul__(self, other: object) -> Expr:
        return _op("*", self, other)

    def __rmul__(self, other: object) -> Expr:
        return _op("*", other, self)

    def __truediv__(self, other: object) -> Expr:
        return _op("/", self, other)

    def __rtruediv__(self, other: object) -> Expr:
        return _op("/", other, self)

    def __mod__(self, other: object) -> Expr:
        return _op("%", self, other)

    def __rmod__(self, other: object) -> Expr:
        return _op("%", other, self)

    def __lshift__(self, other: object) -> Expr:
        return _op("<<", self, other)

    def __rlshift__(self, other: object) -> Expr:
        return _op("<<", other, self)

    def __rshift__(self, other: object) -> Expr:
        return _op(">>", self, other)

    def __rrshift__(self, other: object) -> Expr:
        return _op(">>", other, self)

    def __and__(self, other: object) -> Expr:
        return _op("&", self, other)

    def __rand__(self, other: object) -> Expr:
        return _op("&", other, self)

    def __or__(self, other: object) -> Expr:
        return _op("|", self, other)

    def __ror__(self, other: object) -> Expr:
        return _op("|", other, self)

    def __neg__(self) -> Expr:
        return Expr(("-", _leaf(self)))

    def __pos__(self) -> Expr:
        return Expr(("+", _leaf(self)))

    def __repr__(self) -> str:
        return f"Expr({self.data!r})"


def col(name: str) -> Expr:
    """A name. ``col("m.room")`` is ``m.room``. ``col("*")`` is ``*``."""
    return Expr(name)


def lit(text: str) -> Expr:
    """A string literal. ``lit("open")`` is ``'open'``."""
    return Expr(("lit", text))


def param(name: str) -> Expr:
    """A parameter. ``param("room")`` is ``:room``."""
    if not _PARAM.fullmatch(name):
        raise WriterError(
            "a parameter is a name",
            example='param("room")',
            context={"name": name},
        )
    return Expr(":" + name)


def exists(query: object) -> Expr:
    """``EXISTS (query)``."""
    return Expr(("exists", _leaf(query)))


def not_exists(query: object) -> Expr:
    """``NOT EXISTS (query)``."""
    return Expr(("not exists", _leaf(query)))


def _leaf(raw: object) -> object:
    if isinstance(raw, Expr):
        return raw.data
    return raw


def _compare(name: str, left: Expr, right: object) -> Expr:
    if right is None:
        raise WriterError(
            "a comparison with None never matches",
            help_text="= NULL is unknown. IS NULL matches a missing value.",
            example='col("deleted_at").is_(None)',
        )
    return _op(name, left, right)


def _op(name: str, left: object, right: object) -> Expr:
    return Expr((name, _leaf(left), _leaf(right)))


def _order(name: str, expr: Expr, nulls: str | None) -> Expr:
    leaf = _leaf(expr)
    if nulls is None:
        return Expr((name, leaf))
    return Expr((name, leaf, nulls))


_QUERY = frozenset(
    {
        "select",
        "select_distinct",
        "union",
        "union_all",
        "intersect",
        "except",
        "insert_into",
        "update",
        "delete_from",
    }
)


def _is_query(raw: object) -> bool:
    if not isinstance(raw, Mapping):
        return False
    return any(name in raw for name in _QUERY)
