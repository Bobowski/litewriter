"""Statement objects for SQLite.

``Select``, ``Insert``, ``Update``, and ``Delete`` take the clauses in
the constructor. A method returns a new statement when you add one clause.

A tuple is one call. ``("=", "m.room", ":room")`` is ``m.room = :room``.
A list is many expressions. ``where=[("=", "room", ":room"), (">", "at", 0)]``
is those conditions joined by AND.

An alias is ``("as", expr, "name")`` or ``expr.as_("name")``.
``inner.as_("s")`` is ``FROM (SELECT ...) AS s`` when you pass it to ``from_``.
``col("m.room") == param("room")`` is ``m.room = :room``.

A ``Select`` is a subquery. Nest it in ``from_``, ``join``, ``where``,
or the select list.

``Select`` has the select methods. ``Insert`` has the insert methods.
The constructor is the whole statement. A method adds one clause.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Literal, Self, cast

from litewriter.errors import WriterError
from litewriter.expr import Expr
from litewriter.q import where as add_where

type _Join = Literal["inner", "left", "right", "full", "cross"]


def _copy_list(found: object) -> list[object]:
    if isinstance(found, list | tuple):
        return [item for item in cast(Sequence[object], found)]
    return []


def _copy_map(found: object) -> dict[str, Any]:
    if isinstance(found, Mapping):
        raw = cast(Mapping[object, Any], found)
        return {str(key): value for key, value in raw.items()}
    return {}


def _value(raw: object) -> object:
    """A snapshot. A later edit of the input does not change the query."""
    if isinstance(raw, Expr):
        return _value(raw.data)
    if isinstance(raw, Query):
        return {key: _value(value) for key, value in raw.items()}
    if isinstance(raw, dict):
        pairs = cast(dict[object, object], raw)
        return {str(key): _value(value) for key, value in pairs.items()}
    if type(raw) in {list, tuple}:
        seq = cast(Sequence[object], raw)
        return tuple(_value(item) for item in seq)
    return raw


class Query(Mapping[str, Any]):
    """One statement. The subclasses add the clauses for that statement."""

    __slots__ = ("_data",)

    def __init__(self, data: Mapping[str, Any]) -> None:
        self._data = dict(data)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def as_(self, name: str) -> Expr:
        """This statement as a subquery with an alias."""
        return Expr(("as", self, name))

    def _from(self, data: Mapping[str, Any]) -> Self:
        fresh = object.__new__(type(self))
        fresh._data = dict(data)
        return fresh

    def _new(self, **changes: object) -> Self:
        return self._from({**self._data, **changes})

    def with_(self, name: str, query: Query) -> Self:
        return self._cte("with", name, query)

    def with_recursive(self, name: str, query: Query) -> Self:
        return self._cte("with_recursive", name, query)

    def _cte(self, key: str, name: str, query: Query) -> Self:
        other = "with_recursive" if key == "with" else "with"
        if other in self._data:
            raise WriterError("use with or with_recursive, not both")
        ctes = _copy_map(self._data.get(key))
        ctes[name] = dict(query)
        return self._new(**{key: ctes})

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"


class _HasWhere(Query):
    """``WHERE``."""

    __slots__ = ()

    def where(self, *conditions: object) -> Self:
        """AND each condition onto ``WHERE``. The previous statement stays."""
        return self._from(add_where(self._data, *(_value(item) for item in conditions)))


class _HasFrom(_HasWhere):
    """``FROM`` and ``JOIN``."""

    __slots__ = ()

    def from_(self, source: object) -> Self:
        """``FROM source``. A statement or ``source.as_("name")`` is a subquery."""
        return self._new(**{"from": _value(source)})

    def join(self, kind: _Join, source: object, on: object = None) -> Self:
        """Append one join. ``kind`` is inner, left, right, full, or cross."""
        src = _value(source)
        piece: list[object] = (
            ["cross", src] if kind == "cross" else [kind, src, _value(on)]
        )
        rows = _copy_list(self._data.get("join"))
        rows.append(piece)
        return self._new(join=rows)

    def inner_join(self, source: object, on: object) -> Self:
        return self.join("inner", source, on)

    def left_join(self, source: object, on: object) -> Self:
        return self.join("left", source, on)

    def right_join(self, source: object, on: object) -> Self:
        return self.join("right", source, on)

    def full_join(self, source: object, on: object) -> Self:
        return self.join("full", source, on)

    def cross_join(self, source: object) -> Self:
        return self.join("cross", source)


class _HasTail(Query):
    """``ORDER BY``, ``LIMIT``, and ``OFFSET``."""

    __slots__ = ()

    def order_by(self, *items: object) -> Self:
        rows = _copy_list(self._data.get("order_by"))
        rows.extend(_value(item) for item in items)
        return self._new(order_by=rows)

    def limit(self, count: object) -> Self:
        return self._new(limit=_value(count))

    def offset(self, count: object) -> Self:
        return self._new(offset=_value(count))


class _HasReturning(Query):
    """``RETURNING``."""

    __slots__ = ()

    def returning(self, *items: object) -> Self:
        return self._new(returning=[_value(item) for item in items])


class _InsertBody(_HasReturning):
    """``VALUES``, columns, and ``ON CONFLICT``."""

    __slots__ = ()

    def columns(self, *names: str) -> Self:
        return self._new(columns=list(names))

    def values(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, *rows: Mapping[str, object]
    ) -> Self:
        """``VALUES`` for an insert. This is not ``Mapping.values``."""
        cooked = [{key: _value(val) for key, val in row.items()} for row in rows]
        return self._new(values=cooked)

    def on_conflict(self, *columns: str) -> Self:
        """``ON CONFLICT``. No columns means any conflict."""
        return self._new(on_conflict=list(columns))

    def do_nothing(self) -> Self:
        return self._new(do_nothing=True)

    def do_update(self, **assigns: object) -> Self:
        sets = {key: _value(val) for key, val in assigns.items()}
        return self._new(do_update_set=sets)


_JOIN_KINDS = frozenset({"inner", "left", "right", "full", "cross"})


def _many(raw: object) -> list[object]:
    """A list is many expressions. Any other value is one expression."""
    if isinstance(raw, list):
        items = cast(list[object], raw)
        return [_value(item) for item in items]
    return [_value(raw)]


def _and(raw: object) -> object:
    """One condition, or a list of conditions joined by AND."""
    if isinstance(raw, list):
        items = cast(list[object], raw)
        if not items or not isinstance(items[0], str):
            parts = [_value(item) for item in items]
            if not parts:
                raise WriterError(
                    "where needs a condition",
                    example='where=("=", "id", ":id")',
                )
            return parts[0] if len(parts) == 1 else ("and", *parts)
        return _value(items)
    return _value(raw)


def _is_one_join(raw: object) -> bool:
    if not isinstance(raw, list | tuple) or not raw:
        return False
    seq = cast(Sequence[object], raw)
    kind = seq[0]
    return isinstance(kind, str) and kind in _JOIN_KINDS


def _joins(raw: object) -> list[object]:
    if _is_one_join(raw):
        seq = cast(Sequence[object], raw)
        return [[_value(part) for part in seq]]
    if not isinstance(raw, list | tuple):
        raise WriterError(
            "join is one join or a list of joins",
            example='join=("left", "users", ("=", "m.author", "users.id"))',
        )
    found: list[object] = []
    rows = cast(Sequence[object], raw)
    for item in rows:
        if not _is_one_join(item):
            raise WriterError(
                "a join is (kind, source, on)",
                context={"join": item},
                example='("left", ("as", "users", "u"), ("=", "m.author", "u.id"))',
            )
        seq = cast(Sequence[object], item)
        found.append([_value(part) for part in seq])
    return found


def _names(raw: object, key: str) -> list[str]:
    if isinstance(raw, str) or not isinstance(raw, Sequence):
        raise WriterError(
            f"{key} is a list of names",
            example=f'{key}=["id"]',
        )
    names = cast(Sequence[object], raw)
    return [str(name) for name in names]


def _ctes(raw: object, *, recursive: bool) -> dict[str, Any]:
    if raw is None:
        if recursive:
            raise WriterError(
                "recursive needs with_",
                example='with_={"recent": Select("id", from_="t")}, recursive=True',
            )
        return {}
    if isinstance(raw, Query) or not isinstance(raw, Mapping) or not raw:
        raise WriterError(
            "with_ is a dict of name to query",
            example='with_={"recent": Select("id", from_="messages")}',
        )
    bodies = cast(Mapping[object, object], raw)
    key = "with_recursive" if recursive else "with"
    return {key: {str(name): _value(body) for name, body in bodies.items()}}


def _put_many(data: dict[str, Any], key: str, raw: object) -> None:
    if raw is not None:
        data[key] = _many(raw)


def _put_and(data: dict[str, Any], key: str, raw: object) -> None:
    if raw is not None:
        data[key] = _and(raw)


def _tail(
    data: dict[str, Any],
    *,
    order_by: object,
    limit: object,
    offset: object,
) -> None:
    _put_many(data, "order_by", order_by)
    if limit is not None:
        data["limit"] = _value(limit)
    if offset is not None:
        data["offset"] = _value(offset)


def _value_rows(raw: object) -> list[object]:
    if isinstance(raw, Mapping):
        pairs = cast(Mapping[object, object], raw)
        return [{str(key): _value(val) for key, val in pairs.items()}]
    if isinstance(raw, list | tuple):
        rows: list[object] = []
        incoming = cast(Sequence[object], raw)
        for row in incoming:
            if isinstance(row, Mapping):
                pairs = cast(Mapping[object, object], row)
                rows.append({str(key): _value(val) for key, val in pairs.items()})
            elif isinstance(row, list | tuple):
                seq = cast(Sequence[object], row)
                rows.append(tuple(_value(item) for item in seq))
            else:
                raise WriterError(
                    "a values row is a dict or a list",
                    context={"row": row},
                    example='values=[{"body": ":body"}]',
                )
        if not rows:
            raise WriterError(
                "values is a non-empty list of rows",
                example='values=[{"body": ":body"}]',
            )
        return rows
    raise WriterError(
        "values is a row or a list of rows",
        context={"values": raw},
        example='values={"body": ":body"}',
    )


def _insert_data(
    verb: str,
    table: object,
    *,
    columns: object,
    values: object,
    select: object,
    on_conflict: object,
    do_nothing: bool,
    do_update: Mapping[str, object] | None,
    returning: object,
    with_: object,
    recursive: bool,
) -> dict[str, Any]:
    if values is not None and select is not None:
        raise WriterError(
            "an insert needs values or select (one of them)",
            example='Insert("t", values={"body": ":body"})',
        )
    data: dict[str, Any] = {verb: _value(table)}
    data.update(_ctes(with_, recursive=recursive))
    if columns is not None:
        data["columns"] = _names(columns, "columns")
    if values is not None:
        data["values"] = _value_rows(values)
    if select is not None:
        if not isinstance(select, Mapping):
            raise WriterError(
                "select is a Select",
                example='select=Select("id", from_="t")',
            )
        found = cast(Mapping[object, object], select)
        body: dict[str, Any] = {str(key): val for key, val in found.items()}
        overlap = sorted(set(body) & set(data))
        if overlap:
            raise WriterError(
                "the select and the insert set the same clause",
                context={"keys": overlap},
            )
        data.update(body)
    if on_conflict is not None:
        data["on_conflict"] = _names(on_conflict, "on_conflict")
    if do_nothing:
        data["do_nothing"] = True
    if do_update is not None:
        data["do_update_set"] = {key: _value(val) for key, val in do_update.items()}
    _put_many(data, "returning", returning)
    return data


class Select(_HasFrom, _HasTail):
    """One SELECT. Pass every clause you know."""

    __slots__ = ()

    def select(self, *items: object) -> Self:
        data = {
            key: value for key, value in self._data.items() if key != "select_distinct"
        }
        data["select"] = [_value(item) for item in items]
        return self._from(data)

    def group_by(self, *items: object) -> Self:
        rows = _copy_list(self._data.get("group_by"))
        rows.extend(_value(item) for item in items)
        return self._new(group_by=rows)

    def having(self, condition: object) -> Self:
        return self._new(having=_value(condition))

    def __init__(
        self,
        *items: object,
        distinct: bool = False,
        from_: object = None,
        join: object = None,
        where: object = None,
        group_by: object = None,
        having: object = None,
        order_by: object = None,
        limit: object = None,
        offset: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        if not items:
            raise WriterError(
                "Select needs an expression",
                example='Select("id", from_="t")',
            )
        key = "select_distinct" if distinct else "select"
        data: dict[str, Any] = {key: [_value(item) for item in items]}
        data.update(_ctes(with_, recursive=recursive))
        if from_ is not None:
            data["from"] = _value(from_)
        if join is not None:
            data["join"] = _joins(join)
        _put_and(data, "where", where)
        _put_many(data, "group_by", group_by)
        _put_and(data, "having", having)
        _tail(data, order_by=order_by, limit=limit, offset=offset)
        super().__init__(data)


class Insert(_InsertBody):
    """One INSERT. Pass the table and the row."""

    __slots__ = ()

    def __init__(
        self,
        table: object,
        *,
        columns: object = None,
        values: object = None,
        select: Mapping[str, Any] | None = None,
        on_conflict: object = None,
        do_nothing: bool = False,
        do_update: Mapping[str, object] | None = None,
        returning: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        super().__init__(
            _insert_data(
                "insert_into",
                table,
                columns=columns,
                values=values,
                select=select,
                on_conflict=on_conflict,
                do_nothing=do_nothing,
                do_update=do_update,
                returning=returning,
                with_=with_,
                recursive=recursive,
            )
        )


class Update(_HasFrom, _HasReturning):
    """One UPDATE. ``set`` maps a column to a value."""

    __slots__ = ()

    def set(self, **assigns: object) -> Self:
        """``SET`` for an update."""
        return self._new(**{"set": {key: _value(val) for key, val in assigns.items()}})

    def __init__(
        self,
        table: object,
        *,
        set: Mapping[str, object],
        from_: object = None,
        join: object = None,
        where: object = None,
        returning: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        data: dict[str, Any] = {
            "update": _value(table),
            "set": {key: _value(val) for key, val in set.items()},
        }
        data.update(_ctes(with_, recursive=recursive))
        if from_ is not None:
            data["from"] = _value(from_)
        if join is not None:
            data["join"] = _joins(join)
        _put_and(data, "where", where)
        _put_many(data, "returning", returning)
        super().__init__(data)


class Delete(_HasWhere, _HasReturning):
    """One DELETE."""

    __slots__ = ()

    def __init__(
        self,
        table: object,
        *,
        where: object = None,
        returning: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        data: dict[str, Any] = {"delete_from": _value(table)}
        data.update(_ctes(with_, recursive=recursive))
        _put_and(data, "where", where)
        _put_many(data, "returning", returning)
        super().__init__(data)


def _compound(
    key: str,
    queries: tuple[Query, ...],
    *,
    order_by: object,
    limit: object,
    offset: object,
    with_: object,
    recursive: bool,
) -> dict[str, Any]:
    if len(queries) < 2:
        raise WriterError(
            f"{key} needs two queries",
            example='UnionAll(Select("id", from_="a"), Select("id", from_="b"))',
        )
    data: dict[str, Any] = {key: [dict(query) for query in queries]}
    data.update(_ctes(with_, recursive=recursive))
    _tail(data, order_by=order_by, limit=limit, offset=offset)
    return data


class Union(_HasTail):
    """``UNION`` of two or more statements."""

    __slots__ = ()

    def __init__(
        self,
        *queries: Query,
        order_by: object = None,
        limit: object = None,
        offset: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        super().__init__(
            _compound(
                "union",
                queries,
                order_by=order_by,
                limit=limit,
                offset=offset,
                with_=with_,
                recursive=recursive,
            )
        )


class UnionAll(_HasTail):
    """``UNION ALL`` of two or more statements."""

    __slots__ = ()

    def __init__(
        self,
        *queries: Query,
        order_by: object = None,
        limit: object = None,
        offset: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        super().__init__(
            _compound(
                "union_all",
                queries,
                order_by=order_by,
                limit=limit,
                offset=offset,
                with_=with_,
                recursive=recursive,
            )
        )


class Intersect(_HasTail):
    """``INTERSECT`` of two or more statements."""

    __slots__ = ()

    def __init__(
        self,
        *queries: Query,
        order_by: object = None,
        limit: object = None,
        offset: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        super().__init__(
            _compound(
                "intersect",
                queries,
                order_by=order_by,
                limit=limit,
                offset=offset,
                with_=with_,
                recursive=recursive,
            )
        )


class Except(_HasTail):
    """``EXCEPT`` of two or more statements."""

    __slots__ = ()

    def __init__(
        self,
        *queries: Query,
        order_by: object = None,
        limit: object = None,
        offset: object = None,
        with_: object = None,
        recursive: bool = False,
    ) -> None:
        super().__init__(
            _compound(
                "except",
                queries,
                order_by=order_by,
                limit=limit,
                offset=offset,
                with_=with_,
                recursive=recursive,
            )
        )
