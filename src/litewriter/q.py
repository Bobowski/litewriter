"""Render a statement to SQL text and its bound parameters.

Clause data lives on the statements in ``litewriter.build``; expressions
are node tuples from ``litewriter.expr``. Two contexts matter:

- NAME: the select list, FROM, JOIN sources, GROUP BY, ORDER BY,
  RETURNING, table and column names. A ``str`` there is an identifier.
- VALUE: everything else. A Python ``str``, ``bytes``, ``int``,
  ``float``, or ``bool`` becomes an auto-bound parameter ``:_1``,
  ``:_2``, ... collected in ``params``. ``None`` is ``NULL``.

The numbering restarts per statement, so composing (``.where``,
subqueries, compounds) never collides and the same shape always renders
the same ``.sql`` text.
"""

# pyright: reportPrivateUsage=false

import math
import re
from collections.abc import Callable, Mapping
from typing import Any, cast

import apsw

from litewriter.errors import QueryError, StateError
from litewriter.expr import _RESERVED, Expr, _param_name, _Stmt

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_KEYWORDS = frozenset(word.upper() for word in apsw.keywords)

# Binding strength from https://sqlite.org/lang_expr.html. A child that
# binds less tightly than its parent gets parentheses.
_PREC = {
    "OR": 1,
    "AND": 2,
    "=": 4,
    "==": 4,
    "!=": 4,
    "<>": 4,
    "IS": 4,
    "IS NOT": 4,
    "IS DISTINCT FROM": 4,
    "IS NOT DISTINCT FROM": 4,
    "LIKE": 4,
    "NOT LIKE": 4,
    "GLOB": 4,
    "NOT GLOB": 4,
    "REGEXP": 4,
    "MATCH": 4,
    "IN": 4,
    "NOT IN": 4,
    "BETWEEN": 4,
    "NOT BETWEEN": 4,
    "<": 5,
    "<=": 5,
    ">": 5,
    ">=": 5,
    "&": 6,
    "|": 6,
    "<<": 6,
    ">>": 6,
    "+": 7,
    "-": 7,
    "*": 8,
    "/": 8,
    "%": 8,
    "||": 9,
    "->": 9,
    "->>": 9,
}
_NOT = 3
_TEST = 4
_UNARY = 11

_JOINS = {
    "inner": "JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL JOIN",
    "cross": "CROSS JOIN",
}
_COMPOUND = {
    "union": "UNION",
    "union_all": "UNION ALL",
    "intersect": "INTERSECT",
    "except": "EXCEPT",
}
_TAIL = ("order_by", "limit", "offset")


class _Ctx:
    """One render pass. ``bind`` numbers the auto-bound parameters."""

    __slots__ = ("n", "params")

    def __init__(self) -> None:
        self.n = 0
        self.params: dict[str, object] = {}

    def bind(self, value: object) -> str:
        self.n += 1
        name = f"_{self.n}"
        self.params[name] = value
        return ":" + name


def render(data: dict[str, Any]) -> tuple[str, dict[str, object]]:
    """The SQL text and the auto-bound parameters for one statement."""
    ctx = _Ctx()
    return _render(data, ctx), ctx.params


def _render(data: dict[str, Any], ctx: _Ctx) -> str:
    parts: list[str] = []
    _with(data, parts, ctx)
    kind = data["kind"]
    if kind == "insert":
        _insert(data, parts, ctx)
    elif kind == "update":
        _update(data, parts, ctx)
    elif kind == "delete":
        _delete(data, parts, ctx)
    elif kind == "compound":
        _compound(data, parts, ctx)
    else:
        _select(data, parts, ctx)
    return " ".join(parts)


def _with(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    ctes = data.get("with")
    if not ctes:
        return
    bits = [
        f"{_alias(name)} AS ({_render(body._clauses(), ctx)})"
        for name, body in ctes.items()
    ]
    head = "WITH RECURSIVE" if data.get("recursive") else "WITH"
    parts.append(f"{head} {', '.join(bits)}")


def _select(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    head = "SELECT DISTINCT" if data.get("distinct") else "SELECT"
    parts.append(f"{head} {', '.join(_name(item, ctx) for item in data['items'])}")
    _from(data, parts, ctx)
    if data.get("where"):
        parts.append("WHERE " + _conds(data["where"], ctx))
    if data.get("group_by"):
        group = ", ".join(_name(item, ctx) for item in data["group_by"])
        parts.append("GROUP BY " + group)
    if data.get("having"):
        parts.append("HAVING " + _conds(data["having"], ctx))
    _tail(data, parts, ctx)


def _compound(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    text = _member(data["first"], ctx)
    for key, member in data["ops"]:
        text += f" {_COMPOUND[key]} {_member(member, ctx)}"
    parts.append(text)
    _tail(data, parts, ctx)


def _member(stmt: _Stmt, ctx: _Ctx) -> str:
    data = stmt._clauses()
    stray = [key for key in _TAIL if data.get(key)]
    if stray:
        raise QueryError(
            "order_by, limit, and offset go on the compound, not on a member",
            context={"keys": stray},
            example='Select("id", from_="a").union_all(Select("id", from_="b")).limit(1)',
        )
    return _render(data, ctx)


def _insert(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    head = f"INSERT INTO {_name(data['table'], ctx)}"
    if "values" in data:
        rows = data["values"]
        keys = list(rows[0])
        if not keys:
            if "on_conflict" in data:
                raise QueryError(
                    "on_conflict needs values",
                    example='Insert("t", values={"body": "hi"}, on_conflict=["id"])',
                )
            parts.append(head + " DEFAULT VALUES")
        else:
            for row in rows[1:]:
                if list(row) != keys and set(row) != set(keys):
                    raise QueryError(
                        "every row in values has the same keys",
                        context={"first": keys, "row": list(row)},
                    )
            columns = data.get("columns")
            if columns is not None and set(columns) != set(keys):
                raise QueryError(
                    "columns and the keys of values differ",
                    context={"columns": columns, "keys": keys},
                )
            head += " (" + ", ".join(_column(key) for key in keys) + ")"
            body = ", ".join(
                "(" + ", ".join(_val(row[key], ctx) for key in keys) + ")"
                for row in rows
            )
            parts.append(f"{head} VALUES {body}")
    elif "select" in data:
        columns = data.get("columns")
        if columns:
            head += " (" + ", ".join(_column(col) for col in columns) + ")"
        parts.append(head)
        sel = dict(data["select"]._clauses())
        # SQLite reads "ON" after a bare FROM as a join. WHERE TRUE ends it.
        if "on_conflict" in data and not sel.get("where"):
            sel["where"] = [Expr(("raw", "TRUE"))]
        parts.append(_render(sel, ctx))
    else:
        columns = data.get("columns")
        if not columns:
            raise QueryError(
                "an insert needs columns, values, or select",
                example='Insert("t", columns=["body"])',
            )
        head += " (" + ", ".join(_column(name) for name in columns) + ")"
        bound = ", ".join(":" + _param_name(name) for name in columns)
        parts.append(f"{head} VALUES ({bound})")
    _conflict(data, parts, ctx)
    _returning(data, parts, ctx)


def _conflict(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    doing_nothing = "do_nothing" in data
    doing_update = "do_update" in data
    if "on_conflict" not in data:
        if doing_nothing or doing_update:
            raise QueryError(
                "do_nothing or do_update needs on_conflict",
                example='Insert("t", values=row).on_conflict("id").do_nothing()',
            )
        return
    if doing_nothing == doing_update:
        raise QueryError(
            "on_conflict needs do_nothing or do_update (one of them)",
            example='Insert("t", values=row, on_conflict=["id"], do_nothing=True)',
        )
    target = [_column(col) for col in data["on_conflict"]]
    head = "ON CONFLICT" + (f" ({', '.join(target)})" if target else "")
    if doing_nothing:
        parts.append(f"{head} DO NOTHING")
        return
    if not target:
        raise QueryError(
            "do_update needs the conflict columns",
            example='Insert("t", values=row, on_conflict=["id"], do_update={...})',
        )
    parts.append(f"{head} DO UPDATE SET {_assign(data['do_update'], ctx)}")


def _update(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    parts.append(f"UPDATE {_name(data['table'], ctx)} SET {_assign(data['set'], ctx)}")
    _from(data, parts, ctx)
    if data.get("where"):
        parts.append("WHERE " + _conds(data["where"], ctx))
    _returning(data, parts, ctx)


def _delete(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    parts.append(f"DELETE FROM {_name(data['table'], ctx)}")
    if data.get("where"):
        parts.append("WHERE " + _conds(data["where"], ctx))
    _returning(data, parts, ctx)


def _from(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    if data.get("join") and data.get("from") is None:
        raise QueryError("join needs from", example='from_="messages"')
    if data.get("from") is not None:
        parts.append("FROM " + _name(data["from"], ctx))
    for item in data.get("join") or ():
        kind, source, on = item
        if kind == "cross":
            parts.append(f"CROSS JOIN {_name(source, ctx)}")
        else:
            parts.append(f"{_JOINS[kind]} {_name(source, ctx)} ON {_val(on, ctx)}")


def _tail(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    if data.get("order_by"):
        order = ", ".join(_name(item, ctx) for item in data["order_by"])
        parts.append("ORDER BY " + order)
    if data.get("limit") is not None:
        parts.append("LIMIT " + _val(data["limit"], ctx))
    elif data.get("offset") is not None:
        parts.append("LIMIT -1")
    if data.get("offset") is not None:
        parts.append("OFFSET " + _val(data["offset"], ctx))


def _returning(data: dict[str, Any], parts: list[str], ctx: _Ctx) -> None:
    if data.get("returning"):
        items = ", ".join(_name(item, ctx) for item in data["returning"])
        parts.append("RETURNING " + items)


def _assign(assigns: dict[str, object], ctx: _Ctx) -> str:
    if not assigns:
        raise QueryError(
            "set is a dict of column to value",
            example='Update("t", set={"body": "hi"})',
        )
    return ", ".join(
        f"{_column(key)} = {_val(value, ctx)}" for key, value in assigns.items()
    )


def _conds(conds: list[object], ctx: _Ctx) -> str:
    """The WHERE/HAVING text. The clause itself is one implicit AND."""
    flat: list[object] = []
    for cond in conds:
        if isinstance(cond, Expr) and cond.data[0] == "and":
            flat.extend(cond.data[1])
        else:
            flat.append(cond)
    return " AND ".join(_val(cond, ctx, _PREC["AND"]) for cond in flat)


def _name(raw: object, ctx: _Ctx) -> str:
    """A name position: the select list, a source, GROUP/ORDER BY, a name."""
    if isinstance(raw, Expr):
        return _node(raw.data, ctx, 0)
    if isinstance(raw, _Stmt):
        return f"({_render(raw._clauses(), ctx)})"
    if isinstance(raw, str):
        if raw.startswith("'"):
            raise QueryError(
                "a string in a name position is an identifier",
                context={"name": raw},
                help_text="For an inline literal use lit(); a bound value "
                "is automatic in a value position.",
                example='lit("open")',
            )
        if raw.startswith(":"):
            raise QueryError(
                "a string in a name position is an identifier",
                context={"name": raw},
                help_text="For a named parameter use param().",
                example='param("room")',
            )
        return _ident(raw)
    return _scalar(raw, ctx)


def _val(raw: object, ctx: _Ctx, parent: int = 0) -> str:
    """A value position: every Python value binds, ``None`` is ``NULL``."""
    if isinstance(raw, Expr):
        return _node(raw.data, ctx, parent)
    if isinstance(raw, _Stmt):
        return f"({_render(raw._clauses(), ctx)})"
    if isinstance(raw, float) and not math.isfinite(raw):
        raise QueryError("a float literal is finite", context={"value": raw})
    if isinstance(raw, str | bytes | int | float | bool):
        return ctx.bind(raw)
    if raw is None:
        return "NULL"
    raise QueryError(
        "a value is an expression, a statement, a number, bool, None, str, or bytes",
        context={"value": raw},
    )


def _scalar(raw: object, ctx: _Ctx) -> str:
    if isinstance(raw, bool):
        return "TRUE" if raw else "FALSE"
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, float):
        if not math.isfinite(raw):
            raise QueryError("a float literal is finite", context={"value": raw})
        return repr(raw)
    if raw is None:
        return "NULL"
    if isinstance(raw, bytes):
        return f"X'{raw.hex()}'"
    raise QueryError(
        "a value is an expression, a statement, a number, bool, None, str, or bytes",
        context={"value": raw},
    )


def _node(raw: object, ctx: _Ctx, parent: int) -> str:
    if not isinstance(raw, tuple) or not raw:
        raise QueryError(
            "not an expression",
            context={"expression": raw},
            example='col("name") == "value"',
        )
    data = cast("tuple[Any, ...]", raw)
    tag = data[0]
    if tag == "col":
        return _ident(data[1])
    if tag == "par":
        return ":" + data[1]
    if tag == "lit":
        return _literal(data[1])
    if tag == "raw":
        return data[1]
    if tag == "neg":
        return _wrap("- " + _val(data[1], ctx, _UNARY), _UNARY, parent)
    if tag == "not":
        return _wrap("NOT " + _val(data[1], ctx, _NOT), _NOT, parent)
    if tag == "and" or tag == "or":
        op = tag.upper()
        own = _PREC[op]
        text = f" {op} ".join(_val(item, ctx, own) for item in data[1])
        return _wrap(text, own, parent)
    if tag == "bin":
        op = data[1]
        own = _PREC[op]
        text = f"{_val(data[2], ctx, own)} {op} {_val(data[3], ctx, own)}"
        return _wrap(text, own, parent)
    if tag == "in":
        left = _val(data[1], ctx, _TEST)
        items = data[2]
        if isinstance(items, _Stmt):
            right = _render(items._clauses(), ctx)
        else:
            right = ", ".join(_val(item, ctx) for item in items)
        op = "NOT IN" if data[3] else "IN"
        return _wrap(f"{left} {op} ({right})", _TEST, parent)
    if tag == "btw":
        left = _val(data[1], ctx, _TEST)
        low = _val(data[2], ctx, _TEST)
        high = _val(data[3], ctx, _TEST)
        op = "NOT BETWEEN" if data[4] else "BETWEEN"
        return _wrap(f"{left} {op} {low} AND {high}", _TEST, parent)
    if tag == "case":
        bits = ["CASE"]
        for cond, value in data[1]:
            bits.append(f"WHEN {_val(cond, ctx)} THEN {_val(value, ctx)}")
        if data[2] is not None:
            bits.append(f"ELSE {_val(data[2], ctx)}")
        bits.append("END")
        return " ".join(bits)
    if tag == "as":
        return f"{_val(data[1], ctx)} AS {_alias(data[2])}"
    if tag == "ord":
        text = f"{_val(data[2], ctx)} {data[1]}"
        if data[3] is not None:
            text += f" {data[3]}"
        return text
    if tag == "cast":
        return f"CAST({_val(data[1], ctx)} AS {data[2]})"
    if tag == "coll":
        return _wrap(f"{_val(data[1], ctx, 10)} COLLATE {data[2]}", 10, parent)
    if tag == "fnc":
        args = ", ".join(_val(arg, ctx) for arg in data[2])
        return f"{data[1]}({args})"
    if tag == "ext":
        op = "NOT EXISTS" if data[2] else "EXISTS"
        return f"{op} ({_render(data[1]._clauses(), ctx)})"
    raise QueryError(
        "not an expression",
        context={"expression": data},
        example='col("name") == "value"',
    )


def _wrap(text: str, own: int, parent: int) -> str:
    return f"({text})" if own <= parent else text


def _ident(text: str) -> str:
    parts = text.split(".")
    out: list[str] = []
    for index, part in enumerate(parts):
        if part == "*" and index == len(parts) - 1:
            out.append("*")
        elif _NAME.fullmatch(part):
            out.append(_quote(part))
        else:
            raise QueryError(
                "not a name",
                context={"string": text},
                help_text="A name is letters, digits, and _, dotted ok.",
            )
    return ".".join(out)


def _literal(text: object) -> str:
    if not isinstance(text, str) or "\x00" in text:
        raise QueryError(
            "a string literal has no NUL",
            help_text="SQLite reads the SQL text up to the first NUL.",
        )
    return "'" + text.replace("'", "''") + "'"


def _alias(raw: object) -> str:
    if not isinstance(raw, str) or not _NAME.fullmatch(raw):
        raise QueryError(
            "an alias is a plain name", context={"alias": raw}, example='"author"'
        )
    return _quote(raw)


def _column(raw: object) -> str:
    if not isinstance(raw, str) or not _NAME.fullmatch(raw):
        raise QueryError(
            "a column is a plain name", context={"column": raw}, example='"body"'
        )
    return _quote(raw)


def _quote(name: str) -> str:
    return f'"{name}"' if name.upper() in _KEYWORDS else name


# --- running a query ---------------------------------------------------------

_QUOTED = re.compile(
    r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|--[^\n]*|/\*.*?\*/", re.DOTALL
)
_BINDING = re.compile(r"[:@$]([A-Za-z_][A-Za-z0-9_]*)")


def params_in(text: str) -> frozenset[str]:
    """The named parameters in ``text``, literals and comments stripped."""
    return frozenset(_BINDING.findall(_QUOTED.sub("''", text)))


def as_into(into: object) -> Callable[..., Any]:
    """``into`` builds a row. A non-callable is a caller bug."""
    if not callable(into):
        raise QueryError(
            "into builds a row",
            context={"into": into},
            example="db.query(q, Row)",
        )
    return cast("Callable[..., Any]", into)


def _split(q: object) -> tuple[str, dict[str, object]]:
    """The SQL text and the statement's auto-bound parameters."""
    if isinstance(q, str):
        return q, {}
    if isinstance(q, _Stmt):
        return q._compiled()
    raise QueryError(
        "a query is SQL text or a statement",
        context={"query": q},
        example='db.query(Select("id", from_="t"))',
    )


def merge(
    q: object, call: Mapping[str, object]
) -> tuple[str, Mapping[str, object] | None]:
    """The SQL text plus call params merged over the auto-bound ones.

    A callable ``into`` kwarg is positional in the public calls, not a
    parameter. On a builder statement a call kwarg matching ``_[0-9]+``
    is rejected: ``_1``, ``_2``, ... are reserved for the values the
    builder bound. Plain SQL text may name a parameter ``:_1``.
    """
    if not call and type(q) is str:
        return q, None
    if callable(call.get("into")):
        raise QueryError(
            "into is positional",
            help_text="A keyword into= becomes a SQL parameter.",
            example="db.query(q, Message, room=1)",
        )
    if isinstance(q, _Stmt):
        for key in call:
            if _RESERVED.fullmatch(key):
                raise QueryError(
                    "a named parameter is not an auto-bind name",
                    context={"parameter": key},
                    help_text="_1, _2, ... are reserved on builder statements.",
                )
    text, auto = _split(q)
    if not auto:
        return text, call or None
    return text, {**auto, **call}


def run(
    conn: apsw.Connection,
    text: str,
    bound: Mapping[str, object] | None,
) -> apsw.Cursor:
    """Run SQL on a connection. A missing ``:name`` raises QueryError."""
    try:
        bindings = cast("apsw.Bindings | None", bound)
        return conn.execute(text, bindings)
    except KeyError as exc:
        raise QueryError(
            "missing parameter",
            context={"parameter": f":{exc.args[0]}"},
            example=f"db.query(q, {exc.args[0]}=...)",
        ) from None
    except apsw.BindingsError as exc:
        raise QueryError(
            "the parameters do not match the query",
            context={"detail": str(exc)},
            example="db.query(q, name=...)",
        ) from None
    except apsw.ConnectionClosedError:
        raise StateError("the writer is closed") from None


def rows[T](cursor: apsw.Cursor, into: Callable[..., T]) -> list[T]:
    """Every row, each built as ``into(*row)``."""
    if into is tuple:
        return cast("list[T]", cursor.fetchall())
    return [into(*row) for row in cursor]


def one[T](cursor: apsw.Cursor, into: Callable[..., T]) -> T | None:
    """The first row as ``into(*row)``, or None when there is none."""
    try:
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None:
        return None
    if into is tuple:
        return cast("T", row)
    return into(*row)


def value(cursor: apsw.Cursor) -> Any:
    """The first column of the first row. No row raises QueryError."""
    try:
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None:
        raise QueryError(
            "the query returned no row",
            example='tx.value(Insert("t", values={"body": "hi"}, returning="id"))',
        )
    return row[0]
