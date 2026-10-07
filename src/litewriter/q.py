"""SQL as data.

A query is a dict. Each key is a SQL clause. ``sql`` returns the text you
would write by hand, in clause order, so a key can come in any order.

Inside a clause, a value is one of these:

- ``"u.name"`` — a name. ``*`` and ``u.*`` too.
- ``"'open'"`` — a string literal, as in SQL.
- ``":room"`` — a parameter. Bind it when you run the query.
- a number, ``True``, ``False``, ``None``, or ``bytes`` — a literal.
- a list or a tuple — a call. The first item is the name: ``("lower", "u.name")``.
- ``("as", expr, "name")`` — an alias. It works in every clause:
  ``("as", "messages", "m")`` is ``messages AS m``.
  ``("as", ("lower", "u.name"), "author")`` is ``lower(u.name) AS author``.
- ``("lit", "open")`` — a string literal. ``'open'`` also works.
- a dict — a subquery.
- an ``Expr`` from ``col``, ``lit``, or ``param`` — the value it stands for.

A call name that is not an operator is a function. ``sql`` does not
keep a list of functions, so every SQLite function (and every function
you register) works the same way.
"""

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any, cast

import apsw

from litewriter.connect import Bindings
from litewriter.errors import WriterError
from litewriter.expr import Expr

type Clauses = Mapping[str, Any]

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PARAM = re.compile(r":[A-Za-z_][A-Za-z0-9_]*")
_KEYWORDS = frozenset(word.upper() for word in apsw.keywords)
_STRINGS = "A string is a name (u.name), a literal ('open'), or a parameter (:room)."

# Binding strength. A child that binds less tightly than its parent gets
# parentheses. The numbers follow https://sqlite.org/lang_expr.html.
_INFIX: dict[str, tuple[str, int]] = {
    "or": ("OR", 1),
    "and": ("AND", 2),
    "=": ("=", 4),
    "==": ("==", 4),
    "!=": ("!=", 4),
    "<>": ("<>", 4),
    "is": ("IS", 4),
    "is not": ("IS NOT", 4),
    "is distinct from": ("IS DISTINCT FROM", 4),
    "is not distinct from": ("IS NOT DISTINCT FROM", 4),
    "like": ("LIKE", 4),
    "not like": ("NOT LIKE", 4),
    "glob": ("GLOB", 4),
    "not glob": ("NOT GLOB", 4),
    "regexp": ("REGEXP", 4),
    "match": ("MATCH", 4),
    "<": ("<", 5),
    "<=": ("<=", 5),
    ">": (">", 5),
    ">=": (">=", 5),
    "&": ("&", 6),
    "|": ("|", 6),
    "<<": ("<<", 6),
    ">>": (">>", 6),
    "+": ("+", 7),
    "-": ("-", 7),
    "*": ("*", 8),
    "/": ("/", 8),
    "%": ("%", 8),
    "||": ("||", 9),
    "->": ("->", 9),
    "->>": ("->>", 9),
}
_NOT = 3
_TEST = 4
_COLLATE = 10
_UNARY = 11
_ATOM = 99

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

_WITH = {"with", "with_recursive"}
_TAIL = {"order_by", "limit", "offset"}
_SELECT = {
    *_WITH,
    "select",
    "select_distinct",
    "from",
    "join",
    "where",
    "group_by",
    "having",
    *_TAIL,
}
_KINDS: dict[str, frozenset[str]] = {
    "select": frozenset(_SELECT),
    "compound": frozenset({*_WITH, *_COMPOUND, *_TAIL}),
    "insert": frozenset(
        {
            *_SELECT,
            "insert_into",
            "replace_into",
            "columns",
            "values",
            "on_conflict",
            "do_nothing",
            "do_update_set",
            "returning",
        }
    ),
    "update": frozenset(
        {*_WITH, "update", "set", "from", "join", "where", "returning"}
    ),
    "delete": frozenset({*_WITH, "delete_from", "where", "returning"}),
}
_KEYS: frozenset[str] = frozenset[str]().union(*_KINDS.values())

_EXAMPLE = """\
{"select": ["id", ["lower", "name"]], "from": "users", "where": ["=", "id", ":id"]}"""


def sql(query: Clauses) -> str:
    """The SQL text for ``query``. Parameters stay as ``:name``."""
    return _statement(query)


def execute(
    conn: apsw.Connection, query: str | Clauses, params: Bindings = ()
) -> apsw.Cursor:
    """Run SQL text or a statement. A missing ``:name`` raises WriterError."""
    text = query if isinstance(query, str) else sql(query)
    bound = params if isinstance(params, Mapping) else params or None
    try:
        # A cached statement runs the trace before the authorizer, so the
        # watch never sees that write. Recompile while an authorizer is set.
        if conn.authorizer is None:
            return conn.execute(text, cast("apsw.Bindings", bound))
        return conn.execute(text, cast("apsw.Bindings", bound), can_cache=False)
    except KeyError as exc:
        raise WriterError(
            "missing parameter",
            context={"parameter": f":{exc.args[0]}"},
            example=f"db.query(q, {exc.args[0]}=...)",
        ) from None


def rows(
    conn: apsw.Connection, query: str | Clauses, params: Bindings = ()
) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in execute(conn, query, params)]


def one(
    conn: apsw.Connection, query: str | Clauses, params: Bindings = ()
) -> tuple[Any, ...] | None:
    row = execute(conn, query, params).fetchone()
    if row is None:
        return None
    return tuple(row)


def value(conn: apsw.Connection, query: str | Clauses, params: Bindings = ()) -> Any:
    """The first column of the first row."""
    row = one(conn, query, params)
    if row is None:
        raise WriterError(
            "the query returned no row",
            example=(
                'tx.value(Insert("t", values={"body": ":body"}, returning="id"),'
                ' body="hi")'
            ),
        )
    return row[0]


def where(query: Clauses, *conditions: object) -> dict[str, Any]:
    """A copy of ``query`` with ``conditions`` added to ``where`` by AND."""
    if not conditions:
        return dict(query)
    old = query.get("where")
    if old is None:
        parts: list[object] = []
    elif _is_call(old, "and"):
        parts = list(old[1:])
    else:
        parts = [old]
    parts.extend(conditions)
    return {**query, "where": parts[0] if len(parts) == 1 else ["and", *parts]}


def _is_call(raw: Any, name: str) -> bool:
    if not isinstance(raw, list | tuple):
        return False
    items = cast(Sequence[object], raw)
    return len(items) > 0 and items[0] == name


# --- statements --------------------------------------------------------------


def _statement(query: Any) -> str:
    if not isinstance(query, Mapping):
        raise WriterError("a query is a dict", example=_EXAMPLE)
    q: Clauses = query  # pyright: ignore[reportUnknownVariableType]
    unknown = sorted(set(q) - _KEYS)
    if unknown:
        raise WriterError(
            "unknown query key",
            context={"keys": unknown},
            help_text=f"Keys are {', '.join(sorted(_KEYS))}.",
        )
    kind = _kind(q)
    stray = sorted(set(q) - _KINDS[kind])
    if stray:
        raise WriterError(
            f"this key is not part of a {kind} query",
            context={"keys": stray},
        )
    parts: list[str] = []
    _with(q, parts)
    match kind:
        case "insert":
            _insert(q, parts)
        case "update":
            _update(q, parts)
        case "delete":
            _delete(q, parts)
        case "compound":
            _compound(q, parts)
        case _:
            _select(q, parts)
    return " ".join(parts)


def _kind(q: Clauses) -> str:
    if "insert_into" in q or "replace_into" in q:
        return "insert"
    if "update" in q:
        return "update"
    if "delete_from" in q:
        return "delete"
    if any(key in q for key in _COMPOUND):
        return "compound"
    if "select" in q or "select_distinct" in q:
        return "select"
    raise WriterError(
        "a query needs select, insert_into, update, delete_from, or union",
        example=_EXAMPLE,
    )


def _with(q: Clauses, parts: list[str]) -> None:
    if "with" in q and "with_recursive" in q:
        raise WriterError("use with or with_recursive, not both")
    key = "with_recursive" if "with_recursive" in q else "with"
    if key not in q:
        return
    ctes = q[key]
    if not isinstance(ctes, Mapping) or not ctes:
        raise WriterError(
            f"{key} is a dict of name to query",
            example='"with": {"recent": {"select": ["id"], "from": "messages"}}',
        )
    items: Mapping[object, object] = ctes  # pyright: ignore[reportUnknownVariableType]
    bits = [f"{_alias(name)} AS ({_statement(body)})" for name, body in items.items()]
    head = "WITH RECURSIVE" if key == "with_recursive" else "WITH"
    parts.append(f"{head} {', '.join(bits)}")


def _select(q: Clauses, parts: list[str]) -> None:
    if "select" in q and "select_distinct" in q:
        raise WriterError("use select or select_distinct, not both")
    distinct = "select_distinct" in q
    key = "select_distinct" if distinct else "select"
    head = "SELECT DISTINCT" if distinct else "SELECT"
    parts.append(f"{head} {_exprs(q[key], key)}")
    _from(q, parts)
    if "where" in q:
        parts.append("WHERE " + _expr(q["where"]))
    if "group_by" in q:
        parts.append("GROUP BY " + _exprs(q["group_by"], "group_by"))
    if "having" in q:
        parts.append("HAVING " + _expr(q["having"]))
    _tail(q, parts)


def _compound(q: Clauses, parts: list[str]) -> None:
    keys = [key for key in _COMPOUND if key in q]
    if len(keys) != 1:
        raise WriterError(
            "use one of union, union_all, intersect, except",
            context={"keys": keys},
        )
    key = keys[0]
    members = _items(q[key], key)
    if len(members) < 2:
        raise WriterError(
            f"{key} is a list of two or more queries",
            example=f'"{key}": [{{"select": ["id"], "from": "a"}}, {{"select": ["id"], "from": "b"}}]',
        )
    bits: list[str] = []
    for member in members:
        if isinstance(member, Mapping) and _TAIL & set(member):  # pyright: ignore[reportUnknownArgumentType]
            raise WriterError(
                f"order_by, limit, and offset go on the {key} query, not on a member",
                example='Select("id", from_=Select("id", from_="t", limit=1))',
            )
        bits.append(_statement(member))
    parts.append(f" {_COMPOUND[key]} ".join(bits))
    _tail(q, parts)


def _insert(q: Clauses, parts: list[str]) -> None:
    if "insert_into" in q and "replace_into" in q:
        raise WriterError("use insert_into or replace_into, not both")
    replace = "replace_into" in q
    table = _source(q["replace_into" if replace else "insert_into"])
    head = "REPLACE INTO" if replace else "INSERT INTO"
    selecting = "select" in q or "select_distinct" in q
    if ("values" in q) == selecting:
        raise WriterError(
            "an insert needs values or select (one of them)",
            example='{"insert_into": "t", "values": [{"body": ":body"}]}',
        )
    columns = (
        [_column(c) for c in _items(q["columns"], "columns")]
        if "columns" in q
        else None
    )
    if "values" in q:
        extra = sorted(set(q) & (_SELECT - _WITH))
        if extra:
            raise WriterError(
                "an insert with values takes no select keys", context={"keys": extra}
            )
        columns, rows = _rows(q["values"], columns)
        into = f"{head} {table}"
        if columns:
            into += f" ({', '.join(columns)})"
        parts.append(f"{into} VALUES {rows}")
    else:
        into = f"{head} {table}"
        if columns:
            into += f" ({', '.join(columns)})"
        parts.append(into)
        body = dict(q)
        for key in (
            "insert_into",
            "replace_into",
            "columns",
            "on_conflict",
            "do_nothing",
            "do_update_set",
            "returning",
            *_WITH,
        ):
            body.pop(key, None)
        # SQLite reads "ON" after a bare FROM as a join. WHERE true ends it.
        if "on_conflict" in q and "where" not in body:
            body["where"] = True
        _select(body, parts)
    _conflict(q, parts)
    _returning(q, parts)


def _rows(raw: object, columns: list[str] | None) -> tuple[list[str] | None, str]:
    rows = _items(raw, "values")
    if not rows:
        raise WriterError(
            "values is a non-empty list of rows",
            example='"values": [{"body": ":body"}]',
        )
    if all(isinstance(row, Mapping) for row in rows):
        maps: list[Mapping[str, object]] = rows  # pyright: ignore[reportAssignmentType]
        keys = list(maps[0])
        for row in maps:
            if list(row) != keys and set(row) != set(keys):
                raise WriterError(
                    "every row in values has the same keys",
                    context={"first": keys, "row": list(row)},
                )
        if columns is not None and set(columns) != {_column(k) for k in keys}:
            raise WriterError(
                "columns and the keys of values differ",
                context={"columns": columns, "keys": keys},
            )
        names = [_column(key) for key in keys]
        text = ", ".join(
            "(" + ", ".join(_expr(row[key]) for key in keys) + ")" for row in maps
        )
        return names, text
    if any(isinstance(row, Mapping) for row in rows):
        raise WriterError("values rows are all dicts or all lists")
    lists = [_items(row, "values row") for row in rows]
    width = len(lists[0])
    if any(len(row) != width for row in lists):
        raise WriterError("every row in values has the same length")
    if columns is not None and len(columns) != width:
        raise WriterError(
            "a values row has one item per column",
            context={"columns": columns, "width": width},
        )
    text = ", ".join("(" + ", ".join(_expr(v) for v in row) + ")" for row in lists)
    return columns, text


def _conflict(q: Clauses, parts: list[str]) -> None:
    doing = [key for key in ("do_nothing", "do_update_set") if key in q]
    if "on_conflict" not in q:
        if doing:
            raise WriterError(
                f"{doing[0]} needs on_conflict", example='"on_conflict": ["id"]'
            )
        return
    if len(doing) != 1:
        raise WriterError(
            "on_conflict needs do_nothing or do_update_set (one of them)",
            example='"on_conflict": ["id"], "do_update_set": {"body": "excluded.body"}',
        )
    target = [_column(c) for c in _items(q["on_conflict"], "on_conflict")]
    head = "ON CONFLICT" + (f" ({', '.join(target)})" if target else "")
    if doing[0] == "do_nothing":
        if q["do_nothing"] is not True:
            raise WriterError(
                "do_nothing is True", context={"do_nothing": q["do_nothing"]}
            )
        parts.append(f"{head} DO NOTHING")
        return
    if not target:
        raise WriterError(
            "do_update_set needs the conflict columns", example='"on_conflict": ["id"]'
        )
    parts.append(f"{head} DO UPDATE SET {_assign(q['do_update_set'], 'do_update_set')}")


def _update(q: Clauses, parts: list[str]) -> None:
    if "set" not in q:
        raise WriterError("an update needs set", example='"set": {"body": ":body"}')
    parts.append(f"UPDATE {_source(q['update'])} SET {_assign(q['set'], 'set')}")
    _from(q, parts)
    if "where" in q:
        parts.append("WHERE " + _expr(q["where"]))
    _returning(q, parts)


def _delete(q: Clauses, parts: list[str]) -> None:
    parts.append(f"DELETE FROM {_source(q['delete_from'])}")
    if "where" in q:
        parts.append("WHERE " + _expr(q["where"]))
    _returning(q, parts)


def _from(q: Clauses, parts: list[str]) -> None:
    if "join" in q and "from" not in q:
        raise WriterError("join needs from", example='"from": "messages"')
    if "from" in q:
        parts.append("FROM " + _source(q["from"]))
    if "join" in q:
        parts.extend(_join(item) for item in _items(q["join"], "join"))


def _join(raw: object) -> str:
    item = _items(raw, "join item")
    kind = item[0] if item else None
    if not isinstance(kind, str) or kind not in _JOINS:
        raise WriterError(
            "a join is [kind, source, on]. kind is inner, left, right, full, or cross.",
            context={"join": raw},
            example='["left", ["as", "users", "u"], ["=", "m.author", "u.id"]]',
        )
    if kind == "cross":
        if len(item) != 2:
            raise WriterError(
                'a cross join is ["cross", source]', context={"join": raw}
            )
        return f"CROSS JOIN {_source(item[1])}"
    if len(item) != 3:
        raise WriterError(
            f'a {kind} join is ["{kind}", source, on]', context={"join": raw}
        )
    return f"{_JOINS[kind]} {_source(item[1])} ON {_expr(item[2])}"


def _tail(q: Clauses, parts: list[str]) -> None:
    if "order_by" in q:
        parts.append("ORDER BY " + _exprs(q["order_by"], "order_by"))
    if "limit" in q:
        parts.append("LIMIT " + _expr(q["limit"]))
    elif "offset" in q:
        parts.append("LIMIT -1")
    if "offset" in q:
        parts.append("OFFSET " + _expr(q["offset"]))


def _returning(q: Clauses, parts: list[str]) -> None:
    if "returning" in q:
        parts.append("RETURNING " + _exprs(q["returning"], "returning"))


def _assign(raw: object, key: str) -> str:
    if not isinstance(raw, Mapping) or not raw:
        raise WriterError(
            f"{key} is a dict of column to value",
            example=f'"{key}": {{"body": ":body"}}',
        )
    pairs: Mapping[object, object] = raw  # pyright: ignore[reportUnknownVariableType]
    return ", ".join(f"{_column(col)} = {_expr(val)}" for col, val in pairs.items())


def _source(raw: object) -> str:
    """A table, ``("as", table, alias)``, a table function, or a subquery."""
    if isinstance(raw, str) and raw.startswith(("'", ":")):
        raise WriterError(
            "a source is a table, a call, or a subquery",
            context={"source": raw},
            example='"from": ["as", "messages", "m"]',
        )
    return _expr(raw)


# --- expressions -------------------------------------------------------------


def _items(raw: object, key: str) -> list[object]:
    if not isinstance(raw, list | tuple):
        raise WriterError(f"{key} is a list", context={key: raw})
    return list(raw)  # pyright: ignore[reportUnknownArgumentType]


def _exprs(raw: object, key: str) -> str:
    items = _items(raw, key)
    if not items:
        raise WriterError(f"{key} is a non-empty list", example=f'"{key}": ["id"]')
    if isinstance(items[0], str) and _is_op(items[0]) and len(items) > 1:
        raise WriterError(
            f"{key} is a list of expressions. Put a call in its own list.",
            context={key: raw},
            example=f'"{key}": [["lower", "name"]]',
        )
    return ", ".join(_expr(item) for item in items)


def _is_op(name: str) -> bool:
    low = name.lower()
    return low in _INFIX or low in _FORMS


def _expr(raw: object, parent: int = 0) -> str:
    match raw:
        case bool():
            return "TRUE" if raw else "FALSE"
        case int():
            return str(raw)
        case float():
            if not math.isfinite(raw):
                raise WriterError("a float literal is finite", context={"value": raw})
            return repr(raw)
        case None:
            return "NULL"
        case bytes():
            return f"X'{raw.hex()}'"
        case str():
            return _token(raw)
        case Expr():
            return _expr(raw.data, parent)
        case list() | tuple():
            return _call(list(raw), parent)  # pyright: ignore[reportUnknownArgumentType]
        case Mapping():
            return f"({_statement(raw)})"
        case _:
            raise WriterError(
                "a value is a string, number, bool, None, bytes, list, tuple, or dict",
                context={"value": raw},
                help_text=_STRINGS,
            )


def _token(text: str) -> str:
    if text.startswith("'"):
        inner = text[1:-1]
        if len(text) < 2 or not text.endswith("'") or "'" in inner.replace("''", ""):
            raise WriterError(
                "a string literal is 'text' with each ' inside written as ''",
                context={"literal": text},
                help_text="For a value from outside, use a parameter: :name.",
            )
        return text
    if text.startswith(":"):
        if not _PARAM.fullmatch(text):
            raise WriterError(
                "a parameter is :name", context={"parameter": text}, example=":room"
            )
        return text
    return _ident(text)


def _ident(text: str) -> str:
    parts = text.split(".")
    out: list[str] = []
    for index, part in enumerate(parts):
        if part == "*" and index == len(parts) - 1:
            out.append("*")
        elif _NAME.fullmatch(part):
            out.append(_quote(part))
        else:
            raise WriterError(
                "not a name", context={"string": text}, help_text=_STRINGS
            )
    return ".".join(out)


def _alias(raw: object) -> str:
    if not isinstance(raw, str) or not _NAME.fullmatch(raw):
        raise WriterError(
            "an alias is a plain name", context={"alias": raw}, example='"author"'
        )
    return _quote(raw)


def _column(raw: object) -> str:
    if not isinstance(raw, str) or not _NAME.fullmatch(raw):
        raise WriterError(
            "a column is a plain name", context={"column": raw}, example='"body"'
        )
    return _quote(raw)


def _quote(name: str) -> str:
    return f'"{name}"' if name.upper() in _KEYWORDS else name


def _wrap(text: str, own: int, parent: int) -> str:
    return f"({text})" if own <= parent else text


def _call(items: list[object], parent: int) -> str:
    if not items or not isinstance(items[0], str):
        raise WriterError(
            "a call is a list that starts with a name",
            context={"call": items},
            example='["lower", "u.name"]',
        )
    name = items[0]
    args = items[1:]
    low = name.lower()
    form = _FORMS.get(low)
    if form is not None:
        return form(low, args, parent)
    if low in _INFIX:
        return _infix(low, args, parent)
    if not _NAME.fullmatch(name):
        raise WriterError(
            "a function name is letters, digits, and _",
            context={"name": name},
            example='["lower", "u.name"]',
        )
    return f"{name}({', '.join(_expr(arg) for arg in args)})"


def _infix(low: str, args: list[object], parent: int) -> str:
    op, own = _INFIX[low]
    if low in {"-", "+"} and len(args) == 1:
        return _wrap(f"{op} {_expr(args[0], _UNARY)}", _UNARY, parent)
    if low in {"and", "or"} and len(args) == 1:
        return _expr(args[0], parent)
    if len(args) < 2:
        raise WriterError(
            f"{low} needs two or more arguments",
            example='["and", ["=", "room", ":room"], [">", "at", 0]]',
        )
    text = f" {op} ".join(_expr(arg, own) for arg in args)
    return _wrap(text, own, parent)


def _need(low: str, args: list[object], count: int, shape: str) -> None:
    if len(args) != count:
        raise WriterError(f'{low} is ["{low}", {shape}]', context={"arguments": args})


def _as(low: str, args: list[object], parent: int) -> str:
    _need(low, args, 2, "expression, alias")
    return f"{_expr(args[0])} AS {_alias(args[1])}"


def _cast(low: str, args: list[object], parent: int) -> str:
    _need(low, args, 2, "expression, type")
    kind = args[1]
    if not isinstance(kind, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_ ]*", kind):
        raise WriterError(
            "a cast type is a word", context={"type": kind}, example='"integer"'
        )
    return f"CAST({_expr(args[0])} AS {kind.upper()})"


def _collate(low: str, args: list[object], parent: int) -> str:
    _need(low, args, 2, "expression, collation")
    name = args[1]
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise WriterError(
            "a collation is a name", context={"collation": name}, example='"nocase"'
        )
    return _wrap(f"{_expr(args[0], _COLLATE)} COLLATE {name.upper()}", _COLLATE, parent)


def _order(low: str, args: list[object], parent: int) -> str:
    if len(args) not in {1, 2}:
        raise WriterError(
            f'{low} is ["{low}", expression] or ["{low}", expression, "nulls last"]',
            context={"arguments": args},
        )
    text = f"{_expr(args[0])} {low.upper()}"
    if len(args) == 2:
        nulls = args[1]
        if not isinstance(nulls, str) or nulls.lower() not in {
            "nulls first",
            "nulls last",
        }:
            raise WriterError(
                'the last item is "nulls first" or "nulls last"',
                context={"nulls": nulls},
            )
        text += f" {nulls.upper()}"
    return text


def _distinct(low: str, args: list[object], parent: int) -> str:
    _need(low, args, 1, "expression")
    return f"DISTINCT {_expr(args[0])}"


def _not(low: str, args: list[object], parent: int) -> str:
    _need(low, args, 1, "expression")
    return _wrap(f"NOT {_expr(args[0], _NOT)}", _NOT, parent)


def _exists(low: str, args: list[object], parent: int) -> str:
    if len(args) != 1 or not isinstance(args[0], Mapping):
        raise WriterError(
            f"{low} takes one query",
            example=f'["{low}", {{"select": [1], "from": "rooms", "where": ["=", "id", "m.room"]}}]',
        )
    return f"{low.upper()} ({_statement(args[0])})"


def _in(low: str, args: list[object], parent: int) -> str:
    if len(args) < 2:
        raise WriterError(
            f'{low} is ["{low}", expression, value, ...] or ["{low}", expression, query]',
            example=f'["{low}", "status", "\'open\'", "\'done\'"]',
        )
    left = _expr(args[0], _TEST)
    if len(args) == 2 and isinstance(args[1], Mapping):
        right = _statement(args[1])
    else:
        right = ", ".join(_expr(arg) for arg in args[1:])
    return _wrap(f"{left} {low.upper()} ({right})", _TEST, parent)


def _between(low: str, args: list[object], parent: int) -> str:
    _need(low, args, 3, "expression, low, high")
    a, b, c = (_expr(arg, _TEST) for arg in args)
    return _wrap(f"{a} {low.upper()} {b} AND {c}", _TEST, parent)


def _case(low: str, args: list[object], parent: int) -> str:
    if len(args) < 2:
        raise WriterError(
            'case is ["case", when, then, ..., else]',
            example='["case", ["=", "status", "\'open\'"], 1, 0]',
        )
    pairs = args[:-1] if len(args) % 2 else args
    bits = ["CASE"]
    for index in range(0, len(pairs), 2):
        bits.append(f"WHEN {_expr(pairs[index])} THEN {_expr(pairs[index + 1])}")
    if len(args) % 2:
        bits.append(f"ELSE {_expr(args[-1])}")
    bits.append("END")
    return " ".join(bits)


def _lit(low: str, args: list[object], parent: int) -> str:
    if len(args) != 1 or not isinstance(args[0], str):
        raise WriterError('lit is ["lit", "text"]', context={"arguments": args})
    return "'" + args[0].replace("'", "''") + "'"


def _raw(low: str, args: list[object], parent: int) -> str:
    if len(args) != 1 or not isinstance(args[0], str):
        raise WriterError('raw is ["raw", "SQL text"]', context={"arguments": args})
    return args[0]


_FORMS = {
    "as": _as,
    "cast": _cast,
    "collate": _collate,
    "asc": _order,
    "desc": _order,
    "distinct": _distinct,
    "not": _not,
    "exists": _exists,
    "not exists": _exists,
    "in": _in,
    "not in": _in,
    "between": _between,
    "not between": _between,
    "case": _case,
    "lit": _lit,
    "raw": _raw,
}
