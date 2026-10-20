"""Write functions for ``LiteWriter``: what the writer calls.

The writer thread runs::

    result = fn(tx, *args, **kwargs)

inside one batch transaction. You never ``COMMIT``. Every job runs in
its own savepoint, so a failure undoes only that write; the caller gets
the exception after the rest of the batch commits.

``tx`` is the write connection for this job. It reads the uncommitted
batch: ``tx.query`` is every row, ``tx.one`` the first, ``tx.value`` the
first column of the first row. Use ``tx`` only inside this call. Do not
store it. Do not block (hash, HTTP, sleep).
"""

import itertools
from collections.abc import Callable, Iterable
from threading import current_thread
from typing import Any, overload

import apsw

from litewriter.build import Statement
from litewriter.errors import QueryError, StateError
from litewriter.expr import RESERVED, Stmt, is_params
from litewriter.q import Params, as_into, merge, one, params_in, rows, run, split, value
from litewriter.watch import Tracker


def ended_error() -> StateError:
    return StateError(
        "a write function ended the transaction",
        help_text="The writer commits. Do not run BEGIN, COMMIT, END, "
        "or ROLLBACK inside a write.",
        example="def add(tx: Tx) -> None:\n    tx.execute(...)",
    )


class Tx:
    """The write connection for one job.

    Use it only inside that job. ``value`` is the first column of the
    first row. ``one`` is the first row. ``query`` is every row.
    """

    __slots__ = ("_conn", "_owner", "_track")

    def __init__(self, conn: apsw.Connection, track: Tracker | None = None) -> None:
        self._conn = conn
        self._owner = current_thread()
        self._track = track

    def _open(self) -> apsw.Connection:
        """The connection, or StateError when the batch transaction ended."""
        if current_thread() is not self._owner:
            raise StateError(
                "tx stays on the writer thread",
                help_text="Do not store it or hand it to another thread.",
            )
        if not self._conn.in_transaction:
            raise ended_error()
        return self._conn

    def _note(
        self,
        conn: apsw.Connection,
        text: str,
        bound: Params | None,
    ) -> None:
        """Record this statement's write effect while a watch is open."""
        if self._track is not None:
            self._track.note(conn, text, bound)

    def _cursor(self, q: str | Statement, params: dict[str, object]) -> apsw.Cursor:
        conn = self._open()
        text, bound = merge(q, params)
        self._note(conn, text, bound)
        return run(conn, text, bound)

    def execute(self, q: str | Statement, /, **params: object) -> None:
        """Run SQL text or a statement to completion. Returns nothing."""
        self._cursor(q, params).fetchall()

    def executemany(self, q: str | Statement, rows: Iterable[Params]) -> None:
        """Run one statement for each row. Each row fills the ``:name``s.

        The statement must take named parameters — ``columns=[...]`` or
        ``param()`` on a builder, ``:name`` in SQL text. A statement that
        is all literal values runs once with ``execute``; repeating it
        per row would silently ignore the rows.
        """
        conn = self._open()
        text, auto = split(q)
        built = isinstance(q, Stmt)
        needed = params_in(text)
        fillable = needed - set(auto)
        if not fillable:
            raise QueryError(
                "executemany needs named parameters",
                help_text="A literal statement runs once with execute.",
                example='tx.executemany(Insert("t", columns=["body"]), '
                '[{"body": "a"}])',
            )
        feed = iter(rows)
        head = next(feed, None)

        def bound() -> Iterable[Params]:
            pending: Iterable[object] = (
                feed if head is None else itertools.chain((head,), feed)
            )
            for row in pending:
                if not is_params(row):
                    raise QueryError(
                        "an executemany row is a dict",
                        context={"row": row},
                    )
                for name in row:
                    if built and RESERVED.fullmatch(name):
                        raise QueryError(
                            "a row key is not an auto-bind name",
                            context={"name": name},
                            help_text="_1, _2, ... are reserved on builder statements.",
                        )
                    if name not in fillable:
                        raise QueryError(
                            "a row key is not a parameter of the statement",
                            context={"name": name},
                            example="tx.executemany(q, [{name: value}])",
                        )
                yield row if not auto else {**auto, **row}

        first = head if head is None or not auto else {**auto, **head}
        self._note(conn, text, first)
        try:
            conn.executemany(text, bound()).fetchall()
        except KeyError as exc:
            raise QueryError(
                "missing parameter",
                context={"parameter": f":{exc.args[0]}"},
                example=f"tx.executemany(q, [{{{exc.args[0]}: ...}}])",
            ) from None
        except apsw.BindingsError as exc:
            raise QueryError(
                "the parameters do not match the query",
                context={"detail": str(exc)},
            ) from None

    def last_insert_rowid(self) -> int:
        """The rowid of the last insert on this connection."""
        return self._open().last_insert_rowid()

    @overload
    def query(
        self, q: str | Statement, /, **params: object
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def query[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> list[T]: ...
    def query(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> list[Any]:
        """All rows, each built as ``into(*row)``."""
        into = as_into(into)
        return rows(self._cursor(q, params), into)

    @overload
    def one(
        self, q: str | Statement, /, **params: object
    ) -> tuple[Any, ...] | None: ...
    @overload
    def one[T](
        self, q: str | Statement, into: Callable[..., T], /, **params: object
    ) -> T | None: ...
    def one(
        self,
        q: str | Statement,
        into: Callable[..., Any] = tuple,
        /,
        **params: object,
    ) -> Any:
        """The first row as ``into(*row)``, or None when there is none."""
        into = as_into(into)
        return one(self._cursor(q, params), into)

    def value(self, q: str | Statement, /, **params: object) -> Any:
        """The first column of the first row. No row raises QueryError."""
        return value(self._cursor(q, params))
