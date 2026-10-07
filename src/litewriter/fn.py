"""Write functions for ``LiteWriter``: what the writer calls.

The writer thread runs::

    result = fn(tx, *args, **kwargs)

inside ``BEGIN IMMEDIATE``. You never ``COMMIT``. The writer does that for
the whole batch. ``isolated=True`` is the default, so a failure undoes
only that write. ``isolated=False`` rolls back the whole batch.
``@isolated`` marks a function the same way as the default.

``tx`` is the write connection for this job. Ask it for rows.
``tx.value`` is the first column of the first row. ``tx.one`` is the
first row. ``tx.query`` is every row. ``tx.execute`` runs SQL text.
Use ``tx`` only in this call. Do not store it. Do not block (hash, HTTP, sleep).
"""

from collections.abc import Callable, Iterable, Sequence
from typing import Any, Concatenate, cast

import apsw

from litewriter.connect import Bindings
from litewriter.q import Clauses, execute, one, rows, value


class Tx:
    """The write connection for one job.

    Use it only inside that job. ``value`` is the first column of the
    first row. ``one`` is the first row. ``query`` is every row.
    """

    __slots__ = ("_conn",)

    def __init__(self, conn: apsw.Connection) -> None:
        self._conn = conn

    def execute(self, query: str | Clauses, params: Bindings = ()) -> apsw.Cursor:
        """A cursor. ``params`` is a sequence (``?``) or a mapping (``:name``)."""
        return execute(self._conn, query, params)

    def executemany(
        self, query: str, params: Iterable[Sequence[object]]
    ) -> apsw.Cursor:
        """Run one SQL statement for each parameter sequence."""
        bindings = cast("Iterable[apsw.Bindings]", params)
        if self._conn.authorizer is None:
            return self._conn.executemany(query, bindings)
        return self._conn.executemany(query, bindings, can_cache=False)

    def last_insert_rowid(self) -> int:
        """The rowid of the last insert on this connection."""
        return self._conn.last_insert_rowid()

    def query(self, query: str | Clauses, /, **params: object) -> list[tuple[Any, ...]]:
        """All rows. ``params`` fill each ``:name``."""
        return rows(self._conn, query, params)

    def one(self, query: str | Clauses, /, **params: object) -> tuple[Any, ...] | None:
        """The first row, or None when the query returns nothing."""
        return one(self._conn, query, params)

    def value(self, query: str | Clauses, /, **params: object) -> Any:
        """The first column of the first row."""
        return value(self._conn, query, params)


type WriteFn[**P, R] = Callable[Concatenate[Tx, P], R]


class Isolated[**P, R]:
    """A write function that runs in its own SAVEPOINT. See ``isolated``."""

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[P, R]) -> None:
        self.fn = fn

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.fn(*args, **kwargs)


def isolated[**P, R](fn: Callable[P, R]) -> Isolated[P, R]:
    """Mark a write so its failure undoes only itself.

    Use it as a decorator (``@isolated``) or at the call site
    (``await db.call(isolated(spend), account, 5)``). The error is
    raised on the caller after the rest of the batch is committed.
    """
    if isinstance(fn, Isolated):
        return cast(Isolated[P, R], fn)
    return Isolated(fn)


def unwrap[**P, R](fn: Callable[P, R]) -> tuple[Callable[P, R], bool]:
    """The plain function and whether it is isolated."""
    if isinstance(fn, Isolated):
        return cast(Isolated[P, R], fn).fn, True
    return fn, False
