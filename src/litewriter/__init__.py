"""One SQLite writer thread. Queries are data. Reads stay on the calling thread.

CPython 3.14. ``isolated=True`` is the default. A watch wakes after a
commit that changes a column it reads.
"""

from litewriter.build import (
    Delete,
    Except,
    Insert,
    Intersect,
    Query,
    Replace,
    Select,
    Union,
    UnionAll,
    Update,
)
from litewriter.connect import connect
from litewriter.errors import WriterError, WriterRolledBack, WriterRuntime
from litewriter.expr import Expr, col, exists, lit, not_exists, param
from litewriter.fn import Isolated, Tx, WriteFn, isolated
from litewriter.q import sql, where
from litewriter.watch import Watch
from litewriter.writer import LiteWriter, Outcome, Reader, Stats

__all__ = [
    "Delete",
    "Except",
    "Expr",
    "Insert",
    "Intersect",
    "Isolated",
    "LiteWriter",
    "Outcome",
    "Query",
    "Reader",
    "Replace",
    "Select",
    "Stats",
    "Tx",
    "Union",
    "UnionAll",
    "Update",
    "Watch",
    "WriteFn",
    "WriterError",
    "WriterRolledBack",
    "WriterRuntime",
    "col",
    "connect",
    "exists",
    "isolated",
    "lit",
    "not_exists",
    "param",
    "sql",
    "where",
]
