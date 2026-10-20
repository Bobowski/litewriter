"""Open a SQLite file with the pragmas litewriter wants.

The write connection is a plain path. The read connection is a URI —
``Path.as_uri`` quotes spaces, ``#``, ``?``, and ``%`` so a file named
``a%20b.sqlite3`` reads its own bytes, not ``a b.sqlite3``.
"""

from pathlib import Path

import apsw

CACHE_KIB = 64 * 1024
"""Page cache per connection (KiB)."""

MMAP_BYTES = 1 << 30
"""Reads map up to this many bytes of the file. No copy, no read syscall."""


def connect(
    path: str | Path, *, readonly: bool = False, busy_ms: int = 5000
) -> apsw.Connection:
    """Open a file with WAL, NORMAL, foreign keys, and a busy timeout.

    ``busy_ms`` is the SQLite busy timeout in milliseconds.
    The write connection must be created on the writer thread.
    """
    if readonly:
        uri = Path(path).absolute().as_uri() + "?mode=ro"
        connection = apsw.Connection(
            uri, flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READONLY
        )
    else:
        connection = apsw.Connection(str(path))
    _pragmas(connection, busy_ms)
    return connection


def _pragmas(connection: apsw.Connection, busy_ms: int) -> None:
    connection.execute(f"PRAGMA busy_timeout={busy_ms}")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute(f"PRAGMA cache_size=-{CACHE_KIB}")
    connection.execute(f"PRAGMA mmap_size={MMAP_BYTES}")
