from collections.abc import Mapping, Sequence
from pathlib import Path

import apsw

# A sequence is positional. A mapping fills :name.
type Bindings = Sequence[object] | Mapping[str, object]

CACHE_KIB = 64 * 1024
"""Page cache per connection (KiB)."""

MMAP_BYTES = 1 << 30
"""Reads map up to this many bytes of the file. No copy, no read syscall."""


def connect(path: str | Path, *, readonly: bool = False) -> apsw.Connection:
    """Open a file with WAL, NORMAL, foreign keys, busy timeout.

    The write connection must be created on the writer thread.
    """
    raw = str(path)
    if readonly:
        uri = raw if raw.startswith("file:") else f"file:{raw}"
        uri = f"{uri}&mode=ro" if "?" in uri else f"{uri}?mode=ro"
        connection = apsw.Connection(
            uri, flags=apsw.SQLITE_OPEN_URI | apsw.SQLITE_OPEN_READONLY
        )
    else:
        connection = apsw.Connection(raw)
    _pragmas(connection)
    return connection


def _pragmas(connection: apsw.Connection) -> None:
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute(f"PRAGMA cache_size=-{CACHE_KIB}")
    connection.execute(f"PRAGMA mmap_size={MMAP_BYTES}")
