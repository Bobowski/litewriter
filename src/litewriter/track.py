"""Track which tables and columns a batch changes, for watches.

While a watch is open, ``Tx`` asks ``note`` to classify each statement
before it runs. ``effect`` prepares the SQL with ``explain=1`` under a
temporary authorizer: nothing executes, but triggers, cascades, and
upserts still report the tables and columns they write. The answer is
cached per SQL text. After COMMIT, ``notify`` wakes each watch whose
read columns intersect the change.
"""

# pyright: reportPrivateUsage=false

from asyncio import AbstractEventLoop
from collections.abc import Callable, Mapping
from threading import Lock
from typing import TYPE_CHECKING, Any, cast

import apsw

from litewriter.errors import StateError
from litewriter.wake import LoopWakes, deliver

if TYPE_CHECKING:
    from litewriter.watch import Watch

type _Mark = tuple[frozenset[str], frozenset[tuple[str, str]], bool]
type _Effect = tuple[frozenset[str], frozenset[tuple[str, str]]]
type _Rows = tuple[tuple[Any, ...], ...]


class Tracker:
    """The watch registry and the write-side change log."""

    __slots__ = (
        "_by_column",
        "_by_table",
        "_changed_columns",
        "_changed_tables",
        "_effects",
        "_gen",
        "_rowids",
        "_schema_seen",
        "_shared",
        "_stopped",
        "_unknown",
        "_watch_ids",
        "_watch_lock",
        "_watches",
    )

    def __init__(self) -> None:
        self._by_table: dict[str, set[int]] = {}
        self._by_column: dict[tuple[str, str], set[int]] = {}
        self._effects: dict[str, _Effect] = {}
        self._gen = 0
        self._rowids: dict[str, str | None] = {}
        self._schema_seen: tuple[int, int] | None = None
        self._shared: dict[object, tuple[int, _Rows]] = {}
        self._stopped = False
        self._unknown = False
        self._changed_tables: set[str] = set()
        self._changed_columns: set[tuple[str, str]] = set()
        self._watch_lock = Lock()
        self._watches: dict[int, Watch[Any]] = {}
        self._watch_ids = 0

    def live(self) -> bool:
        """True while any watch is open."""
        with self._watch_lock:
            return bool(self._watches)

    def open(self) -> None:
        """The writer is starting: new watches may register."""
        with self._watch_lock:
            self._stopped = False
            self._effects.clear()
            self._rowids.clear()
            self._shared.clear()

    def check_schema(self, versions: tuple[int, int]) -> bool:
        """True when the schema moved since the last observation.

        The first call only records the baseline. A later move clears
        the cached effects — they may miss a trigger the change added —
        so the batch must wake every watch.
        """
        if self._schema_seen is None:
            self._schema_seen = versions
            return False
        if versions == self._schema_seen:
            return False
        self._effects.clear()
        self._rowids.clear()
        return True

    def seen(self, versions: tuple[int, int]) -> None:
        """The schema versions after the last traced commit."""
        self._schema_seen = versions

    def begin(self) -> None:
        """Start a batch: nothing changed yet."""
        self._changed_tables.clear()
        self._changed_columns.clear()
        self._unknown = False

    def mark(self) -> _Mark:
        """A snapshot of the changed sets for one job's savepoint."""
        return (
            frozenset(self._changed_tables),
            frozenset(self._changed_columns),
            self._unknown,
        )

    def restore(self, mark: _Mark) -> None:
        """Undo one job's changes after a savepoint rollback."""
        tables, columns, unknown = mark
        self._changed_tables.clear()
        self._changed_tables.update(tables)
        self._changed_columns.clear()
        self._changed_columns.update(columns)
        self._unknown = unknown

    def note(
        self,
        write: apsw.Connection,
        sql: str,
        bound: Mapping[str, object] | None,
    ) -> None:
        """Fold the write effect of ``sql`` into this batch's changed sets."""
        found = self.effect(write, sql, bound)
        if found is None:
            self._unknown = True
            return
        tables, columns = found
        self._changed_tables.update(tables)
        self._changed_columns.update(columns)

    def effect(
        self,
        write: apsw.Connection,
        sql: str,
        bound: Mapping[str, object] | None,
    ) -> _Effect | None:
        """The tables and columns ``sql`` writes. None when unknown.

        A miss prepares the text under a temporary authorizer; the real
        bindings let every statement of a multi-statement text prepare.
        A text that cannot fully prepare — a bad statement, a missing
        binding, a table that does not exist yet — answers None, so the
        batch wakes every watch.
        """
        found = self._effects.get(sql)
        if found is not None:
            return found
        tables: set[str] = set()
        columns: set[tuple[str, str]] = set()
        rowid_tables: set[str] = set()
        unknown = False

        def auth(
            op: int,
            a: str | None,
            b: str | None,
            _db: str | None,
            _trigger: str | None,
        ) -> int:
            nonlocal unknown
            if op == apsw.SQLITE_ATTACH or op == apsw.SQLITE_DETACH:
                unknown = True
                return apsw.SQLITE_OK
            if not a or a.startswith("sqlite_"):
                return apsw.SQLITE_OK
            if op == apsw.SQLITE_INSERT or op == apsw.SQLITE_DELETE:
                tables.add(a)
            elif op == apsw.SQLITE_UPDATE and b:
                columns.add((a, b))
                if b.upper() == "ROWID":
                    rowid_tables.add(a)
            return apsw.SQLITE_OK

        write.authorizer = auth
        try:
            write.execute(
                sql,
                cast("apsw.Bindings | None", bound),
                explain=1,
                can_cache=False,
            ).fetchall()
        except apsw.Error, KeyError:
            return None
        finally:
            write.authorizer = None
        if unknown:
            return None
        for table in rowid_tables:
            alias = self._rowid_alias(write, table)
            if alias is not None:
                columns.add((table, alias))
        found = (frozenset(tables), frozenset(columns))
        if len(self._effects) >= 1024:
            self._effects.clear()
        self._effects[sql] = found
        return found

    def _rowid_alias(self, write: apsw.Connection, table: str) -> str | None:
        """The INTEGER PRIMARY KEY name of ``table``, when it has one.

        The authorizer reports a ``rowid`` write as column ``ROWID`` but
        a ``rowid`` read as the declared key name. Normalizing the write
        to that name keeps the column match exact.
        """
        if table in self._rowids:
            return self._rowids[table]
        try:
            pks = write.execute(
                "SELECT name, type FROM pragma_table_info(?) WHERE pk > 0",
                (table,),
            ).fetchall()
        except apsw.Error:
            pks = []
        name = (
            str(pks[0][0])
            if len(pks) == 1 and str(pks[0][1]).upper() == "INTEGER"
            else None
        )
        self._rowids[table] = name
        return name

    def read_shared(self, key: object, read: Callable[[], _Rows]) -> _Rows:
        """One re-read per query per commit. Identical watches share it.

        The entry is fresh only when its generation equals the commit
        counter — a stale row set would hide the wake's own change.
        """
        with self._watch_lock:
            entry = self._shared.get(key)
            if entry is not None and entry[0] == self._gen:
                return entry[1]
        rows = read()
        with self._watch_lock:
            if len(self._shared) >= 512:
                self._shared.clear()
            self._shared[key] = (self._gen, rows)
        return rows

    def notify(self, schema_changed: bool, traced: bool, wakes: LoopWakes) -> None:
        with self._watch_lock:
            self._gen += 1
            if schema_changed:
                self._effects.clear()
                self._rowids.clear()
            if not self._watches:
                return
            if schema_changed or self._unknown or not traced:
                found = list(self._watches.values())
            else:
                ids: set[int] = set()
                for table in self._changed_tables:
                    hit = self._by_table.get(table)
                    if hit:
                        ids.update(hit)
                for column in self._changed_columns:
                    hit = self._by_column.get(column)
                    if hit:
                        ids.update(hit)
                found = [self._watches[wid] for wid in ids if wid in self._watches]
        notes: list[tuple[AbstractEventLoop, Callable[[], None]]] = []
        for watch in found:
            armed = watch.advance()
            if armed is not None:
                notes.append(armed)
        if notes:
            deliver(wakes, notes)

    def add(self, watch: Watch[Any]) -> None:
        with self._watch_lock:
            if self._stopped:
                raise StateError("the writer is closed")
            self._watch_ids += 1
            watch.wid = self._watch_ids
            self._watches[watch.wid] = watch
            for table in watch.tables:
                self._by_table.setdefault(table, set()).add(watch.wid)
            for column in watch.columns:
                self._by_column.setdefault(column, set()).add(watch.wid)

    def drop(self, watch: Watch[Any]) -> None:
        with self._watch_lock:
            self._watches.pop(watch.wid, None)
            for table in watch.tables:
                bucket = self._by_table.get(table)
                if bucket is not None:
                    bucket.discard(watch.wid)
                    if not bucket:
                        del self._by_table[table]
            for column in watch.columns:
                bucket = self._by_column.get(column)
                if bucket is not None:
                    bucket.discard(watch.wid)
                    if not bucket:
                        del self._by_column[column]

    def stop_all(self) -> None:
        """Close every watch. The writer is stopping."""
        with self._watch_lock:
            self._stopped = True
            found = list(self._watches.values())
            self._watches.clear()
            self._by_table.clear()
            self._by_column.clear()
            self._shared.clear()
        for watch in found:
            watch.stop()
