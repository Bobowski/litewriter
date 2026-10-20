"""An advisory lock beside the database file.

The file is ``{database}-claim``. SQLite's own locks stay as they are.
The kernel drops this lock when the process dies. Readers keep working.
A process that never claims can still write.

``acquire`` polls the lock with a doubling sleep (10 ms to 250 ms) until
the deadline. ``timeout=0`` tries once. No threads, no ctypes. The async
form sleeps on the loop; cancelling it just closes the fd.
"""

import asyncio
import os
import sys
import time
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from threading import Event, Lock

from litewriter.errors import ClaimBusy, StateError, check_seconds


def claim_path(database: Path) -> Path:
    """The lock file for ``database``."""
    return database.with_name(database.name + "-claim")


def validate_timeout(timeout: object) -> float:
    """Reject a timeout that is not a count of seconds."""
    return check_seconds(
        timeout,
        "claim timeout",
        "LiteWriter(path, claim=0)",
        help_text="0 tries once.",
    )


class Claim:
    """Exclusive claim on one sibling file.

    ``acquire`` returns True only when this call took the lock. A lock
    that is already held stays held until ``release``.
    """

    __slots__ = ("_fd", "_gate", "path")

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None
        self._gate = Lock()

    def held(self) -> bool:
        return self._fd is not None

    def acquire(self, timeout: float, cancelled: Event) -> bool:
        """Wait for the lock. True when this call took it.

        A set ``cancelled`` ends the wait with StateError. The deadline
        ending raises ClaimBusy.
        """
        timeout = validate_timeout(timeout)
        with self._gate:
            if self._fd is not None:
                return False
            fd = _open(self.path)
            try:
                for pause in self._poll(fd, timeout, cancelled):
                    cancelled.wait(pause)
            except BaseException:
                os.close(fd)
                raise
            return True

    def _poll(self, fd: int, timeout: float, cancelled: Event) -> Iterator[float]:
        """Yield each pause until the lock is taken. Raise on close or deadline."""
        deadline = time.monotonic() + timeout
        delay = 0.01
        while True:
            if cancelled.is_set():
                raise _closed()
            if _try_lock(fd):
                _write_pid(fd)
                self._fd = fd
                return
            left = deadline - time.monotonic()
            if timeout == 0 or left <= 0:
                raise _busy(self.path, timeout)
            yield min(delay, left)
            delay = min(delay * 2, 0.25)

    async def aacquire(self, timeout: float, cancelled: Event) -> bool:
        """The same wait on the event loop. Cancelling closes the fd.

        A set ``cancelled`` ends the wait with StateError. A second
        contender returns False — the gate is taken, not awaited, so the
        loop never parks on it.
        """
        timeout = validate_timeout(timeout)
        if not self._gate.acquire(blocking=False):
            return False
        try:
            if self._fd is not None:
                return False
            fd = _open(self.path)
            try:
                for pause in self._poll(fd, timeout, cancelled):
                    await asyncio.sleep(pause)
            except BaseException:
                os.close(fd)
                raise
            return True
        finally:
            self._gate.release()

    def release(self) -> None:
        """Unlock and close. A lock never held is a no-op."""
        fd = self._fd
        self._fd = None
        if fd is not None:
            _unlock_close(fd)


def _busy(path: Path, timeout: float) -> ClaimBusy:
    context: dict[str, object] = {"path": str(path), "timeout": timeout}
    holder = _holder_pid(path)
    if holder is not None:
        context["holder"] = holder
    return ClaimBusy(
        "another process holds the claim",
        context=context,
        help_text="The holder has to close. A longer timeout waits longer.",
        example="LiteWriter(path, claim=30)",
    )


def _closed() -> StateError:
    return StateError(
        "the writer closed before the claim",
        help_text="close() ends the wait.",
    )


def _holder_pid(path: Path) -> int | None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return None
    try:
        raw = os.read(fd, 32)
    except OSError:
        return None
    finally:
        os.close(fd)
    line = raw.split(b"\n", 1)[0].strip()
    if not line:
        return None
    try:
        return int(line)
    except ValueError:
        return None


def _write_pid(fd: int) -> None:
    data = f"{os.getpid()}\n".encode()
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    view = data
    while view:
        wrote = os.write(fd, view)
        view = view[wrote:]


if sys.platform == "win32":
    import msvcrt

    def _open(path: Path) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_BINARY, 0o644)
        os.set_inheritable(fd, False)
        return fd

    def _try_lock(fd: int) -> bool:
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock_close(fd: int) -> None:
        with suppress(OSError):
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        with suppress(OSError):
            os.close(fd)

else:
    import fcntl

    def _open(path: Path) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        fd = os.open(path, flags, 0o644)
        os.set_inheritable(fd, False)
        return fd

    def _try_lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        return True

    def _unlock_close(fd: int) -> None:
        with suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        with suppress(OSError):
            os.close(fd)
