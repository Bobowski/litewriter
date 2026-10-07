"""An advisory lock beside the database file.

The file is ``{database}-claim``. SQLite's own locks stay as they are.
The kernel drops this lock when the process dies. Readers keep working.
A process that never claims can still write.
"""

from __future__ import annotations

import math
import os
import sys
from collections.abc import Callable
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Literal

from litewriter.errors import WriterBusy, WriterError, WriterRuntime

_Wait = Literal["held", "busy", "closed"]


def claim_path(database: Path) -> Path:
    """The lock file for ``database``."""
    return database.with_name(database.name + "-claim")


def validate_timeout(timeout: object) -> float:
    """Reject a timeout that is not a count of seconds."""
    if isinstance(timeout, bool) or not isinstance(timeout, int | float):
        raise WriterError(
            "claim timeout must be a number of seconds",
            example="db.claim(timeout=30)",
        )
    try:
        seconds = float(timeout)
    except OverflowError:
        seconds = math.inf
    if not math.isfinite(seconds) or seconds < 0:
        raise WriterError(
            "claim timeout must be 0 or greater",
            help_text="0 tries once.",
            example="db.claim(timeout=30)\ndb.claim(timeout=0)",
        )
    return seconds


class Claim:
    """Exclusive claim on one sibling file.

    ``acquire`` returns True only when this call took the lock.
    ``abort`` unblocks a wait. A lock that is already held stays held
    until ``release``.
    """

    __slots__ = ("_done", "_fd", "_gate", "_mu", "_token", "_wake", "path")

    def __init__(self, path: Path) -> None:
        self.path = path
        self._mu = Lock()
        self._gate = Lock()
        self._fd: int | None = None
        self._token: object | None = None
        self._done: Event | None = None
        self._wake: Callable[[], None] | None = None

    def held(self) -> bool:
        with self._mu:
            return self._fd is not None

    def acquire(self, timeout: float, taken: list[bool] | None = None) -> bool:
        """Lock. True when this call took the lock."""
        timeout = validate_timeout(timeout)
        with self._gate:
            if self.held():
                return False
            self._acquire(timeout, taken)
            if not self.held():
                raise WriterRuntime(
                    "the writer closed before the claim was kept",
                    help_text="close() releases the claim.",
                )
            return True

    def release(self) -> None:
        with self._mu:
            fd = self._fd
            self._fd = None
            self._token = None
        if fd is not None:
            _unlock_close(fd)

    def abort(self) -> None:
        """Unblock a wait. A lock already held stays held."""
        with self._mu:
            self._token = None
            done = self._done
            wake = self._wake
        if wake is not None:
            wake()
        if done is not None:
            done.set()

    def publish(
        self, fd: int, token: object, done: Event, taken: list[bool] | None
    ) -> bool:
        """Store ``fd`` when ``token`` still owns the wait."""
        with self._mu:
            if self._token is not token:
                return False
            self._fd = fd
            self._token = None
            if taken is not None:
                taken.append(True)
            done.set()
            return True

    def finish_wait(self, token: object, fd: int, *, timed_out: bool) -> _Wait:
        """Decide a posix wait. Cancel the attempt when the clock won."""
        with self._mu:
            if self._fd == fd:
                return "held"
            still = self._token is token
            if timed_out and still:
                self._token = None
                return "busy"
            if not still:
                return "closed"
            return "busy"

    def arm_wake(self, wake: Callable[[], None]) -> None:
        with self._mu:
            self._wake = wake
            fire = self._token is None
        if fire:
            wake()

    def disarm_wake(self) -> None:
        with self._mu:
            self._wake = None

    def _acquire(self, timeout: float, taken: list[bool] | None) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        token = object()
        done = Event()
        with self._mu:
            self._token = token
            self._done = done
        try:
            if sys.platform == "win32":
                _acquire_windows(self, token, done, timeout, taken)
            else:
                _acquire_posix(self, token, done, timeout, taken)
        finally:
            with self._mu:
                if self._done is done:
                    self._done = None


def _busy(claim: Claim, timeout: float) -> WriterBusy:
    holder = _holder_pid(claim.path)
    context: dict[str, object] = {"path": str(claim.path), "timeout": timeout}
    if holder is not None:
        context["holder"] = holder
    return WriterBusy(
        "another process holds the claim",
        context=context,
        help_text="The holder has to close. A longer timeout waits longer.",
        example="db.claim(timeout=30)",
    )


def _closed() -> WriterRuntime:
    return WriterRuntime(
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
    finally:
        os.close(fd)
    line = raw.split(b"\n", 1)[0].strip()
    if not line:
        return None
    try:
        return int(line)
    except ValueError:
        return None


def _write_pid(path: Path, fd: int) -> None:
    data = f"{os.getpid()}\n".encode()
    if sys.platform == "win32":
        raw = os.open(path, os.O_WRONLY)
        try:
            os.write(raw, data)
        finally:
            os.close(raw)
        return
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    view = data
    while view:
        wrote = os.write(fd, view)
        view = view[wrote:]


if sys.platform == "win32":
    import ctypes

    def _unlock_close(fd: int) -> None:
        _win_unlock(fd)

    def _acquire_windows(
        claim: Claim,
        token: object,
        done: Event,
        timeout: float,
        taken: list[bool] | None,
    ) -> None:
        handle, lock_event, abort_event, overlapped = _win_prepare(claim.path)
        claim.arm_wake(lambda: _win_set(abort_event))
        try:
            kind = _win_lock(handle, lock_event, abort_event, overlapped, timeout)
        finally:
            claim.disarm_wake()
            _win_close_event(lock_event)
            _win_close_event(abort_event)
        if kind != "held":
            _win_close_handle(handle)
            if kind == "closed":
                raise _closed()
            raise _busy(claim, timeout)
        _write_pid(claim.path, handle)
        if not claim.publish(handle, token, done, taken):
            _unlock_close(handle)
            raise _closed()

    def _win_prepare(path: Path) -> tuple[int, int, int, ctypes.Structure]:
        """Open the claim file for an overlapped exclusive lock."""
        import ctypes
        from ctypes import wintypes

        kernel = _kernel32()
        handle = kernel.CreateFileW(
            str(path),
            0x80000000 | 0x40000000,  # GENERIC_READ | GENERIC_WRITE
            0x1 | 0x2 | 0x4,  # share read, write, delete
            None,
            4,  # OPEN_ALWAYS
            0x40000000,  # FILE_FLAG_OVERLAPPED
            None,
        )
        raw = ctypes.cast(handle, ctypes.c_void_p).value
        invalid = ctypes.c_void_p(-1).value
        if raw is None or raw == invalid:
            raise OSError(ctypes.get_last_error(), "CreateFileW")
        lock_event = _win_event()
        abort_event = _win_event()
        overlapped = _overlapped()
        overlapped.Offset = 4096
        overlapped.hEvent = wintypes.HANDLE(lock_event)
        return raw, lock_event, abort_event, overlapped

    def _win_lock(
        handle: int,
        lock_event: int,
        abort_event: int,
        overlapped: ctypes.Structure,
        timeout: float,
    ) -> _Wait:
        import ctypes
        from ctypes import wintypes

        kernel = _kernel32()
        flags = 0x2  # LOCKFILE_EXCLUSIVE_LOCK
        if timeout == 0:
            flags |= 0x1  # LOCKFILE_FAIL_IMMEDIATELY
        ok = kernel.LockFileEx(
            wintypes.HANDLE(handle),
            flags,
            0,
            1,
            0,
            ctypes.byref(overlapped),
        )
        if ok:
            return "held"
        err = ctypes.get_last_error()
        # 33 is ERROR_LOCK_VIOLATION. 997 is ERROR_IO_PENDING.
        if timeout == 0 or err != 997:
            if err == 33:
                return "busy"
            if err == 997:
                kernel.CancelIoEx(wintypes.HANDLE(handle), ctypes.byref(overlapped))
                return "busy"
            raise OSError(err, "LockFileEx")
        waiters = (wintypes.HANDLE * 2)(
            wintypes.HANDLE(lock_event),
            wintypes.HANDLE(abort_event),
        )
        result = kernel.WaitForMultipleObjects(2, waiters, False, int(timeout * 1000))
        if result == 0:
            return "held"
        kernel.CancelIoEx(wintypes.HANDLE(handle), ctypes.byref(overlapped))
        if result == 1:
            return "closed"
        return "busy"

    def _win_unlock(handle: int) -> None:
        import ctypes
        from contextlib import suppress
        from ctypes import wintypes

        kernel = _kernel32()
        overlapped = _overlapped()
        overlapped.Offset = 4096
        with suppress(OSError):
            kernel.UnlockFileEx(
                wintypes.HANDLE(handle),
                0,
                1,
                0,
                ctypes.byref(overlapped),
            )
        _win_close_handle(handle)

    def _win_set(event: int) -> None:
        from ctypes import wintypes

        _kernel32().SetEvent(wintypes.HANDLE(event))

    def _win_event() -> int:
        import ctypes

        handle = _kernel32().CreateEventW(None, True, False, None)
        raw = ctypes.cast(handle, ctypes.c_void_p).value
        if not raw:
            raise OSError(ctypes.get_last_error(), "CreateEventW")
        return raw

    def _win_close_event(event: int) -> None:
        from contextlib import suppress
        from ctypes import wintypes

        with suppress(OSError):
            _kernel32().CloseHandle(wintypes.HANDLE(event))

    def _win_close_handle(handle: int) -> None:
        from contextlib import suppress
        from ctypes import wintypes

        with suppress(OSError):
            _kernel32().CloseHandle(wintypes.HANDLE(handle))

    _kernel_dll: Any = None

    def _kernel32() -> Any:
        global _kernel_dll
        if _kernel_dll is None:
            import ctypes
            from ctypes import wintypes

            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.CreateFileW.argtypes = [
                wintypes.LPCWSTR,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.c_void_p,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.HANDLE,
            ]
            kernel.CreateFileW.restype = wintypes.HANDLE
            kernel.LockFileEx.argtypes = [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.c_void_p,
            ]
            kernel.LockFileEx.restype = wintypes.BOOL
            kernel.UnlockFileEx.argtypes = [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.c_void_p,
            ]
            kernel.UnlockFileEx.restype = wintypes.BOOL
            kernel.CancelIoEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
            kernel.CancelIoEx.restype = wintypes.BOOL
            kernel.WaitForMultipleObjects.argtypes = [
                wintypes.DWORD,
                ctypes.c_void_p,
                wintypes.BOOL,
                wintypes.DWORD,
            ]
            kernel.WaitForMultipleObjects.restype = wintypes.DWORD
            kernel.CreateEventW.argtypes = [
                ctypes.c_void_p,
                wintypes.BOOL,
                wintypes.BOOL,
                wintypes.LPCWSTR,
            ]
            kernel.CreateEventW.restype = wintypes.HANDLE
            kernel.SetEvent.argtypes = [wintypes.HANDLE]
            kernel.SetEvent.restype = wintypes.BOOL
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel.CloseHandle.restype = wintypes.BOOL
            _kernel_dll = kernel
        return _kernel_dll

    def _overlapped() -> ctypes.Structure:
        import ctypes
        from ctypes import wintypes

        class Overlapped(ctypes.Structure):
            _fields_ = [
                ("Internal", ctypes.c_size_t),
                ("InternalHigh", ctypes.c_size_t),
                ("Offset", wintypes.DWORD),
                ("OffsetHigh", wintypes.DWORD),
                ("hEvent", wintypes.HANDLE),
            ]

        return Overlapped()

else:

    def _open_posix(path: Path) -> int:
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        fd = os.open(path, flags, 0o644)
        os.set_inheritable(fd, False)
        return fd

    def _flock(fd: int, *, block: bool) -> None:
        import fcntl

        flags = fcntl.LOCK_EX
        if not block:
            flags |= fcntl.LOCK_NB
        while True:
            try:
                fcntl.flock(fd, flags)
                return
            except InterruptedError:
                if not block:
                    raise

    def _unlock_close(fd: int) -> None:
        import fcntl
        from contextlib import suppress

        with suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        with suppress(OSError):
            os.close(fd)

    def _acquire_posix(
        claim: Claim,
        token: object,
        done: Event,
        timeout: float,
        taken: list[bool] | None,
    ) -> None:
        fd = _open_posix(claim.path)
        if timeout == 0:
            try:
                _flock(fd, block=False)
            except BlockingIOError:
                os.close(fd)
                raise _busy(claim, timeout) from None
            _write_pid(claim.path, fd)
            if not claim.publish(fd, token, done, taken):
                _unlock_close(fd)
                raise _closed()
            return

        def blocker() -> None:
            try:
                _flock(fd, block=True)
            except OSError:
                os.close(fd)
                done.set()
                return
            _write_pid(claim.path, fd)
            if not claim.publish(fd, token, done, taken):
                _unlock_close(fd)

        Thread(target=blocker, name="litewriter-claim", daemon=True).start()
        kind = claim.finish_wait(token, fd, timed_out=not done.wait(timeout))
        if kind == "held":
            return
        if kind == "closed":
            raise _closed()
        raise _busy(claim, timeout)
