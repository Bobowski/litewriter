"""Wake one asyncio loop once per batch. No per-job ``call_soon_threadsafe``.

A socketpair mailbox is made on its loop's own thread (``ensure``),
because ``add_reader`` is not thread-safe. The writer only posts.
A loop with no ``add_reader`` gets one ``call_soon_threadsafe`` for the
batch. That is the Windows proactor loop.
"""

import asyncio
import socket
from asyncio import AbstractEventLoop
from collections.abc import Callable
from contextlib import suppress
from functools import partial
from threading import Lock
from weakref import WeakKeyDictionary

type WakeFn = Callable[[], None]


class LoopWake:
    """One mailbox for one loop.

    The writer posts work. The loop runs it. Many results, one wake.
    A socketpair is that wake when the loop has ``add_reader``.
    """

    __slots__ = ("_lock", "_pending", "_rs", "_ws", "loop")

    def __init__(self, loop: AbstractEventLoop) -> None:
        self.loop = loop
        self._lock = Lock()
        self._pending: list[WakeFn] = []
        self._rs: socket.socket | None = None
        self._ws: socket.socket | None = None
        rs, ws = socket.socketpair()
        rs.setblocking(False)
        ws.setblocking(False)
        try:
            loop.add_reader(rs.fileno(), self._drain)
        except NotImplementedError:
            rs.close()
            ws.close()
            return
        self._rs = rs
        self._ws = ws

    def post(self, fn: WakeFn) -> None:
        with self._lock:
            first = not self._pending
            self._pending.append(fn)
        if not first:
            return
        ws = self._ws
        if ws is None:
            with suppress(RuntimeError):
                self.loop.call_soon_threadsafe(self._drain)
            return
        with suppress(BlockingIOError, OSError):
            ws.send(b"\0")

    def _drain(self) -> None:
        rs = self._rs
        if rs is not None:
            with suppress(BlockingIOError, InterruptedError):
                while rs.recv(256):
                    pass
        with self._lock:
            work = self._pending
            self._pending = []
        for fn in work:
            fn()

    def close(self) -> None:
        """Run the work the writer already posted, then close the mailbox."""
        loop = self.loop
        if loop.is_closed():
            self._shut()
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            self._finish()
        else:
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(self._finish)

    def _finish(self) -> None:
        self._drain()
        self._shut()

    def _shut(self) -> None:
        rs = self._rs
        ws = self._ws
        self._rs = None
        self._ws = None
        if rs is not None:
            with suppress(ValueError, OSError):
                if not self.loop.is_closed():
                    self.loop.remove_reader(rs.fileno())
            rs.close()
        if ws is not None:
            ws.close()


class LoopWakes:
    """One mailbox per event loop, reused for the life of the writer."""

    __slots__ = ("_items", "_lock")

    def __init__(self) -> None:
        self._lock = Lock()
        self._items: WeakKeyDictionary[AbstractEventLoop, LoopWake] = (
            WeakKeyDictionary()
        )

    def ensure(self, loop: AbstractEventLoop) -> None:
        """Make the mailbox for ``loop``. Call on that loop's thread."""
        with self._lock:
            if loop in self._items:
                return
            self._items[loop] = LoopWake(loop)

    def post(self, loop: AbstractEventLoop, fn: WakeFn) -> None:
        """Run ``fn`` on ``loop`` soon. Safe from any thread."""
        with self._lock:
            wake = self._items.get(loop)
        if wake is not None:
            wake.post(fn)
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(fn)

    def close(self) -> None:
        with self._lock:
            wakes = list(self._items.values())
            self._items.clear()
        for wake in wakes:
            wake.close()


def deliver(wakes: LoopWakes, notes: list[tuple[AbstractEventLoop, WakeFn]]) -> None:
    """Post one function per loop. That function sets every event for the loop."""
    grouped: dict[AbstractEventLoop, list[WakeFn]] = {}
    for loop, fn in notes:
        grouped.setdefault(loop, []).append(fn)
    for loop, fns in grouped.items():
        wakes.post(loop, partial(_set_all, tuple(fns)))


def _set_all(fns: tuple[WakeFn, ...]) -> None:
    for fn in fns:
        fn()
