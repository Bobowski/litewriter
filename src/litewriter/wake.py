"""Wake many waiters on a loop with one posted callback.

Waiting coroutines stay parked. Each carries a future. When a batch
commits, the writer gathers every pending waiter and watch on that loop
into one delivery. A socketpair attached with ``add_reader`` carries it
when the platform supports readers. Otherwise a single
``call_soon_threadsafe`` does. Either way the waiters are set on their
own loop inside one callback.

``deliver`` groups wakes by loop. Each watch that woke joins that
loop's delivery.
"""

import asyncio
import contextlib
import socket
import weakref
from asyncio import AbstractEventLoop
from collections.abc import Callable
from functools import partial
from itertools import groupby
from operator import itemgetter
from threading import Lock
from weakref import WeakKeyDictionary

type Wake = Callable[[], None]


def running_loop() -> AbstractEventLoop | None:
    """The event loop running on this thread, if there is one."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def _pair() -> tuple[socket.socket, socket.socket]:
    """One in-process mailbox, out and in. Non-blocking both ways."""
    out, side_in = socket.socketpair()
    out.setblocking(False)
    side_in.setblocking(False)
    return out, side_in


def _close_pair(sock_in: socket.socket, sock_out: socket.socket | None) -> None:
    for sock in (sock_in, sock_out):
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.close()


class _Mailbox:
    """One posted wake per loop. Reclaimable: add_reader says unsupported."""

    __slots__ = (
        "__weakref__",
        "_close",
        "_closed",
        "_fin",
        "_in",
        "_loop",
        "_out",
        "_pending",
    )

    def __init__(self, loop: AbstractEventLoop, sock_in: socket.socket) -> None:
        # A weak ref: LoopWakes keys on the loop, so this strong ref
        # would keep every loop alive for the writer's lifetime.
        self._loop = weakref.ref(loop)
        self._in = sock_in
        self._out: socket.socket | None = None
        self._fin: weakref.finalize[..., _Mailbox] | None = None
        self._pending: list[Wake] = []
        self._closed = False
        self._close = Lock()

    def arm(self, sock_out: socket.socket) -> None:
        """Let GC close both sockets when this mailbox is collected."""
        self._out = sock_out
        self._fin = weakref.finalize(self, _close_pair, self._in, sock_out)

    def attach(self, sock_out: socket.socket) -> bool:
        """Start the reader. False means the loop cannot watch a descriptor."""
        loop = self._loop()
        if loop is None:
            return False
        try:
            loop.add_reader(self._in.fileno(), self._fire)
        except NotImplementedError, RuntimeError, OSError:
            return False
        return True

    def post(self, wake: Wake) -> None:
        with self._close:
            if self._closed:
                return
            self._pending.append(wake)
            out = self._out
            if out is not None:
                with contextlib.suppress(BlockingIOError, OSError):
                    out.send(b"\0")

    def close(self) -> list[Wake]:
        """Detach the reader. Return wakes that were posted but never ran."""
        with self._close:
            if self._closed:
                return []
            self._closed = True
            pending = self._pending
            self._pending = []
        loop = self._loop()
        if loop is None or loop.is_closed():
            self._shut()
            return pending

        def detach() -> None:
            # remove_reader is only safe on the loop's own thread.
            with contextlib.suppress(RuntimeError, OSError):
                loop.remove_reader(self._in.fileno())
            self._shut()

        if running_loop() is loop:
            detach()
        else:
            try:
                loop.call_soon_threadsafe(detach)
            except RuntimeError:
                self._shut()
        return pending

    def _shut(self) -> None:
        """Close both sockets once, through the finalizer when armed."""
        with self._close:
            fin = self._fin
            self._fin = None
        if fin is not None:
            fin()
        else:
            _close_pair(self._in, self._out)

    def _fire(self) -> None:
        # Drain first, then take the list: a post that lands in between
        # appends its wake AND sends a byte, so the next fire finds it.
        # Taking the list first would eat that byte and strand the wake.
        with contextlib.suppress(OSError):
            while self._in.recv(4096):
                pass
        with self._close:
            if self._closed:
                return
            pending = self._pending
            self._pending = []
        for wake in pending:
            wake()


class LoopWakes:
    """All posted wakes for the writer's loops.

    ``post(loop, wake)`` collapses to one loop callback per cycle.
    """

    __slots__ = ("_lock", "_loops")

    def __init__(self) -> None:
        self._lock = Lock()
        self._loops: WeakKeyDictionary[AbstractEventLoop, list[Wake] | _Mailbox] = (
            WeakKeyDictionary()
        )

    def ensure(self, loop: AbstractEventLoop) -> None:
        """Attach a mailbox for this loop, or a pending list to drain."""
        with self._lock:
            if loop in self._loops:
                return
            for dead, entry in list(self._loops.items()):
                if dead.is_closed():
                    del self._loops[dead]
                    if isinstance(entry, _Mailbox):
                        entry.close()
            try:
                out, side_in = _pair()
            except AttributeError, OSError:
                self._loops[loop] = []
                return
            mailbox = _Mailbox(loop, side_in)
            if not mailbox.attach(out):
                side_in.close()
                out.close()
                self._loops[loop] = []
                return
            mailbox.arm(out)
            self._loops[loop] = mailbox

    def post(self, loop: AbstractEventLoop, wake: Wake) -> None:
        """Deliver ``wake`` on ``loop``. A dead loop drops it."""
        with self._lock:
            entry = self._loops.get(loop)
        if entry is None:
            # ``ensure`` never ran for this loop, or the weak key was
            # collected. Hand the wake to the loop directly.
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(wake)
            return
        if isinstance(entry, _Mailbox):
            entry.post(wake)
            return
        entry.append(wake)
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(partial(_drain, entry))

    def close(self) -> list[tuple[AbstractEventLoop, Wake]]:
        """Detach every mailbox. Return wakes that never reached their loop.

        The writer drains the inbox before it closes the wakes, so every
        posted wake is either inside a mailbox or back here. The caller
        delivers these on their own loops.
        """
        with self._lock:
            entries = list(self._loops.items())
            self._loops.clear()
        late: list[tuple[AbstractEventLoop, Wake]] = []
        for loop, entry in entries:
            if isinstance(entry, _Mailbox):
                late.extend((loop, wake) for wake in entry.close())
        return late


def _drain(pending: list[Wake]) -> None:
    while pending:
        try:
            queued = pending.pop(0)
        except IndexError:
            return
        queued()


def _fire_all(batch: list[Wake]) -> None:
    for set_it in batch:
        set_it()


def deliver(wakes: LoopWakes, notes: list[tuple[AbstractEventLoop, Wake]]) -> None:
    """Post each watch's wake on its own loop, one post per loop.

    Loops are not orderable, so sort on ``id`` — equal loop objects end
    up next to each other, which is all ``groupby`` needs.
    """
    notes.sort(key=lambda note: id(note[0]))
    for loop, group in groupby(notes, key=itemgetter(0)):
        batch = [set_it for _, set_it in group]
        wakes.post(loop, partial(_fire_all, batch))
