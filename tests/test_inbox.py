from threading import Thread

import pytest

from litewriter.errors import WriterRuntime
from litewriter.inbox import Inbox, Slot


def test_put_get_order() -> None:
    box: Inbox[int] = Inbox()
    box.put(1)
    box.put(2)
    assert box.get() == 1
    assert box.take_rest() == [2]


def test_take_rest_empty() -> None:
    box: Inbox[int] = Inbox()
    box.put(1)
    assert box.get() == 1
    assert box.take_rest() == []


def test_many_producers() -> None:
    box: Inbox[int] = Inbox()
    n = 200

    def produce(start: int) -> None:
        for i in range(start, start + n):
            box.put(i)

    threads = [Thread(target=produce, args=(i * n,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    seen: list[int] = []
    first = box.get()
    seen.append(first)
    seen.extend(box.take_rest())
    assert sorted(seen) == list(range(4 * n))


def test_take_batch_waits_then_drains() -> None:
    box: Inbox[int] = Inbox()
    box.put(1)
    box.put(2)
    box.put(3)
    assert box.take_batch() == [1, 2, 3]
    box.put(4)
    assert box.take_batch() == [4]


def test_slot_result_from_another_thread() -> None:
    slot: Slot[int] = Slot()

    def wait() -> None:
        assert slot.result(timeout=2) == 5

    thread = Thread(target=wait)
    thread.start()
    slot.set_result(5)
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert slot.done()


def test_slot_result_and_error() -> None:
    ok: Slot[int] = Slot()
    ok.set_result(7)
    assert ok.done()
    assert ok.result() == 7
    bad: Slot[int] = Slot()
    bad.set_exception(ValueError("nope"))
    with pytest.raises(ValueError, match="nope"):
        bad.result()


def test_closed_inbox_refuses_and_returns_the_rest() -> None:
    box: Inbox[int] = Inbox()
    box.put(1)
    assert box.close() == [1]
    with pytest.raises(WriterRuntime, match="closed"):
        box.put(2)
    box.reopen()
    box.put(3)
    assert box.take_batch() == [3]
