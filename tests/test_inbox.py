import asyncio
from concurrent.futures import Future
from threading import Thread

import pytest

from litewriter.errors import StateError
from litewriter.inbox import Inbox, Job, settle


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
    assert sorted(box.take_batch()) == list(range(4 * n))


def test_take_batch_waits_then_drains() -> None:
    box: Inbox[int] = Inbox()
    box.put(1)
    box.put(2)
    box.put(3)
    assert box.take_batch() == [1, 2, 3]
    box.put(4)
    assert box.take_batch() == [4]


def test_job_runs_the_callable() -> None:
    job = Job(lambda tx, a, b, *, y: (tx, a, b, y), (1, 2), {"y": 3}, None, None)
    assert job.run("t") == ("t", 1, 2, 3)
    bare = Job(lambda tx: "ok", (), {}, None, None)
    assert bare.run("t") == "ok"


def test_future_result_from_another_thread() -> None:
    future: Future[int] = Future()

    def wait() -> None:
        assert future.result(timeout=2) == 5

    thread = Thread(target=wait)
    thread.start()
    settle(future, 5, None)
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert future.done()


def test_settle_result_and_error() -> None:
    ok: Future[int] = Future()
    settle(ok, 7, None)
    assert ok.done()
    assert ok.result() == 7
    bad: Future[int] = Future()
    settle(bad, None, ValueError("nope"))
    with pytest.raises(ValueError, match="nope"):
        bad.result()


def test_settle_skips_a_cancelled_waiter() -> None:
    future: Future[int] = Future()
    future.cancel()
    settle(future, 1, None)
    assert future.cancelled()


async def test_settle_an_async_future() -> None:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[int] = loop.create_future()
    settle(future, 9, None)
    assert await future == 9


def test_closed_inbox_refuses_and_returns_the_rest() -> None:
    box: Inbox[int] = Inbox()
    box.put(1)
    assert box.close() == [1]
    with pytest.raises(StateError, match="closed"):
        box.put(2)
    box.reopen()
    box.put(3)
    assert box.take_batch() == [3]
