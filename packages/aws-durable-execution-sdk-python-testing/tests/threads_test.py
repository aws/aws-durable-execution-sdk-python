"""Tests for the bounded daemon-thread pool."""

from __future__ import annotations

import threading
import time
from concurrent.futures import CancelledError

import pytest

from aws_durable_execution_sdk_python_testing.threads import (
    DaemonThreadPool,
    default_max_workers,
)


def test_work_runs_on_a_daemon_thread():
    pool = DaemonThreadPool(thread_name_prefix="t")

    future = pool.submit(lambda: threading.current_thread())
    worker = future.result(timeout=5)

    assert worker.daemon is True
    assert worker.name.startswith("t_")
    pool.shutdown()


def test_default_cap_matches_thread_pool_executor():
    assert DaemonThreadPool().max_workers == default_max_workers()
    with pytest.raises(ValueError, match="greater than 0"):
        DaemonThreadPool(max_workers=0)


def test_queued_work_runs_in_submission_order_beyond_the_cap():
    pool = DaemonThreadPool(max_workers=1)
    gate = threading.Event()
    order: list[int] = []

    def first() -> None:
        gate.wait(5)
        order.append(0)

    futures = [pool.submit(first)] + [pool.submit(order.append, n) for n in (1, 2, 3)]
    gate.set()
    for future in futures:
        future.result(timeout=5)

    assert order == [0, 1, 2, 3]
    pool.shutdown()


def test_never_runs_more_than_the_cap_at_once():
    pool = DaemonThreadPool(max_workers=2)
    release = threading.Event()
    running = 0
    peak = 0
    lock = threading.Lock()

    def work() -> None:
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        release.wait(5)
        with lock:
            running -= 1

    futures = [pool.submit(work) for _ in range(6)]
    time.sleep(0.2)
    with lock:
        assert running == 2
    release.set()
    for future in futures:
        future.result(timeout=5)

    assert peak == 2
    pool.shutdown()


def test_an_idle_worker_is_reused_instead_of_starting_another():
    pool = DaemonThreadPool(max_workers=4)

    pool.submit(lambda: None).result(timeout=5)
    pool.submit(lambda: None).result(timeout=5)

    assert len(pool._threads) == 1  # noqa: SLF001
    pool.shutdown()


def test_shutdown_cancels_queued_work_and_refuses_new_work():
    pool = DaemonThreadPool(max_workers=1)
    release = threading.Event()
    busy = pool.submit(release.wait, 5)
    queued = pool.submit(lambda: "never")

    pool.shutdown()

    assert queued.cancelled()
    with pytest.raises(CancelledError):
        queued.result(timeout=0)
    with pytest.raises(RuntimeError, match="after shutdown"):
        pool.submit(lambda: None)
    release.set()
    assert busy.result(timeout=5) is True
    pool.shutdown()  # idempotent


def test_shutdown_returns_while_a_worker_is_blocked():
    """The blocked worker is left alone: shutdown neither waits for it
    nor fails. Its result lands on a future nobody reads."""
    pool = DaemonThreadPool(max_workers=1)
    release = threading.Event()
    blocked = pool.submit(release.wait, 30)

    started = time.monotonic()
    pool.shutdown(wait=True)
    elapsed = time.monotonic() - started

    assert elapsed < 1
    assert not blocked.done()
    release.set()
    assert blocked.result(timeout=5) is True


def test_an_exception_lands_on_the_future():
    pool = DaemonThreadPool(max_workers=1)

    def boom() -> None:
        msg = "boom"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="boom"):
        pool.submit(boom).result(timeout=5)
    pool.shutdown()
