"""Unit tests for :class:`SerialTaskLane`."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from aws_durable_execution_sdk_python_testing.worker.lane import SerialTaskLane


def _appender(target: list[int], value: int) -> Callable[[], None]:
    """Return a zero-arg task that appends ``value`` to ``target``."""
    return lambda: target.append(value)


def _double(value: int) -> Callable[[], int]:
    """Return a zero-arg task that yields ``value * 2``."""
    return lambda: value * 2


def test_runs_tasks_in_submit_order() -> None:
    lane = SerialTaskLane.create()
    try:
        order: list[int] = []
        futures = [lane.submit(_appender(order, i)) for i in range(20)]
        for f in futures:
            f.result(timeout=5)
        assert order == list(range(20))
    finally:
        lane.stop()


def test_runs_one_at_a_time() -> None:
    lane = SerialTaskLane.create()
    try:
        active = 0
        max_active = 0
        guard = threading.Lock()

        def task() -> None:
            nonlocal active, max_active
            with guard:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.01)
            with guard:
                active -= 1

        futures = [lane.submit(task) for _ in range(10)]
        for f in futures:
            f.result(timeout=5)
        assert max_active == 1
    finally:
        lane.stop()


def test_exception_does_not_break_lane() -> None:
    lane = SerialTaskLane.create()
    try:

        def boom() -> None:
            raise ValueError("boom")

        failing = lane.submit(boom)
        with pytest.raises(ValueError, match="boom"):
            failing.result(timeout=5)

        # The lane keeps serving after a task raises.
        ok = lane.submit(lambda: 42)
        assert ok.result(timeout=5) == 42
    finally:
        lane.stop()


def test_concurrent_submitters_get_correct_results() -> None:
    lane = SerialTaskLane.create()
    try:
        results: dict[int, int] = {}
        results_lock = threading.Lock()
        start = threading.Barrier(8)

        def submit_many(base: int) -> None:
            start.wait()
            local = {n: lane.submit(_double(n)) for n in range(base, base + 25)}
            for n, fut in local.items():
                with results_lock:
                    results[n] = fut.result(timeout=5)

        threads = [
            threading.Thread(target=submit_many, args=(base,))
            for base in range(0, 200, 25)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert results == {n: n * 2 for n in range(200)}
    finally:
        lane.stop()


def test_stop_drains_queued_tasks_then_joins() -> None:
    lane = SerialTaskLane.create()
    done: list[int] = []
    futures = [lane.submit(_appender(done, i)) for i in range(50)]

    lane.stop()  # must drain all queued work before the thread exits

    assert lane.is_running() is False
    assert all(f.done() for f in futures)
    assert done == list(range(50))


def test_submit_after_stop_raises() -> None:
    lane = SerialTaskLane.create()
    lane.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        lane.submit(lambda: None)


def test_stop_is_idempotent() -> None:
    lane = SerialTaskLane.create()
    lane.stop()
    lane.stop()  # second call is a no-op, does not raise
    assert lane.is_running() is False


def test_cancelled_future_is_skipped() -> None:
    lane = SerialTaskLane.create()
    try:
        started = threading.Event()
        release = threading.Event()

        def blocker() -> None:
            started.set()
            release.wait(timeout=5)

        blocking = lane.submit(blocker)
        started.wait(timeout=5)

        # Queue a second task and cancel it while the lane is still busy
        # with the blocker, so the consumer skips it when it dequeues.
        skipped = lane.submit(lambda: None)
        assert skipped.cancel()

        release.set()
        blocking.result(timeout=5)

        assert skipped.cancelled()
    finally:
        lane.stop()
