# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the asynchronous export scheduler (``_export_scheduler``).

These drive the scheduler directly with plain record dicts and purpose-built
exporter doubles. Synchronization uses events/predicates (not sleeps) so the
coalescing, fairness, drain, flush-ordering, timeout and thread-lifecycle
invariants are asserted deterministically rather than by timing luck.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from aws_durable_execution_sdk_python_insight._export_scheduler import (
    _ExportScheduler,
)


ARN_A = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/exec-a/inv-1"
ARN_B = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/exec-b/inv-1"
ARN_C = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/exec-c/inv-1"
ARN_D = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/exec-d/inv-1"


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.005) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _rec(arn: str, value: str, *, status: str = "RUNNING") -> dict[str, Any]:
    return {"executionArn": arn, "status": status, "v": value, "operations": []}


def _insight_thread_count() -> int:
    return sum(
        1 for t in threading.enumerate() if t.name.startswith("workflow-insight-export")
    )


class RecordingExporter:
    """Records every export/flush in call order (fast, non-blocking)."""

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        self.max_record_size_bytes = max_record_size_bytes
        self.calls: list[tuple[str, Any]] = []
        self._lock = threading.Lock()

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        with self._lock:
            self.calls.append(("export", record.get("v")))

    def flush(self) -> None:
        with self._lock:
            self.calls.append(("flush", None))

    def exported_values(self) -> list[Any]:
        with self._lock:
            return [v for kind, v in self.calls if kind == "export"]


class BlockingExporter:
    """Blocks inside ``export`` until released; signals when an export starts."""

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        self.max_record_size_bytes = max_record_size_bytes
        self._release = threading.Event()
        self.started = threading.Event()
        self.exported: list[Any] = []
        self.flushed = 0
        self._lock = threading.Lock()

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.started.set()
        self._release.wait(5.0)
        with self._lock:
            self.exported.append(record.get("v"))

    def flush(self) -> None:
        with self._lock:
            self.flushed += 1

    def release(self) -> None:
        self._release.set()

    def exported_values(self) -> list[Any]:
        with self._lock:
            return list(self.exported)


class FailingExporter:
    """Raises in both export and flush."""

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        self.max_record_size_bytes = max_record_size_bytes
        self.export_calls = 0
        self.flush_calls = 0

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.export_calls += 1
        raise RuntimeError("export boom")

    def flush(self) -> None:
        self.flush_calls += 1
        raise RuntimeError("flush boom")


# -- lazy worker creation / one worker per exporter --------------------------


def test_no_worker_before_first_schedule():
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    assert lane._worker is None
    assert not lane._worker_alive()


def test_worker_created_lazily_on_first_schedule():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(lane._worker_alive)
    exporter.release()
    scheduler.end_invocation(5.0)


def test_one_worker_per_exporter():
    base = _insight_thread_count()
    e1, e2 = BlockingExporter(), BlockingExporter()
    scheduler = _ExportScheduler([e1, e2])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(lambda: e1.started.is_set() and e2.started.is_set())
    assert _wait_until(lambda: _insight_thread_count() - base == 2)
    e1.release()
    e2.release()
    scheduler.end_invocation(5.0)


def test_repeated_scheduling_does_not_grow_threads():
    base = _insight_thread_count()
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    for i in range(100):
        scheduler.schedule(ARN_A, _rec(ARN_A, f"v{i}", status="RUNNING"))
    # A single lane never runs more than one worker at a time.
    assert _insight_thread_count() - base <= 1
    scheduler.end_invocation(5.0)
    assert _wait_until(lambda: not lane._worker_alive())


# -- coalescing / fairness / isolation ---------------------------------------


def test_same_execution_coalescing_exports_inflight_then_latest():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # a1 is in flight
    # While a1 is in flight, a2 and a3 arrive and coalesce to the latest (a3).
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "a3"))
    exporter.release()
    assert _wait_until(lambda: exporter.exported_values() == ["a1", "a3"])
    scheduler.end_invocation(5.0)


def test_different_executions_isolated_and_fair():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # a1 in flight
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))  # queued: [B]
    scheduler.schedule(ARN_B, _rec(ARN_B, "b2"))  # coalesce B -> b2
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))  # queued: [B, A]
    exporter.release()
    # a1 (in flight) first, then FIFO fairness B before the re-added A, each
    # carrying its latest coalesced value.
    assert _wait_until(lambda: exporter.exported_values() == ["a1", "b2", "a2"])
    scheduler.end_invocation(5.0)


def test_terminal_record_supersedes_pending_running():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "r1", status="RUNNING"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_A, _rec(ARN_A, "r2", status="RUNNING"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    exporter.release()
    assert _wait_until(lambda: exporter.exported_values() == ["r1", "final"])
    scheduler.end_invocation(5.0)


# -- non-blocking hook return / fast-vs-slow isolation -----------------------


def test_schedule_returns_immediately_while_exporter_blocked():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    # The exporter is now blocked mid-export; a further schedule must not block.
    start = time.monotonic()
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    assert time.monotonic() - start < 0.5
    exporter.release()
    scheduler.end_invocation(5.0)


def test_fast_lane_proceeds_while_other_lane_blocked():
    blocked = BlockingExporter()
    fast = RecordingExporter()
    scheduler = _ExportScheduler([blocked, fast])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    # Fast lane delivers even though the blocked lane is stuck on the same record.
    assert _wait_until(lambda: fast.exported_values() == ["a1"])
    assert blocked.exported_values() == []
    blocked.release()
    scheduler.end_invocation(5.0)


# -- drain / flush ordering ---------------------------------------------------


def test_drain_waits_for_final_export():
    class SlowExporter(RecordingExporter):
        def export(self, record: dict[str, Any]) -> None:
            time.sleep(0.2)
            super().export(record)

    exporter = SlowExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    ok = scheduler.end_invocation(5.0)
    assert ok is True
    assert exporter.exported_values() == ["final"]


def test_flush_happens_after_export():
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    scheduler.end_invocation(5.0)
    kinds = [kind for kind, _ in exporter.calls]
    assert kinds == ["export", "flush"]


def test_export_and_flush_exceptions_are_isolated():
    failing = FailingExporter()
    good = RecordingExporter()
    scheduler = _ExportScheduler([failing, good])
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    # Must not raise even though one exporter fails in both export and flush.
    ok = scheduler.end_invocation(5.0)
    assert ok is True
    assert failing.export_calls == 1
    assert failing.flush_calls == 1
    # The healthy exporter still delivered and flushed.
    assert good.exported_values() == ["final"]
    assert ("flush", None) in good.calls


# -- shared timeout -----------------------------------------------------------


def test_shared_timeout_bounds_invocation_end_delay():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    start = time.monotonic()
    ok = scheduler.end_invocation(0.2)
    elapsed = time.monotonic() - start
    assert ok is False  # degraded to best-effort
    assert elapsed < 2.0  # bounded by the shared deadline, not the blocked export
    exporter.release()  # let the daemon drain and exit


def test_shared_timeout_across_multiple_lanes_is_not_additive():
    e1, e2 = BlockingExporter(), BlockingExporter()
    scheduler = _ExportScheduler([e1, e2])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(lambda: e1.started.is_set() and e2.started.is_set())
    start = time.monotonic()
    ok = scheduler.end_invocation(0.3)
    elapsed = time.monotonic() - start
    assert ok is False
    # One shared deadline covers both lanes, so total wait is ~0.3s, not 0.6s.
    assert elapsed < 0.9
    e1.release()
    e2.release()


# -- worker lifecycle ---------------------------------------------------------


def test_blocked_worker_is_not_replaced():
    base = _insight_thread_count()
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    worker = lane._worker
    # More scheduling and an invocation-end (which enqueues a flush + requests
    # stop) must not spawn a replacement while the worker is blocked.
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))
    scheduler.end_invocation(0.1)
    assert lane._worker is worker
    assert _insight_thread_count() - base == 1
    exporter.release()


def test_idle_worker_exits_after_drain():
    base = _insight_thread_count()
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    scheduler.end_invocation(5.0)
    assert _wait_until(lambda: not lane._worker_alive())
    assert _wait_until(lambda: _insight_thread_count() <= base)


def test_repeated_invocation_cycles_do_not_leak_threads():
    base = _insight_thread_count()
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    for i in range(20):
        scheduler.schedule(ARN_A, _rec(ARN_A, f"final-{i}", status="SUCCEEDED"))
        scheduler.end_invocation(5.0)
        assert _wait_until(lambda: not lane._worker_alive())
    assert _wait_until(lambda: _insight_thread_count() <= base)
    assert len(exporter.exported_values()) == 20


# -- pending cap / cancelled barrier cleanup ---------------------------------


def test_pending_execution_cap_evicts_oldest():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_executions=2)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # a1 in flight (not pending)
    # Three distinct pending executions with cap 2 -> oldest (B) is evicted.
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))
    scheduler.schedule(ARN_C, _rec(ARN_C, "c1"))
    scheduler.schedule(ARN_D, _rec(ARN_D, "d1"))
    assert _wait_until(lambda: lane._pending_count() == 2)
    exporter.release()
    scheduler.end_invocation(5.0)


def test_cancelled_barrier_is_cleaned_up_and_worker_exits():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    ok = scheduler.end_invocation(0.1)  # times out -> barrier cancelled
    assert ok is False
    # Once the exporter unblocks, the worker drains the cancelled barrier
    # (skipping the pointless flush) and exits idle -- no permanent leak.
    exporter.release()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.flushed == 0  # cancelled barrier did not flush
