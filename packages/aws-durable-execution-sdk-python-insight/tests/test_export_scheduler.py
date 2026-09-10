# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the asynchronous export scheduler (``_export_scheduler``).

These drive the scheduler directly with plain record dicts and purpose-built
exporter doubles. Synchronization uses events/predicates (not sleeps) so the
bounded FIFO, fairness, drain, flush-ordering, timeout and thread-lifecycle
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


def _lane_worker_count(lane) -> int:
    """Count live worker threads that belong to *this* lane by identity.

    Each lane names its worker ``workflow-insight-export-{id(lane)}``, so this is
    scoped to the given lane and is unaffected by daemon workers other tests may
    still be winding down -- unlike a process-global thread-count delta.
    """
    name = f"workflow-insight-export-{id(lane)}"
    return sum(1 for t in threading.enumerate() if t.name == name and t.is_alive())


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


class BlockingFlushExporter(RecordingExporter):
    """Exports normally but blocks inside ``flush`` until released.

    Lets a test drive the worker until it has already popped a flush barrier and
    is stuck mid-``flush`` -- the "already in flight" cancellation race.
    """

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        super().__init__(max_record_size_bytes)
        self.flush_started = threading.Event()
        self._flush_release = threading.Event()

    def flush(self) -> None:
        self.flush_started.set()
        self._flush_release.wait(5.0)
        super().flush()

    def release_flush(self) -> None:
        self._flush_release.set()


class FirstExportBlockingRecorder(RecordingExporter):
    """Records call order but blocks the first export until released."""

    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self._release = threading.Event()

    def export(self, record: dict[str, Any]) -> None:
        if not self.started.is_set():
            self.started.set()
            self._release.wait(5.0)
        super().export(record)

    def release(self) -> None:
        self._release.set()


class BlockingBufferedExporter:
    """Blocks export and publishes buffered records only when flush runs."""

    max_record_size_bytes = None

    def __init__(self) -> None:
        self.started = threading.Event()
        self._release = threading.Event()
        self.buffered: list[Any] = []
        self.published: list[Any] = []
        self.flushed = 0

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.started.set()
        self._release.wait(5.0)
        self.buffered.append(record.get("v"))

    def flush(self) -> None:
        self.flushed += 1
        self.published.extend(self.buffered)
        self.buffered.clear()

    def release(self) -> None:
        self._release.set()


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


class _Uncopyable(dict[str, str]):
    """A JSON-serializable payload whose ``deepcopy`` raises."""

    def __init__(self) -> None:
        super().__init__({"value": "safe"})

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        raise RuntimeError("uncopyable payload")


class _SlottedPayload:
    __slots__ = ("payload",)

    def __init__(self, payload: Any) -> None:
        self.payload = payload


class _UnsizedSlottedPayload(_SlottedPayload):
    def __sizeof__(self) -> int:
        raise RuntimeError("size unavailable")


class _Unsized:
    """A payload whose custom ``__sizeof__`` raises."""

    def __sizeof__(self) -> int:
        raise RuntimeError("size unavailable")


class _TrackedLargeList(list[Any]):
    def __init__(self) -> None:
        super().__init__([None] * 10_000)
        self.iterated = False

    def __iter__(self):
        self.iterated = True
        return super().__iter__()


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
    assert _wait_until(lambda: not lane._worker_alive())


def test_one_worker_per_exporter():
    e1, e2 = BlockingExporter(), BlockingExporter()
    scheduler = _ExportScheduler([e1, e2])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(lambda: e1.started.is_set() and e2.started.is_set())
    assert _lane_worker_count(scheduler._lanes[0]) == 1
    assert _lane_worker_count(scheduler._lanes[1]) == 1
    e1.release()
    e2.release()
    scheduler.end_invocation(5.0)
    assert _wait_until(
        lambda: not any(lane._worker_alive() for lane in scheduler._lanes)
    )


def test_worker_start_failure_disables_lane_and_fails_barrier(monkeypatch):
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]

    def fail_start(self):
        raise RuntimeError("cannot start new thread")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))

    assert lane._disabled is True
    assert lane._pending_count() == 0
    assert lane._pending_bytes_count() == 0
    assert lane._queue_len() == 0
    assert scheduler.end_invocation(0.1) is False
    assert exporter.exported_values() == []


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


# -- bounded FIFO / fairness / isolation -------------------------------------


def test_same_execution_fifo_exports_all_pending_records():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # a1 is in flight
    # While a1 is in flight, a2 and a3 remain ordered in the pending FIFO.
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "a3"))
    exporter.release()
    assert _wait_until(lambda: exporter.exported_values() == ["a1", "a2", "a3"])
    scheduler.end_invocation(5.0)


def test_different_executions_isolated_and_fair():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # a1 in flight
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))  # queued: [B]
    scheduler.schedule(ARN_B, _rec(ARN_B, "b2"))  # B FIFO: [b1, b2]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))  # queued: [B, A]
    exporter.release()
    # a1 (in flight) first, then one record per ARN turn: B, A, then B again.
    assert _wait_until(lambda: exporter.exported_values() == ["a1", "b1", "a2", "b2"])
    scheduler.end_invocation(5.0)


def test_terminal_record_follows_pending_running_records():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "r1", status="RUNNING"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_A, _rec(ARN_A, "r2", status="RUNNING"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    exporter.release()
    assert _wait_until(lambda: exporter.exported_values() == ["r1", "r2", "final"])
    scheduler.end_invocation(5.0)


def test_pending_fifo_cap_drops_oldest_and_retains_terminal():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_records_per_execution=2)
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_A, _rec(ARN_A, "oldest"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "newer"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "final", status="SUCCEEDED"))
    exporter.release()
    assert _wait_until(
        lambda: exporter.exported_values() == ["inflight", "newer", "final"]
    )
    scheduler.end_invocation(5.0)


# -- copy failure isolation ---------------------------------------------------


def test_uncopyable_custom_value_reaches_exporter_render():
    class CustomRenderExporter(RecordingExporter):
        def __init__(self) -> None:
            super().__init__(max_record_size_bytes=10_000)
            self.rendered_values: list[str] = []

        def render(self, record: dict[str, Any]) -> Any:
            value = record["payload"]["value"]
            self.rendered_values.append(value)
            return {"value": value}

    exporter = CustomRenderExporter()
    scheduler = _ExportScheduler([exporter])
    record = _rec(ARN_A, "before-render")
    record["payload"] = _Uncopyable()

    scheduler.schedule(ARN_A, record)
    scheduler.end_invocation(5.0)

    assert exporter.rendered_values == ["safe"]
    assert exporter.exported_values() == ["before-render"]


def test_uncopyable_custom_value_does_not_alias_record_containers():
    class MutatingRenderExporter(RecordingExporter):
        def render(self, record: dict[str, Any]) -> Any:
            record["mutated"] = True
            return record

    exporter = MutatingRenderExporter()
    scheduler = _ExportScheduler([exporter])
    record = _rec(ARN_A, "custom")
    record["payload"] = _Uncopyable()

    scheduler.schedule(ARN_A, record)
    scheduler.end_invocation(5.0)

    assert "mutated" not in record
    assert exporter.exported_values() == ["custom"]


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


def test_flush_barrier_waits_for_entire_pending_fifo():
    exporter = FirstExportBlockingRecorder()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "a3"))
    barrier = lane.enqueue_flush()
    exporter.release()

    assert barrier.wait(5.0)
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.calls == [
        ("export", "a1"),
        ("export", "a2"),
        ("export", "a3"),
        ("flush", None),
    ]


def test_flush_barrier_splits_same_execution_fifo_by_generation():
    exporter = FirstExportBlockingRecorder()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)

    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    barrier = lane.enqueue_flush()
    scheduler.schedule(ARN_A, _rec(ARN_A, "a3"))
    exporter.release()

    assert barrier.wait(5.0)
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.calls == [
        ("export", "a1"),
        ("export", "a2"),
        ("flush", None),
        ("export", "a3"),
    ]


def test_record_scheduled_after_barrier_waits_for_later_flush():
    exporter = FirstExportBlockingRecorder()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)

    barrier = lane.enqueue_flush()
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    exporter.release()

    assert barrier.wait(5.0)
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.calls == [
        ("export", "a1"),
        ("flush", None),
        ("export", "a2"),
    ]


def test_lane_flush_includes_other_execution_records_already_queued():
    exporter = FirstExportBlockingRecorder()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)

    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))
    barrier = lane.enqueue_flush()
    exporter.release()

    assert barrier.wait(5.0)
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.calls == [
        ("export", "a1"),
        ("export", "b1"),
        ("flush", None),
    ]


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
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    start = time.monotonic()
    ok = scheduler.end_invocation(0.2)
    elapsed = time.monotonic() - start
    assert ok is False  # degraded to best-effort
    assert elapsed < 2.0  # bounded by the shared deadline, not the blocked export
    exporter.release()  # let the daemon drain and exit
    # Wait for the released worker to actually stop so it cannot leak into a
    # later test's baseline thread count.
    assert _wait_until(lambda: not lane._worker_alive())


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
    # Wait for both released workers to actually stop so neither leaks into a
    # later test's baseline thread count.
    assert _wait_until(
        lambda: not any(lane._worker_alive() for lane in scheduler._lanes)
    )


# -- worker lifecycle ---------------------------------------------------------


def test_blocked_worker_is_not_replaced():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    worker = lane._worker
    assert worker is not None and worker.is_alive()
    # The blocked lane already has exactly one live worker of its own.
    assert _lane_worker_count(lane) == 1
    # More scheduling and an invocation-end (which enqueues a flush + requests
    # stop) must not spawn a replacement while the worker is blocked.
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))
    scheduler.end_invocation(0.1)
    # Identity: the lane still holds the SAME blocked worker -- no replacement
    # thread was swapped in -- and it is still the only live worker for this
    # lane. Both checks are scoped to this lane, so they cannot flake on daemon
    # workers other tests are winding down.
    assert lane._worker is worker
    assert worker.is_alive()
    assert _lane_worker_count(lane) == 1
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


def test_pending_record_cap_preserves_original_lane_memory_bound():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_records=3)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # a1 in flight (not pending)
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))
    scheduler.schedule(ARN_C, _rec(ARN_C, "c1"))
    scheduler.schedule(ARN_D, _rec(ARN_D, "d1"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))
    assert lane._pending_record_count() == 3
    exporter.release()
    scheduler.end_invocation(5.0)
    assert exporter.exported_values() == ["a1", "c1", "d1", "a2"]


def test_non_json_record_reaches_exporter_without_evicting_backlog():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1"))
    scheduler.schedule(ARN_C, _rec(ARN_C, "c1"))

    non_json = _rec(ARN_D, "custom")
    non_json["payload"] = {"not-json"}
    scheduler.schedule(ARN_D, non_json)

    assert lane._pending_count() == 3
    exporter.release()
    scheduler.end_invocation(5.0)
    assert exporter.exported_values() == ["inflight", "b1", "c1", "custom"]


def test_individually_over_budget_record_does_not_evict_existing_backlog():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_bytes=3_500)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_B, _rec(ARN_B, "b" * 700))
    scheduler.schedule(ARN_C, _rec(ARN_C, "c" * 700))
    scheduler.schedule(ARN_D, _rec(ARN_D, "d" * 3_000))

    assert lane._pending_count() == 2
    assert lane._pending_bytes_count() <= 3_500
    exporter.release()
    scheduler.end_invocation(5.0)
    exported = exporter.exported_values()
    assert exported[0] == "inflight"
    assert exported[1:] == ["b" * 700, "c" * 700]


def test_over_budget_replacement_removes_superseded_same_arn_only():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_bytes=3_500)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)
    scheduler.schedule(ARN_A, _rec(ARN_A, "stale-running"))
    scheduler.schedule(ARN_B, _rec(ARN_B, "unrelated"))
    scheduler.schedule(
        ARN_A,
        _rec(ARN_A, "terminal" * 500, status="SUCCEEDED"),
    )

    assert lane._pending_count() == 1
    assert lane._pending_bytes_count() <= 3_500
    exporter.release()
    scheduler.end_invocation(5.0)
    assert exporter.exported_values() == ["inflight", "unrelated"]


def test_retained_size_traverses_slots_after_shallow_size_failure():
    for payload in (
        _SlottedPayload("x" * 4_000),
        _UnsizedSlottedPayload("x" * 4_000),
    ):
        exporter = BlockingExporter()
        scheduler = _ExportScheduler([exporter], max_pending_bytes=2_500)
        lane = scheduler._lanes[0]
        scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
        assert _wait_until(exporter.started.is_set)
        record = _rec(ARN_B, "opaque")
        record["payload"] = payload
        scheduler.schedule(ARN_B, record)

        assert lane._pending_count() == 0
        assert lane._pending_bytes_count() == 0
        exporter.release()
        scheduler.end_invocation(5.0)
        assert exporter.exported_values() == ["inflight"]


def test_retained_size_saturates_before_traversing_large_shallow_container():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_bytes=2_500)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)

    payload = _TrackedLargeList()
    record = _rec(ARN_B, "large-shallow")
    record["payload"] = payload
    scheduler.schedule(ARN_B, record)

    assert payload.iterated is False
    assert lane._pending_count() == 0
    assert lane._pending_bytes_count() == 0
    exporter.release()
    scheduler.end_invocation(5.0)


def test_retained_size_counts_memoryview_backing_buffer():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_bytes=2_500)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)

    record = _rec(ARN_B, "memoryview")
    record["payload"] = memoryview(bytearray(4_000))
    scheduler.schedule(ARN_B, record)

    assert lane._pending_count() == 0
    assert lane._pending_bytes_count() == 0
    exporter.release()
    scheduler.end_invocation(5.0)


def test_record_sizing_exception_does_not_escape_schedule():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)

    record = _rec(ARN_B, "custom-sized")
    record["payload"] = _Unsized()
    scheduler.schedule(ARN_B, record)

    assert scheduler._lanes[0]._pending_count() == 1
    exporter.release()
    scheduler.end_invocation(5.0)
    assert exporter.exported_values() == ["inflight", "custom-sized"]


def test_pending_record_cap_evicts_true_oldest_across_arns():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_records=2)
    scheduler.schedule(ARN_C, _rec(ARN_C, "inflight"))
    assert _wait_until(exporter.started.is_set)

    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    scheduler.schedule(ARN_B, _rec(ARN_B, "b1", status="SUCCEEDED"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "a2"))

    exporter.release()
    scheduler.end_invocation(5.0)
    # A1 is globally oldest. B1 remains even though scheduling A2 moved A's
    # fairness token behind B's token.
    assert exporter.exported_values() == ["inflight", "b1", "a2"]


def test_pending_byte_budget_evicts_oldest_large_record():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter], max_pending_bytes=3_000)
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)

    scheduler.schedule(ARN_B, _rec(ARN_B, "b" * 1_500))
    scheduler.schedule(ARN_C, _rec(ARN_C, "c" * 1_500))

    assert lane._pending_record_count() == 1
    assert lane._pending_bytes_count() <= 3_000
    exporter.release()
    scheduler.end_invocation(5.0)
    exported = exporter.exported_values()
    assert exported[0] == "inflight"
    assert exported[1] == "c" * 1_500


def test_cancelled_barrier_preserves_generation_order_and_terminal():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "inflight"))
    assert _wait_until(exporter.started.is_set)

    scheduler.schedule(ARN_A, _rec(ARN_A, "pre-1"))
    scheduler.schedule(ARN_A, _rec(ARN_A, "pre-2"))
    barrier = lane.enqueue_flush()
    scheduler.schedule(ARN_A, _rec(ARN_A, "terminal", status="SUCCEEDED"))
    lane.cancel_flush(barrier)
    lane.request_stop_when_idle()
    exporter.release()

    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.exported_values() == [
        "inflight",
        "pre-1",
        "pre-2",
        "terminal",
    ]
    assert lane._pending_record_count() == 0
    assert lane._queue_len() == 0


def test_timed_out_barrier_flushes_eventually_and_worker_exits():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    ok = scheduler.end_invocation(0.1)
    assert ok is False
    # The caller returns on time, but one detached flush remains queued so a
    # buffered exporter can publish before the worker exits idle.
    assert lane._queued_flush_count() == 1
    exporter.release()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.flushed == 1


def test_timed_out_buffered_export_is_published_eventually():
    exporter = BlockingBufferedExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "terminal", status="SUCCEEDED"))
    assert _wait_until(exporter.started.is_set)

    assert scheduler.end_invocation(0.1) is False
    assert exporter.published == []
    exporter.release()

    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.published == ["terminal"]
    assert exporter.flushed == 1


def test_repeated_timeouts_behind_blocked_exporter_stay_bounded():
    """Warm timeouts coalesce to one eventual flush on the same worker."""
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]

    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    worker = lane._worker
    assert worker is not None and worker.is_alive()

    for i in range(50):
        scheduler.schedule(ARN_A, _rec(ARN_A, f"a{i + 2}"))
        assert scheduler.end_invocation(0.02) is False
        # At most 16 generation tokens plus one detached eventual flush.
        assert lane._queued_flush_count() == 1
        assert lane._queue_len() <= 17
        assert lane._pending_record_count() <= 16

    assert lane._queue_len() <= 17
    assert lane._pending_count() <= 1
    assert lane._pending_record_count() <= 16
    assert lane._queued_flush_count() == 1
    assert lane._worker is worker
    assert worker.is_alive()
    assert _lane_worker_count(lane) == 1
    assert exporter.flushed == 0

    exporter.release()
    assert _wait_until(lambda: not lane._worker_alive())
    exported = exporter.exported_values()
    assert exported[0] == "a1"
    assert len(exported) <= 17
    assert exported[-1] == "a51"
    assert exporter.flushed == 1


def test_cancel_flush_replaces_queued_barrier_with_detached_flush():
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    barrier = lane.enqueue_flush()
    assert lane._queued_flush_count() == 1

    lane.cancel_flush(barrier)

    assert lane._queued_flush_count() == 1
    assert barrier.canceled is True
    assert barrier.is_done()
    exporter.release()
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.flushed == 1
    assert exporter.exported_values() == ["a1"]


def test_cancel_flush_after_pop_lets_worker_complete_barrier():
    """Already-popped race: the worker has taken the barrier and is mid-flush,
    so cancel_flush only marks it cancelled and leaves completion to the worker.
    The in-flight flush is not interrupted."""
    exporter = BlockingFlushExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    barrier = lane.enqueue_flush()
    # Worker exports a1, pops the barrier, and enters flush (now in flight).
    assert _wait_until(exporter.flush_started.is_set)
    assert lane._queued_flush_count() == 0  # already popped from the queue
    assert not barrier.is_done()  # worker still inside flush
    # Cancelling now must NOT complete it here (the worker owns completion) and
    # must NOT interrupt the in-flight flush.
    lane.cancel_flush(barrier)
    assert barrier.canceled is True
    assert not barrier.is_done()
    # Release the in-flight flush; the worker completes the barrier itself.
    exporter.release_flush()
    assert _wait_until(barrier.is_done)
    # The flush already in flight ran to completion exactly once (not killed).
    assert exporter.calls.count(("flush", None)) == 1
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
