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

import logging
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


class _Uncopyable:
    """A payload whose ``deepcopy`` raises, to force a per-record copy failure."""

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        raise RuntimeError("uncopyable payload")


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


# -- copy failure isolation ---------------------------------------------------


def test_deepcopy_failure_skips_record_and_lane_continues(caplog):
    exporter = RecordingExporter()
    scheduler = _ExportScheduler([exporter])
    # A record whose deepcopy raises must be skipped for this lane -- never
    # exported by aliasing the shared object -- and the lane must keep draining.
    bad = _rec(ARN_A, "bad")
    bad["payload"] = _Uncopyable()
    good = _rec(ARN_B, "good")
    with caplog.at_level(
        logging.WARNING, logger="aws_durable_execution_sdk_python_insight"
    ):
        scheduler.schedule(ARN_A, bad)  # queued first: copy fails -> skipped
        scheduler.schedule(ARN_B, good)  # queued behind it: must still export
        # The good record delivering proves the lane continued past the failure;
        # a single-lane worker drains FIFO, so "bad" was processed (and skipped)
        # before "good" ran.
        assert _wait_until(lambda: exporter.exported_values() == ["good"])
        scheduler.end_invocation(5.0)
    # The exporter was never called for the un-copyable record.
    assert exporter.exported_values() == ["good"]
    # The failure was logged through the module logger.
    assert any(
        "record copy failed" in record.getMessage()
        for record in caplog.records
        if record.name == "aws_durable_execution_sdk_python_insight"
    )


def test_deepcopy_failure_does_not_alias_shared_record():
    # Before the fix a copy failure aliased the shared record and passed it to
    # truncate_record -> render, which could mutate the canonical object other
    # lanes still read. With the fix the record is skipped before render, so it
    # is never aliased or mutated in place.
    class MutatingRenderExporter(RecordingExporter):
        def render(self, record: dict[str, Any]) -> Any:
            record["mutated"] = True  # would corrupt an aliased shared record
            return record

    exporter = MutatingRenderExporter()
    scheduler = _ExportScheduler([exporter])
    bad = _rec(ARN_A, "bad")
    bad["payload"] = _Uncopyable()
    scheduler.schedule(ARN_A, bad)
    # A good record behind it lets us deterministically wait for the lane to
    # drain past the bad one (single lane drains FIFO).
    scheduler.schedule(ARN_B, _rec(ARN_B, "good"))
    assert _wait_until(lambda: exporter.exported_values() == ["good"])
    scheduler.end_invocation(5.0)
    # render never ran on the un-copyable record, so the canonical object was
    # neither aliased into export nor mutated in place.
    assert "mutated" not in bad
    assert exporter.exported_values() == ["good"]


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


def test_repeated_timeouts_behind_blocked_exporter_stay_bounded():
    """A blocked exporter across many warm invocations must not accumulate
    barriers or grow queue state, must keep the SAME worker (no replacement),
    must not execute any cancelled flush, and must drain + exit after release.
    """
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]

    # First record puts the single worker into a blocked export.
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)
    worker = lane._worker
    assert worker is not None and worker.is_alive()

    # Many warm invocations. Each schedules a coalescing record for the same
    # ARN then ends with a short timeout; the barrier always times out because
    # the worker is still stuck in the first export.
    for i in range(50):
        scheduler.schedule(ARN_A, _rec(ARN_A, f"a{i + 2}"))
        ok = scheduler.end_invocation(0.02)
        assert ok is False  # degraded every time -- worker is blocked
        # The cancelled barrier is pulled from the queue immediately, so no
        # _FLUSH marker lingers behind the blocked worker.
        assert lane._queued_flush_count() == 0
        # Queue holds at most the single coalesced record token; it never grows.
        assert lane._queue_len() <= 1

    # Bounded state: one in-flight ARN coalesced to a single pending record, and
    # no growing pile of barriers.
    assert lane._queue_len() <= 1
    assert lane._pending_count() <= 1
    assert lane._queued_flush_count() == 0
    # The blocked worker was never replaced.
    assert lane._worker is worker
    assert worker.is_alive()
    assert _lane_worker_count(lane) == 1
    # No cancelled flush ran while the worker was blocked.
    assert exporter.flushed == 0

    # Release: the worker drains the latest coalesced record, then exits idle.
    exporter.release()
    assert _wait_until(lambda: not lane._worker_alive())
    exported = exporter.exported_values()
    assert exported[0] == "a1"  # the in-flight record delivered first
    assert len(exported) <= 2  # a1 plus at most one final coalesced record
    # Cancelled barriers never triggered a flush, and the idle-stop path does
    # not flush either.
    assert exporter.flushed == 0


def test_cancel_flush_removes_queued_barrier_immediately():
    """Queued-barrier race: while the worker is blocked the barrier is still in
    the queue, so cancel_flush pulls it out and completes it synchronously --
    without waiting for the worker and without ever flushing."""
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    lane = scheduler._lanes[0]
    scheduler.schedule(ARN_A, _rec(ARN_A, "a1"))
    assert _wait_until(exporter.started.is_set)  # worker blocked in export
    barrier = lane.enqueue_flush()
    assert lane._queued_flush_count() == 1
    lane.cancel_flush(barrier)
    # Removed from the queue and completed here, without the worker.
    assert lane._queued_flush_count() == 0
    assert barrier.canceled is True
    assert barrier.is_done()
    # Finish the in-flight export and go idle; the pulled barrier never flushed.
    exporter.release()
    lane.request_stop_when_idle()
    assert _wait_until(lambda: not lane._worker_alive())
    assert exporter.flushed == 0
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
