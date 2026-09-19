# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from aws_durable_execution_sdk_python_insight._export_scheduler import (
    _MAX_CONSECUTIVE_WORKER_FAULTS,
    _ExportScheduler,
    _ExportState,
)


ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-{}/inv-1"
ARN_A = ARN.format("a")
ARN_B = ARN.format("b")


class _ArnScheduler(_ExportScheduler):
    """Supplies the ARN -> execution map that the SDK's plugin lifecycle provides.

    ``schedule()`` and ``drain()`` take the per-execution object: the scheduler
    holds the object the caller already has -- in production the caller's own
    per-invocation plugin instance, which carries its export bookkeeping as an
    ``_ExportState`` -- instead of resolving an ARN to bookkeeping of its own, so
    it does not know what an ARN is. These tests drive the scheduler directly, so
    they own an ARN map of their own and are otherwise unchanged.
    """

    def __init__(self, exporters: list[Any]) -> None:
        super().__init__(exporters)
        self.executions: dict[str, _ExportState] = {}
        self._executions_lock = threading.Lock()

    def _execution(self, execution_arn: str) -> _ExportState:
        with self._executions_lock:
            execution = self.executions.get(execution_arn)
            if execution is None:
                execution = _ExportState()
                self.executions[execution_arn] = execution
            return execution

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:  # type: ignore[override]
        super().schedule(self._execution(execution_arn), record)

    def drain(self, execution_arn: str) -> None:  # type: ignore[override]
        super().drain(self._execution(execution_arn))


def _record(value: str) -> dict[str, Any]:
    return {"status": "RUNNING", "value": value, "operations": []}


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


class CaptureExporter:
    max_record_size_bytes: int | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.calls.append(("export", record["value"]))

    def flush(self) -> None:
        self.calls.append(("flush", None))


class BlockingExporter(CaptureExporter):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def export(self, record: dict[str, Any]) -> None:
        self.started.set()
        self.release.wait(5.0)
        super().export(record)


class FailingExporter(CaptureExporter):
    def export(self, record: dict[str, Any]) -> None:
        raise RuntimeError("export failed")

    def flush(self) -> None:
        raise RuntimeError("flush failed")


class ExporterBaseException(BaseException):
    """Stands in for the BaseExceptions a customer exporter can raise."""


class BaseExceptionExporter(CaptureExporter):
    """Raises a ``BaseException`` out of its first ``export()`` call."""

    def __init__(self) -> None:
        super().__init__()
        self.exports = 0

    def export(self, record: dict[str, Any]) -> None:
        self.exports += 1
        if self.exports == 1:
            raise ExporterBaseException("export exploded")
        super().export(record)


class AlwaysBaseExceptionExportExporter(CaptureExporter):
    """Raises a ``BaseException`` out of every ``export()`` call, and counts them."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.export_attempts = 0

    def export(self, record: dict[str, Any]) -> None:
        with self.lock:
            self.export_attempts += 1
        raise ExporterBaseException("export exploded")

    def exports(self) -> int:
        with self.lock:
            return self.export_attempts


class AlwaysBaseExceptionFlushExporter(CaptureExporter):
    """Raises a ``BaseException`` out of every ``flush()`` call, and counts them."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.flush_attempts = 0

    def flush(self) -> None:
        with self.lock:
            self.flush_attempts += 1
        raise ExporterBaseException("flush exploded")

    def flushes(self) -> int:
        with self.lock:
            return self.flush_attempts


def _drain_off_thread(
    scheduler: _ArnScheduler, arn: str
) -> tuple[threading.Thread, threading.Event]:
    """Start drain() on its own thread and return it with its completion event.

    A regression that parks the drain would block whichever thread called it, so
    no test may call drain() on the thread it asserts from. Waiting on the event
    with a timeout turns such a regression into a failure instead of a hung run.
    """
    returned = threading.Event()

    def drain() -> None:
        scheduler.drain(arn)
        returned.set()

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    return thread, returned


def test_latest_pending_coalesces_within_one_execution() -> None:
    exporter = BlockingExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _record("first"))
    assert exporter.started.wait(5.0)

    start = time.monotonic()
    scheduler.schedule(ARN_A, _record("middle"))
    scheduler.schedule(ARN_A, _record("latest"))
    assert time.monotonic() - start < 0.5
    assert scheduler._pending_count() == 1

    exporter.release.set()
    scheduler.drain(ARN_A)
    assert exporter.calls == [
        ("export", "first"),
        ("export", "latest"),
        ("flush", None),
    ]
    assert _wait_until(lambda: not scheduler._worker_alive())


def test_exporter_failure_does_not_block_other_exporters() -> None:
    failing = FailingExporter()
    capture = CaptureExporter()
    scheduler = _ArnScheduler([failing, capture])

    scheduler.schedule(ARN_A, _record("terminal"))

    scheduler.drain(ARN_A)
    assert capture.calls == [("export", "terminal"), ("flush", None)]


def test_base_exception_from_export_still_releases_drain() -> None:
    # _export() contains every Exception, but a BaseException from a customer
    # exporter -- asyncio.CancelledError is one -- unwinds out of the worker
    # instead. The record has already been taken out of _pending by then and
    # nothing will re-export that snapshot, so the export has to count and the
    # worker slot has to be vacated anyway; otherwise the drain, and with it the
    # invocation thread, parks forever. Every wait here is bounded so a
    # regression fails instead of hanging the suite.
    exporter = BaseExceptionExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _record("terminal"))
    returned = threading.Event()

    def drain() -> None:
        scheduler.drain(ARN_A)
        returned.set()

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()

    assert returned.wait(10.0), "drain() never returned after export() raised"
    thread.join(5.0)
    assert not thread.is_alive()
    # The snapshot was consumed, not retried, and the drain still got its flush.
    assert exporter.exports == 1
    assert exporter.calls == [("flush", None)]
    assert _wait_until(lambda: not scheduler._worker_alive())


def test_always_failing_flush_releases_the_drain_without_a_respawn() -> None:
    # A drain is only released by a flush that COMPLETED, and a worker that dies
    # is replaced by whoever is waiting. An exporter whose flush() raises a
    # BaseException on every call therefore used to make the drain start a
    # replacement worker, which ran the same flush and died the same way, without
    # bound: the flush was attempted thousands of times per second, thousands of
    # threads were created, and the invocation parked on that drain never
    # returned. The flush has to be attempted once, the failure reported, and the
    # flush counted as completed so the drain returns.
    #
    # The wait is bounded, so the regression this pins fails the test rather than
    # blocking the run.
    exporter = AlwaysBaseExceptionFlushExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _record("terminal"))
    thread, returned = _drain_off_thread(scheduler, ARN_A)

    assert returned.wait(10.0), "drain() never returned while flush() kept failing"
    thread.join(5.0)
    assert not thread.is_alive()
    assert _wait_until(lambda: not scheduler._worker_alive())

    # One attempt, not one per replacement worker.
    assert exporter.flushes() == 1
    # The record still reached the exporter, and the failing flush was not retried
    # after the drain returned either.
    assert exporter.calls == [("export", "terminal")]
    assert exporter.flushes() == 1


def test_base_exception_from_one_exporter_never_skips_the_next() -> None:
    # Consuming a record from the pending slot is what advances the completion
    # bookkeeping, and nothing re-exports a consumed snapshot. A first exporter
    # that raised a BaseException used to abort the fan-out loop, so every
    # exporter after it missed that record permanently while the drain was
    # released as though the record had been delivered. Each exporter's failure
    # has to be contained at its own call, so the next exporter still receives the
    # record that the bookkeeping counts as offered.
    failing = AlwaysBaseExceptionExportExporter()
    healthy = CaptureExporter()
    scheduler = _ArnScheduler([failing, healthy])
    scheduler.schedule(ARN_A, _record("terminal"))
    thread, returned = _drain_off_thread(scheduler, ARN_A)

    assert returned.wait(10.0), "drain() never returned after export() raised"
    thread.join(5.0)
    assert not thread.is_alive()
    assert _wait_until(lambda: not scheduler._worker_alive())

    assert healthy.calls == [("export", "terminal"), ("flush", None)]
    # Offered once. The snapshot is gone from the pending slot, so a retry is not
    # available and must not be implied.
    assert failing.exports() == 1


def test_base_exception_from_one_exporters_flush_never_skips_the_next() -> None:
    # The same containment at the flush call. A first exporter whose flush()
    # raises a BaseException must not stop a later exporter from flushing, and the
    # flush must still count as completed so the waiting drain is released.
    failing = AlwaysBaseExceptionFlushExporter()
    healthy = CaptureExporter()
    scheduler = _ArnScheduler([failing, healthy])
    scheduler.schedule(ARN_A, _record("terminal"))
    thread, returned = _drain_off_thread(scheduler, ARN_A)

    assert returned.wait(10.0), "drain() never returned while flush() kept failing"
    thread.join(5.0)
    assert not thread.is_alive()
    assert _wait_until(lambda: not scheduler._worker_alive())

    assert healthy.calls == [("export", "terminal"), ("flush", None)]
    assert failing.flushes() == 1


class _FaultingScheduler(_ArnScheduler):
    """Kills every export worker with a ``BaseException`` before it does any work.

    Stands in for a fault in the scheduler's own code rather than in an exporter:
    exporter failures are contained at the exporter call, so they can no longer
    reach the worker's exit path, and this is the only way left to drive it.
    """

    def __init__(self, exporters: list[Any]) -> None:
        super().__init__(exporters)
        self.runs = 0

    def _run_loop(self) -> None:
        with self._condition:
            self.runs += 1
        raise ExporterBaseException("worker exploded")


def test_worker_deaths_stop_at_the_bound_instead_of_respawning_forever() -> None:
    # A waiting drain starts a replacement worker for every worker that dies, so a
    # fault the worker reproduces on every attempt is retried as fast as threads
    # can be created and the drain never returns. Releasing the waiter matters
    # more than delivering the records, because the waiter is an invocation
    # thread: bound the replacements, then latch asynchronous export off, which
    # wakes every waiter and drops what is queued.
    exporter = CaptureExporter()
    scheduler = _FaultingScheduler([exporter])
    scheduler.schedule(ARN_A, _record("terminal"))
    thread, returned = _drain_off_thread(scheduler, ARN_A)

    assert returned.wait(10.0), "drain() never returned while the worker kept dying"
    thread.join(5.0)
    assert not thread.is_alive()

    with scheduler._condition:
        assert scheduler.runs == _MAX_CONSECUTIVE_WORKER_FAULTS
        assert scheduler._worker_faults == _MAX_CONSECUTIVE_WORKER_FAULTS
        # The latch is what released the drain, and it retains nothing.
        assert scheduler._disabled
        assert scheduler._pending == {}
        assert scheduler._flush_requested is False
        assert scheduler._flush_in_flight is None
    # No further worker is started once the latch is set, so the count cannot
    # creep up after the drain returned.
    scheduler.schedule(ARN_B, _record("after-the-latch"))
    scheduler.drain(ARN_B)
    with scheduler._condition:
        assert scheduler.runs == _MAX_CONSECUTIVE_WORKER_FAULTS
    assert exporter.calls == []


def test_drain_flushes_after_export() -> None:
    capture = CaptureExporter()
    scheduler = _ArnScheduler([capture])

    scheduler.schedule(ARN_A, _record("terminal"))

    scheduler.drain(ARN_A)
    assert capture.calls == [("export", "terminal"), ("flush", None)]


def test_worker_start_failure_never_escapes_hook(monkeypatch) -> None:
    def fail_start(self) -> None:  # noqa: ARG001
        raise RuntimeError("cannot start")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    scheduler = _ArnScheduler([CaptureExporter()])

    scheduler.schedule(ARN_A, _record("dropped"))
    scheduler.drain(ARN_A)

    assert scheduler._pending_count() == 0


def test_superseded_record_finalizes_after_lane_unlock() -> None:
    scheduler = _ArnScheduler([BlockingExporter()])
    exporter = scheduler._exporters[0]
    assert isinstance(exporter, BlockingExporter)
    scheduler.schedule(ARN_A, _record("inflight"))
    assert exporter.started.wait(5.0)
    finalized = threading.Event()

    class ReentrantValue:
        def __del__(self) -> None:
            scheduler.schedule(ARN_A, _record("from-finalizer"))
            finalized.set()

    pending = _record("superseded")
    pending["payload"] = ReentrantValue()
    scheduler.schedule(ARN_A, pending)
    del pending

    scheduler.schedule(ARN_A, _record("replacement"))

    assert finalized.wait(5.0)
    exporter.release.set()
    scheduler.drain(ARN_A)


def test_drain_waits_for_blocked_exporter() -> None:
    exporter = BlockingExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _record("terminal"))
    assert exporter.started.wait(5.0)
    drain_thread = threading.Thread(target=scheduler.drain, args=(ARN_A,))

    drain_thread.start()
    assert _wait_until(drain_thread.is_alive)
    exporter.release.set()
    drain_thread.join(5.0)

    assert not drain_thread.is_alive()
    assert exporter.calls == [("export", "terminal"), ("flush", None)]


# -- concurrent executions in one environment (LMI) ---------------------------


class EventLogExporter:
    """Records ``(kind, executionArn, status)`` events from every thread."""

    max_record_size_bytes: int | None = None

    def __init__(self) -> None:
        self.events: list[tuple[str, str | None, str | None]] = []
        self.lock = threading.Lock()

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.log("export", record["executionArn"], record["status"])

    def flush(self) -> None:
        self.log("flush", None, None)

    def log(self, kind: str, arn: str | None, status: str | None) -> None:
        with self.lock:
            self.events.append((kind, arn, status))

    def snapshot(self) -> list[tuple[str, str | None, str | None]]:
        with self.lock:
            return list(self.events)


class GatedEventLogExporter(EventLogExporter):
    """Blocks inside the first ``export()`` until released."""

    def __init__(self) -> None:
        super().__init__()
        self.first_export_started = threading.Event()
        self.release = threading.Event()

    def export(self, record: dict[str, Any]) -> None:
        if not self.first_export_started.is_set():
            self.first_export_started.set()
            self.release.wait(10.0)
        super().export(record)


class FlushGateEventLogExporter(EventLogExporter):
    """Logs flush begin/end and blocks inside the first ``flush()`` until released."""

    def __init__(self) -> None:
        super().__init__()
        self.first_flush_started = threading.Event()
        self.release_flush = threading.Event()
        self.flush_count = 0

    def flush(self) -> None:
        with self.lock:
            self.flush_count += 1
            first = self.flush_count == 1
        self.log("flush_begin", None, None)
        if first:
            self.first_flush_started.set()
            self.release_flush.wait(10.0)
        self.log("flush_end", None, None)

    def flushes(self) -> int:
        with self.lock:
            return self.flush_count


def _execution_record(arn: str, status: str) -> dict[str, Any]:
    return {"executionArn": arn, "status": status, "operations": []}


def _scheduler_is_empty(scheduler: _ExportScheduler) -> bool:
    with scheduler._condition:
        return not scheduler._pending


def _drain_waiters(scheduler: _ArnScheduler, arn: str) -> int:
    """How many drain() calls are currently parked on this execution."""
    with scheduler._condition:
        return scheduler._execution(arn)._waiters


def test_drain_stays_parked_until_a_flush_covering_its_record_completes() -> None:
    # The guarantee the per-execution scheduler exists for: drain() returns only
    # after the calling execution's own record reached the exporters AND a flush
    # that started after that export has itself completed. Gating the exporter
    # inside flush() makes that deterministic -- while the gate is held the flush
    # provably cannot have completed, so a drain that returns is a violation.
    exporter = FlushGateEventLogExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _execution_record(ARN_A, "SUCCEEDED"))
    returned = threading.Event()

    def drain() -> None:
        scheduler.drain(ARN_A)
        exporter.log("drain", ARN_A, None)
        returned.set()

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    assert exporter.first_flush_started.wait(5.0)
    # Its own record went to the exporters before this flush was even started.
    assert ("export", ARN_A, "SUCCEEDED") in exporter.snapshot()
    assert not returned.wait(0.25), "drain returned while its flush was still running"
    assert thread.is_alive()

    exporter.release_flush.set()
    thread.join(5.0)
    assert not thread.is_alive()

    events = exporter.snapshot()
    exported = events.index(("export", ARN_A, "SUCCEEDED"))
    flush_begin = events.index(("flush_begin", None, None))
    flush_end = events.index(("flush_end", None, None))
    drained = events.index(("drain", ARN_A, None))
    assert exported < flush_begin < flush_end < drained


def test_no_redundant_flush_runs_after_drain_returned() -> None:
    # A drain woken while a flush is in flight must recognise that the in-flight
    # flush already covers it. Otherwise it re-requests one (its own request has
    # been consumed and the coverage is not published yet) and that second flush
    # calls the exporters after drain(), and with it the invocation, returned.
    exporter = FlushGateEventLogExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _execution_record(ARN_A, "SUCCEEDED"))
    returned = threading.Event()
    flushes_at_return: list[int] = []

    def drain() -> None:
        scheduler.drain(ARN_A)
        flushes_at_return.append(exporter.flushes())
        returned.set()

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    # The worker is inside the flush it committed to: it has consumed the drain's
    # request and has not published the flush's coverage yet.
    assert exporter.first_flush_started.wait(5.0)
    # Wake the parked drain exactly inside that window, which is the interleaving
    # a loaded environment produces by itself.
    for _ in range(3):
        with scheduler._condition:
            scheduler._condition.notify_all()
        time.sleep(0.01)
    assert not returned.is_set()

    exporter.release_flush.set()
    thread.join(5.0)
    assert not thread.is_alive()
    # The worker only retires once nothing is pending and no flush is requested,
    # so this settles the question without sleeping for a late flush.
    assert _wait_until(lambda: not scheduler._worker_alive())

    assert flushes_at_return == [1]
    assert exporter.flushes() == 1, "a second flush ran after drain() returned"


def test_drain_with_nothing_to_export_still_flushes_exactly_once() -> None:
    # A drain for an execution that scheduled no record -- the invocation end
    # emitted nothing -- has `need` == 0: no export has to be covered to release
    # it. `_flush_in_flight` uses 0 for "no flush is running", so a naive
    # `_flush_in_flight >= need` reads as "a flush already covers me" exactly
    # when nothing is running at all; the drain would skip its request and park
    # until some other execution happened to flush. It has to flush once and
    # return.
    capture = CaptureExporter()
    scheduler = _ArnScheduler([capture])
    returned = threading.Event()

    def drain() -> None:
        scheduler.drain(ARN_A)
        returned.set()

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    assert returned.wait(10.0), "drain() parked with nothing of its own to export"
    thread.join(5.0)
    assert not thread.is_alive()
    assert capture.calls == [("flush", None)]
    # Nothing arrives after it returned either.
    assert _wait_until(lambda: not scheduler._worker_alive())
    assert capture.calls == [("flush", None)]


class GatedFlushExporter(CaptureExporter):
    """Holds each ``flush()`` open until the test releases it, and counts them."""

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self.flush_count = 0
        self.started: dict[int, threading.Event] = {}
        self.release: dict[int, threading.Event] = {}
        for index in (1, 2):
            self.started[index] = threading.Event()
            self.release[index] = threading.Event()

    def flush(self) -> None:
        with self._lock:
            self.flush_count += 1
            index = self.flush_count
        if index in self.started:
            self.started[index].set()
            self.release[index].wait(10.0)
        super().flush()


def test_concurrent_drains_with_nothing_to_export_share_one_flush() -> None:
    # Two invocations that emitted no record drain at the same time. Neither has
    # an export to be covered, so both need a flush that covers zero exports.
    # While `_flush_in_flight` used 0 for "no flush is running", the second drain
    # could not tell that the flush it needs was already running, and asked for
    # another. The completion of the first flush then released both drains, and
    # the second flush ran after both invocations had already returned -- customer
    # exporter code running past the invocation boundary, which is what the flush
    # contract forbids. One flush must serve both, and whichever drain a flush
    # belongs to must stay parked until that flush completes.
    exporter = GatedFlushExporter()
    scheduler = _ArnScheduler([exporter])
    returned: list[str] = []
    returned_lock = threading.Lock()

    def drain(name: str, execution_arn: str) -> None:
        scheduler.drain(execution_arn)
        with returned_lock:
            returned.append(name)

    threads = [
        threading.Thread(target=drain, args=("first", ARN_A), daemon=True),
        threading.Thread(target=drain, args=("second", ARN_B), daemon=True),
    ]
    try:
        threads[0].start()
        assert exporter.started[1].wait(10.0), "the first drain never flushed"
        threads[1].start()

        first = scheduler.executions[ARN_A]

        def both_parked() -> bool:
            second = scheduler.executions.get(ARN_B)
            if second is None:
                return False
            with scheduler._condition:
                return first._waiters == 1 and second._waiters == 1

        assert _wait_until(both_parked), "a drain raced past the flush it needs"
        with returned_lock:
            assert returned == [], "a drain returned before its flush completed"

        exporter.release[1].set()
        for thread in threads:
            thread.join(10.0)
        assert not any(thread.is_alive() for thread in threads)
        with returned_lock:
            assert sorted(returned) == ["first", "second"]

        # The redundant request, if one was made, was recorded before either
        # drain returned, so the worker starts that flush without further
        # prompting. Nothing arriving here is what proves no second flush was
        # requested.
        assert not exporter.started[2].wait(0.75), (
            "a second flush ran after both invocations had returned"
        )
        assert exporter.flush_count == 1
    finally:
        exporter.release[1].set()
        exporter.release[2].set()
        _wait_until(lambda: not scheduler._worker_alive())


def test_drain_never_rides_on_a_flush_that_finished_before_it_started() -> None:
    # Export coverage alone would let the second drain return immediately: every
    # export is already covered by the first drain's flush. A drain must wait for
    # a flush that completed after it was called, so that one invocation end
    # means one flush.
    capture = CaptureExporter()
    scheduler = _ArnScheduler([capture])
    scheduler.schedule(ARN_A, _record("terminal"))

    scheduler.drain(ARN_A)
    scheduler.drain(ARN_A)

    assert capture.calls == [
        ("export", "terminal"),
        ("flush", None),
        ("flush", None),
    ]


def test_disabled_latch_retains_no_lanes_or_pending_records(monkeypatch) -> None:
    # The _disabled latch is permanent: nothing will ever be exported again, so
    # the scheduler must not hold on to records or per-execution bookkeeping for
    # the remaining life of the environment.
    def fail_start(self) -> None:  # noqa: ARG001
        raise RuntimeError("cannot start")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    scheduler = _ArnScheduler([CaptureExporter()])

    scheduler.schedule(ARN_A, _record("dropped"))
    scheduler.drain(ARN_A)
    scheduler.schedule(ARN_B, _record("also-dropped"))
    scheduler.drain(ARN_B)

    with scheduler._condition:
        assert scheduler._disabled
        assert scheduler._pending == {}
        # ...and no execution kept the record it was carrying. The per-execution
        # bookkeeping that used to need clearing in a second map is on these
        # objects now, so the queue and the records are one thing to release.
        assert all(
            execution._pending_record is None
            for execution in scheduler.executions.values()
        )
        assert scheduler._flush_requested is False
        assert scheduler._flush_in_flight is None


def test_disabled_latch_clears_a_published_flush_in_flight_marker(monkeypatch) -> None:
    # `_flush_in_flight` is the coverage of the flush the worker is running right
    # now, published so a waiter woken during that flush can tell it is already
    # covered and skip requesting another. None means "no flush is running", so
    # any integer -- 0 included -- is a claim that a flush is in flight and will
    # complete.
    #
    # The _disabled latch makes that claim permanently false: no worker exists and
    # none will ever be started again, so the published flush can never complete.
    # The latch therefore has to retire the marker along with the pending records
    # and the lanes. test_disabled_latch_retains_no_lanes_or_pending_records
    # asserts the same field, but reaches the latch with the marker already at 0,
    # so it holds whether or not the latch clears it; this one arms the marker
    # first.
    scheduler = _ArnScheduler([CaptureExporter()])
    with scheduler._condition:
        scheduler._flush_in_flight = 7

    def fail_start(self) -> None:  # noqa: ARG001
        raise RuntimeError("cannot start")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    scheduler.schedule(ARN_A, _record("dropped"))

    with scheduler._condition:
        assert scheduler._disabled
        assert scheduler._flush_in_flight is None, (
            "the _disabled latch left a flush-in-flight marker behind for a flush "
            "that can never run"
        )


def test_every_concurrent_execution_delivers_its_terminal_record_once() -> None:
    # One plugin instance (one scheduler) serves every execution the environment
    # hosts. Ten executions running at once must each land their terminal record
    # exactly once: a pending record keyed per execution is never displaced by a
    # different execution's record.
    executions = 10
    exporter = EventLogExporter()
    scheduler = _ArnScheduler([exporter])
    arns = [ARN.format(index) for index in range(executions)]
    ready = threading.Barrier(executions)

    def run(arn: str) -> None:
        ready.wait(10.0)
        scheduler.schedule(arn, _execution_record(arn, "RUNNING"))
        scheduler.schedule(arn, _execution_record(arn, "SUCCEEDED"))
        scheduler.drain(arn)

    threads = [threading.Thread(target=run, args=(arn,)) for arn in arns]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30.0)
    assert not any(thread.is_alive() for thread in threads)

    terminal = [
        arn
        for kind, arn, status in exporter.snapshot()
        if kind == "export" and status == "SUCCEEDED" and arn is not None
    ]
    assert sorted(terminal) == sorted(arns)  # each exactly once, none lost
    # Nothing per-execution is retained once every execution has drained.
    assert _wait_until(lambda: _scheduler_is_empty(scheduler))


def test_blocked_export_never_loses_another_executions_terminal_record() -> None:
    # The worker is busy exporting when two executions queue their terminal
    # records. Neither may be dropped, and each drain must be released by its own
    # record reaching the exporters -- not by another execution's flush.
    exporter = GatedEventLogExporter()
    scheduler = _ArnScheduler([exporter])
    scheduler.schedule(ARN_A, _execution_record(ARN_A, "RUNNING"))
    assert exporter.first_export_started.wait(5.0)

    scheduler.schedule(ARN_A, _execution_record(ARN_A, "SUCCEEDED"))
    scheduler.schedule(ARN_B, _execution_record(ARN_B, "SUCCEEDED"))
    returned: dict[str, bool] = {}

    def drain(arn: str) -> None:
        scheduler.drain(arn)
        exporter.log("drain", arn, None)
        returned[arn] = True

    drains = [threading.Thread(target=drain, args=(arn,)) for arn in (ARN_A, ARN_B)]
    for thread in drains:
        thread.start()
    # Both drains really parked: each registered on its own lane (which drain()
    # only does from inside its wait loop) and neither has returned while the
    # exporter still holds the worker inside the very first export.
    assert _wait_until(
        lambda: (
            _drain_waiters(scheduler, ARN_A) == 1
            and _drain_waiters(scheduler, ARN_B) == 1
        )
    )
    assert returned == {}
    assert all(thread.is_alive() for thread in drains)

    exporter.release.set()
    for thread in drains:
        thread.join(10.0)
    assert not any(thread.is_alive() for thread in drains)
    assert returned == {ARN_A: True, ARN_B: True}

    events = exporter.snapshot()
    assert ("export", ARN_A, "SUCCEEDED") in events
    assert ("export", ARN_B, "SUCCEEDED") in events
    for arn in (ARN_A, ARN_B):
        exported = events.index(("export", arn, "SUCCEEDED"))
        returned_at = events.index(("drain", arn, None))
        # Each drain returned only after its own record was exported and a flush
        # covering it completed.
        assert exported < returned_at
        assert ("flush", None, None) in events[exported:returned_at]
    assert _wait_until(lambda: _scheduler_is_empty(scheduler))


class ReentrantDrainExporter(CaptureExporter):
    """Exporter that queues a record and drains from inside export().

    This is what an exporter re-entering a plugin hook produces: the hook emits
    its record and then, at an invocation end, drains. Both arrive on the export
    worker, which is the one thread able to serve the wait.
    """

    def __init__(self) -> None:
        super().__init__()
        self.scheduler: _ArnScheduler | None = None
        self.returned = threading.Event()
        self._reentered = False

    def export(self, record: dict[str, Any]) -> None:
        super().export(record)
        assert self.scheduler is not None
        if self._reentered:
            return
        self._reentered = True
        self.scheduler.schedule(ARN_B, _record("r2"))
        self.scheduler.drain(ARN_B)
        self.returned.set()


def test_drain_from_the_export_worker_is_refused_rather_than_deadlocking(
    caplog,
) -> None:
    """A drain on the worker thread returns instead of parking it.

    Only the export worker exports records and completes flushes. A drain made on
    that thread would wait for work only that thread can do, so the wait never
    ends and the invocation hangs until Lambda times it out. The call is refused
    and reported, and the worker goes back to its loop.
    """
    exporter = ReentrantDrainExporter()
    scheduler = _ArnScheduler([exporter])
    exporter.scheduler = scheduler

    with caplog.at_level(logging.WARNING):
        scheduler.schedule(ARN_A, _record("r1"))

        assert exporter.returned.wait(timeout=10), "the refused drain must return"
        assert _wait_until(lambda: "refused rather than deadlocking" in caplog.text)

    assert ("export", "r1") in exporter.calls
    # The refused drain still leaves a flush behind, with no external drain to ask
    # for one. The re-entering hook queued a record, and the worker exits once
    # nothing is pending and no flush is requested, so without the request a
    # buffering exporter would be holding that record when the environment froze.
    assert _wait_until(lambda: ("export", "r2") in exporter.calls)
    assert _wait_until(
        lambda: exporter.calls.index(("flush", None))
        > exporter.calls.index(("export", "r2"))
    ), "a refused drain must request a flush that covers the record it queued"
    # The worker is still serving: a drain from any other thread completes.
    scheduler.drain(ARN_B)


class ReentrantFlushExporter(CaptureExporter):
    """Exporter whose flush() drains, as a hook re-entered from a flush would.

    An exporter that re-enters a plugin hook from ``flush()`` reaches
    ``on_invocation_end``, which drains. The drain arrives on the export worker
    from inside a flush, with nothing pending.
    """

    def __init__(self) -> None:
        super().__init__()
        self.scheduler: _ArnScheduler | None = None
        self.flushes = 0

    def flush(self) -> None:
        super().flush()
        self.flushes += 1
        assert self.scheduler is not None
        self.scheduler.drain(ARN_A)


def test_a_drain_refused_from_inside_a_flush_does_not_re_arm_it() -> None:
    """A refused drain with nothing pending asks for no further flush.

    Requesting one unconditionally would keep the worker flushing for as long as
    the environment lived: the flush re-enters the hook, the hook drains, the
    refused drain asks for the next flush. A pending record is what distinguishes
    new work from that loop, so a drain refused from inside a flush leaves no
    request behind.
    """
    exporter = ReentrantFlushExporter()
    scheduler = _ArnScheduler([exporter])
    exporter.scheduler = scheduler

    scheduler.schedule(ARN_A, _record("r1"))
    scheduler.drain(ARN_A)

    flushes_after_drain = exporter.flushes
    assert flushes_after_drain >= 1, "the drain must have flushed"

    # Give a re-armed flush time to appear. The worker retires when nothing is
    # pending and no flush is requested, so a bounded count here is the whole
    # assertion: an unconditional request never settles.
    assert not _wait_until(lambda: exporter.flushes > flushes_after_drain, timeout=1.0)
    assert _wait_until(lambda: not scheduler._worker_alive())


class ReentrantDrainDuringExportExporter(CaptureExporter):
    """Exporter whose export() drains without queueing anything new.

    The worker takes a record out of the pending map before it calls the
    exporter, so a hook re-entered from inside ``export()`` and reaching an
    invocation end that emits no record finds nothing pending -- while the record
    it was just handed is still only in the exporter's buffer.
    """

    def __init__(self) -> None:
        super().__init__()
        self.scheduler: _ArnScheduler | None = None
        self.returned = threading.Event()
        self._reentered = False

    def export(self, record: dict[str, Any]) -> None:
        super().export(record)
        assert self.scheduler is not None
        if self._reentered:
            return
        self._reentered = True
        self.scheduler.drain(ARN_B)
        self.returned.set()


def test_a_drain_refused_from_inside_an_export_still_flushes() -> None:
    """A refused drain from inside an export asks for a flush.

    The record it must cover has already left the pending map, so a
    pending-only condition would leave that snapshot in a buffering exporter
    when the environment froze -- which is the loss the refusal path exists to
    prevent, arriving by the other door.
    """
    exporter = ReentrantDrainDuringExportExporter()
    scheduler = _ArnScheduler([exporter])
    exporter.scheduler = scheduler

    scheduler.schedule(ARN_A, _record("r1"))

    assert exporter.returned.wait(timeout=10), "the refused drain must return"
    assert _wait_until(lambda: ("flush", None) in exporter.calls), (
        "the exported record must be flushed even though nothing was pending"
    )
    assert ("export", "r1") in exporter.calls
    assert _wait_until(lambda: not scheduler._worker_alive())
