# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import threading
import time
from typing import Any

from aws_durable_execution_sdk_python_insight._export_scheduler import (
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
        return scheduler._execution(arn).waiters


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
                return first.waiters == 1 and second.waiters == 1

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
            execution.pending_record is None
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
