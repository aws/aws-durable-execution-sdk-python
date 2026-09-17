# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Per-execution latest-pending asynchronous export scheduling for Workflow Insight.

One plugin instance serves every execution its environment hosts, and Lambda
Managed Instances makes concurrent executions in one environment routine, so the
pending record is keyed by execution ARN: coalescing happens only within a single
execution and one execution's record can never displace another's.

Export itself stays strictly serialized -- one worker thread, one ``export()`` at
a time -- so exporters never see concurrent calls. Parallel export is a later
phase and a contract change.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from aws_durable_execution_sdk_python_insight.truncation import truncate_record
from aws_durable_execution_sdk_python_insight.types import InsightExporter


_logger = logging.getLogger("aws_durable_execution_sdk_python_insight")


class _Lane:
    """Per-execution export bookkeeping. One lane per execution ARN."""

    __slots__ = ("scheduled_seq", "exported_seq", "exported_at", "waiters")

    def __init__(self) -> None:
        # Newest sequence number scheduled for this execution.
        self.scheduled_seq = 0
        # Newest sequence number already handed to every exporter.
        self.exported_seq = 0
        # Value of the scheduler's export counter when that export finished, so
        # a waiter can tell whether a completed flush covered its own record.
        self.exported_at = 0
        # drain() calls currently blocked on this lane; the lane is only
        # forgotten once nobody is waiting on it.
        self.waiters = 0


class _ExportScheduler:
    """Run all exporters on one lazy worker, keeping the latest record per execution."""

    def __init__(self, exporters: list[InsightExporter]) -> None:
        self._exporters = exporters
        self._condition = threading.Condition(threading.Lock())
        # execution ARN -> (sequence, latest record), oldest arrival first. A
        # repeat schedule for an ARN replaces the value and keeps the position,
        # so coalescing never lets one execution jump the queue.
        self._pending: dict[str, tuple[int, dict[str, Any]]] = {}
        self._lanes: dict[str, _Lane] = {}
        self._seq = 0
        self._export_count = 0
        # Highest export counter value covered by a completed flush.
        self._flushed_through = 0
        # Completed flushes, monotonic. Export coverage alone cannot express
        # "a flush ran for this invocation end": a drain with nothing of its own
        # to export -- an invocation end that emitted no record -- is trivially
        # covered by an older flush, so it would return without flushing at all.
        # JS and Java flush once per sampled-in invocation end whether or not a
        # record was emitted, so a drain also requires a flush that COMPLETED
        # AFTER it was called. Concurrent drains still share one flush: they all
        # entered before it completed.
        self._flushes_completed = 0
        self._flush_requested = False
        # Export counter coverage of the flush the worker is running right now, or
        # 0 when no flush is in flight. Published when the worker commits to a
        # flush, so a waiter woken while that flush runs -- before its coverage
        # reaches _flushed_through -- can tell it is already covered instead of
        # requesting a second flush that would run after its drain returned.
        self._flush_in_flight = 0
        # Value of the global schedule counter (_seq) when a flush was requested.
        # The worker defers the flush until no record scheduled at or before that
        # point is still pending. That is deliberately wider than the requester's
        # own record: a drain therefore also waits for records other executions
        # had pending when it was called. It excludes records scheduled after the
        # request, so a steady stream of other executions cannot starve a waiting
        # drain.
        self._flush_barrier = 0
        self._worker: threading.Thread | None = None
        self._disabled = False

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:
        """Replace this execution's pending snapshot; never runs exporters inline."""
        displaced: tuple[int, dict[str, Any]] | None = None
        failed_pending: dict[str, tuple[int, dict[str, Any]]] | None = None
        start_error: Exception | None = None
        with self._condition:
            if self._disabled:
                return
            self._seq += 1
            lane = self._lane_locked(execution_arn)
            lane.scheduled_seq = self._seq
            displaced = self._pending.get(execution_arn)
            self._pending[execution_arn] = (self._seq, record)
            failed_pending, start_error = self._ensure_worker_locked()
            self._condition.notify_all()
        # Releasing either record may run custom finalizers, so do it unlocked.
        del displaced, failed_pending
        if start_error is not None:
            _logger.warning(
                "workflow-insight: could not start export worker; disabling "
                "asynchronous export: %s",
                start_error,
            )

    def drain(self, execution_arn: str) -> None:
        """Wait until this execution's latest record is exported and exporters flush.

        Returns once the calling execution's own record has reached every exporter
        and a flush covering it has completed; a flush triggered by another
        execution never releases a waiter whose record is still pending.

        Every call waits for a flush that completed after the call started, so an
        invocation end that emitted no record still flushes -- the cadence JS and
        Java have. Concurrent calls can share one flush, since they all started
        before it completed.

        Two paths return without exporting or flushing anything, because the
        permanent ``_disabled`` latch means no record will ever be exported: the
        latch was already set when this call started, or it is set while this call
        is parked. Failing to start the export worker sets that latch, so a drain
        that hits a worker-start failure also returns without a flush.
        """
        failed_pending: dict[str, tuple[int, dict[str, Any]]] | None = None
        start_error: Exception | None = None
        with self._condition:
            if self._disabled:
                return
            lane = self._lane_locked(execution_arn)
            lane.waiters += 1
            try:
                want_seq = lane.scheduled_seq
                # A drain always flushes, so require a flush that covers every
                # export completed before this call as well as our own.
                want_flush = self._export_count
                # ...and one that completed after this call, so an invocation end
                # that emitted nothing still flushes exactly once instead of
                # riding on a flush that finished before it started.
                want_flushes = self._flushes_completed
                while not self._disabled:
                    # Export counter value a flush has to cover to release us:
                    # our own record's export plus everything already exported
                    # when this call started. Recomputed every pass, because
                    # lane.exported_at only becomes ours once our record is out.
                    need = max(lane.exported_at, want_flush)
                    if (
                        lane.exported_seq >= want_seq
                        and self._flushed_through >= need
                        and self._flushes_completed > want_flushes
                    ):
                        break
                    # A flush already in flight whose coverage reaches `need` was
                    # committed after our record was handed to the exporters, so
                    # its completion releases us. Requesting another one here --
                    # which is what a waiter woken inside that flush would do,
                    # since the coverage is not published yet and the request it
                    # made has already been consumed -- runs an extra flush after
                    # this drain, and the invocation, returned.
                    #
                    # `_flush_in_flight` uses 0 as its "no flush is running"
                    # sentinel, so the naive `self._flush_in_flight >= need`
                    # reads as "already covered" when `need` is 0 -- precisely
                    # when nothing is running at all. `need` is 0 for a drain
                    # whose invocation emitted no record, so that form would let
                    # such a drain skip its request and park until some other
                    # execution happened to flush. Require a marker that is
                    # actually set AND that reaches `need`.
                    covered = 0 < self._flush_in_flight >= need
                    if not self._flush_requested and not covered:
                        self._flush_requested = True
                        self._flush_barrier = max(self._flush_barrier, self._seq)
                    failed_pending, start_error = self._ensure_worker_locked()
                    if start_error is not None:
                        break
                    self._condition.notify_all()
                    self._condition.wait()
            finally:
                lane.waiters -= 1
                self._forget_lane_locked(execution_arn, lane)
        del failed_pending
        if start_error is not None:
            _logger.warning(
                "workflow-insight: could not start export worker; disabling "
                "asynchronous export: %s",
                start_error,
            )

    # -- internals ------------------------------------------------------------

    def _lane_locked(self, execution_arn: str) -> _Lane:
        lane = self._lanes.get(execution_arn)
        if lane is None:
            lane = _Lane()
            self._lanes[execution_arn] = lane
        return lane

    def _forget_lane_locked(self, execution_arn: str, lane: _Lane) -> None:
        # Keep the lane while anything still depends on it; bookkeeping for a
        # fully exported execution with no waiters is safe to drop, because a
        # later drain then only needs a flush covering the exports so far.
        if self._lanes.get(execution_arn) is not lane:
            return
        if lane.waiters:
            return
        if execution_arn in self._pending:
            return
        if lane.exported_seq < lane.scheduled_seq:
            return
        del self._lanes[execution_arn]

    def _ensure_worker_locked(
        self,
    ) -> tuple[dict[str, tuple[int, dict[str, Any]]] | None, Exception | None]:
        if self._worker is not None and self._worker.is_alive():
            return None, None
        worker = threading.Thread(
            target=self._run,
            name=f"workflow-insight-export-{id(self)}",
            daemon=True,
        )
        self._worker = worker
        try:
            worker.start()
        except Exception as exc:  # noqa: BLE001 - instrumentation must not escape hooks
            self._disabled = True
            self._worker = None
            failed_pending = self._pending
            self._pending = {}
            # Lanes hold plain counters, never customer objects, so they can be
            # dropped under the lock. Nothing is retained once the plugin has
            # given up on asynchronous export for good.
            self._lanes = {}
            self._flush_requested = False
            self._flush_barrier = 0
            self._flush_in_flight = 0
            # Release every waiter; the permanent disable latch means no record
            # will ever be exported.
            self._condition.notify_all()
            return failed_pending, exc
        return None, None

    def _blocking_pending_locked(self) -> bool:
        """True while a record scheduled at or before the flush barrier is pending."""
        barrier = self._flush_barrier
        return any(seq <= barrier for seq, _ in self._pending.values())

    def _run(self) -> None:
        # The worker slot must be empty whenever no worker is running, or
        # _ensure_worker_locked() never starts a replacement and every later
        # record sits pending forever. The loop's own exits clear it, but a
        # BaseException from a customer exporter -- asyncio.CancelledError is one,
        # so an exporter that merely touches asyncio can raise it without writing
        # `raise` -- unwinds past them, and a thread that is unwinding still
        # reports is_alive(), so the slot would stay occupied by a dead thread.
        # Vacate it here, on every exit path, and wake anyone parked so they can
        # ask for the replacement.
        try:
            self._run_loop()
        finally:
            with self._condition:
                if self._worker is threading.current_thread():
                    self._worker = None
                self._condition.notify_all()

    def _run_loop(self) -> None:
        while True:
            arn: str | None = None
            seq = 0
            record: dict[str, Any] | None = None
            flush_covers = 0
            with self._condition:
                while True:
                    if self._flush_requested and not self._blocking_pending_locked():
                        self._flush_requested = False
                        self._flush_barrier = 0
                        flush_covers = self._export_count
                        # Publish what this flush will cover before releasing the
                        # lock, so a waiter that wakes while it runs can see that
                        # this flush releases it and skip asking for another.
                        self._flush_in_flight = flush_covers
                        break
                    if self._pending:
                        arn, (seq, record) = next(iter(self._pending.items()))
                        del self._pending[arn]
                        break
                    self._condition.wait()

            if record is not None:
                # Popping the record consumed this execution's pending slot, so
                # nothing will ever export that snapshot again. The lane must
                # therefore advance whatever export() did: skip it and
                # lane.exported_seq never reaches a waiter's want_seq, so a drain
                # parked on this execution is never released. _export() already
                # contains every Exception, but a BaseException from a customer
                # exporter unwinds through here. Count the attempt in a finally
                # and let the exception continue out to the wrapper -- and into
                # the thread's traceback -- with nothing swallowed.
                try:
                    self._export(record)
                finally:
                    # Release the exported record before re-locking: a custom
                    # finalizer may re-enter schedule().
                    del record
                    assert arn is not None
                    with self._condition:
                        self._export_count += 1
                        lane = self._lanes.get(arn)
                        if lane is not None:
                            if seq > lane.exported_seq:
                                lane.exported_seq = seq
                            lane.exported_at = self._export_count
                            self._forget_lane_locked(arn, lane)
                        self._condition.notify_all()
                continue

            flushed = False
            try:
                self._flush()
                flushed = True
            finally:
                with self._condition:
                    # Retire the marker whatever happened: a stale one would park
                    # every later waiter that trusted this flush to cover it. Only
                    # a flush that ran to completion publishes its coverage.
                    self._flush_in_flight = 0
                    if flushed:
                        self._flushes_completed += 1
                        if flush_covers > self._flushed_through:
                            self._flushed_through = flush_covers
                    self._condition.notify_all()
            with self._condition:
                if not self._pending and not self._flush_requested:
                    self._worker = None
                    return

    def _export(self, record: dict[str, Any]) -> None:
        for exporter in self._exporters:
            try:
                shaped = truncate_record(
                    record, exporter.max_record_size_bytes, exporter.render
                )
                exporter.export(shaped)
            except Exception as exc:  # noqa: BLE001 - one exporter must not break others
                _logger.warning(
                    "workflow-insight: exporter %s failed: %s",
                    type(exporter).__name__,
                    exc,
                )

    def _flush(self) -> None:
        for exporter in self._exporters:
            try:
                exporter.flush()
            except Exception as exc:  # noqa: BLE001 - one exporter must not break others
                _logger.warning(
                    "workflow-insight: exporter %s flush failed: %s",
                    type(exporter).__name__,
                    exc,
                )

    # Test helpers.
    def _worker_alive(self) -> bool:
        with self._condition:
            return self._worker is not None and self._worker.is_alive()

    def _pending_count(self) -> int:
        with self._condition:
            return len(self._pending)
