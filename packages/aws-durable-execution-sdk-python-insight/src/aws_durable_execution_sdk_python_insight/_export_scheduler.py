# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Per-execution latest-pending asynchronous export scheduling for Workflow Insight.

One scheduler serves every execution its environment hosts -- it is owned by the
handler-lifetime plugin factory, because serializing export is a cross-execution
job -- and Lambda Managed Instances makes concurrent executions in one
environment routine. So the pending record and the bookkeeping that goes with it
live on the per-execution object the caller passes in, which is the caller's own
per-invocation plugin instance (:class:`_ExportState` is mixed into it):
coalescing happens only within a single execution and one execution's record can
never displace another's. The scheduler holds those objects; it has no notion of
an execution ARN and nothing to look up.

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


class _ExportState:
    """One execution's export bookkeeping, and its slot in the export queue.

    Mixed into the per-invocation plugin instance, so one object carries both an
    execution's hook-facing state and its export bookkeeping. Those used to live
    in three ARN-keyed structures -- the plugin's execution registry, the
    scheduler's pending record and its per-execution lane -- three views of one
    execution that had to agree about whether it still had work outstanding. In
    Java the same shape produced a defect where two of the views disagreed. There
    is one view now, and the scheduler holds the object itself.

    Every field here is guarded by ``_ExportScheduler._condition``. They belong to
    the scheduler: nothing outside it reads or writes them, and it never touches
    the hook-facing state on the same object.
    """

    def __init__(self) -> None:
        # Newest sequence number the scheduler assigned to this execution.
        self.scheduled_seq = 0
        # This execution's latest record, waiting for the export worker, or None
        # when nothing of its own is outstanding. A repeat emission replaces it,
        # which is what per-execution coalescing means; the scheduler's queue
        # holds this object exactly while this field is set.
        self.pending_record: dict[str, Any] | None = None
        # Newest sequence number already handed to every exporter.
        self.exported_seq = 0
        # Value of the scheduler's export counter when that export finished, so
        # a waiter can tell whether a completed flush covered its own record.
        self.exported_at = 0
        # drain() calls currently parked on this execution. Nothing depends on
        # it: a waiter holds this object directly, so the bookkeeping it waits
        # on can no longer be reclaimed from under it. It is kept because it is
        # the only way to observe that a drain really parked rather than raced
        # past.
        self.waiters = 0


# What a caller has to release once it is back outside the lock: the records the
# `_disabled` latch dropped, each with the execution object that was carrying it.
# Both can run customer finalizers.
_Dropped = list[tuple[_ExportState, dict[str, Any] | None]]


class _ExportScheduler:
    """Run all exporters on one lazy worker, keeping the latest record per execution."""

    def __init__(self, exporters: list[InsightExporter]) -> None:
        self._exporters = exporters
        self._condition = threading.Condition(threading.Lock())
        # Executions with a record waiting, oldest arrival first -- an ordered
        # set, keyed by the execution object itself. A repeat schedule for an
        # execution replaces the record the object carries and keeps the object's
        # position, so coalescing never lets one execution jump the queue. An
        # execution is in here exactly while its `pending_record` is set.
        self._pending: dict[_ExportState, None] = {}
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

    def schedule(self, execution: _ExportState, record: dict[str, Any]) -> None:
        """Replace this execution's pending snapshot; never runs exporters inline."""
        displaced: dict[str, Any] | None = None
        failed_pending: _Dropped | None = None
        start_error: Exception | None = None
        with self._condition:
            if self._disabled:
                return
            self._seq += 1
            execution.scheduled_seq = self._seq
            displaced = execution.pending_record
            execution.pending_record = record
            # Re-queuing an execution that is already queued is a no-op that
            # keeps its arrival position.
            self._pending[execution] = None
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

    def drain(self, execution: _ExportState) -> None:
        """Wait until this execution's latest record is exported and exporters flush.

        Returns once the calling execution's own record has reached every exporter
        and a flush covering it has completed; a flush triggered by another
        execution never releases a waiter whose record is still pending.

        Every call waits for a flush that completed after the call started, so an
        invocation end that emitted no record still flushes -- the cadence JS and
        Java have. Concurrent calls can share one flush, since they all started
        before it completed.

        The caller passes the execution object rather than an ARN, so there is
        nothing to look up and nothing to create: an execution that never
        scheduled a record simply carries zeroed bookkeeping, which is exactly
        "nothing of my own is outstanding, flush and return".

        Two paths return without exporting or flushing anything, because the
        permanent ``_disabled`` latch means no record will ever be exported: the
        latch was already set when this call started, or it is set while this call
        is parked. Failing to start the export worker sets that latch, so a drain
        that hits a worker-start failure also returns without a flush.
        """
        failed_pending: _Dropped | None = None
        start_error: Exception | None = None
        with self._condition:
            if self._disabled:
                return
            execution.waiters += 1
            try:
                want_seq = execution.scheduled_seq
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
                    # execution.exported_at only becomes ours once our record is
                    # out.
                    need = max(execution.exported_at, want_flush)
                    if (
                        execution.exported_seq >= want_seq
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
                execution.waiters -= 1
        del failed_pending
        if start_error is not None:
            _logger.warning(
                "workflow-insight: could not start export worker; disabling "
                "asynchronous export: %s",
                start_error,
            )

    # -- internals ------------------------------------------------------------

    def _ensure_worker_locked(self) -> tuple[_Dropped | None, Exception | None]:
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
            # Nothing is retained once the plugin has given up on asynchronous
            # export for good: the queue is the scheduler's only per-execution
            # structure, so emptying it drops every reference it holds. Both the
            # records and the execution objects go back to the CALLER to release
            # outside the lock -- a record can carry customer objects whose
            # finalizers run arbitrary code, and so can an execution whose hook
            # state the plugin has already discarded.
            failed_pending = [
                (execution, execution.pending_record) for execution in self._pending
            ]
            for execution, _ in failed_pending:
                execution.pending_record = None
            self._pending = {}
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
        return any(execution.scheduled_seq <= barrier for execution in self._pending)

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
            execution: _ExportState | None = None
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
                        execution = next(iter(self._pending))
                        del self._pending[execution]
                        record = execution.pending_record
                        execution.pending_record = None
                        seq = execution.scheduled_seq
                        break
                    self._condition.wait()

            if record is not None:
                assert execution is not None
                # Taking the record consumed this execution's pending slot, so
                # nothing will ever export that snapshot again. The bookkeeping
                # must therefore advance whatever export() did: skip it and
                # execution.exported_seq never reaches a waiter's want_seq, so a
                # drain parked on this execution is never released. _export()
                # already contains every Exception, but a BaseException from a
                # customer exporter unwinds through here. Count the attempt in a
                # finally and let the exception continue out to the wrapper --
                # and into the thread's traceback -- with nothing swallowed.
                #
                # The record and its bookkeeping are one object, so there is no
                # second lookup left to come back empty: publishing cannot miss.
                try:
                    self._export(record)
                finally:
                    # Release the exported record before re-locking: a custom
                    # finalizer may re-enter schedule().
                    del record
                    with self._condition:
                        self._export_count += 1
                        if seq > execution.exported_seq:
                            execution.exported_seq = seq
                        execution.exported_at = self._export_count
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
