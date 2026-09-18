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


# Consecutive export-worker deaths tolerated before asynchronous export is
# disabled for good.
#
# A replacement worker is started by whoever is waiting, so a worker that dies on
# every attempt is retried as fast as threads can be created, and each retry
# leaves the waiting drain -- an invocation thread -- exactly where it was. The
# bound converts that unbounded retry into a bounded one.
#
# The bound is not 1, because a single death can come from a transient condition
# that the next attempt would not hit, and disabling instrumentation for the rest
# of the environment's life on one transient is too coarse. A deterministic defect
# reproduces on every attempt, so a small constant separates the two cases. Every
# completed export attempt and every completed flush resets the count, so only
# deaths with no work completed in between accumulate.
_MAX_CONSECUTIVE_WORKER_FAULTS = 3


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
        # None when no flush is in flight. Presence and coverage are separate
        # facts: a flush that covers zero exports is an ordinary flush -- it is
        # what an invocation that emitted no record asks for -- and a single
        # integer cannot say both "no flush is running" and "a flush covering
        # nothing is running". Encoding the first as 0 made those two states
        # identical, so a waiter needing zero coverage could not tell that its
        # flush was already running and requested a second one that then ran
        # after its invocation had returned. Published when the worker commits to
        # a flush, so a waiter woken while that flush runs -- before its coverage
        # reaches _flushed_through -- can tell it is already covered.
        self._flush_in_flight: int | None = None
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
        # Export workers that died without completing any work, counted since the
        # last completed export attempt or flush. Only deaths accumulate here, so
        # a worker that keeps making progress never approaches the bound.
        self._worker_faults = 0

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
        is parked. Two things set that latch, and a drain that meets either
        returns without a flush: failing to start the export worker, and an export
        worker that has died ``_MAX_CONSECUTIVE_WORKER_FAULTS`` times without
        completing any work.

        A third path returns immediately: a call made on the export worker thread
        itself. Only that worker exports records and completes flushes, so a wait
        there would park the one thread able to release it. That happens when an
        exporter re-enters a plugin hook and the hook reaches an invocation end.
        The call is refused and reported rather than deadlocking the invocation;
        the record stays queued and this same worker exports it once it resumes
        its loop, and a flush covering it is requested on the way out. (Mirrors
        the Java ``ExportScheduler.refuseWaitThatWouldBlockThePump``.)
        """
        if self._is_export_worker():
            _logger.warning(
                "workflow-insight: drain() was called on the export worker "
                "thread, the only thread able to serve it, so the call was "
                "refused rather than deadlocking the invocation; an exporter "
                "re-entered a plugin hook"
            )
            # The refused call still leaves a flush behind, but only when there is
            # something for it to cover. The hook that re-entered may have queued a
            # record, and this worker exits its loop once nothing is pending and no
            # flush is requested -- so without the request the record would be
            # handed to the exporters and the worker would stop, leaving a
            # buffering exporter holding an execution's terminal telemetry when
            # Lambda freezes the environment.
            #
            # Requesting one unconditionally would livelock instead: an exporter
            # whose flush() re-enters a hook arrives here from inside a flush, and
            # an unconditional request would ask for the next one, which re-enters
            # again, for as long as the environment lives. A pending record is what
            # distinguishes new work from that loop.
            self._request_flush_for_pending_records()
            return
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
                    # `_flush_in_flight` is None exactly while no flush is
                    # running, so a flush that covers zero exports is still a
                    # flush in flight. That case is the common one, not a corner:
                    # `need` is 0 for a drain whose invocation emitted no record,
                    # and the flush it asks for covers 0 exports when nothing has
                    # ever been exported. Two such drains at once both see the
                    # other's flush and neither asks for a second.
                    in_flight = self._flush_in_flight
                    covered = in_flight is not None and in_flight >= need
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

    def _is_export_worker(self) -> bool:
        """Report whether the calling thread is this scheduler's export worker.

        Read under the condition's lock, because ``_worker`` is replaced by the
        waiter that starts a replacement and cleared by a worker that exits.
        """
        with self._condition:
            return self._worker is threading.current_thread()

    def _request_flush_for_pending_records(self) -> None:
        """Ask the worker for a flush, but only if a record is waiting for one.

        Returns without waiting, so it is safe to call from the worker itself. The
        barrier is raised to the current schedule counter, which is what makes the
        flush cover a record queued moments ago rather than running before it.

        A pending record is the condition, not a formality. This is called from a
        drain refused on the worker thread, and one way to reach that is an
        exporter whose ``flush()`` re-enters a plugin hook: the call then arrives
        from inside a flush, and requesting the next one unconditionally would
        produce a flush that re-enters, requests, and flushes again for as long as
        the environment lives -- after the invocation has returned. Nothing is
        pending in that case, so nothing is requested.
        """
        with self._condition:
            if self._disabled or not self._pending:
                return
            self._flush_requested = True
            self._flush_barrier = max(self._flush_barrier, self._seq)
            self._condition.notify_all()

    def _disable_locked(self) -> _Dropped:
        """Latch asynchronous export off for good and surrender everything queued.

        The latch is permanent, so no record the scheduler still holds will ever
        be exported: keeping any of them would pin customer objects for the
        remaining life of the environment. Empty the queue, which is the
        scheduler's only per-execution structure, and hand what came out back to
        the CALLER to release once it is outside the lock -- a record can carry
        customer objects whose finalizers run arbitrary code, and so can an
        execution whose hook state the plugin has already discarded.

        Every parked waiter is woken, because the latch means the export and the
        flush it is waiting for are never going to happen.

        Callers hold ``self._condition``.
        """
        self._disabled = True
        self._worker = None
        dropped = [(execution, execution.pending_record) for execution in self._pending]
        for execution, _ in dropped:
            execution.pending_record = None
        self._pending = {}
        self._flush_requested = False
        self._flush_barrier = 0
        self._flush_in_flight = None
        self._condition.notify_all()
        return dropped

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
            failed_pending = self._disable_locked()
            return failed_pending, exc
        return None, None

    def _blocking_pending_locked(self) -> bool:
        """True while a record scheduled at or before the flush barrier is pending."""
        barrier = self._flush_barrier
        return any(execution.scheduled_seq <= barrier for execution in self._pending)

    def _run(self) -> None:
        # The worker slot must be empty whenever no worker is running, or
        # _ensure_worker_locked() never starts a replacement and every later
        # record sits pending forever. The loop's own exits clear it, but an
        # exception that unwinds out of the loop passes them by, and a thread that
        # is unwinding still reports is_alive(), so the slot would stay occupied
        # by a dead thread. Vacate it here, on every exit path, and wake anyone
        # parked so they can ask for the replacement.
        #
        # A replacement alone is not enough when the death repeats. The waiter
        # that starts the replacement runs the same work again, so a fault the
        # work reproduces every time is retried as fast as threads can be
        # created, and the drain that keeps starting them never returns: the
        # invocation hangs and the environment fills with dead threads.
        # _export() and _flush() contain everything a customer exporter can
        # raise, so a fault reaching here comes from the scheduler's own code or
        # from a failure-reporting call that a customer object subverted, and
        # neither is something a retry can be expected to clear. Count
        # consecutive faults and give up on asynchronous export at the bound.
        faulted = True
        dropped: _Dropped | None = None
        gave_up = False
        try:
            self._run_loop()
            faulted = False
        finally:
            with self._condition:
                if self._worker is threading.current_thread():
                    self._worker = None
                if faulted:
                    self._worker_faults += 1
                    if self._worker_faults >= _MAX_CONSECUTIVE_WORKER_FAULTS:
                        # Releasing the waiters matters more than delivering the
                        # records. A waiter is an invocation thread inside
                        # on_invocation_end, so leaving it parked turns an
                        # instrumentation defect into a stalled customer
                        # execution; dropping records loses instrumentation data
                        # only. The drop is reported below, so the scheduler
                        # never claims delivery it did not make.
                        dropped = self._disable_locked()
                        gave_up = True
                self._condition.notify_all()
            # A dropped record can run customer finalizers, so release it outside
            # the lock. The exception that brought us here keeps unwinding once
            # this block finishes, into the thread's traceback, with nothing
            # swallowed.
            del dropped
            if gave_up:
                _logger.warning(
                    "workflow-insight: export worker died %d times without "
                    "completing any work; disabling asynchronous export and "
                    "dropping every record still queued",
                    self._worker_faults,
                )

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
                        # this flush releases it and skip asking for another. A
                        # coverage of 0 is published like any other: it means this
                        # flush covers every export so far, of which there are
                        # none, and it is still a flush in flight.
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
                # drain parked on this execution is never released. Count the
                # attempt in a finally so that holds even if _export() raises.
                #
                # Advancing here means the record was OFFERED to every exporter,
                # not that every exporter accepted it. _export() reports each
                # exporter's own failure and moves to the next, so no exporter is
                # skipped because another one failed, and coverage never stands
                # for a delivery that was never attempted.
                #
                # The record and its bookkeeping are one object, so there is no
                # second lookup left to come back empty: publishing cannot miss.
                exported = False
                try:
                    self._export(record)
                    exported = True
                finally:
                    # Release the exported record before re-locking: a custom
                    # finalizer may re-enter schedule().
                    del record
                    with self._condition:
                        self._export_count += 1
                        if seq > execution.exported_seq:
                            execution.exported_seq = seq
                        execution.exported_at = self._export_count
                        if exported:
                            # This worker completed work, so any earlier worker
                            # death was not the start of a fault the work
                            # reproduces every time.
                            self._worker_faults = 0
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
                    self._flush_in_flight = None
                    if flushed:
                        self._flushes_completed += 1
                        if flush_covers > self._flushed_through:
                            self._flushed_through = flush_covers
                        self._worker_faults = 0
                    self._condition.notify_all()
            with self._condition:
                if not self._pending and not self._flush_requested:
                    self._worker = None
                    return

    # Isolation of one exporter's failure from the others is what both loops below
    # exist for, and it holds for every exception type a customer exporter can
    # raise, BaseException included.
    #
    # A BaseException is normally not caught, because KeyboardInterrupt,
    # SystemExit and asyncio.CancelledError each mean that the operation the
    # current thread is running must stop. None of those three can arrive here
    # that way. This is the export worker thread: the interpreter raises
    # KeyboardInterrupt only in the main thread, threading discards a SystemExit
    # raised in a worker thread, and nothing cancels this thread because nothing
    # outside the scheduler knows it exists. A BaseException seen at these call
    # sites was therefore raised by the exporter itself, which makes it a report
    # of a defective exporter rather than an instruction to this thread. An
    # exporter that touches asyncio can raise CancelledError without writing
    # `raise`, so the case is reachable without a customer intending it.
    #
    # Containing it here is what keeps the two guarantees the worker owes. Every
    # remaining exporter still receives the record, so one exporter cannot make
    # the others miss a snapshot that is then discarded. And the flush the waiting
    # drain asked for still completes, so the drain is released by this worker
    # instead of by a replacement that runs the same failing exporter and dies the
    # same way.
    #
    # The containment is confined to these two call sites. Nowhere else does the
    # scheduler catch a BaseException, and neither loop runs on an invocation
    # thread.

    def _export(self, record: dict[str, Any]) -> None:
        for exporter in self._exporters:
            try:
                shaped = truncate_record(
                    record, exporter.max_record_size_bytes, exporter.render
                )
                exporter.export(shaped)
            except BaseException as exc:  # noqa: BLE001 - one exporter must not break others
                _logger.warning(
                    "workflow-insight: exporter %s failed: %s",
                    type(exporter).__name__,
                    exc,
                )

    def _flush(self) -> None:
        for exporter in self._exporters:
            try:
                exporter.flush()
            except BaseException as exc:  # noqa: BLE001 - one exporter must not break others
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
