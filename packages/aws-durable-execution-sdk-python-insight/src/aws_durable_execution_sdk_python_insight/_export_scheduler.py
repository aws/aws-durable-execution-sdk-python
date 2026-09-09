# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Asynchronous export scheduler for the Workflow Insight plugin.

Exporter work runs outside the SDK checkpoint thread. Each exporter has one
lazy daemon worker that serializes copying, rendering, truncation, export, and
flush calls.

Each exporter lane:

* Keeps a bounded FIFO per execution ARN and processes ARNs round-robin.
* Drops the oldest pending snapshots when the per-execution or lane-wide limit
  is reached, favoring the newest progress and terminal snapshots.
* Uses a lane-wide flush barrier at invocation end. All barriers share one
  timeout; timed-out barriers are removed without replacing a blocked worker.
* Stops its worker after an invocation has drained and the lane becomes idle.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections import OrderedDict, deque
from typing import Any

from aws_durable_execution_sdk_python_insight.truncation import truncate_record
from aws_durable_execution_sdk_python_insight.types import InsightExporter


_logger = logging.getLogger("aws_durable_execution_sdk_python_insight")

# Upper bound on distinct executions with a record waiting in a single lane.
# Only reached when a lane's exporter is blocked or slow; the oldest pending
# execution is then evicted (best-effort delivery) so plugin memory stays
# bounded regardless of how long a worker stays blocked.
_DEFAULT_MAX_PENDING_EXECUTIONS = 1024

# Upper bound on all pending records in a lane. Keeping this equal to the
# original distinct-execution cap preserves the scheduler's previous worst-case
# record count even though one execution can now retain a short burst.
_DEFAULT_MAX_PENDING_RECORDS = 1024

# Upper bound on records waiting for one execution in one lane. This is large
# enough to preserve the known 11-progress-plus-terminal burst without relying
# on daemon-thread scheduling, while still bounding memory behind a blocked
# exporter. The in-flight record is not included in this count.
_DEFAULT_MAX_PENDING_RECORDS_PER_EXECUTION = 16

# Queue entry kinds.
_RECORD = "record"
_FLUSH = "flush"


class _FlushBarrier:
    """A one-shot flush marker the invocation-end thread waits on.

    The worker completes the barrier after it has flushed (or skipped a cancelled
    barrier). ``canceled`` is set by the waiter when the shared timeout elapses so
    a later, still-blocked worker skips the now-pointless flush.
    """

    __slots__ = ("_event", "canceled")

    def __init__(self) -> None:
        self._event = threading.Event()
        self.canceled = False

    def complete(self) -> None:
        self._event.set()

    def wait(self, timeout: float) -> bool:
        return self._event.wait(timeout if timeout > 0 else 0)

    def is_done(self) -> bool:
        return self._event.is_set()


class _ExporterLane:
    """A single exporter's serial worker lane.

    All mutable state is guarded by ``_cond``. The worker is the only consumer of
    the queue; scheduling threads are producers that wake it via ``notify``.
    """

    def __init__(
        self,
        exporter: InsightExporter,
        *,
        max_pending_executions: int = _DEFAULT_MAX_PENDING_EXECUTIONS,
        max_pending_records: int = _DEFAULT_MAX_PENDING_RECORDS,
        max_pending_records_per_execution: int = (
            _DEFAULT_MAX_PENDING_RECORDS_PER_EXECUTION
        ),
    ) -> None:
        self._exporter = exporter
        self._max_pending = max(1, max_pending_executions)
        self._max_pending_records = max(1, max_pending_records)
        self._max_pending_per_execution = max(1, max_pending_records_per_execution)
        # Explicit non-reentrant Lock rather than Condition()'s default RLock:
        # the lane never re-acquires ``_cond`` while already holding it (worker
        # I/O -- export/flush -- runs outside the lock and no locked helper
        # re-enters), so recursion support is unnecessary. A plain Lock also
        # makes any accidental recursive acquisition fail loudly instead of
        # silently succeeding.
        self._cond = threading.Condition(threading.Lock())
        # Ordered work list: entries are (_RECORD, arn) or (_FLUSH, barrier).
        self._queue: deque[tuple[str, Any]] = deque()
        # arn -> bounded FIFO of pending records. Insertion order is the ARN
        # fairness order; scheduling an existing ARN moves its queue token to
        # the back, and the worker requeues an ARN that has more records.
        self._pending: OrderedDict[str, deque[dict[str, Any]]] = OrderedDict()
        self._stop_when_idle = False
        self._worker: threading.Thread | None = None

    # -- producer API (checkpoint / invocation-end threads) -------------------

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:
        with self._cond:
            self._stop_when_idle = False
            if execution_arn in self._pending:
                pending = self._pending[execution_arn]
                if len(pending) >= self._max_pending_per_execution:
                    pending.popleft()
                    _logger.warning(
                        "workflow-insight: pending export FIFO for %s on %s is "
                        "full (cap=%d); dropping oldest pending record",
                        execution_arn,
                        type(self._exporter).__name__,
                        self._max_pending_per_execution,
                    )
                pending.append(record)
                self._pending.move_to_end(execution_arn)
                self._move_record_token_to_back(execution_arn)
            else:
                self._pending[execution_arn] = deque([record])
                self._queue.append((_RECORD, execution_arn))
                self._enforce_pending_execution_cap()
            self._enforce_pending_record_cap()
            self._ensure_worker_locked()
            self._cond.notify()

    def enqueue_flush(self) -> _FlushBarrier:
        barrier = _FlushBarrier()
        with self._cond:
            self._queue.append((_FLUSH, barrier))
            self._ensure_worker_locked()
            self._cond.notify()
        return barrier

    def request_stop_when_idle(self) -> None:
        with self._cond:
            self._stop_when_idle = True
            self._cond.notify()

    def cancel_flush(self, barrier: _FlushBarrier) -> None:
        """Cancel a timed-out flush barrier so it cannot pile up behind a
        blocked worker.

        Under the lane lock: mark the barrier cancelled and, if its ``_FLUSH``
        marker is still queued, remove that exact marker and complete the
        barrier here. Removing it is what keeps queue/barrier state bounded
        across many warm invocations behind a blocked exporter -- otherwise one
        stale barrier per invocation would accumulate behind the stuck worker.

        If the worker has already popped the marker (the flush is in flight or
        about to run) the marker is no longer in the queue: we only set
        ``canceled`` and leave completion to the worker, which skips the
        now-pointless flush and completes the barrier itself. A synchronous
        in-flight ``flush()`` is never interrupted.
        """
        with self._cond:
            barrier.canceled = True
            for index, (kind, payload) in enumerate(self._queue):
                if kind == _FLUSH and payload is barrier:
                    del self._queue[index]
                    barrier.complete()
                    return

    # -- queue bookkeeping (must hold ``_cond``) ------------------------------

    def _move_record_token_to_back(self, execution_arn: str) -> None:
        for index, (kind, payload) in enumerate(self._queue):
            if kind == _RECORD and payload == execution_arn:
                del self._queue[index]
                self._queue.append((_RECORD, execution_arn))
                return
        # No token means the ARN's last queued record is currently in flight.
        # The next schedule sees it absent from ``_pending`` and appends a fresh
        # FIFO plus token, preserving the in-flight record before new work.

    def _requeue_record_before_flush(self, execution_arn: str) -> None:
        """Requeue an ARN behind peer records but before its drain barrier.

        A record token represents the ARN's whole pending FIFO at the moment the
        barrier is enqueued. Consuming one record must not move the remaining
        pre-barrier records behind that barrier, or invocation end could flush
        and return while part of its FIFO is still waiting.
        """
        for index, (kind, _) in enumerate(self._queue):
            if kind == _FLUSH:
                self._queue.insert(index, (_RECORD, execution_arn))
                return
        self._queue.append((_RECORD, execution_arn))

    def _enforce_pending_execution_cap(self) -> None:
        while len(self._pending) > self._max_pending:
            old_arn, _ = self._pending.popitem(last=False)
            self._remove_record_token(old_arn)
            _logger.warning(
                "workflow-insight: export lane for %s is full "
                "(cap=%d); dropping pending records for %s",
                type(self._exporter).__name__,
                self._max_pending,
                old_arn,
            )

    def _enforce_pending_record_cap(self) -> None:
        while sum(len(records) for records in self._pending.values()) > (
            self._max_pending_records
        ):
            old_arn = next(iter(self._pending))
            records = self._pending[old_arn]
            records.popleft()
            _logger.warning(
                "workflow-insight: export lane for %s reached its pending "
                "record cap (%d); dropping oldest pending record for %s",
                type(self._exporter).__name__,
                self._max_pending_records,
                old_arn,
            )
            if not records:
                del self._pending[old_arn]
                self._remove_record_token(old_arn)

    def _remove_record_token(self, execution_arn: str) -> None:
        for index, (kind, payload) in enumerate(self._queue):
            if kind == _RECORD and payload == execution_arn:
                del self._queue[index]
                return

    def _ensure_worker_locked(self) -> None:
        # Never create a replacement while a prior worker is alive (a blocked
        # worker keeps ``_worker`` non-None). A worker that exits cleanly nulls
        # ``_worker`` under the lock before returning, so this check is a
        # race-free "start iff there is no live worker".
        if self._worker is None or not self._worker.is_alive():
            worker = threading.Thread(
                target=self._run_worker,
                name=f"workflow-insight-export-{id(self)}",
                daemon=True,
            )
            self._worker = worker
            worker.start()

    # -- worker (single daemon thread) ---------------------------------------

    def _run_worker(self) -> None:
        while True:
            with self._cond:
                while not self._queue and not self._stop_when_idle:
                    self._cond.wait()
                if not self._queue and self._stop_when_idle:
                    # Idle stop: null ``_worker`` under the lock so a concurrent
                    # scheduler starts a fresh worker rather than assuming this
                    # one will pick the work up.
                    self._worker = None
                    return
                kind, payload = self._queue.popleft()
                record: dict[str, Any] | None = None
                if kind == _RECORD:
                    pending = self._pending.get(payload)
                    if not pending:
                        continue
                    record = pending.popleft()
                    if pending:
                        # Round-robin across ARNs: one record per turn, then the
                        # ARN goes behind all queue entries already waiting.
                        self._pending.move_to_end(payload)
                        self._requeue_record_before_flush(payload)
                    else:
                        del self._pending[payload]

            if kind == _RECORD and record is not None:
                self._export_one(record)
            else:  # _FLUSH
                barrier: _FlushBarrier = payload
                if not barrier.canceled:
                    self._flush()
                barrier.complete()

    def _export_one(self, record: dict[str, Any]) -> None:
        exporter = self._exporter
        # Copy for exporter isolation: every lane shares the same canonical
        # record, and truncation/export must never mutate what another lane
        # sees. If the copy fails we must NOT fall back to the shared record --
        # exporting the alias would let this lane's truncation mutate the object
        # other lanes still read, breaking workflow isolation. Treat a copy
        # failure like a render/truncation failure: log and skip this record for
        # this lane, then continue processing the lane's queue.
        try:
            local = copy.deepcopy(record)
        except Exception as exc:  # noqa: BLE001 - a non-copyable payload must not alias the shared record or break the lane
            _logger.warning(
                "workflow-insight: record copy failed for exporter %s; "
                "skipping export for this record: %s",
                type(exporter).__name__,
                exc,
            )
            return
        try:
            shaped = truncate_record(
                local, exporter.max_record_size_bytes, exporter.render
            )
        except Exception as exc:  # noqa: BLE001 - render/truncation is best-effort
            _logger.warning(
                "workflow-insight: render/truncation failed for exporter %s: %s",
                type(exporter).__name__,
                exc,
            )
            return
        try:
            exporter.export(shaped)
        except Exception as exc:  # noqa: BLE001 - one export must not break the lane
            _logger.warning(
                "workflow-insight: exporter %s export failed: %s",
                type(exporter).__name__,
                exc,
            )

    def _flush(self) -> None:
        try:
            self._exporter.flush()
        except Exception as exc:  # noqa: BLE001 - a failing flush completes the barrier
            _logger.warning(
                "workflow-insight: exporter %s flush failed: %s",
                type(self._exporter).__name__,
                exc,
            )

    # -- test / introspection helpers ----------------------------------------

    def _worker_alive(self) -> bool:
        with self._cond:
            return self._worker is not None and self._worker.is_alive()

    def _pending_count(self) -> int:
        with self._cond:
            return len(self._pending)

    def _pending_record_count(self) -> int:
        with self._cond:
            return sum(len(records) for records in self._pending.values())

    def _queue_len(self) -> int:
        with self._cond:
            return len(self._queue)

    def _queued_flush_count(self) -> int:
        with self._cond:
            return sum(1 for kind, _ in self._queue if kind == _FLUSH)


class _ExportScheduler:
    """Owns one :class:`_ExporterLane` per exporter and fans records out to them."""

    def __init__(
        self,
        exporters: list[InsightExporter],
        *,
        max_pending_executions: int = _DEFAULT_MAX_PENDING_EXECUTIONS,
        max_pending_records: int = _DEFAULT_MAX_PENDING_RECORDS,
        max_pending_records_per_execution: int = (
            _DEFAULT_MAX_PENDING_RECORDS_PER_EXECUTION
        ),
    ) -> None:
        self._lanes = [
            _ExporterLane(
                exporter,
                max_pending_executions=max_pending_executions,
                max_pending_records=max_pending_records,
                max_pending_records_per_execution=max_pending_records_per_execution,
            )
            for exporter in exporters
        ]

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:
        """Fan a canonical record out to every lane. Returns immediately."""
        for lane in self._lanes:
            lane.schedule(execution_arn, record)

    def end_invocation(self, timeout_seconds: float) -> bool:
        """Drain and flush every touched lane under one shared timeout.

        Enqueues a flush barrier per lane (after that lane's latest record),
        waits for all barriers against a single deadline, then asks every worker
        to stop once idle. Returns ``True`` if every barrier completed within the
        deadline, ``False`` if delivery degraded to best-effort on timeout.
        """
        barriers = [(lane, lane.enqueue_flush()) for lane in self._lanes]
        deadline = time.monotonic() + timeout_seconds
        degraded = False
        for lane, barrier in barriers:
            remaining = deadline - time.monotonic()
            if not barrier.wait(remaining):
                # Timed out: cancel this lane's barrier and pull its still-queued
                # _FLUSH marker out now, so a stale barrier per invocation cannot
                # accumulate behind a blocked worker.
                lane.cancel_flush(barrier)
                degraded = True
        for lane in self._lanes:
            lane.request_stop_when_idle()
        if degraded:
            _logger.warning(
                "workflow-insight: export drain/flush exceeded %.3fs; "
                "record delivery is best-effort for this invocation",
                timeout_seconds,
            )
        return not degraded
