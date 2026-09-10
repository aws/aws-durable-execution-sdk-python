# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Asynchronous export scheduler for the Workflow Insight plugin.

Exporter work runs outside the SDK checkpoint thread. Each exporter has one
lazy daemon worker that serializes copying, rendering, truncation, export, and
flush calls.

Each exporter lane:

* Keeps a bounded FIFO per execution ARN and processes ARNs round-robin.
* Drops the oldest pending snapshots when the per-execution, lane-wide record,
  or byte limit is reached, favoring recent progress and terminal snapshots.
* Uses a lane-wide flush barrier at invocation end. All barriers share one
  timeout; timed-out barriers are removed without replacing a blocked worker.
* Stops its worker after an invocation has drained and the lane becomes idle.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from aws_durable_execution_sdk_python_insight.truncation import (
    json_byte_size,
    truncate_record,
)
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

# Canonical JSON-byte estimate retained by one blocked lane. This keeps the
# instrumentation backlog well below Lambda's 128 MiB memory floor.
_DEFAULT_MAX_PENDING_BYTES = 16_000_000

# Queue entry kinds.
_RECORD = "record"
_FLUSH = "flush"

_RecordToken = tuple[str, int]


@dataclass(slots=True)
class _PendingRecord:
    sequence: int
    generation: int
    value: dict[str, Any]
    size: int


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
        max_pending_bytes: int = _DEFAULT_MAX_PENDING_BYTES,
    ) -> None:
        self._exporter = exporter
        self._max_pending = max(1, max_pending_executions)
        self._max_pending_records = max(1, max_pending_records)
        self._max_pending_per_execution = max(1, max_pending_records_per_execution)
        self._max_pending_bytes = max(1, max_pending_bytes)
        self._pending_bytes = 0
        # Explicit non-reentrant Lock rather than Condition()'s default RLock:
        # the lane never re-acquires ``_cond`` while already holding it (worker
        # I/O -- export/flush -- runs outside the lock and no locked helper
        # re-enters), so recursion support is unnecessary. A plain Lock also
        # makes any accidental recursive acquisition fail loudly instead of
        # silently succeeding.
        self._cond = threading.Condition(threading.Lock())
        # Ordered work list: entries are (_RECORD, (arn, generation)) or
        # (_FLUSH, barrier). A generation changes whenever a flush is queued, so
        # one ARN can have independent tokens on both sides of a barrier.
        self._queue: deque[tuple[str, Any]] = deque()
        # arn -> FIFO ordered by record sequence. Queue-token order, not this
        # mapping, controls round-robin fairness.
        self._pending: dict[str, deque[_PendingRecord]] = {}
        self._generation = 0
        self._next_sequence = 0
        self._stop_when_idle = False
        self._worker: threading.Thread | None = None

    # -- producer API (checkpoint / invocation-end threads) -------------------

    def schedule(
        self,
        execution_arn: str,
        record: dict[str, Any],
        record_size: int | None,
    ) -> None:
        with self._cond:
            self._stop_when_idle = False
            if record_size is None:
                _logger.warning(
                    "workflow-insight: cannot measure pending record for %s on "
                    "%s; dropping this record",
                    execution_arn,
                    type(self._exporter).__name__,
                )
                return
            size = max(0, record_size)
            if size > self._max_pending_bytes:
                _logger.warning(
                    "workflow-insight: pending record for %s on %s exceeds the "
                    "byte budget (%d > %d); dropping this record",
                    execution_arn,
                    type(self._exporter).__name__,
                    size,
                    self._max_pending_bytes,
                )
                return
            pending = self._pending.get(execution_arn)
            if pending is None:
                pending = deque()
                self._pending[execution_arn] = pending
            elif len(pending) >= self._max_pending_per_execution:
                self._drop_oldest_pending_record(execution_arn)
                pending = self._pending.setdefault(execution_arn, deque())
                _logger.warning(
                    "workflow-insight: pending export FIFO for %s on %s is "
                    "full (cap=%d); dropping oldest pending record",
                    execution_arn,
                    type(self._exporter).__name__,
                    self._max_pending_per_execution,
                )

            generation = self._generation
            has_generation = bool(pending and pending[-1].generation == generation)
            pending.append(
                _PendingRecord(self._next_sequence, generation, record, size)
            )
            self._pending_bytes += size
            self._next_sequence += 1
            token = (execution_arn, generation)
            if has_generation:
                self._move_record_token_to_back(token)
            else:
                self._queue.append((_RECORD, token))
                self._enforce_pending_execution_cap()
            self._enforce_pending_record_cap()
            self._enforce_pending_byte_cap()
            self._ensure_worker_locked()
            self._cond.notify()

    def enqueue_flush(self) -> _FlushBarrier:
        barrier = _FlushBarrier()
        with self._cond:
            self._queue.append((_FLUSH, barrier))
            self._generation += 1
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

    def _move_record_token_to_back(self, token: _RecordToken) -> None:
        for index, (kind, payload) in enumerate(self._queue):
            if kind == _RECORD and payload == token:
                del self._queue[index]
                self._queue.append((_RECORD, token))
                return

    def _requeue_record_before_flush(self, token: _RecordToken) -> None:
        """Requeue behind peers without crossing this ARN's next generation."""
        execution_arn, generation = token
        for index, (kind, payload) in enumerate(self._queue):
            later_same_arn = (
                kind == _RECORD
                and payload[0] == execution_arn
                and payload[1] > generation
            )
            if kind == _FLUSH or later_same_arn:
                self._queue.insert(index, (_RECORD, token))
                return
        self._queue.append((_RECORD, token))

    def _oldest_pending_arn(self) -> str:
        return min(
            self._pending,
            key=lambda arn: self._pending[arn][0].sequence,
        )

    def _enforce_pending_execution_cap(self) -> None:
        while len(self._pending) > self._max_pending:
            old_arn = self._oldest_pending_arn()
            self._drop_pending_execution(old_arn)
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
            old_arn = self._oldest_pending_arn()
            self._drop_oldest_pending_record(old_arn)
            _logger.warning(
                "workflow-insight: export lane for %s reached its pending "
                "record cap (%d); dropping oldest pending record for %s",
                type(self._exporter).__name__,
                self._max_pending_records,
                old_arn,
            )

    def _enforce_pending_byte_cap(self) -> None:
        while self._pending_bytes > self._max_pending_bytes and self._pending:
            old_arn = self._oldest_pending_arn()
            dropped_size = self._drop_oldest_pending_record(old_arn)
            _logger.warning(
                "workflow-insight: export lane for %s reached its pending byte "
                "budget (%d); dropping %d-byte pending record for %s",
                type(self._exporter).__name__,
                self._max_pending_bytes,
                dropped_size,
                old_arn,
            )

    def _drop_oldest_pending_record(self, execution_arn: str) -> int:
        records = self._pending[execution_arn]
        dropped = records.popleft()
        self._pending_bytes -= dropped.size
        token = (execution_arn, dropped.generation)
        if not records or records[0].generation != dropped.generation:
            self._remove_record_token(token)
        if not records:
            del self._pending[execution_arn]
        return dropped.size

    def _drop_pending_execution(self, execution_arn: str) -> None:
        records = self._pending.pop(execution_arn)
        self._pending_bytes -= sum(record.size for record in records)
        for index in range(len(self._queue) - 1, -1, -1):
            kind, payload = self._queue[index]
            if kind == _RECORD and payload[0] == execution_arn:
                del self._queue[index]

    def _remove_record_token(self, token: _RecordToken) -> None:
        for index, (kind, payload) in enumerate(self._queue):
            if kind == _RECORD and payload == token:
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
                    token: _RecordToken = payload
                    execution_arn, generation = token
                    pending = self._pending.get(execution_arn)
                    if not pending or pending[0].generation != generation:
                        continue
                    pending_record = pending.popleft()
                    self._pending_bytes -= pending_record.size
                    record = pending_record.value
                    if pending and pending[0].generation == generation:
                        # One record per ARN turn within this barrier generation.
                        self._requeue_record_before_flush(token)
                    elif not pending:
                        del self._pending[execution_arn]

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

    def _pending_bytes_count(self) -> int:
        with self._cond:
            return self._pending_bytes

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
        max_pending_bytes: int = _DEFAULT_MAX_PENDING_BYTES,
    ) -> None:
        self._lanes = [
            _ExporterLane(
                exporter,
                max_pending_executions=max_pending_executions,
                max_pending_records=max_pending_records,
                max_pending_records_per_execution=max_pending_records_per_execution,
                max_pending_bytes=max_pending_bytes,
            )
            for exporter in exporters
        ]

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:
        """Fan a canonical record out to every lane. Returns immediately."""
        record_size = json_byte_size(record)
        for lane in self._lanes:
            lane.schedule(execution_arn, record, record_size)

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
