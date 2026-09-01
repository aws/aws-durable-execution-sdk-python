# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Asynchronous, coalescing export scheduler for the Workflow Insight plugin.

The plugin builds one canonical ``WorkflowInsight`` record on the SDK checkpoint
thread and hands it to :class:`_ExportScheduler`. The scheduler keeps all
exporter-specific work -- per-exporter copy, ``render``, truncation, ``export``
and ``flush`` -- off the checkpoint thread by running it in a lazily-created
daemon worker, one per exporter ("lane"). Scheduling a record only enqueues it
and returns immediately, so ``on_operation_change`` never blocks on a slow
exporter.

Design (``workflow-insight-async-export-design.md``):

* One lazy daemon worker per exporter lane; never more than one live worker per
  lane, and a blocked worker is retained -- never replaced -- so threads cannot
  grow without bound.
* Per lane, at most one in-flight record and one latest *pending* record per
  execution ARN. Records are cumulative snapshots, so a newer pending record for
  an ARN replaces the older one (coalescing); an in-flight record is never
  cancelled. Updating a pending ARN moves it to the back of the queue for
  fairness across ARNs. Pending ARNs are capped; the oldest is evicted when the
  cap is exceeded (only reachable behind a blocked/slow exporter).
* Invocation end enqueues one flush barrier per touched lane after the latest
  record and waits for all barriers under a single shared timeout deadline. On
  timeout the workflow response is returned, degradation is logged, stale
  barriers are cancelled, and any blocked worker stays daemonized (a synchronous
  Python ``export()`` cannot be safely killed).
* Idle workers exit after the drain/flush request, so a normal invocation leaves
  no lingering thread.
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
    ) -> None:
        self._exporter = exporter
        self._max_pending = max(1, max_pending_executions)
        self._cond = threading.Condition()
        # Ordered work list: entries are (_RECORD, arn) or (_FLUSH, barrier).
        self._queue: deque[tuple[str, Any]] = deque()
        # arn -> latest pending record (coalesced). Insertion order is the
        # fairness order; updating an arn moves it to the back.
        self._pending: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._stop_when_idle = False
        self._worker: threading.Thread | None = None

    # -- producer API (checkpoint / invocation-end threads) -------------------

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:
        with self._cond:
            self._stop_when_idle = False
            if execution_arn in self._pending:
                # Coalesce: replace the pending record and move it to the back so
                # a busy execution cannot starve the others.
                self._pending[execution_arn] = record
                self._pending.move_to_end(execution_arn)
                self._move_record_token_to_back(execution_arn)
            else:
                self._pending[execution_arn] = record
                self._queue.append((_RECORD, execution_arn))
                self._enforce_pending_cap()
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

    # -- queue bookkeeping (must hold ``_cond``) ------------------------------

    def _move_record_token_to_back(self, execution_arn: str) -> None:
        for index, (kind, payload) in enumerate(self._queue):
            if kind == _RECORD and payload == execution_arn:
                del self._queue[index]
                self._queue.append((_RECORD, execution_arn))
                return
        # No token means the arn is currently in flight; a fresh token will be
        # appended when it leaves flight (the next schedule sees it absent from
        # ``_pending``), which yields the "export A then latest" behavior.

    def _enforce_pending_cap(self) -> None:
        while len(self._pending) > self._max_pending:
            old_arn, _ = self._pending.popitem(last=False)
            self._remove_record_token(old_arn)
            _logger.warning(
                "workflow-insight: export lane for %s is full "
                "(cap=%d); dropping pending record for %s",
                type(self._exporter).__name__,
                self._max_pending,
                old_arn,
            )

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
                    record = self._pending.pop(payload, None)
                    if record is None:
                        continue

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


class _ExportScheduler:
    """Owns one :class:`_ExporterLane` per exporter and fans records out to them."""

    def __init__(
        self,
        exporters: list[InsightExporter],
        *,
        max_pending_executions: int = _DEFAULT_MAX_PENDING_EXECUTIONS,
    ) -> None:
        self._lanes = [
            _ExporterLane(exporter, max_pending_executions=max_pending_executions)
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
        barriers = [lane.enqueue_flush() for lane in self._lanes]
        deadline = time.monotonic() + timeout_seconds
        degraded = False
        for barrier in barriers:
            remaining = deadline - time.monotonic()
            if not barrier.wait(remaining):
                barrier.canceled = True
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
