# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Asynchronous export scheduler for the Workflow Insight plugin.

Exporter work runs outside the SDK checkpoint thread. Each exporter has one
lazy daemon worker that serializes copying, rendering, truncation, export, and
flush calls.

Each exporter lane:

* Keeps at most one record in flight and one latest pending snapshot. A newer
  pending snapshot replaces the older one.
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
from typing import Any

from aws_durable_execution_sdk_python_insight.truncation import truncate_record
from aws_durable_execution_sdk_python_insight.types import InsightExporter


_logger = logging.getLogger("aws_durable_execution_sdk_python_insight")


def _copy_record_containers(record: dict[str, Any]) -> dict[str, Any]:
    """Copy built-in containers while treating custom values as opaque leaves."""
    memo: dict[int, Any] = {}
    seen: set[int] = set()
    stack: list[Any] = [record]
    while stack:
        item = stack.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        if type(item) is dict:
            stack.extend(item.keys())
            stack.extend(item.values())
        elif type(item) in {list, tuple, set, frozenset, deque}:
            stack.extend(item)
        else:
            memo[identity] = item
    return copy.deepcopy(record, memo)


# Queue entry kinds.
_RECORD = "record"
_FLUSH = "flush"


class _FlushBarrier:
    """A one-shot flush marker the invocation-end thread waits on.

    The worker completes the barrier after it has flushed (or skipped a cancelled
    barrier). ``canceled`` is set by the waiter when the shared timeout elapses so
    a later, still-blocked worker skips the now-pointless flush.
    """

    __slots__ = ("_event", "canceled", "failed")

    def __init__(self) -> None:
        self._event = threading.Event()
        self.canceled = False
        self.failed = False

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

    def __init__(self, exporter: InsightExporter) -> None:
        self._exporter = exporter
        # Explicit non-reentrant Lock rather than Condition()'s default RLock:
        # the lane never re-acquires ``_cond`` while already holding it (worker
        # I/O -- export/flush -- runs outside the lock and no locked helper
        # re-enters), so recursion support is unnecessary. A plain Lock also
        # makes any accidental recursive acquisition fail loudly instead of
        # silently succeeding.
        self._cond = threading.Condition(threading.Lock())
        # Ordered work list: entries are (_RECORD, None) or (_FLUSH, barrier).
        self._queue: deque[tuple[str, Any]] = deque()
        # At most one record waits behind the in-flight export. Replacing this
        # snapshot moves its queue token to the newest scheduling position.
        self._pending: dict[str, Any] | None = None
        self._stop_when_idle = False
        self._worker: threading.Thread | None = None
        self._disabled = False

    # -- producer API (checkpoint / invocation-end threads) -------------------

    def schedule(self, record: dict[str, Any]) -> None:
        with self._cond:
            if self._disabled:
                return
            self._stop_when_idle = False
            if self._pending is None:
                self._queue.append((_RECORD, None))
            else:
                self._move_record_token_to_back()
            self._pending = record
            self._ensure_worker_locked()
            self._cond.notify()

    def enqueue_flush(self) -> _FlushBarrier:
        barrier = _FlushBarrier()
        with self._cond:
            if self._disabled:
                barrier.canceled = True
                barrier.failed = True
                barrier.complete()
                return barrier
            self._queue.append((_FLUSH, barrier))
            self._ensure_worker_locked()
            self._cond.notify()
        return barrier

    def request_stop_when_idle(self) -> None:
        with self._cond:
            self._stop_when_idle = True
            self._cond.notify()

    def cancel_flush(self, barrier: _FlushBarrier) -> None:
        """Stop waiting while retaining one flush at the latest covered point."""
        with self._cond:
            barrier.canceled = True
            marker_indexes = {
                index
                for index, (kind, payload) in enumerate(self._queue)
                if kind == _FLUSH and (payload is barrier or payload is None)
            }
            if not any(
                kind == _FLUSH and payload is barrier for kind, payload in self._queue
            ):
                # The worker already owns this barrier. Do not erase a detached
                # flush installed by a later invocation while this one was in flight.
                return
            # A flush covers every record before its queue position. Keep the
            # rightmost canceled/detached marker so coalescing never narrows the
            # set of records that will eventually be published.
            rightmost = max(marker_indexes)
            coalesced: deque[tuple[str, Any]] = deque()
            for index, item in enumerate(self._queue):
                if index == rightmost:
                    coalesced.append((_FLUSH, None))
                elif index not in marker_indexes:
                    coalesced.append(item)
            self._queue = coalesced
            barrier.complete()

    # -- queue bookkeeping (must hold ``_cond``) ------------------------------

    def _move_record_token_to_back(self) -> None:
        for index, (kind, _) in enumerate(self._queue):
            if kind == _RECORD:
                del self._queue[index]
                self._queue.append((_RECORD, None))
                return

    def _disable_locked(self, exc: Exception) -> None:
        self._disabled = True
        self._worker = None
        self._pending = None
        for kind, payload in self._queue:
            if kind == _FLUSH and payload is not None:
                barrier: _FlushBarrier = payload
                barrier.canceled = True
                barrier.failed = True
                barrier.complete()
        self._queue.clear()
        _logger.warning(
            "workflow-insight: could not start worker for exporter %s; "
            "disabling this lane: %s",
            type(self._exporter).__name__,
            exc,
        )

    def _ensure_worker_locked(self) -> None:
        # Never create a replacement while a prior worker is alive (a blocked
        # worker keeps ``_worker`` non-None). A worker that exits cleanly nulls
        # ``_worker`` under the lock before returning, so this check is a
        # race-free "start iff there is no live worker".
        if self._disabled:
            return
        if self._worker is None or not self._worker.is_alive():
            worker = threading.Thread(
                target=self._run_worker,
                name=f"workflow-insight-export-{id(self)}",
                daemon=True,
            )
            self._worker = worker
            try:
                worker.start()
            except Exception as exc:  # noqa: BLE001 - instrumentation must not break hooks
                self._disable_locked(exc)

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
                    record = self._pending
                    self._pending = None
                    if record is None:
                        continue

            if kind == _RECORD and record is not None:
                self._export_one(record)
            else:  # _FLUSH
                barrier: _FlushBarrier | None = payload
                self._flush()
                if barrier is not None:
                    barrier.complete()

    def _export_one(self, record: dict[str, Any]) -> None:
        exporter = self._exporter
        # Copy the record's built-in containers for lane isolation, but preserve
        # custom values as opaque leaves for exporter-specific rendering. This
        # keeps one lane's render/truncation mutations out of other lanes without
        # requiring custom-renderable values to implement ``deepcopy``.
        try:
            local = _copy_record_containers(record)
        except Exception as exc:  # noqa: BLE001 - export remains best-effort
            _logger.warning(
                "workflow-insight: record container copy failed for exporter %s; "
                "using the original record without lane isolation: %s",
                type(exporter).__name__,
                exc,
            )
            local = record
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
            return int(self._pending is not None)

    def _queue_len(self) -> int:
        with self._cond:
            return len(self._queue)

    def _queued_flush_count(self) -> int:
        with self._cond:
            return sum(1 for kind, _ in self._queue if kind == _FLUSH)


class _ExportScheduler:
    """Owns one :class:`_ExporterLane` per exporter and fans records out to them."""

    def __init__(self, exporters: list[InsightExporter]) -> None:
        self._lanes = [_ExporterLane(exporter) for exporter in exporters]

    def schedule(self, _execution_arn: str, record: dict[str, Any]) -> None:
        """Fan a canonical record out to every lane. Returns immediately."""
        for lane in self._lanes:
            lane.schedule(record)

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
            elif barrier.failed:
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
