# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Asynchronous export scheduler for the Workflow Insight plugin.

Exporter work runs outside the SDK checkpoint thread. Each exporter has one
lazy daemon worker that serializes copying, rendering, truncation, export, and
flush calls.

Each exporter lane:

* Keeps the latest pending snapshot per execution ARN and processes ARNs
  round-robin.
* Drops the oldest pending snapshot when the execution-count or byte budget is
  reached, keeping memory bounded.
* Uses a lane-wide flush barrier at invocation end. All barriers share one
  timeout; timed-out barriers are removed without replacing a blocked worker.
* Stops its worker after an invocation has drained and the lane becomes idle.
"""

from __future__ import annotations

import copy
import logging
import sys
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

# Estimated Python object memory retained by one blocked lane. Lambda functions
# can be configured with 128 MiB, so keep the instrumentation backlog well below
# that floor. This is a conservative budget signal, not an exact heap measurement.
_DEFAULT_MAX_PENDING_BYTES = 16_000_000


def _estimate_retained_size(value: Any, max_size: int | None = None) -> int:
    """Estimate retained Python memory without serializing or calling render()."""
    total = 0
    seen: set[int] = set()
    stack: list[Any] = [value]
    while stack:
        item = stack.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        try:
            total += sys.getsizeof(item)
        except Exception:  # noqa: BLE001 - estimation must never break a hook
            total += 1_024
        if max_size is not None and total > max_size:
            return max_size + 1
        try:
            if isinstance(item, memoryview):
                stack.append(item.obj)
            elif isinstance(item, dict):
                stack.extend(item.keys())
                stack.extend(item.values())
            elif isinstance(item, (list, tuple, set, frozenset, deque)):
                stack.extend(item)
            else:
                try:
                    stack.append(vars(item))
                except Exception:  # noqa: BLE001 - custom objects may use slots
                    pass
                for cls in type(item).__mro__:
                    slots = vars(cls).get("__slots__", ())
                    if isinstance(slots, str):
                        slots = (slots,)
                    for slot in slots:
                        if slot in {"__dict__", "__weakref__"}:
                            continue
                        if slot.startswith("__") and not slot.endswith("__"):
                            slot = f"_{cls.__name__.lstrip('_')}{slot}"
                        try:
                            stack.append(getattr(item, slot))
                        except Exception:  # noqa: BLE001 - unset/custom slots are best-effort
                            pass
        except Exception:  # noqa: BLE001 - traversal must never break a hook
            pass
    return total


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

    def __init__(
        self,
        exporter: InsightExporter,
        *,
        max_pending_executions: int = _DEFAULT_MAX_PENDING_EXECUTIONS,
        max_pending_bytes: int = _DEFAULT_MAX_PENDING_BYTES,
    ) -> None:
        self._exporter = exporter
        self._max_pending = max(1, max_pending_executions)
        self._max_pending_bytes = max(1, max_pending_bytes)
        self._pending_bytes = 0
        # Explicit non-reentrant Lock rather than Condition()'s default RLock:
        # the lane never re-acquires ``_cond`` while already holding it (worker
        # I/O -- export/flush -- runs outside the lock and no locked helper
        # re-enters), so recursion support is unnecessary. A plain Lock also
        # makes any accidental recursive acquisition fail loudly instead of
        # silently succeeding.
        self._cond = threading.Condition(threading.Lock())
        # Ordered work list: entries are (_RECORD, arn) or (_FLUSH, barrier).
        self._queue: deque[tuple[str, Any]] = deque()
        # arn -> (latest pending record, retained-memory estimate). Insertion
        # order is both record age and fairness order because replacing an ARN
        # moves it to the back.
        self._pending: OrderedDict[str, tuple[dict[str, Any], int]] = OrderedDict()
        self._stop_when_idle = False
        self._worker: threading.Thread | None = None
        self._disabled = False

    # -- producer API (checkpoint / invocation-end threads) -------------------

    def schedule(
        self,
        execution_arn: str,
        record: dict[str, Any],
        record_size: int,
    ) -> None:
        with self._cond:
            if self._disabled:
                return
            self._stop_when_idle = False
            size = max(0, record_size)
            if size > self._max_pending_bytes:
                superseded = self._pending.pop(execution_arn, None)
                if superseded is not None:
                    _, superseded_size = superseded
                    self._pending_bytes -= superseded_size
                    self._remove_record_token(execution_arn)
                _logger.warning(
                    "workflow-insight: pending record for %s on %s exceeds the "
                    "byte budget (%d > %d); dropping this record%s",
                    execution_arn,
                    type(self._exporter).__name__,
                    size,
                    self._max_pending_bytes,
                    " and its superseded pending snapshot"
                    if superseded is not None
                    else "",
                )
                return
            if execution_arn in self._pending:
                # Coalesce: replace the pending record and move it to the back so
                # a busy execution cannot starve the others.
                _, old_size = self._pending[execution_arn]
                self._pending_bytes -= old_size
                self._pending[execution_arn] = (record, size)
                self._pending_bytes += size
                self._pending.move_to_end(execution_arn)
                self._move_record_token_to_back(execution_arn)
            else:
                self._pending[execution_arn] = (record, size)
                self._pending_bytes += size
                self._queue.append((_RECORD, execution_arn))
            self._enforce_pending_caps()
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
        """Stop waiting for a timed-out barrier while retaining one later flush."""
        with self._cond:
            barrier.canceled = True
            # Keep at most one detached flush. Moving it to this barrier's
            # position makes it cover all work scheduled before the latest
            # timeout without accumulating one marker per warm invocation.
            for index in range(len(self._queue) - 1, -1, -1):
                kind, payload = self._queue[index]
                if kind == _FLUSH and payload is None:
                    del self._queue[index]
            for index, (kind, payload) in enumerate(self._queue):
                if kind == _FLUSH and payload is barrier:
                    self._queue[index] = (_FLUSH, None)
                    barrier.complete()
                    return

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

    def _enforce_pending_caps(self) -> None:
        while len(self._pending) > self._max_pending:
            old_arn, _ = self._drop_oldest_pending()
            _logger.warning(
                "workflow-insight: export lane for %s reached its execution cap "
                "(%d); dropping pending record for %s",
                type(self._exporter).__name__,
                self._max_pending,
                old_arn,
            )
        while self._pending_bytes > self._max_pending_bytes and self._pending:
            old_arn, dropped_size = self._drop_oldest_pending()
            _logger.warning(
                "workflow-insight: export lane for %s reached its pending byte "
                "budget (%d); dropping %d-byte pending record for %s",
                type(self._exporter).__name__,
                self._max_pending_bytes,
                dropped_size,
                old_arn,
            )

    def _drop_oldest_pending(self) -> tuple[str, int]:
        old_arn, (_, old_size) = self._pending.popitem(last=False)
        self._pending_bytes -= old_size
        self._remove_record_token(old_arn)
        return old_arn, old_size

    def _remove_record_token(self, execution_arn: str) -> None:
        for index, (kind, payload) in enumerate(self._queue):
            if kind == _RECORD and payload == execution_arn:
                del self._queue[index]
                return

    def _disable_locked(self, exc: Exception) -> None:
        self._disabled = True
        self._worker = None
        self._pending.clear()
        self._pending_bytes = 0
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
                    pending = self._pending.pop(payload, None)
                    if pending is None:
                        continue
                    record, record_size = pending
                    self._pending_bytes -= record_size

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
        except Exception as exc:  # noqa: BLE001 - malformed containers must not break the lane
            _logger.warning(
                "workflow-insight: record container copy failed for exporter %s; "
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
        max_pending_bytes: int = _DEFAULT_MAX_PENDING_BYTES,
    ) -> None:
        self._max_pending_bytes = max(1, max_pending_bytes)
        self._lanes = [
            _ExporterLane(
                exporter,
                max_pending_executions=max_pending_executions,
                max_pending_bytes=self._max_pending_bytes,
            )
            for exporter in exporters
        ]

    def schedule(self, execution_arn: str, record: dict[str, Any]) -> None:
        """Fan a canonical record out to every lane. Returns immediately."""
        record_size = _estimate_retained_size(record, self._max_pending_bytes)
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
