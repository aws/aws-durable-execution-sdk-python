# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Latest-pending asynchronous export scheduling for Workflow Insight.

One pending slot is kept **per execution** (keyed by ``executionArn``). Each
record is a complete snapshot of its execution, so a newer snapshot for the same
execution supersedes an older one that has not been exported yet, while records
for different executions never displace each other. The plugin already tracks
state per execution, and the local test runner drives independent executions
concurrently through one shared plugin instance, so a single plugin-wide slot
would silently drop one execution's terminal record whenever another execution
scheduled a snapshot first.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from aws_durable_execution_sdk_python_insight.truncation import truncate_record
from aws_durable_execution_sdk_python_insight.types import InsightExporter


_logger = logging.getLogger("aws_durable_execution_sdk_python_insight")

_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED"})


def _is_terminal(record: dict[str, Any]) -> bool:
    return record.get("status") in _TERMINAL_STATUSES


class _ExportScheduler:
    """Run all exporters on one lazy worker with one pending record per execution."""

    def __init__(self, exporters: list[InsightExporter]) -> None:
        self._exporters = exporters
        self._condition = threading.Condition(threading.Lock())
        # executionArn -> latest pending snapshot for that execution. Insertion
        # ordered, so the worker exports executions in first-arrival order;
        # replacing an entry keeps its position.
        self._pending: dict[str, dict[str, Any]] = {}
        self._flush_requested = False
        self._flush_event: threading.Event | None = None
        self._worker: threading.Thread | None = None
        self._start_failure_logged = False

    def schedule(self, record: dict[str, Any]) -> None:
        """Replace this execution's pending snapshot and return without exporting."""
        key = str(record.get("executionArn", ""))
        displaced: dict[str, Any] | None = None
        start_error: Exception | None = None
        with self._condition:
            displaced = self._pending.get(key)
            # A terminal snapshot is final. A RUNNING snapshot for the same
            # execution that arrives after it (an operation-change hook from a
            # checkpoint completing during the end-of-invocation drain) must not
            # replace it, or the execution would be reported as still running.
            if (
                displaced is not None
                and _is_terminal(displaced)
                and not _is_terminal(record)
            ):
                return
            self._pending[key] = record
            start_error = self._ensure_worker_locked()
            self._condition.notify()
        # Releasing the displaced record may run custom finalizers, so do it unlocked.
        del displaced
        self._log_start_failure(start_error)

    def drain(self) -> None:
        """Wait until every pending record is exported and exporters flush.

        Records scheduled by any execution are exported before the flush, so a
        drain issued at one execution's invocation end also delivers snapshots
        that a concurrently running execution scheduled earlier.
        """
        start_error: Exception | None = None
        with self._condition:
            if not self._flush_requested:
                self._flush_requested = True
                self._flush_event = threading.Event()
            flush_event = self._flush_event
            assert flush_event is not None
            start_error = self._ensure_worker_locked()
            worker_running = self._worker is not None
            self._condition.notify()
        self._log_start_failure(start_error)
        if worker_running:
            flush_event.wait()
            return
        # No worker could be started. Export and flush on the calling thread so
        # nothing scheduled is dropped; this is the invocation-end path, which
        # already waits for delivery.
        self._pump(flush_event)

    def _ensure_worker_locked(self) -> Exception | None:
        if self._worker is not None and self._worker.is_alive():
            return None
        worker = threading.Thread(
            target=self._run,
            name=f"workflow-insight-export-{id(self)}",
            daemon=True,
        )
        self._worker = worker
        try:
            worker.start()
        except Exception as exc:  # noqa: BLE001 - instrumentation must not escape hooks
            # Leave the pending records in place: drain() exports them inline,
            # and a later schedule() retries starting a worker.
            self._worker = None
            return exc
        return None

    def _log_start_failure(self, start_error: Exception | None) -> None:
        if start_error is None or self._start_failure_logged:
            return
        self._start_failure_logged = True
        _logger.warning(
            "workflow-insight: could not start export worker; records are "
            "exported inline at invocation end instead: %s",
            start_error,
        )

    def _pop_pending_locked(self) -> dict[str, Any] | None:
        if not self._pending:
            return None
        key = next(iter(self._pending))
        return self._pending.pop(key)

    def _take_flush_locked(self) -> threading.Event | None:
        flush_event = self._flush_event
        self._flush_event = None
        self._flush_requested = False
        return flush_event

    def _run(self) -> None:
        while True:
            record: dict[str, Any] | None = None
            flush_event: threading.Event | None = None
            with self._condition:
                while not self._pending and not self._flush_requested:
                    self._condition.wait()
                record = self._pop_pending_locked()
                if record is None:
                    # Every pending record is exported: honor the flush request.
                    flush_event = self._take_flush_locked()

            if record is not None:
                self._export(record)
                continue

            self._flush()
            if flush_event is not None:
                flush_event.set()
            with self._condition:
                if not self._pending and not self._flush_requested:
                    self._worker = None
                    return

    def _pump(self, flush_event: threading.Event) -> None:
        """Export every pending record, then flush, on the calling thread."""
        while True:
            with self._condition:
                record = self._pop_pending_locked()
                if record is None:
                    # Another inline drain may already have taken the request;
                    # flushing twice is harmless, losing a record is not.
                    self._take_flush_locked()
            if record is None:
                break
            self._export(record)
        self._flush()
        flush_event.set()

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
