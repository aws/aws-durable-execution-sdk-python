# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Latest-pending asynchronous export scheduling for Workflow Insight."""

from __future__ import annotations

import logging
import threading
from typing import Any

from aws_durable_execution_sdk_python_insight.truncation import truncate_record
from aws_durable_execution_sdk_python_insight.types import InsightExporter


_logger = logging.getLogger("aws_durable_execution_sdk_python_insight")


class _ExportScheduler:
    """Run all exporters on one lazy worker with one latest pending record."""

    def __init__(self, exporters: list[InsightExporter]) -> None:
        self._exporters = exporters
        self._condition = threading.Condition(threading.Lock())
        self._pending: dict[str, Any] | None = None
        self._flush_requested = False
        self._flush_event: threading.Event | None = None
        self._worker: threading.Thread | None = None
        self._disabled = False

    def schedule(self, record: dict[str, Any]) -> None:
        """Replace the pending snapshot and return without running exporters."""
        displaced: dict[str, Any] | None = None
        failed_pending: dict[str, Any] | None = None
        start_error: Exception | None = None
        with self._condition:
            if self._disabled:
                return
            displaced = self._pending
            self._pending = record
            failed_pending, start_error = self._ensure_worker_locked()
            self._condition.notify()
        # Releasing either record may run custom finalizers, so do it unlocked.
        del displaced, failed_pending
        if start_error is not None:
            _logger.warning(
                "workflow-insight: could not start export worker; disabling "
                "asynchronous export: %s",
                start_error,
            )

    def drain(self) -> None:
        """Wait until the latest pending record is exported and exporters flush."""
        failed_pending: dict[str, Any] | None = None
        start_error: Exception | None = None
        with self._condition:
            if self._disabled:
                return
            if not self._flush_requested:
                self._flush_requested = True
                self._flush_event = threading.Event()
            flush_event = self._flush_event
            assert flush_event is not None
            failed_pending, start_error = self._ensure_worker_locked()
            started = not self._disabled
            self._condition.notify()
        del failed_pending
        if start_error is not None:
            _logger.warning(
                "workflow-insight: could not start export worker; disabling "
                "asynchronous export: %s",
                start_error,
            )
        if started:
            flush_event.wait()

    def _ensure_worker_locked(
        self,
    ) -> tuple[dict[str, Any] | None, Exception | None]:
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
            self._pending = None
            failed_event = self._flush_event
            self._flush_event = None
            self._flush_requested = False
            if failed_event is not None:
                failed_event.set()
            return failed_pending, exc
        return None, None

    def _run(self) -> None:
        while True:
            record: dict[str, Any] | None = None
            flush_event: threading.Event | None = None
            with self._condition:
                while self._pending is None and not self._flush_requested:
                    self._condition.wait()
                if self._pending is not None:
                    record = self._pending
                    self._pending = None
                else:
                    flush_event = self._flush_event
                    self._flush_event = None
                    self._flush_requested = False

            if record is not None:
                self._export(record)
                continue

            self._flush()
            if flush_event is not None:
                flush_event.set()
            with self._condition:
                if self._pending is None and not self._flush_requested:
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
            return int(self._pending is not None)
