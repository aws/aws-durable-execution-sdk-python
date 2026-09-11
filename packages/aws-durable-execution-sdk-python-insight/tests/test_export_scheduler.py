# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import threading
import time
from typing import Any

from aws_durable_execution_sdk_python_insight._export_scheduler import (
    _ExportScheduler,
)


def _record(value: str) -> dict[str, Any]:
    return {"status": "RUNNING", "value": value, "operations": []}


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


class CaptureExporter:
    max_record_size_bytes: int | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None]] = []

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.calls.append(("export", record["value"]))

    def flush(self) -> None:
        self.calls.append(("flush", None))


class BlockingExporter(CaptureExporter):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def export(self, record: dict[str, Any]) -> None:
        self.started.set()
        self.release.wait(5.0)
        super().export(record)


class FailingExporter(CaptureExporter):
    def export(self, record: dict[str, Any]) -> None:
        raise RuntimeError("export failed")

    def flush(self) -> None:
        raise RuntimeError("flush failed")


def test_latest_pending_coalesces_without_blocking_schedule() -> None:
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(_record("first"))
    assert exporter.started.wait(5.0)

    start = time.monotonic()
    scheduler.schedule(_record("middle"))
    scheduler.schedule(_record("latest"))
    assert time.monotonic() - start < 0.5
    assert scheduler._pending_count() == 1

    exporter.release.set()
    scheduler.drain()
    assert exporter.calls == [
        ("export", "first"),
        ("export", "latest"),
        ("flush", None),
    ]
    assert _wait_until(lambda: not scheduler._worker_alive())


def test_exporter_failure_does_not_block_other_exporters() -> None:
    failing = FailingExporter()
    capture = CaptureExporter()
    scheduler = _ExportScheduler([failing, capture])

    scheduler.schedule(_record("terminal"))

    scheduler.drain()
    assert capture.calls == [("export", "terminal"), ("flush", None)]


def test_drain_flushes_after_export() -> None:
    capture = CaptureExporter()
    scheduler = _ExportScheduler([capture])

    scheduler.schedule(_record("terminal"))

    scheduler.drain()
    assert capture.calls == [("export", "terminal"), ("flush", None)]


def test_worker_start_failure_never_escapes_hook(monkeypatch) -> None:
    def fail_start(self) -> None:  # noqa: ARG001
        raise RuntimeError("cannot start")

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    scheduler = _ExportScheduler([CaptureExporter()])

    scheduler.schedule(_record("dropped"))
    scheduler.drain()

    assert scheduler._pending_count() == 0


def test_superseded_record_finalizes_after_lane_unlock() -> None:
    scheduler = _ExportScheduler([BlockingExporter()])
    exporter = scheduler._exporters[0]
    assert isinstance(exporter, BlockingExporter)
    scheduler.schedule(_record("inflight"))
    assert exporter.started.wait(5.0)
    finalized = threading.Event()

    class ReentrantValue:
        def __del__(self) -> None:
            scheduler.schedule(_record("from-finalizer"))
            finalized.set()

    pending = _record("superseded")
    pending["payload"] = ReentrantValue()
    scheduler.schedule(pending)
    del pending

    scheduler.schedule(_record("replacement"))

    assert finalized.wait(5.0)
    exporter.release.set()
    scheduler.drain()


def test_drain_waits_for_blocked_exporter() -> None:
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(_record("terminal"))
    assert exporter.started.wait(5.0)
    drain_thread = threading.Thread(target=scheduler.drain)

    drain_thread.start()
    assert _wait_until(drain_thread.is_alive)
    exporter.release.set()
    drain_thread.join(5.0)

    assert not drain_thread.is_alive()
    assert exporter.calls == [("export", "terminal"), ("flush", None)]
