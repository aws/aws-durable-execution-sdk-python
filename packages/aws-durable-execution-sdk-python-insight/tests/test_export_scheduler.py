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


def _record(value: str, arn: str = "exec-a") -> dict[str, Any]:
    return {
        "executionArn": arn,
        "status": "RUNNING",
        "value": value,
        "operations": [],
    }


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


# -- pending map is keyed per execution (#719 review, issue 1) ----------------


def test_pending_is_keyed_per_execution() -> None:
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(_record("a-first", arn="exec-a"))
    assert exporter.started.wait(5.0)

    # B's snapshot must not displace A's pending terminal snapshot.
    scheduler.schedule(_record("a-terminal", arn="exec-a"))
    scheduler.schedule(_record("b-running", arn="exec-b"))
    assert scheduler._pending_count() == 2

    exporter.release.set()
    scheduler.drain()
    assert exporter.calls == [
        ("export", "a-first"),
        ("export", "a-terminal"),
        ("export", "b-running"),
        ("flush", None),
    ]


def test_drain_delivers_every_pending_execution_before_flush() -> None:
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(_record("a", arn="exec-a"))
    assert exporter.started.wait(5.0)
    scheduler.schedule(_record("b", arn="exec-b"))
    scheduler.schedule(_record("c", arn="exec-c"))

    exporter.release.set()
    scheduler.drain()

    exported = [value for kind, value in exporter.calls if kind == "export"]
    assert exported == ["a", "b", "c"]
    assert exporter.calls[-1] == ("flush", None)


def test_concurrent_executions_each_deliver_terminal_record() -> None:
    rounds = 50
    exporter = CaptureExporter()
    scheduler = _ExportScheduler([exporter])

    def drive(arn: str) -> None:
        for i in range(rounds):
            scheduler.schedule(_record(f"{arn}-running-{i}", arn=arn))
            scheduler.schedule(_record(f"{arn}-terminal-{i}", arn=arn))
            scheduler.drain()

    threads = [threading.Thread(target=drive, args=(arn,)) for arn in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30.0)
    assert not any(thread.is_alive() for thread in threads)

    exported = [
        value
        for kind, value in exporter.calls
        if kind == "export" and value is not None
    ]
    for arn in ("a", "b"):
        terminals = [v for v in exported if v.startswith(f"{arn}-terminal-")]
        assert terminals == [f"{arn}-terminal-{i}" for i in range(rounds)]


# -- coalescing contract (#719 review, issue 2) -------------------------------


def test_same_execution_coalesces_to_first_and_latest_while_blocked() -> None:
    exporter = BlockingExporter()
    scheduler = _ExportScheduler([exporter])
    scheduler.schedule(_record("first"))
    assert exporter.started.wait(5.0)

    scheduler.schedule(_record("second"))
    scheduler.schedule(_record("third"))
    scheduler.schedule(_record("latest"))
    assert scheduler._pending_count() == 1

    exporter.release.set()
    scheduler.drain()
    assert exporter.calls == [
        ("export", "first"),
        ("export", "latest"),
        ("flush", None),
    ]
