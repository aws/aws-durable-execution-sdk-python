"""Concurrent plugin callbacks must emit separate parseable JSON records."""

from __future__ import annotations

import importlib.util
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from typing import Any

import pytest

import aws_durable_execution_sdk_python.execution as execution


def test_concurrent_wait_hooks_keep_complete_stdout_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Import the real fixture without constructing a Lambda client: this test
    # exercises its stdout producer, not a deployed durable invocation.
    monkeypatch.setattr(execution, "durable_execution", lambda **_: lambda fn: fn)
    path = (
        Path(__file__).resolve().parents[1]
        / "handlers/plugin/plugin_wait_replay_flag.py"
    )
    spec = importlib.util.spec_from_file_location("wait_replay_log_fixture", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    first_body = Event()
    second_ready = Event()
    second_done = Event()
    chunks: list[str] = []

    class FragmentingStdout:
        def write(self, text: str) -> int:
            chunks.append(text)
            if '"operation-start"' in text:
                first_body.set()
                # print writes its body and newline separately. Permit the
                # other real hook to run between them unless _emit serializes it.
                second_done.wait(0.2)
            return len(text)

        def flush(self) -> None:
            pass

    records: list[dict[str, Any]] = [
        {"plugin": "CONFPLUGIN", "hook": "operation-start", "name": "long"},
        {"plugin": "CONFPLUGIN", "hook": "operation-end", "name": "short"},
    ]

    def emit_end() -> None:
        second_ready.set()
        assert first_body.wait(2)
        module._emit(records[1], "execution-arn")
        second_done.set()

    with monkeypatch.context() as capture:
        capture.setattr("sys.stdout", FragmentingStdout())
        with ThreadPoolExecutor(max_workers=2) as executor:
            end = executor.submit(emit_end)
            assert second_ready.wait(2)
            start = executor.submit(module._emit, records[0], "execution-arn")
            start.result(timeout=2)
            end.result(timeout=2)
    actual = [json.loads(line) for line in "".join(chunks).splitlines() if line]
    assert actual == [
        {"durableExecutionArn": "execution-arn", **record} for record in records
    ]
