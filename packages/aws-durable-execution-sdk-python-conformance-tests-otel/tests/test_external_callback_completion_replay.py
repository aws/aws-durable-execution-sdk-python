# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""A real callback completion is exported before two later public replays."""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from opentelemetry import context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC_DIR = Path(__file__).resolve().parents[1] / "src"


def _suspended_callback(
    runner: DurableFunctionTestRunner, arn: str, name: str
) -> tuple[str, tuple[int, int]]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        events = runner.get_execution_history(arn, include_execution_data=True).events
        starts = [
            event
            for event in events
            if event.event_type == "CallbackStarted" and event.name == name
        ]
        completions = [
            event
            for event in events
            if event.event_type == "InvocationCompleted"
            and starts
            and event.event_id > starts[0].event_id
        ]
        if starts and completions:
            details = starts[0].callback_started_details
            assert details is not None and details.callback_id is not None
            return details.callback_id, (starts[0].event_id, completions[0].event_id)
        time.sleep(0.01)
    raise AssertionError(f"Callback {name} did not suspend")


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_external_callback_completion_precedes_later_replays(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    before_context = context.get_current()
    common_spec = importlib.util.spec_from_file_location(
        "common", SRC_DIR / "common.py"
    )
    assert common_spec is not None and common_spec.loader is not None
    common = importlib.util.module_from_spec(common_spec)
    monkeypatch.setitem(sys.modules, "common", common)
    common_spec.loader.exec_module(common)
    monkeypatch.setattr(common, "otel_plugin", lambda: plugin)
    spec = importlib.util.spec_from_file_location(
        "otel_26_external_callback_completion_replay",
        SRC_DIR / "otel_26_external_callback_completion_replay.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    calls: list[str] = []
    original_observe = module.observe_target

    def counted_observe(value: str) -> Any:
        step = original_observe(value)

        def counted(step_context: Any) -> str:
            calls.append(value)
            return step(step_context)

        return counted

    monkeypatch.setattr(module, "observe_target", counted_observe)
    gates: list[tuple[int, int]] = []
    try:
        with DurableFunctionTestRunner(
            handler=module.handler,
            store=FileSystemExecutionStore.create(tmp_path),
            poll_interval=0.01,
            execution_timeout=25,
            skip_time=False,
        ) as runner:
            arn = runner.run_async(
                input=json.dumps({"scenario": "external-callback-completion-replay"})
            )
            for name, payload in (
                ("otel-external-target", "target"),
                ("otel-external-barrier-one create callback id", "one"),
                ("otel-external-barrier-two create callback id", "two"),
            ):
                callback_id, gate = _suspended_callback(runner, arn, name)
                gates.append(gate)
                runner.send_callback_success(
                    callback_id, result=json.dumps(payload).encode("utf-8")
                )
            result = runner.wait_for_result(arn, timeout=10)
            history = runner.get_execution_history(arn, include_execution_data=True)

        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == "target/one/two"
        assert calls == ["target"]
        assert gates == [(2, 3), (8, 11), (15, 18)]
        assert [
            event.event_id
            for event in history.events
            if event.event_type == "InvocationCompleted"
        ] == [3, 11, 18, 21]
        assert history.events[-1].event_type == "ExecutionSucceeded"
        assert history.events[-1].event_id == 22

        spans = exporter.get_finished_spans()
        assert all(span.context is not None for span in spans)
        assert len({span.context.trace_id for span in spans if span.context}) == 1
        invocations = sorted(
            [span for span in spans if span.name == "Invocation"],
            key=lambda span: span.start_time or 0,
        )
        assert len(invocations) == 4
        assert [
            (span.attributes or {}).get("durable.invocation.status")
            for span in invocations
        ] == ["PENDING", "PENDING", "PENDING", "SUCCEEDED"]
        targets = [
            span
            for span in spans
            if span.name == "otel-external-target"
            and (span.attributes or {}).get("durable.operation.status") == "SUCCEEDED"
        ]
        # Count raw exports, including duplicate records with identical span IDs.
        assert len(targets) == 1
        target = targets[0]
        assert target.status.status_code.name == "OK"
        observed = [
            span
            for span in spans
            if span.name == "otel-external-target-observed attempt 1"
        ]
        assert len(observed) == 1
        assert target.end_time is not None and observed[0].start_time is not None
        assert target.end_time <= observed[0].start_time
        assert invocations[2].start_time is not None
        assert invocations[3].start_time is not None
        assert target.end_time < invocations[2].start_time < invocations[3].start_time
        parent = (
            invocations[1]
            if plugin_type is InvocationOtelPlugin
            else next(span for span in spans if span.name == "Workflow")
        )
        assert target.parent is not None and parent.context is not None
        assert target.parent.span_id == parent.context.span_id
        # Invocation view also exports the legitimate first-invocation segment.
        pending_target = [
            span
            for span in spans
            if span.name == "otel-external-target"
            and (span.attributes or {}).get("durable.operation.status") == "STARTED"
        ]
        assert len(pending_target) == (1 if plugin_type is InvocationOtelPlugin else 0)
        assert context.get_current() == before_context
    finally:
        provider.shutdown()
