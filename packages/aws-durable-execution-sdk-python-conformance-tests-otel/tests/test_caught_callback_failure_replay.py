# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The real case 25 handler catches only its intended failure across replay."""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, StepContext
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    ChildContextError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    UserFunctionStartInfo,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


SRC = Path(__file__).resolve().parents[1] / "src"
FAILED = "otel-failed-callback"
OBSERVED = FAILED + "-observed"
BARRIERS = [FAILED + "-barrier-one", FAILED + "-barrier-two"]
EXPECTED_HISTORY = [
    "ExecutionStarted",
    "ContextStarted",
    "CallbackStarted",
    "StepStarted",
    "StepSucceeded",
    "InvocationCompleted",
    "CallbackFailed",
    "ContextFailed",
    "StepStarted",
    "StepSucceeded",
    "ContextStarted",
    "CallbackStarted",
    "StepStarted",
    "StepSucceeded",
    "InvocationCompleted",
    "CallbackSucceeded",
    "ContextSucceeded",
    "ContextStarted",
    "CallbackStarted",
    "StepStarted",
    "StepSucceeded",
    "InvocationCompleted",
    "CallbackSucceeded",
    "ContextSucceeded",
    "InvocationCompleted",
    "ExecutionSucceeded",
]


def _callback_after_suspend(
    runner: DurableFunctionTestRunner, arn: str, name: str
) -> tuple[str, tuple[int, int]]:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        events = runner.get_execution_history(arn, include_execution_data=True).events
        starts = [
            event
            for event in events
            if event.event_type == "CallbackStarted"
            and event.name == name + " create callback id"
        ]
        ends = [
            event
            for event in events
            if starts
            and event.event_type == "InvocationCompleted"
            and event.event_id > starts[0].event_id
        ]
        if starts and ends:
            details = starts[0].callback_started_details
            assert details is not None and details.callback_id is not None
            return details.callback_id, (starts[0].event_id, ends[0].event_id)
        time.sleep(0.01)
    raise AssertionError(f"No creating invocation completion for {name}")


def _load(
    monkeypatch: pytest.MonkeyPatch,
    plugin: ExecutionOtelPluginFactory | InvocationOtelPluginFactory,
) -> ModuleType:
    spec = importlib.util.spec_from_file_location("common", SRC / "common.py")
    assert spec is not None and spec.loader is not None
    common = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "common", common)
    spec.loader.exec_module(common)
    monkeypatch.setattr(common, "otel_plugin_factory", lambda: plugin)
    spec = importlib.util.spec_from_file_location(
        "otel_25_caught_callback_failure_replay",
        SRC / "otel_25_caught_callback_failure_replay.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("view", "mode"),
    [
        (ExecutionOtelPluginFactory, "normal"),
        (InvocationOtelPluginFactory, "normal"),
        (ExecutionOtelPluginFactory, "unexpected-success"),
        (ExecutionOtelPluginFactory, "barrier-failure"),
    ],
)
def test_case25_public_caught_failure_and_replay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    view: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    mode: str,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    starts: list[InvocationStartInfo] = []
    invocation_ends: list[InvocationEndInfo] = []
    exported: list[tuple[ReadableSpan, int]] = []
    user_starts: list[tuple[int, UserFunctionStartInfo]] = []
    failures: list[tuple[int, CallbackExternalError | ChildContextError]] = []
    marker_calls: list[int] = []

    class Exporter(InMemorySpanExporter):
        def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
            exported.extend((span, len(starts)) for span in spans)
            return super().export(spans)

    exporter = Exporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    original_create = plugin.create_plugin

    def create_plugin(info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        instance = original_create(info)
        original_start = instance.on_invocation_start
        original_end = instance.on_invocation_end
        original_user_start = instance.on_user_function_start

        def on_start(info: InvocationStartInfo) -> None:
            starts.append(info)
            original_start(info)

        def on_end(info: InvocationEndInfo) -> None:
            invocation_ends.append(info)
            original_end(info)

        def on_user_start(info: UserFunctionStartInfo) -> None:
            user_starts.append((len(starts), info))
            original_user_start(info)

        monkeypatch.setattr(instance, "on_invocation_start", on_start)
        monkeypatch.setattr(instance, "on_invocation_end", on_end)
        monkeypatch.setattr(instance, "on_user_function_start", on_user_start)
        return instance

    monkeypatch.setattr(plugin, "create_plugin", create_plugin)
    original_wait = DurableContext.wait_for_callback

    def wait(context: DurableContext, *args: Any, **kwargs: Any) -> Any:
        try:
            return original_wait(context, *args, **kwargs)
        except (CallbackExternalError, ChildContextError) as error:
            if kwargs.get("name") == FAILED:
                failures.append((len(starts), error))
            raise

    monkeypatch.setattr(DurableContext, "wait_for_callback", wait)
    module = _load(monkeypatch, plugin)
    original_marker = module.observe_failure

    def counted_marker() -> Callable[[StepContext], str]:
        step = original_marker()

        def run(context: StepContext) -> str:
            marker_calls.append(len(starts))
            return step(context)

        return run

    monkeypatch.setattr(module, "observe_failure", counted_marker)
    gates: list[tuple[int, int]] = []
    try:
        with DurableFunctionTestRunner(
            handler=module.handler,
            store=FileSystemExecutionStore.create(tmp_path / "store"),
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=30,
        ) as runner:
            arn = runner.run_async(
                input=json.dumps({"scenario": "caught-callback-failure-replay"})
            )
            callback, gate = _callback_after_suspend(runner, arn, FAILED)
            gates.append(gate)
            if mode == "unexpected-success":
                runner.send_callback_success(callback, result=b'"unexpected"')
            else:
                runner.send_callback_failure(callback)
                for name, payload in zip(BARRIERS, ("one", "two")):
                    callback, gate = _callback_after_suspend(runner, arn, name)
                    gates.append(gate)
                    if mode == "barrier-failure":
                        runner.send_callback_failure(
                            callback, error=ErrorObject.from_message("barrier failure")
                        )
                        break
                    runner.send_callback_success(
                        callback, result=json.dumps(payload).encode()
                    )
            result = runner.wait_for_result(arn, timeout=10)
            history = runner.get_execution_history(
                arn, include_execution_data=True
            ).to_dict()
            metadata_history = runner.get_execution_history(
                arn, include_execution_data=False
            ).to_dict()
        if mode != "normal":
            assert result.status.value == "FAILED"
            assert marker_calls == ([] if mode == "unexpected-success" else [2])
            assert not any(
                event.get("Name") == BARRIERS[1] for event in history["Events"]
            )
            return
        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == "caught/one/two"
        assert [info.status.value for info in invocation_ends] == [
            "PENDING",
            "PENDING",
            "PENDING",
            "SUCCEEDED",
        ]
        assert [event["EventType"] for event in history["Events"]] == EXPECTED_HISTORY
        assert [event["EventId"] for event in history["Events"]] == list(range(1, 27))
        assert gates == [(3, 6), (12, 15), (19, 22)]
        assert marker_calls == [2]
        assert [invocation for invocation, _ in failures] == [2, 3, 4]
        assert {type(error) for _, error in failures} == {CallbackExternalError}
        assert len({error.error_type for _, error in failures}) == 1
        failed = history["Events"][6]
        # Intended local history contract from #862. The exact empty Payload
        # service shape remains unverified against deployed AWS Lambda.
        assert failed["CallbackFailedDetails"]["Error"] == {
            "Payload": {},
            "Truncated": False,
        }
        assert metadata_history["Events"][6]["CallbackFailedDetails"]["Error"] == {
            "Truncated": True,
        }
        target_id = failed["Id"]
        leaves = [
            (span, invocation)
            for span, invocation in exported
            if (span.attributes or {}).get("durable.operation.id") == target_id
            and (span.attributes or {}).get("durable.operation.status") == "FAILED"
        ]
        assert len(leaves) == 1
        leaf, invocation = leaves[0]
        assert invocation == 2
        assert leaf.status.status_code.name == "ERROR"
        assert not leaf.events
        marker_spans = [
            (span, invocation)
            for span, invocation in exported
            if (span.attributes or {}).get("durable.operation.name") == OBSERVED
        ]
        assert marker_spans and all(invocation == 2 for _, invocation in marker_spans)
        assert leaf.end_time is not None
        for span, _ in marker_spans:
            assert span.start_time is not None
            assert leaf.end_time <= span.start_time
        workflows = [span for span, _ in exported if span.name == "Workflow"]
        assert len(workflows) == 1 and workflows[0].status.status_code.name == "OK"
        assert leaf.parent is not None
        parent = next(
            span
            for span, _ in exported
            if span.context is not None and span.context.span_id == leaf.parent.span_id
        )
        assert (parent.attributes or {}).get("durable.operation.id") == (
            history["Events"][1]["Id"]
        )
        if view is InvocationOtelPluginFactory:
            assert parent.parent is not None
            invocation_span, invocation = next(
                (span, number)
                for span, number in exported
                if span.context is not None
                and span.context.span_id == parent.parent.span_id
            )
            assert invocation_span.name == "Invocation" and invocation == 2
        submitter_id = history["Events"][3]["Id"]
        assert sum(info.operation_id == submitter_id for _, info in user_starts) == 1
    finally:
        provider.shutdown()
