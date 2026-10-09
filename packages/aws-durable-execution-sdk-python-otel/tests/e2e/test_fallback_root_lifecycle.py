"""Fallback anchor coverage through the decorator and local durable runner."""

import inspect
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import context
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


class _CaptureInvocationEnd(DurableInstrumentationPlugin):
    def __init__(self, exporter: InMemorySpanExporter) -> None:
        self.exporter = exporter
        self.snapshots: list[tuple[InvocationStatus, tuple[ReadableSpan, ...]]] = []

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.snapshots.append((info.status, tuple(self.exporter.get_finished_spans())))


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("outcome", ["success", "failure", "timeout"])
def test_fallback_anchor_survives_suspension(
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    outcome: str,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(
        BatchSpanProcessor(exporter, schedule_delay_millis=60000)
    )
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    observer = _CaptureInvocationEnd(exporter)
    completed_steps: list[str] = []
    before = context.get_current()

    def step(_step_context: Any) -> str:
        completed_steps.append("step")
        return "stored"

    def handler(_event: Any, durable: DurableContext) -> str:
        value = durable.step(step, name="completed-step")
        durable.wait(
            Duration.from_seconds(60 if outcome == "timeout" else 1), name="wait"
        )
        if outcome == "failure":
            raise ValueError("failed after resume")
        return value

    wrapped = durable_execution(handler, plugins=[plugin, observer])
    try:
        # Published runners use real time; newer workspace runners default to
        # skipping durable waits. Exercise timeout without advancing that wait.
        runner_options: dict[str, Any] = {"handler": wrapped}
        if "skip_time" in inspect.signature(DurableFunctionTestRunner).parameters:
            runner_options["skip_time"] = outcome != "timeout"
        with DurableFunctionTestRunner(**runner_options) as runner:
            arn = runner.run_async(
                input="{}", timeout=3 if outcome == "timeout" else 15
            )
            result = runner.wait_for_result(arn, timeout=10)
        assert completed_steps == ["step"]
        assert observer.snapshots[0][0] is InvocationStatus.PENDING
        first_spans = observer.snapshots[0][1]
        first_roots = [s for s in first_spans if s.name == "DurableExecutionRoot"]
        assert len(first_roots) == 1
        assert not any(s.name == "Workflow" for s in first_spans)
        roots = [
            s for s in exporter.get_finished_spans() if s.name == "DurableExecutionRoot"
        ]
        assert all(s.to_json() == first_roots[0].to_json() for s in roots)
        if outcome == "timeout":
            assert result.status.value == "FAILED"
            assert result.error is not None
            assert "timed out" in (result.error.message or "")
            assert len(observer.snapshots) == 1
            assert len(roots) == 1
            assert not any(s.name == "Workflow" for s in exporter.get_finished_spans())
        else:
            assert result.status.value == (
                "FAILED" if outcome == "failure" else "SUCCEEDED"
            )
            workflows = [
                s for s in exporter.get_finished_spans() if s.name == "Workflow"
            ]
            assert len(workflows) == 1
            assert workflows[0].parent is not None
            assert first_roots[0].context is not None
            assert workflows[0].parent.span_id == first_roots[0].context.span_id
        assert context.get_current() == before
    finally:
        provider.shutdown()
