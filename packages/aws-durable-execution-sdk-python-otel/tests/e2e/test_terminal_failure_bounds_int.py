"""Caught operation failures preserve telemetry through real suspension/replay."""

import json
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import ChildConfig, Duration
from aws_durable_execution_sdk_python.exceptions import ChildContextError, InvokeError
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


@pytest.mark.parametrize(
    "view", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("failure_kind", ["child", "virtual", "invoke"])
@pytest.mark.parametrize("resume", [False, True])
def test_caught_failure_keeps_events_bounded_and_workflow_successful(
    view: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    failure_kind: str,
    resume: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )
    saved_calls: list[str] = []
    failure_calls: list[str] = []
    observations: list[OperationEndInfo] = []

    class Observe(DurableInstrumentationPlugin):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.name == "caught-failure":
                observations.append(info)

    class ObserveFactory:
        def create_plugin(self, _info: InvocationStartInfo) -> Observe:
            return Observe()

    def saved_step(_context: Any) -> str:
        saved_calls.append("saved")
        return "saved"

    def failing_child(_context: DurableContext) -> str:
        failure_calls.append("failed")
        raise ValueError("actual child failure")

    def failing_function(_event: Any, _context: Any) -> None:
        failure_calls.append("failed")
        raise ValueError("actual invoke failure")

    def body(_event: Any, context: DurableContext) -> str:
        saved = context.step(saved_step, name="saved-step")
        try:
            if failure_kind == "invoke":
                context.invoke("failing-target", {}, name="caught-failure")
            else:
                context.run_in_child_context(
                    failing_child,
                    name="caught-failure",
                    config=ChildConfig(is_virtual=failure_kind == "virtual"),
                )
        except (ChildContextError, InvokeError):
            if resume:
                context.wait(Duration.from_seconds(1), name="replay-wait")
            return saved
        raise AssertionError("Expected the original operation failure")

    try:
        with DurableFunctionTestRunner(
            handler=durable_execution(body, plugins=[factory, ObserveFactory()]),
            poll_interval=0.01,
        ) as runner:
            runner.register_function("failing-target", failing_function)
            result = runner.run(input="{}", execution_timeout=15)
        assert result.status.value == "SUCCEEDED"
        assert result.result is not None and json.loads(result.result) == "saved"
        assert saved_calls == ["saved"]
        # Virtual contexts intentionally execute on replay; checkpointed child
        # failures and invoke outcomes must return their stored failure instead.
        assert len(failure_calls) == (2 if resume and failure_kind == "virtual" else 1)
        assert observations
        if failure_kind == "virtual":
            assert all(info.start_time is None for info in observations)
        spans = exporter.get_finished_spans()
        failed = [
            span
            for span in spans
            if span.name == "caught-failure"
            and span.attributes is not None
            and span.attributes.get("durable.operation.status") == "FAILED"
        ]
        assert failed
        for span in failed:
            assert span.status.status_code is StatusCode.ERROR
            (event,) = span.events
            assert span.start_time is not None and span.end_time is not None
            assert span.start_time <= event.timestamp == span.end_time
            assert event.attributes is not None
            assert event.attributes["exception.message"] in {
                "actual child failure",
                "actual invoke failure",
            }
        invocations = [span for span in spans if span.name == "Invocation"]
        # An invoke can add its own suspension before the explicit replay wait.
        assert len(invocations) >= (2 if resume else 1)
        assert all(span.status.status_code is StatusCode.OK for span in invocations)
        (workflow,) = [span for span in spans if span.name == "Workflow"]
        assert workflow.status.status_code is StatusCode.OK
    finally:
        provider.shutdown()
