"""Public handlers retry a failed terminal export before invocation cleanup."""

import json
import logging
from threading import Event
from typing import Any

import pytest
from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


class _FailTargetOnceProcessor(SimpleSpanProcessor):
    def __init__(
        self, exporter: InMemorySpanExporter, invocation_ending: Event
    ) -> None:
        super().__init__(exporter)
        self.invocation_ending = invocation_ending
        self.target_starts: list[Span] = []
        self.target_ends: list[tuple[bool, ReadableSpan]] = []
        self.failed_after_close = False
        self.error = RuntimeError("target span processor failed before export")

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        if span.name == "target":
            self.target_starts.append(span)
        super().on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        if span.name == "target":
            self.target_ends.append((self.invocation_ending.is_set(), span))
            if len(self.target_ends) == 1:
                original = self.target_starts[0]
                self.failed_after_close = (
                    not original.is_recording() and original.end_time is not None
                )
                raise self.error
        super().on_end(span)


@pytest.mark.parametrize(
    "factory_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_handler_retries_closed_terminal_span_during_invocation_end(
    factory_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    caplog.set_level(logging.ERROR, logger="aws_durable_execution_sdk_python.plugin")
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    invocation_ending = Event()
    processor = _FailTargetOnceProcessor(exporter, invocation_ending)
    provider.add_span_processor(processor)
    invocation_statuses: list[InvocationStatus] = []
    exports_before_drain: list[ReadableSpan] = []
    step_calls: list[str] = []

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            invocation_statuses.append(info.status)
            exports_before_drain.extend(
                span for span in exporter.get_finished_spans() if span.name == "target"
            )
            invocation_ending.set()

    class ObserveFactory:
        def create_plugin(self, info: InvocationStartInfo) -> Observe:
            return Observe()

    factory = factory_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )

    def saved(_context: StepContext) -> str:
        step_calls.append("target")
        return "saved result"

    def body(_event: Any, context: DurableContext) -> str:
        return context.step(saved, name="target")

    handler = durable_execution(body, plugins=[ObserveFactory(), factory])
    try:
        with DurableFunctionTestRunner(
            handler=handler, poll_interval=0.01, execution_timeout=10
        ) as runner:
            result = runner.run(input="{}")

        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == "saved result"
        assert step_calls == ["target"]
        assert invocation_statuses == [InvocationStatus.SUCCEEDED]
        assert processor.failed_after_close
        assert [during_end for during_end, _ in processor.target_ends] == [False, True]
        assert exports_before_drain == []
        (failed_attempt, successful_attempt) = [
            span for _, span in processor.target_ends
        ]
        (target,) = [
            span for span in exporter.get_finished_spans() if span.name == "target"
        ]
        assert target is successful_attempt
        assert target.context is not None
        assert target.context == failed_attempt.context
        assert target.parent == failed_attempt.parent
        assert target.start_time == failed_attempt.start_time
        assert target.end_time == failed_attempt.end_time
        assert target.status.status_code is StatusCode.OK
        assert target.attributes is not None
        assert target.attributes["durable.operation.status"] == "SUCCEEDED"
        assert target.attributes["durable.operation.type"] == "STEP"
        assert target.events == ()
        parent_name = (
            "Workflow" if factory_type is ExecutionOtelPluginFactory else "Invocation"
        )
        (parent,) = [
            span for span in exporter.get_finished_spans() if span.name == parent_name
        ]
        assert parent.context is not None
        assert target.parent == parent.context
        assert target.context.trace_id == parent.context.trace_id
        failures = [
            record
            for record in caplog.records
            if record.name == "aws_durable_execution_sdk_python.plugin"
            and record.exc_info is not None
            and record.exc_info[1] is processor.error
        ]
        assert len(failures) == 1
        assert "exception ignored" in failures[0].getMessage()
    finally:
        provider.shutdown()
