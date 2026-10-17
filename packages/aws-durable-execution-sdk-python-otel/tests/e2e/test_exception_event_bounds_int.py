"""Real checkpointless child failures keep diagnostic events inside their span."""

import json

import pytest
from aws_durable_execution_sdk_python import (
    durable_execution,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import ChildContextError
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_caught_virtual_child_failure_has_bounded_event_time(view, monkeypatch):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )
    hooks = []

    class Observe(DurableInstrumentationPlugin):
        def on_operation_end(self, info):
            if info.name == "virtual-error":
                hooks.append(info)

    @durable_with_child_context
    def child(_context):
        raise ValueError("actual child failure")

    def body(_event, context):
        try:
            context.run_in_child_context(
                child(), name="virtual-error", config=ChildConfig(is_virtual=True)
            )
        except ChildContextError:
            return "caught"
        raise AssertionError("Expected the stored child failure")

    try:
        with DurableFunctionTestRunner(
            handler=durable_execution(body, plugins=[plugin, Observe()]),
            poll_interval=0.01,
        ) as runner:
            result = runner.run(input="{}")
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == "caught"
        (info,) = hooks
        assert info.start_time is None
        assert info.status.value == "FAILED"
        assert info.error.message == "actual child failure"
        (span,) = [
            s for s in exporter.get_finished_spans() if s.name == "virtual-error"
        ]
        assert span.status.status_code.name == "ERROR"
        assert len(span.events) == 1
        assert span.events[0].attributes["exception.message"] == "actual child failure"
        assert span.start_time <= span.events[0].timestamp <= span.end_time
    finally:
        provider.shutdown()
