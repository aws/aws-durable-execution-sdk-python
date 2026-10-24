"""Factory-created invocations stay isolated through inherited Contexts."""

import contextvars
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)
from opentelemetry import context as otel_context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


@pytest.mark.parametrize(
    "view", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_copied_context_on_same_thread_does_not_clear_its_parent_invocation(view):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    a = contextvars.Context()
    info_a = dict(
        request_id="a",
        execution_arn="execution/a",
        execution_start_time=datetime.now(UTC),
        is_first_invocation=True,
    )
    info_b = dict(
        request_id="b",
        execution_arn="execution/b",
        execution_start_time=datetime.now(UTC),
        is_first_invocation=True,
    )
    plugin_a = factory.create_plugin(InvocationStartInfo(**info_a))
    plugin_b = factory.create_plugin(InvocationStartInfo(**info_b))
    assert plugin_a is not plugin_b
    try:
        original = a.run(otel_context.get_current)
        a.run(plugin_a.on_invocation_start, InvocationStartInfo(**info_a))
        span_a = a.run(plugin_a.get_current_span_context)
        b = a.copy()
        b.run(plugin_b.on_invocation_start, InvocationStartInfo(**info_b))
        b.run(
            plugin_b.on_invocation_end,
            InvocationEndInfo(**info_b, status=InvocationStatus.SUCCEEDED),
        )
        assert a.run(plugin_a.get_current_span_context) == span_a
        assert plugin_a._execution_arn == "execution/a" and plugin_a._tracing_enabled
        a.run(
            plugin_a.on_invocation_end,
            InvocationEndInfo(**info_a, status=InvocationStatus.SUCCEEDED),
        )
        assert a.run(otel_context.get_current) == original
        assert not plugin_a._tracing_enabled and not plugin_b._tracing_enabled
        workflows = [s for s in exporter.get_finished_spans() if s.name == "Workflow"]
        assert len(workflows) == 2
        assert {s.attributes["durable.execution.arn"] for s in workflows} == {
            "execution/a",
            "execution/b",
        }
        assert len({s.context.trace_id for s in workflows}) == 2
    finally:
        provider.shutdown()


@pytest.mark.parametrize(
    "view", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_factory_creates_fresh_state_after_previous_invocation_end(view):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
    factory = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    before = otel_context.get_current()
    first = dict(
        request_id="first",
        execution_arn="execution/first",
        execution_start_time=datetime.now(UTC),
        is_first_invocation=True,
    )
    second = dict(
        request_id="second",
        execution_arn="execution/second",
        execution_start_time=datetime.now(UTC),
        is_first_invocation=True,
    )
    try:
        first_plugin = factory.create_plugin(InvocationStartInfo(**first))
        first_plugin.on_invocation_start(InvocationStartInfo(**first))
        first_plugin.on_invocation_end(
            InvocationEndInfo(**first, status=InvocationStatus.PENDING)
        )
        assert first_plugin._context_tokens == {}
        assert not first_plugin._tracing_enabled
        plugin = factory.create_plugin(InvocationStartInfo(**second))
        assert plugin is not first_plugin
        assert plugin._pending_operation_ends == {}
        plugin.on_invocation_start(InvocationStartInfo(**second))
        plugin.on_invocation_end(
            InvocationEndInfo(**second, status=InvocationStatus.SUCCEEDED)
        )
        assert otel_context.get_current() == before
        assert plugin._context_tokens == {}
        assert not plugin._tracing_enabled
    finally:
        provider.shutdown()
