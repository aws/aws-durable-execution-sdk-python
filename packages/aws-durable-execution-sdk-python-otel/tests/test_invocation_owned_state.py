"""Invocation-owned state must not be reset through an inherited Context."""

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
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_copied_context_on_same_thread_does_not_clear_its_parent_invocation(view):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
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
    try:
        original = a.run(otel_context.get_current)
        a.run(plugin.on_invocation_start, InvocationStartInfo(**info_a))
        state_a = a.run(plugin._state.get)
        span_a = a.run(plugin.get_current_span_context)
        b = a.copy()
        b.run(plugin.on_invocation_start, InvocationStartInfo(**info_b))
        state_b = b.run(plugin._state.get)
        assert state_a is not state_b
        b.run(
            plugin.on_invocation_end,
            InvocationEndInfo(**info_b, status=InvocationStatus.SUCCEEDED),
        )
        assert a.run(plugin._state.get) is state_a
        assert a.run(plugin.get_current_span_context) == span_a
        assert state_a.execution_arn == "execution/a" and state_a.tracing_enabled
        a.run(
            plugin.on_invocation_end,
            InvocationEndInfo(**info_a, status=InvocationStatus.SUCCEEDED),
        )
        assert a.run(otel_context.get_current) == original
        assert plugin._state not in a
        workflows = [s for s in exporter.get_finished_spans() if s.name == "Workflow"]
        assert len(workflows) == 2
        assert {s.attributes["durable.execution.arn"] for s in workflows} == {
            "execution/a",
            "execution/b",
        }
        assert len({s.context.trace_id for s in workflows}) == 2
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_same_owner_warm_restart_releases_abandoned_scope(view):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
    plugin = view(
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
        plugin.on_invocation_start(InvocationStartInfo(**first))
        abandoned = plugin._state.get()
        plugin.on_invocation_start(InvocationStartInfo(**second))
        assert abandoned.context_tokens == {}
        assert abandoned.owner_token is None
        plugin.on_invocation_end(
            InvocationEndInfo(**second, status=InvocationStatus.SUCCEEDED)
        )
        assert otel_context.get_current() == before
        assert plugin._state not in contextvars.copy_context()
    finally:
        provider.shutdown()
