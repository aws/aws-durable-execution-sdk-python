"""Tests for the OTel context logging filter."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from aws_durable_execution_sdk_python.lambda_service import (
    InvocationStatus,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
import opentelemetry.context as otel_context
from opentelemetry import trace
import pytest
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import context_scope
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.log_filter import (
    OtelContextLogFilter,
    install_log_filter,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
    ProviderSource,
)


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _assert_otel_context_balanced():
    """Fail any test that leaves an OTel context scope attached."""
    before = otel_context.get_current()
    before_depth = context_scope.depth()
    yield
    assert context_scope.depth() == before_depth
    assert otel_context.get_current() is before


def _create_execution_plugin() -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    """Create an ExecutionOtelPlugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            provider_source=ProviderSource.EXPLICIT,
            tracer_provider=trace_provider,
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )
    return plugin, exporter


def _create_plugin(
    enrich_logger: bool = True,
) -> tuple[InvocationOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            provider_source=ProviderSource.EXPLICIT,
            tracer_provider=trace_provider,
            context_extractor=lambda _: Context(),
            enrich_logger=enrich_logger,
        )
    )
    return plugin, exporter


def _invocation_start_info() -> InvocationStartInfo:
    """Create standard invocation start info for tests."""
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )


def _user_function_start_info(operation_id: str) -> UserFunctionStartInfo:
    """Create standard user function start info for tests."""
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=None,
        name="fetch-user",
        parent_id=None,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )


def _invocation_end_info() -> InvocationEndInfo:
    """Create standard invocation end info for tests."""
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
        status=InvocationStatus.SUCCEEDED,
        error=None,
    )


def _user_function_end_info(operation_id: str) -> UserFunctionEndInfo:
    """Create standard user function end info for tests."""
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=None,
        name="fetch-user",
        parent_id=None,
        start_time=START_TIME,
        end_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        is_replay_children=False,
        attempt=1,
        outcome=UserFunctionOutcome.SUCCEEDED,
        error=None,
    )


def _make_record() -> logging.LogRecord:
    """Create a bare LogRecord for filtering."""
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )


def _remove_otel_filters(handler: logging.Handler) -> None:
    """Remove any OtelContextLogFilter from a handler (test cleanup)."""
    for log_filter in [
        f for f in handler.filters if isinstance(f, OtelContextLogFilter)
    ]:
        handler.removeFilter(log_filter)


def test_filter_always_returns_true():
    """The filter never drops a record, even with no active span."""
    plugin, _ = _create_plugin()
    log_filter = OtelContextLogFilter(plugin)

    assert log_filter.filter(_make_record()) is True


def test_filter_does_not_set_fields_without_active_span():
    """With no invocation active, the filter leaves the record unmodified."""
    plugin, _ = _create_plugin()
    log_filter = OtelContextLogFilter(plugin)

    record = _make_record()
    log_filter.filter(record)

    assert not hasattr(record, "traceId")
    assert not hasattr(record, "spanId")
    assert not hasattr(record, "otelTraceSampled")


def test_filter_injects_trace_context_from_invocation_span():
    """The filter stamps the invocation span context for top-level code."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    log_filter = OtelContextLogFilter(plugin)

    record = _make_record()
    log_filter.filter(record)

    assert len(record.traceId) == 32
    assert len(record.spanId) == 16
    assert isinstance(record.otelTraceSampled, bool)


def test_filter_uses_attempt_span_inside_user_function():
    """spanId reflects the active attempt span during user code."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))

    record = _make_record()
    OtelContextLogFilter(plugin).filter(record)

    attempt_span = plugin._get_span("step-1:attempt:1")
    assert attempt_span is not None
    expected_span_id = format(attempt_span.get_span_context().span_id, "016x")
    assert record.spanId == expected_span_id

    plugin.on_user_function_end(_user_function_end_info(operation_id))
    plugin.on_invocation_end(_invocation_end_info())


def test_execution_plugin_handler_thread_uses_the_invocation_span():
    """Handler-thread records correlate to the Invocation span, not Workflow.

    ExecutionOtelPlugin attaches nothing on the handler thread, so the filter
    resolves through the plugin registry, which prefers the Invocation span. The
    trace ID is shared with the Workflow span either way.
    """
    plugin, _ = _create_execution_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    record = _make_record()
    OtelContextLogFilter(plugin).filter(record)

    invocation_context = plugin._invocation_span.get_span_context()
    workflow_context = plugin._workflow_span.get_span_context()
    assert record.spanId == format(invocation_context.span_id, "016x")
    assert record.spanId != format(workflow_context.span_id, "016x")
    assert record.traceId == format(workflow_context.trace_id, "032x")

    plugin.on_invocation_end(_invocation_end_info())


def test_ambient_lambda_span_does_not_displace_the_invocation_span(monkeypatch):
    """An ambient ADOT span must not be reported in place of the durable span.

    In GLOBAL mode the Lambda invocation span from the ADOT layer stays current on
    the handler thread. Log records emitted there must still carry the durable
    Invocation span, so the filter only trusts the current span while the plugin
    holds an operation scope on that thread.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            provider_source=ProviderSource.GLOBAL,
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )

    ambient = provider.get_tracer("ambient").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        record = _make_record()
        OtelContextLogFilter(plugin).filter(record)

        ambient_span_id = format(ambient.get_span_context().span_id, "016x")
        invocation_span_id = format(
            plugin._invocation_span.get_span_context().span_id, "016x"
        )
        assert record.spanId == invocation_span_id
        assert record.spanId != ambient_span_id

        plugin.on_invocation_end(_invocation_end_info())
    finally:
        otel_context.detach(token)
        ambient.end()


def test_install_log_filter_attaches_to_handlers():
    """install_log_filter adds the filter to each handler on the target logger."""
    plugin, _ = _create_plugin()
    target = logging.getLogger("test.install")
    handler = logging.NullHandler()
    target.addHandler(handler)
    try:
        installed = install_log_filter(plugin, target_logger=target)

        assert isinstance(installed, OtelContextLogFilter)
        assert any(isinstance(f, OtelContextLogFilter) for f in handler.filters)
    finally:
        target.removeHandler(handler)


def test_install_log_filter_is_idempotent():
    """Repeated installs do not stack duplicate filters on a handler."""
    plugin, _ = _create_plugin()
    target = logging.getLogger("test.idempotent")
    handler = logging.NullHandler()
    target.addHandler(handler)
    try:
        install_log_filter(plugin, target_logger=target)
        install_log_filter(plugin, target_logger=target)

        otel_filters = [
            f for f in handler.filters if isinstance(f, OtelContextLogFilter)
        ]
        assert len(otel_filters) == 1
    finally:
        target.removeHandler(handler)


def test_install_log_filter_reuses_single_instance_across_handlers():
    """A single filter instance is shared across all handlers."""
    plugin, _ = _create_plugin()
    target = logging.getLogger("test.shared")
    handler_a = logging.NullHandler()
    handler_b = logging.NullHandler()
    target.addHandler(handler_a)
    target.addHandler(handler_b)
    try:
        installed = install_log_filter(plugin, target_logger=target)

        filter_a = next(
            f for f in handler_a.filters if isinstance(f, OtelContextLogFilter)
        )
        filter_b = next(
            f for f in handler_b.filters if isinstance(f, OtelContextLogFilter)
        )
        assert filter_a is filter_b is installed
    finally:
        target.removeHandler(handler_a)
        target.removeHandler(handler_b)


def test_install_log_filter_returns_none_without_handlers():
    """With no handlers, install_log_filter has nothing to attach to."""
    plugin, _ = _create_plugin()
    target = logging.getLogger("test.nohandlers")

    assert install_log_filter(plugin, target_logger=target) is None


def test_plugin_installs_filter_on_root_logger_at_construction():
    """The plugin installs the filter on the root logger when constructed."""
    root = logging.getLogger()
    handler = logging.NullHandler()
    root.addHandler(handler)
    try:
        _create_plugin(enrich_logger=True)

        assert any(isinstance(f, OtelContextLogFilter) for f in handler.filters)
    finally:
        for h in root.handlers:
            _remove_otel_filters(h)
        root.removeHandler(handler)


def test_plugin_skips_filter_when_disabled():
    """No filter is installed when enrich_logger is disabled."""
    root = logging.getLogger()
    handler = logging.NullHandler()
    root.addHandler(handler)
    try:
        _create_plugin(enrich_logger=False)

        assert not any(isinstance(f, OtelContextLogFilter) for f in handler.filters)
    finally:
        for h in root.handlers:
            _remove_otel_filters(h)
        root.removeHandler(handler)
