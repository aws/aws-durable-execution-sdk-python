"""Tests for the OTel context logging filter."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    UserFunctionStartInfo,
)
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel.log_filter import (
    OtelContextLogFilter,
    install_log_filter,
)
from aws_durable_execution_sdk_python_otel.plugin import OtelPlugin


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


def _create_plugin(
    enrich_logger: bool = True,
) -> tuple[OtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = OtelPlugin(
        trace_provider=trace_provider,
        context_extractor=lambda _: Context(),
        enrich_logger=enrich_logger,
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
