"""Tests for the OTel-enriched logger."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

from aws_durable_execution_sdk_python.lambda_service import OperationType
from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    UserFunctionStartInfo,
)
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel.logger import OtelEnrichedLogger
from aws_durable_execution_sdk_python_otel.plugin import DurableExecutionOtelPlugin


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


def _create_plugin(
    enrich_logger: bool = True,
) -> tuple[DurableExecutionOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = DurableExecutionOtelPlugin(
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
        start_time=START_TIME,
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
        is_replay_children=False,
        attempt=1,
    )


def test_wrap_logger_returns_enriched_logger_when_enabled():
    """Verify wrap_logger wraps the logger when enrich_logger is enabled."""
    plugin, _ = _create_plugin(enrich_logger=True)
    inner = Mock()

    wrapped = plugin.wrap_logger(inner)

    assert isinstance(wrapped, OtelEnrichedLogger)


def test_wrap_logger_returns_none_when_disabled():
    """Verify wrap_logger is a no-op when enrich_logger is disabled."""
    plugin, _ = _create_plugin(enrich_logger=False)
    inner = Mock()

    assert plugin.wrap_logger(inner) is None


def test_wrap_logger_is_idempotent():
    """Verify wrap_logger does not double-wrap an already-wrapped logger."""
    plugin, _ = _create_plugin(enrich_logger=True)
    inner = Mock()

    wrapped = plugin.wrap_logger(inner)
    assert plugin.wrap_logger(wrapped) is None


def test_enriched_logger_delegates_to_inner():
    """Verify all log levels delegate to the underlying logger."""
    plugin, _ = _create_plugin()
    inner = Mock()
    logger = OtelEnrichedLogger(inner=inner, plugin=plugin)

    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.exception("exception message")

    inner.debug.assert_called_once()
    inner.info.assert_called_once()
    inner.warning.assert_called_once()
    inner.error.assert_called_once()
    inner.exception.assert_called_once()


def test_enriched_logger_injects_trace_id_from_invocation_span():
    """Verify trace_id is injected from the plugin's invocation span."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    inner = Mock()
    logger = OtelEnrichedLogger(inner=inner, plugin=plugin)

    logger.info("hello")

    _, kwargs = inner.info.call_args
    extra = kwargs["extra"]
    assert "otel.trace_id" in extra
    assert len(extra["otel.trace_id"]) == 32
    assert "otel.span_id" in extra
    assert len(extra["otel.span_id"]) == 16
    assert "otel.trace_sampled" in extra


def test_enriched_logger_uses_operation_span_inside_user_function():
    """Verify span_id reflects the active operation span during user code."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))

    inner = Mock()
    logger = OtelEnrichedLogger(inner=inner, plugin=plugin)
    logger.info("inside step")

    _, kwargs = inner.info.call_args
    operation_span = plugin._get_span(operation_id)
    expected_span_id = format(operation_span.get_span_context().span_id, "016x")
    assert kwargs["extra"]["otel.span_id"] == expected_span_id


def test_enriched_logger_preserves_existing_extra():
    """Verify caller-provided extra fields are preserved alongside otel fields."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    inner = Mock()
    logger = OtelEnrichedLogger(inner=inner, plugin=plugin)

    logger.info("hello", extra={"order_id": "123"})

    _, kwargs = inner.info.call_args
    assert kwargs["extra"]["order_id"] == "123"
    assert "otel.trace_id" in kwargs["extra"]


def test_enriched_logger_handles_none_extra():
    """Verify None extra is handled without error."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    inner = Mock()
    logger = OtelEnrichedLogger(inner=inner, plugin=plugin)

    logger.info("hello", extra=None)

    _, kwargs = inner.info.call_args
    assert isinstance(kwargs["extra"], dict)


def test_enriched_logger_passes_positional_args():
    """Verify positional format args are forwarded to the inner logger."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    inner = Mock()
    logger = OtelEnrichedLogger(inner=inner, plugin=plugin)

    logger.info("hello %s %s", "a", "b")

    args, _ = inner.info.call_args
    assert args == ("hello %s %s", "a", "b")
