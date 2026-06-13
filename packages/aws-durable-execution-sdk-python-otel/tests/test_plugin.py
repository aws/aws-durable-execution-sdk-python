"""Tests for the OpenTelemetry durable execution plugin."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from aws_durable_execution_sdk_python.lambda_service import (
    InvocationStatus,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.plugin import DurableExecutionOtelPlugin


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


def _create_plugin() -> tuple[DurableExecutionOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = DurableExecutionOtelPlugin(
        trace_provider=trace_provider,
        context_extractor=lambda _: Context(),
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


def _invocation_end_info() -> InvocationEndInfo:
    """Create standard invocation end info for tests."""
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        start_time=START_TIME,
        is_first_invocation=True,
        status=InvocationStatus.SUCCEEDED,
        end_time=END_TIME,
        error=None,
    )


def test_invocation_start_and_end_emit_invocation_span():
    """Verify invocation lifecycle callbacks create and finish the root span."""
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    assert plugin._get_span(None) is not None

    plugin.on_invocation_end(_invocation_end_info())

    spans = exporter.get_finished_spans()
    assert [span.name for span in spans] == ["invocation"]
    assert spans[0].attributes["durable.execution.arn"] == EXECUTION_ARN
    assert plugin._get_span(None) is None


def test_operation_callbacks_emit_child_span_with_deterministic_span_id():
    """Verify non-user-function operations are traced beneath the invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-1"

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=None,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=None,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {span.name: span for span in exporter.get_finished_spans()}
    wait_span = spans_by_name["wait-for-signal"]
    invocation_span = spans_by_name["invocation"]
    assert wait_span.context.span_id == operation_id_to_span_id(operation_id)
    assert wait_span.parent.span_id == invocation_span.context.span_id
    assert wait_span.attributes["durable.operation.id"] == operation_id
    assert wait_span.attributes["durable.operation.type"] == OperationType.WAIT.value


def test_operation_end_without_start_emits_continuation_span_with_link():
    """Verify completed existing operations link to their logical operation span."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-existing"
    random_span_id = int("1234567890abcdef", 16)
    plugin._id_generator._fallback_id_generator.generate_span_id = lambda: (
        random_span_id
    )

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=None,
            name="existing-wait",
            parent_id=None,
            start_time=START_TIME,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "existing-wait"
    assert span.context.span_id == random_span_id
    assert span.links[0].context.span_id == operation_id_to_span_id(operation_id)


def test_user_function_callbacks_emit_attempt_span_attributes():
    """Verify user-function end refreshes all extractable span attributes."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"

    start_info = UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=None,
        name="fetch-user",
        parent_id=None,
        start_time=START_TIME,
        is_replay_children=False,
        attempt=1,
    )
    plugin.on_user_function_start(start_info)
    active_span = plugin._get_span(operation_id)
    assert active_span is not None
    active_span.set_attributes(
        {
            "durable.operation.name": "stale-name",
            "durable.attempt.number": 99,
        }
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replay_children=False,
            attempt=1,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user"
    assert span.context.span_id == operation_id_to_span_id(operation_id)
    assert span.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert span.attributes["durable.operation.id"] == operation_id
    assert span.attributes["durable.operation.type"] == OperationType.STEP.value
    assert span.attributes["durable.operation.name"] == "fetch-user"
    assert span.attributes["durable.attempt.number"] == 1
    assert (
        span.attributes["durable.attempt.outcome"]
        == UserFunctionOutcome.SUCCEEDED.value
    )


def test_span_registry_helpers_can_be_called_from_multiple_threads():
    """Verify active span registry helpers are safe under concurrent access."""
    plugin, _ = _create_plugin()

    def update_span(index: int) -> None:
        operation_id = f"operation-{index}"
        plugin._set_span(operation_id, object())  # type: ignore[arg-type]
        assert plugin._get_span(operation_id) is not None
        plugin._delete_span(operation_id)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(update_span, range(100)))

    with plugin._operation_spans_lock:
        assert plugin._operation_spans == {}
