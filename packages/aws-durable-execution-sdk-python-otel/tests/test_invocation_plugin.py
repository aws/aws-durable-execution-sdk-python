"""Tests for the OpenTelemetry durable execution plugin."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    InvocationStatus,
    OperationStatus,
    OperationSubType,
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
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _reset_otel_context():
    """Reset the OTel thread-local context before and after each test.

    The plugin attaches spans via context.attach() without ever detaching,
    so state would otherwise leak between tests running on the same thread.
    """
    token = otel_context.attach(Context())
    try:
        yield
    finally:
        otel_context.detach(token)


def _create_plugin() -> tuple[InvocationOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=trace_provider,
            context_extractor=lambda _: Context(),
        )
    )
    return plugin, exporter


def _invocation_start_info(
    is_first_invocation: bool = True,
) -> InvocationStartInfo:
    """Create standard invocation start info for tests."""
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=is_first_invocation,
    )


def _invocation_end_info(
    status: InvocationStatus = InvocationStatus.SUCCEEDED,
) -> InvocationEndInfo:
    """Create standard invocation end info for tests."""
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
        status=status,
        error=None,
    )


def _user_function_start_info(
    operation_id: str,
    attempt: int = 1,
    parent_id: str | None = None,
    operation_type: OperationType = OperationType.STEP,
) -> UserFunctionStartInfo:
    """Create standard user function start info for tests."""
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=f"step-{operation_id}",
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
    )


def _user_function_end_info(
    operation_id: str,
    outcome: UserFunctionOutcome = UserFunctionOutcome.SUCCEEDED,
    attempt: int = 1,
    parent_id: str | None = None,
    operation_type: OperationType = OperationType.STEP,
) -> UserFunctionEndInfo:
    """Create standard user function end info for tests."""
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=f"step-{operation_id}",
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
        outcome=outcome,
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
    spans_by_name = {span.name: span for span in spans}
    # Terminal invocation also exports the Workflow span.
    assert set(spans_by_name) == {"Invocation", "Workflow"}
    invocation = spans_by_name["Invocation"]
    assert invocation.kind is SpanKind.INTERNAL
    assert invocation.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert invocation.attributes["durable.invocation.first"] is True
    assert (
        invocation.attributes["durable.invocation.status"]
        == InvocationStatus.SUCCEEDED.value
    )
    assert plugin._get_span(None) is None


def test_invocation_span_records_subsequent_invocation():
    """Invocation spans preserve a false first-invocation attribute."""
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info(is_first_invocation=False))
    plugin.on_invocation_end(_invocation_end_info())

    spans = exporter.get_finished_spans()
    invocation = next(s for s in spans if s.name == "Invocation")
    assert invocation.attributes["durable.invocation.first"] is False


@pytest.mark.parametrize(
    ("invocation_status", "expected_span_status"),
    [
        (InvocationStatus.PENDING, StatusCode.OK),
        (InvocationStatus.RETRY, StatusCode.UNSET),
        (InvocationStatus.SUCCEEDED, StatusCode.OK),
        (InvocationStatus.FAILED, StatusCode.ERROR),
    ],
)
def test_invocation_span_status_reflects_execution_status(
    invocation_status: InvocationStatus,
    expected_span_status: StatusCode,
):
    """Only terminal invocation spans receive a success or failure status."""
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(invocation_status))

    spans = exporter.get_finished_spans()
    invocation = next(s for s in spans if s.name == "Invocation")
    attributes = invocation.attributes
    assert attributes is not None
    assert attributes["durable.invocation.status"] == invocation_status.value
    assert invocation.status.status_code is expected_span_status


def test_invocation_end_closes_callback_child_before_parent_context():
    """Invocation shutdown preserves containment for open callback spans."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    context_start_time = datetime.now(UTC)
    context_id = "callback-context"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=context_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.WAIT_FOR_CALLBACK,
            name="wait for callback",
            parent_id=None,
            start_time=context_start_time,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="callback",
            operation_type=OperationType.CALLBACK,
            sub_type=OperationSubType.CALLBACK,
            name="create callback id",
            parent_id=context_id,
            start_time=context_start_time,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )

    plugin.on_invocation_end(_invocation_end_info(InvocationStatus.PENDING))

    spans = {span.name: span for span in exporter.get_finished_spans()}
    invocation_span = spans["Invocation"]
    context_span = spans["wait for callback"]
    callback_span = spans["create callback id"]
    assert callback_span.parent is not None
    assert callback_span.parent.span_id == context_span.context.span_id
    assert callback_span.end_time <= context_span.end_time <= invocation_span.end_time


def test_operation_callbacks_emit_child_span_with_deterministic_span_id():
    """Verify non-user-function operations are traced beneath the invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-1"

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    active_wait_span = plugin._get_span(operation_id)
    assert active_wait_span is not None
    invocation_span = plugin._get_span(None)
    assert invocation_span is not None
    assert invocation_span.start_time <= active_wait_span.start_time
    assert (
        active_wait_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )
    assert (
        active_wait_span.attributes["durable.operation.subtype"]
        == OperationSubType.WAIT.value
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {span.name: span for span in exporter.get_finished_spans()}
    assert all(span.kind is SpanKind.INTERNAL for span in spans_by_name.values())
    wait_span = spans_by_name["wait-for-signal"]
    invocation_span = spans_by_name["Invocation"]
    assert wait_span.context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, operation_id
    )
    assert wait_span.parent.span_id == invocation_span.context.span_id
    assert (
        invocation_span.start_time
        <= wait_span.start_time
        <= wait_span.end_time
        <= invocation_span.end_time
    )
    assert wait_span.attributes["durable.operation.id"] == operation_id
    assert wait_span.attributes["durable.operation.type"] == OperationType.WAIT.value
    assert (
        wait_span.attributes["durable.operation.subtype"] == OperationSubType.WAIT.value
    )
    assert (
        wait_span.attributes["durable.operation.status"]
        == OperationStatus.SUCCEEDED.value
    )


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
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "existing-wait"
    assert span.context.span_id == random_span_id
    assert span.links[0].context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, operation_id
    )
    assert (
        span.attributes["durable.operation.status"] == OperationStatus.SUCCEEDED.value
    )


def test_continuation_span_uses_current_start_and_end_times():
    """Continuation spans use current times within the invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span = plugin._get_span(None)
    assert invocation_span is not None
    before_callback = time.time_ns()

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id="fast-step",
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fast-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    after_callback = time.time_ns()

    span = exporter.get_finished_spans()[0]
    assert invocation_span.start_time <= span.start_time
    assert before_callback <= span.start_time <= span.end_time <= after_callback


def test_retried_operation_start_emits_continuation_span_with_link():
    """Retried operation spans should not reuse the original deterministic span ID."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-retried"
    random_span_id = int("abcdef1234567890", 16)
    plugin._id_generator._fallback_id_generator.generate_span_id = lambda: (
        random_span_id
    )

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="retried-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=True,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="retried-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "retried-step"
    assert span.context.span_id == random_span_id
    assert span.links[0].context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, operation_id
    )


def test_step_operation_span_parents_attempt_span():
    """STEP operations have a logical span with attempt spans beneath it."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    step_span = plugin._get_span(operation_id)
    assert step_span is not None
    assert step_span.name == "fetch-user"
    assert step_span.context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, operation_id
    )

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    active_attempt_span = trace.get_current_span()
    assert active_attempt_span.parent.span_id == step_span.context.span_id
    assert active_attempt_span.get_span_context().span_id != step_span.context.span_id

    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {span.name: span for span in exporter.get_finished_spans()}
    finished_step_span = spans_by_name["fetch-user"]
    attempt_span = spans_by_name["fetch-user attempt 1"]
    assert finished_step_span.kind is SpanKind.INTERNAL
    assert attempt_span.kind is SpanKind.INTERNAL
    assert attempt_span.parent.span_id == finished_step_span.context.span_id
    assert (
        finished_step_span.attributes["durable.operation.status"]
        == OperationStatus.SUCCEEDED.value
    )
    assert attempt_span.attributes["durable.attempt.number"] == 1
    assert "durable.operation.status" not in attempt_span.attributes


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
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )
    plugin.on_user_function_start(start_info)
    active_span = plugin._get_span("step-1:attempt:1")
    assert active_span is not None
    assert "durable.operation.status" not in active_span.attributes
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
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user attempt 1"
    assert span.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert span.attributes["durable.operation.id"] == operation_id
    assert span.attributes["durable.operation.type"] == OperationType.STEP.value
    assert span.attributes["durable.operation.name"] == "fetch-user"
    assert span.attributes["durable.attempt.number"] == 1
    assert (
        span.attributes["durable.attempt.outcome"]
        == UserFunctionOutcome.SUCCEEDED.value
    )
    assert "durable.operation.status" not in span.attributes


def test_step_attempt_span_name_includes_attempt_number():
    """Step attempt spans include the attempt number in the display name."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-retry"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=2,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=2,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user attempt 2"


def test_step_attempt_span_name_defaults_to_first_attempt():
    """Step attempt spans default to attempt 1 when no attempt is provided."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-no-attempt"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=None,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=None,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user attempt 1"


@pytest.mark.parametrize(
    ("outcome", "terminal_status", "error", "expected_span_status"),
    [
        (
            UserFunctionOutcome.SUCCEEDED,
            OperationStatus.SUCCEEDED,
            None,
            StatusCode.OK,
        ),
        (
            UserFunctionOutcome.SUCCEEDED,
            OperationStatus.FAILED,
            ErrorObject(
                message="serialization failed",
                type="SerializationError",
                data=None,
                stack_trace=None,
            ),
            StatusCode.ERROR,
        ),
    ],
)
def test_context_span_waits_for_terminal_status_and_omits_attempt_attributes(
    outcome,
    terminal_status,
    error,
    expected_span_status,
):
    """Completed CONTEXT spans carry terminal status and no attempt attributes.

    durable.attempt.number and durable.attempt.outcome are meaningful for
    STEP operations (each retry is an attempt) but not for CONTEXT, so the
    plugin omits them on CONTEXT spans for cross-SDK consistency.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "ctx-1"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="book-trip",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="book-trip",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
            outcome=outcome,
            end_time=END_TIME,
            error=None,
        )
    )

    active_span = plugin._get_span(operation_id)
    assert active_span is not None
    assert (
        active_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )
    assert not exporter.get_finished_spans()

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="book-trip",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=terminal_status,
            end_time=END_TIME,
            error=error,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.attributes["durable.operation.type"] == OperationType.CONTEXT.value
    assert span.attributes["durable.operation.status"] == terminal_status.value
    assert "durable.attempt.number" not in span.attributes
    assert "durable.attempt.outcome" not in span.attributes
    assert span.status.status_code is expected_span_status


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


# ----------------------------------------------------------------------
# on_user_function_end restores the invocation span to the context
# ----------------------------------------------------------------------
def test_user_function_end_restores_invocation_span():
    """Verify the invocation span is current again after a step completes."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id

    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    # Inside the step, the current span is the attempt span.
    active_attempt_span = plugin._get_span("step-1:attempt:1")
    assert active_attempt_span is not None
    assert (
        trace.get_current_span().get_span_context().span_id
        == active_attempt_span.get_span_context().span_id
    )

    plugin.on_user_function_end(_user_function_end_info(operation_id))

    # After the step, the invocation span is restored.
    assert trace.get_current_span().get_span_context().span_id == invocation_span_id


def test_user_function_end_restores_invocation_span_on_failure():
    """Verify the invocation span is restored even when the step fails."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id

    operation_id = "step-fail"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(
        _user_function_end_info(operation_id, outcome=UserFunctionOutcome.FAILED)
    )

    assert trace.get_current_span().get_span_context().span_id == invocation_span_id


def test_user_function_end_restores_invocation_span_across_multiple_steps():
    """Verify between-step context is the invocation span across many steps."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id

    for index in range(3):
        operation_id = f"step-{index}"
        plugin.on_user_function_start(_user_function_start_info(operation_id))
        plugin.on_user_function_end(_user_function_end_info(operation_id))
        # Between each step, the invocation span is the current span.
        assert trace.get_current_span().get_span_context().span_id == invocation_span_id


# ----------------------------------------------------------------------
# get_current_span_context resolves the right span context
# ----------------------------------------------------------------------
def test_get_current_span_context_returns_none_before_invocation_start():
    """Verify no span context is returned when nothing is active."""
    plugin, _ = _create_plugin()

    assert plugin.get_current_span_context() is None


def test_get_current_span_context_returns_invocation_span_at_top_level():
    """Verify top-level code resolves to the invocation span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    span_context = plugin.get_current_span_context()
    invocation_span = plugin._get_span(None)
    assert span_context is not None
    assert span_context.span_id == invocation_span.get_span_context().span_id


def test_get_current_span_context_returns_operation_span_inside_step():
    """Verify code inside a step resolves to the attempt span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))

    span_context = plugin.get_current_span_context()
    active_attempt_span = plugin._get_span("step-1:attempt:1")
    assert span_context is not None
    assert active_attempt_span is not None
    assert span_context.span_id == active_attempt_span.get_span_context().span_id


def test_get_current_span_context_returns_invocation_span_between_steps():
    """Verify between-step code resolves back to the invocation span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(_user_function_end_info(operation_id))

    span_context = plugin.get_current_span_context()
    invocation_span = plugin._get_span(None)
    assert span_context is not None
    assert span_context.span_id == invocation_span.get_span_context().span_id


# ----------------------------------------------------------------------
# on_user_function_end restores the ENCLOSING operation span (nested case)
# ----------------------------------------------------------------------
def test_user_function_end_restores_parent_context_span_for_nested_step():
    """Verify ending a nested step restores its enclosing child-context span.

    Inside a child context, code that runs after an inner step (e.g. between
    inner steps) must correlate to the child context span, not the invocation.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    # Enter a child context (CONTEXT operation at the top level).
    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    context_span_id = trace.get_current_span().get_span_context().span_id

    # Run an inner step whose parent is the child context.
    inner_step_id = "ctx-1-step"
    plugin.on_user_function_start(
        _user_function_start_info(inner_step_id, parent_id=context_id)
    )
    active_attempt_span = plugin._get_span("ctx-1-step:attempt:1")
    assert active_attempt_span is not None
    assert (
        trace.get_current_span().get_span_context().span_id
        == active_attempt_span.get_span_context().span_id
    )

    plugin.on_user_function_end(
        _user_function_end_info(inner_step_id, parent_id=context_id)
    )

    # After the inner step, the enclosing child-context span is current again,
    # NOT the invocation span.
    assert trace.get_current_span().get_span_context().span_id == context_span_id
    assert (
        trace.get_current_span().get_span_context().span_id
        != plugin._get_span(None).get_span_context().span_id
    )


def test_user_function_end_falls_back_to_invocation_when_parent_missing():
    """Verify a top-level step (parent_id=None) restores the invocation span."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id

    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(_user_function_end_info(operation_id))

    assert trace.get_current_span().get_span_context().span_id == invocation_span_id


def test_get_current_span_context_returns_context_span_between_nested_steps():
    """Verify between-step code inside a child context resolves to that context.

    This is the log-correlation path: after an inner step completes,
    get_current_span_context must return the enclosing child-context span so
    logs emitted between inner steps correlate to the child context.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    context_span = plugin._get_span(context_id)

    inner_step_id = "ctx-1-step"
    plugin.on_user_function_start(
        _user_function_start_info(inner_step_id, parent_id=context_id)
    )
    plugin.on_user_function_end(
        _user_function_end_info(inner_step_id, parent_id=context_id)
    )

    span_context = plugin.get_current_span_context()
    assert span_context is not None
    assert span_context.span_id == context_span.get_span_context().span_id
    assert span_context.span_id != plugin._get_span(None).get_span_context().span_id


def test_nested_steps_restore_context_span_across_multiple_iterations():
    """Verify each inner step restores the child-context span between iterations."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    context_span_id = trace.get_current_span().get_span_context().span_id

    for index in range(3):
        inner_step_id = f"ctx-1-step-{index}"
        plugin.on_user_function_start(
            _user_function_start_info(inner_step_id, parent_id=context_id)
        )
        plugin.on_user_function_end(
            _user_function_end_info(inner_step_id, parent_id=context_id)
        )
        # Between each inner step, the child-context span is current.
        assert trace.get_current_span().get_span_context().span_id == context_span_id


@pytest.mark.parametrize(
    ("status", "expected_code"),
    [
        (InvocationStatus.SUCCEEDED, StatusCode.OK),
        (InvocationStatus.FAILED, StatusCode.ERROR),
    ],
)
def test_workflow_span_exported_on_terminal(status, expected_code):
    """A terminal invocation exports a deterministic Workflow root span."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status))

    workflow = next(s for s in exporter.get_finished_spans() if s.name == "Workflow")
    # Root span: no parent.
    assert workflow.parent is None
    assert workflow.kind is SpanKind.INTERNAL
    # Deterministic span id derived from the execution ARN.
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert workflow.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert workflow.attributes["durable.execution.status"] == status.value
    assert workflow.status.status_code is expected_code
    # Anchored to the execution start time.
    assert workflow.start_time == int(START_TIME.timestamp() * 1_000_000_000)


@pytest.mark.parametrize("status", [InvocationStatus.PENDING, InvocationStatus.RETRY])
def test_workflow_span_not_exported_on_non_terminal(status):
    """Non-terminal invocations do not export (end) the Workflow span."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status))

    names = [s.name for s in exporter.get_finished_spans()]
    assert "Workflow" not in names
    assert "Invocation" in names


def test_operation_span_links_to_workflow_span():
    """Operation spans link to the Workflow span while parented to invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-1"
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {s.name: s for s in exporter.get_finished_spans()}
    op_span = spans_by_name["wait-for-signal"]
    workflow_span_id = derive_workflow_span_id(EXECUTION_ARN)
    linked_span_ids = {link.context.span_id for link in op_span.links}
    assert workflow_span_id in linked_span_ids
    # Still parented to the invocation span (not the Workflow span).
    assert op_span.parent is not None
    assert op_span.parent.span_id == spans_by_name["Invocation"].context.span_id


def test_workflow_span_name_is_configurable():
    """The Workflow span name can be overridden via constructor kwarg."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=trace_provider,
            context_extractor=lambda _: Context(),
            workflow_span_name="MyExecution",
        )
    )
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    names = [s.name for s in exporter.get_finished_spans()]
    assert "MyExecution" in names
    assert "Workflow" not in names
