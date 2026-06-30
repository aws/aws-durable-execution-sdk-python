"""Tests for the OpenTelemetry durable execution plugin."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import opentelemetry.context as otel_context
import pytest
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
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.plugin import OtelPlugin


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


def _create_plugin() -> tuple[OtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = OtelPlugin(
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
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    active_wait_span = plugin._get_span(operation_id)
    assert active_wait_span is not None
    assert (
        active_wait_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=None,
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
    wait_span = spans_by_name["wait-for-signal"]
    invocation_span = spans_by_name["invocation"]
    assert wait_span.context.span_id == operation_id_to_span_id(operation_id)
    assert wait_span.parent.span_id == invocation_span.context.span_id
    assert wait_span.attributes["durable.operation.id"] == operation_id
    assert wait_span.attributes["durable.operation.type"] == OperationType.WAIT.value
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
    assert span.links[0].context.span_id == operation_id_to_span_id(operation_id)
    assert (
        span.attributes["durable.operation.status"] == OperationStatus.SUCCEEDED.value
    )


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


def test_context_span_omits_attempt_attributes():
    """CONTEXT operations do not carry per-attempt attributes.

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
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.attributes["durable.operation.type"] == OperationType.CONTEXT.value
    assert "durable.attempt.number" not in span.attributes
    assert "durable.attempt.outcome" not in span.attributes


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
    # Inside the step, the current span is the operation span.
    assert (
        trace.get_current_span().get_span_context().span_id
        == operation_id_to_span_id(operation_id)
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
    """Verify code inside a step resolves to the operation span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))

    span_context = plugin.get_current_span_context()
    assert span_context is not None
    assert span_context.span_id == operation_id_to_span_id(operation_id)


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
    assert (
        trace.get_current_span().get_span_context().span_id
        == operation_id_to_span_id(inner_step_id)
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
