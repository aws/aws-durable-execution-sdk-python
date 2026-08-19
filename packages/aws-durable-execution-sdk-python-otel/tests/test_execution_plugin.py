"""Tests for the execution-view OpenTelemetry plugin (Workflow-rooted trace)."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStatus,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionIncompleteInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _assert_otel_context_balanced():
    """Assert each test leaves the OTel thread-local context as it found it.

    The plugin pairs every context.attach() with a context.detach(), so no
    global reset is needed to keep tests isolated. Asserting the invariant here
    turns a plugin lifecycle leak into a test failure instead of hiding it.
    """
    before = otel_context.get_current()
    yield
    assert otel_context.get_current() == before, (
        "test leaked OTel context state: an attach() was not detached"
    )


def _create_plugin() -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    """Create an ExecutionOtelPlugin wired to an in-memory exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )
    return plugin, exporter


def _invocation_start_info() -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )


def _invocation_end_info(
    status: InvocationStatus = InvocationStatus.SUCCEEDED,
) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
        status=status,
        error=None,
    )


def test_operation_attributes_use_structural_user_function_marker():
    plugin, _ = _create_plugin()

    operation_attributes = plugin._operation_attributes(
        SimpleNamespace(
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        )
    )
    assert (
        operation_attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )

    user_function_attributes = plugin._operation_attributes(
        SimpleNamespace(
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
            is_replay_children=False,
        )
    )
    assert "durable.operation.status" not in user_function_attributes


# ---------------------------------------------------------------------------
# derive_workflow_span_id
# ---------------------------------------------------------------------------
def test_derive_workflow_span_id_is_deterministic():
    assert derive_workflow_span_id(EXECUTION_ARN) == derive_workflow_span_id(
        EXECUTION_ARN
    )


def test_derive_workflow_span_id_differs_by_arn():
    assert derive_workflow_span_id(EXECUTION_ARN) != derive_workflow_span_id(
        EXECUTION_ARN + "-other"
    )


def test_derive_workflow_span_id_is_64_bit():
    span_id = derive_workflow_span_id(EXECUTION_ARN)
    assert 0 < span_id < 2**64


def test_derive_workflow_span_id_rejects_empty_arn():
    with pytest.raises(ValueError, match="execution ARN is required"):
        derive_workflow_span_id("")


# ---------------------------------------------------------------------------
# Workflow + invocation span hierarchy
# ---------------------------------------------------------------------------
def test_workflow_and_invocation_are_separate_roots_without_ambient_parent():
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "Workflow" in spans
    assert "Invocation" in spans

    workflow = spans["Workflow"]
    invocation = spans["Invocation"]

    # Workflow is a root span (no parent) with the deterministic span ID.
    assert workflow.parent is None
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert workflow.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert (
        workflow.attributes["durable.execution.status"]
        == InvocationStatus.SUCCEEDED.value
    )

    # Without ambient context, Invocation starts a separate provider trace.
    assert invocation.parent is None
    assert invocation.context.trace_id != workflow.context.trace_id


def test_explicit_mode_invocation_span_parented_to_ambient_span():
    plugin, exporter = _create_plugin()

    ambient = plugin._provider.get_tracer("ambient").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        plugin.on_invocation_end(_invocation_end_info())
    finally:
        otel_context.detach(token)
        ambient.end()

    spans = {s.name: s for s in exporter.get_finished_spans()}
    workflow = spans["Workflow"]
    invocation = spans["Invocation"]
    assert invocation.parent is not None
    assert invocation.parent.span_id == ambient.get_span_context().span_id
    assert invocation.context.trace_id == ambient.get_span_context().trace_id
    assert workflow.context.trace_id != ambient.get_span_context().trace_id


def test_workflow_span_dropped_on_non_terminal_status():
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))

    names = [s.name for s in exporter.get_finished_spans()]
    # Invocation span is always ended/exported; the Workflow span is dropped
    # (not ended) on a non-terminal status, so it must not be exported.
    assert "Invocation" in names
    assert "Workflow" not in names


def test_operation_parented_under_workflow_and_linked_to_invocation():
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

    spans = {s.name: s for s in exporter.get_finished_spans()}
    workflow = spans["Workflow"]
    invocation = spans["Invocation"]
    operation = spans["wait-for-signal"]

    # Parented under the Workflow span (no parentId => Workflow fallback).
    assert operation.parent is not None
    assert operation.parent.span_id == workflow.context.span_id

    # Linked (not parented) to the invocation span.
    linked_span_ids = {link.context.span_id for link in operation.links}
    assert invocation.context.span_id in linked_span_ids


def test_cross_invocation_operation_end_uses_deterministic_span_id():
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    # Backend-updated completion for an operation started in a prior invocation.
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id="step-earlier",
            operation_type=OperationType.STEP,
            sub_type=None,
            name="earlier-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    matching = [s for s in exporter.get_finished_spans() if s.name == "earlier-step"]
    # Exported exactly once, using the deterministic logical-operation span ID
    # (no separate continuation span).
    assert len(matching) == 1
    assert matching[0].context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, "step-earlier"
    )


@pytest.mark.parametrize(
    ("outcome", "terminal_status", "error", "expected_span_status"),
    [
        (
            UserFunctionOutcome.SUCCEEDED,
            OperationStatus.SUCCEEDED,
            None,
            trace.StatusCode.OK,
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
            trace.StatusCode.ERROR,
        ),
    ],
)
def test_context_span_waits_for_terminal_operation_status(
    outcome,
    terminal_status,
    error,
    expected_span_status,
):
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "context-1"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            name="book-trip",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    active_span = plugin._get_span(operation_id)
    assert active_span is not None
    assert (
        active_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )

    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
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

    assert plugin._get_span(operation_id) is active_span
    assert (
        active_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )
    assert not exporter.get_finished_spans()

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
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
    assert span.attributes["durable.operation.status"] == terminal_status.value
    assert span.status.status_code is expected_span_status

    plugin.on_invocation_end(_invocation_end_info())


def test_step_attempt_span_omits_operation_status():
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"

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
    active_span = plugin._get_span("step-1:attempt:1")
    assert active_span is not None
    assert "durable.operation.status" not in active_span.attributes

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

    span = exporter.get_finished_spans()[0]
    assert (
        span.attributes["durable.attempt.outcome"]
        == UserFunctionOutcome.SUCCEEDED.value
    )
    assert "durable.operation.status" not in span.attributes

    plugin.on_invocation_end(_invocation_end_info())


# ---------------------------------------------------------------------------
# Default-provider mode: invocation span
# ---------------------------------------------------------------------------
def _create_default_mode_plugin(
    monkeypatch,
) -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    """ExecutionOtelPlugin in global (ADOT) mode wired to an in-memory exporter.

    The capture provider is installed as the global provider so
    the default configuration resolves to it, letting the test assert spans while
    exercising the ambient-parenting path.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )
    return plugin, exporter


def test_default_mode_creates_invocation_span(monkeypatch):
    plugin, exporter = _create_default_mode_plugin(monkeypatch)

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    spans = {s.name: s for s in exporter.get_finished_spans()}
    # The invocation span is now created even in default-provider mode.
    assert "Invocation" in spans
    invocation = spans["Invocation"]
    assert invocation.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert invocation.attributes["durable.invocation.first"] is True
    assert invocation.parent is None
    assert invocation.context.trace_id != spans["Workflow"].context.trace_id


def test_default_mode_invocation_span_parented_to_ambient_span(monkeypatch):
    plugin, exporter = _create_default_mode_plugin(monkeypatch)

    # Simulate the ambient Lambda invocation span from the ADOT layer.
    ambient = plugin._provider.get_tracer("ambient").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        plugin.on_invocation_end(_invocation_end_info())
    finally:
        otel_context.detach(token)
        ambient.end()

    invocation = {s.name: s for s in exporter.get_finished_spans()}["Invocation"]
    assert invocation.parent is not None
    assert invocation.parent.span_id == ambient.get_span_context().span_id
    assert invocation.context.trace_id == ambient.get_span_context().trace_id


def test_open_operation_span_not_exported_at_invocation_end():
    """A suspended operation (started, not ended) must not be exported.

    on_invocation_end drops the reference without ending it; the
    span is ended only when on_operation_end fires in a later invocation.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    # No on_operation_end: the operation suspended.
    plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))

    exported = {s.name for s in exporter.get_finished_spans()}
    # The open operation span is NOT exported (never ended).
    assert "wait-for-signal" not in exported


@pytest.mark.parametrize(
    ("status", "expected_code"),
    [
        (InvocationStatus.PENDING, trace.StatusCode.OK),
        (InvocationStatus.SUCCEEDED, trace.StatusCode.OK),
        (InvocationStatus.RETRY, trace.StatusCode.UNSET),
        (InvocationStatus.FAILED, trace.StatusCode.ERROR),
    ],
)
def test_invocation_span_status_kind_and_attributes(status, expected_code):
    """Invocation span is INTERNAL, carries status/first; SUCCEEDED/PENDING are
    OK, FAILED is ERROR, and RETRY is UNSET (STOPPED/TIMED_OUT indistinguishable)."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status=status))

    invocation = {s.name: s for s in exporter.get_finished_spans()}["Invocation"]
    assert invocation.kind is trace.SpanKind.INTERNAL
    assert invocation.attributes is not None
    assert invocation.attributes["durable.invocation.status"] == status.value
    assert invocation.attributes["durable.invocation.first"] is True
    assert invocation.status.status_code is expected_code


# ---------------------------------------------------------------------------
# Context attach/detach balance across the plugin lifecycle
# ---------------------------------------------------------------------------
def _step_start_info(
    operation_id: str,
    parent_id: str | None = None,
    attempt: int = 1,
) -> UserFunctionStartInfo:
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
    )


def _step_end_info(
    operation_id: str,
    parent_id: str | None = None,
    attempt: int = 1,
    outcome: UserFunctionOutcome = UserFunctionOutcome.SUCCEEDED,
) -> UserFunctionEndInfo:
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=operation_id,
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


def _step_incomplete_info(
    operation_id: str,
    parent_id: str | None = None,
    attempt: int = 1,
) -> UserFunctionIncompleteInfo:
    return UserFunctionIncompleteInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
    )


def _context_incomplete_info(
    operation_id: str, parent_id: str | None = None
) -> UserFunctionIncompleteInfo:
    return UserFunctionIncompleteInfo(
        operation_id=operation_id,
        operation_type=OperationType.CONTEXT,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )


def _context_start_info(
    operation_id: str, parent_id: str | None = None
) -> UserFunctionStartInfo:
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.CONTEXT,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )


def _context_end_info(
    operation_id: str, parent_id: str | None = None
) -> UserFunctionEndInfo:
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=OperationType.CONTEXT,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
        outcome=UserFunctionOutcome.SUCCEEDED,
        end_time=END_TIME,
        error=None,
    )


def test_workflow_span_is_current_during_the_invocation():
    """Verify the Workflow span is the active span while the invocation runs."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    assert (
        trace.get_current_span().get_span_context().span_id
        == plugin._workflow_span.get_span_context().span_id
    )

    plugin.on_invocation_end(_invocation_end_info())


@pytest.mark.parametrize(
    "status",
    [InvocationStatus.PENDING, InvocationStatus.SUCCEEDED, InvocationStatus.FAILED],
)
def test_invocation_end_restores_context_from_before_invocation_start(status):
    """Verify invocation cleanup leaves no plugin span current.

    A non-terminal invocation used to leave the Workflow span attached, so work
    after cleanup was parented to a span that had not ended.
    """
    plugin, _ = _create_plugin()
    before_context = otel_context.get_current()

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status=status))

    assert otel_context.get_current() == before_context
    assert not trace.get_current_span().get_span_context().is_valid
    assert plugin._context_tokens == {}


@pytest.mark.parametrize(
    "outcome", [UserFunctionOutcome.SUCCEEDED, UserFunctionOutcome.FAILED]
)
def test_step_scope_is_released_at_user_function_end(outcome):
    """Verify a finished step restores the context that enclosed it."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    enclosing_context = otel_context.get_current()

    plugin.on_user_function_start(_step_start_info("step-1"))
    attempt_span = plugin._get_span("step-1:attempt:1")
    assert attempt_span is not None
    assert (
        trace.get_current_span().get_span_context().span_id
        == attempt_span.get_span_context().span_id
    )

    plugin.on_user_function_end(_step_end_info("step-1", outcome=outcome))

    assert otel_context.get_current() == enclosing_context
    # Only the invocation-level scope remains open.
    assert set(plugin._context_tokens) == {"__invocation_context__"}

    plugin.on_invocation_end(_invocation_end_info())
    assert plugin._context_tokens == {}


def test_sequential_steps_do_not_accumulate_scopes():
    """Verify repeated steps unwind to the same enclosing context each time."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    enclosing_context = otel_context.get_current()

    for index in range(3):
        operation_id = f"step-{index}"
        plugin.on_user_function_start(_step_start_info(operation_id))
        plugin.on_user_function_end(_step_end_info(operation_id))
        assert otel_context.get_current() == enclosing_context
        assert set(plugin._context_tokens) == {"__invocation_context__"}

    plugin.on_invocation_end(_invocation_end_info())
    assert plugin._context_tokens == {}


def test_nested_scopes_are_released_without_accumulating():
    """Verify a child context and its inner step unwind to their entry contexts."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    context_id = "ctx-1"
    plugin.on_user_function_start(_context_start_info(context_id))
    inside_context = otel_context.get_current()
    context_span = plugin._get_span(context_id)
    assert context_span is not None

    plugin.on_user_function_start(_step_start_info("ctx-1-step", parent_id=context_id))
    plugin.on_user_function_end(_step_end_info("ctx-1-step", parent_id=context_id))

    # The inner step restored the child-context scope, not a copy of it.
    assert otel_context.get_current() == inside_context
    assert (
        trace.get_current_span().get_span_context().span_id
        == context_span.get_span_context().span_id
    )
    assert set(plugin._context_tokens) == {"__invocation_context__", context_id}

    plugin.on_user_function_end(_context_end_info(context_id))

    assert otel_context.get_current() == before_context
    assert set(plugin._context_tokens) == {"__invocation_context__"}

    plugin.on_invocation_end(_invocation_end_info())
    assert plugin._context_tokens == {}


def test_invocation_end_releases_scope_of_suspended_user_function():
    """Verify a user function that never ends does not leak its scope.

    A suspending user function raises before ``on_user_function_end`` runs, so
    invocation cleanup is what releases the scope it attached.
    """
    plugin, _ = _create_plugin()
    before_context = otel_context.get_current()
    plugin.on_invocation_start(_invocation_start_info())

    plugin.on_user_function_start(_step_start_info("step-suspends"))
    assert plugin._context_tokens

    plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))

    assert otel_context.get_current() == before_context
    assert plugin._context_tokens == {}


def test_warm_invocation_reuse_restores_ambient_span_each_time():
    """Verify repeated invocations leave the ambient Lambda span current."""
    plugin, _ = _create_plugin()
    ambient_provider = TracerProvider()
    ambient = ambient_provider.get_tracer("ambient").start_span("AmbientLambda")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        warm_context = otel_context.get_current()
        for index in range(3):
            plugin.on_invocation_start(_invocation_start_info())
            operation_id = f"step-{index}"
            plugin.on_user_function_start(_step_start_info(operation_id))
            plugin.on_user_function_end(_step_end_info(operation_id))
            plugin.on_invocation_end(_invocation_end_info())

            assert otel_context.get_current() == warm_context
            assert (
                trace.get_current_span().get_span_context().span_id
                == ambient.get_span_context().span_id
            )
            assert plugin._context_tokens == {}
    finally:
        otel_context.detach(token)
        ambient.end()


def test_detach_ignores_token_attached_on_another_thread():
    """Verify a scope attached on another thread is dropped, not reset here.

    A context token can only be reset on the thread that created it, so the
    plugin drops foreign tokens instead of asking OpenTelemetry to fail.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(
            plugin.on_user_function_start, _step_start_info("step-1")
        ).result()

    plugin._detach_context("step-1:attempt:1")

    assert "step-1:attempt:1" not in plugin._context_tokens
    assert otel_context.get_current() == before_context

    plugin.on_invocation_end(_invocation_end_info())


# ---------------------------------------------------------------------------
# Re-entering an operation whose scope was never released
# ---------------------------------------------------------------------------
def test_reentered_child_context_does_not_leave_abandoned_span_current():
    """Verify a timed in-process resume unwinds the abandoned scope.

    A suspended child context never reaches on_user_function_end, and the
    map/parallel coordinator can resume that branch in-process, re-entering the
    same operation ID on the same thread. Without releasing the first scope, the
    second scope's detach would restore the abandoned span and leave it current.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()
    context_id = "ctx-1"

    # First run: the child context suspends, so no end hook fires.
    plugin.on_user_function_start(_context_start_info(context_id))
    suspended_span = plugin._get_span(context_id)
    assert suspended_span is not None

    # Timed in-process resume re-enters the same operation.
    plugin.on_user_function_start(_context_start_info(context_id))
    assert len([key for key in plugin._context_tokens if key == context_id]) == 1

    plugin.on_user_function_end(_context_end_info(context_id))

    assert otel_context.get_current() == before_context
    assert context_id not in plugin._context_tokens
    assert (
        trace.get_current_span().get_span_context().span_id
        != suspended_span.get_span_context().span_id
    )

    plugin.on_invocation_end(_invocation_end_info())


def test_reentered_step_attempt_releases_the_previous_scope():
    """Verify re-entering the same attempt key unwinds the previous scope."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    plugin.on_user_function_start(_step_start_info("step-1"))
    plugin.on_user_function_start(_step_start_info("step-1"))
    plugin.on_user_function_end(_step_end_info("step-1"))

    assert otel_context.get_current() == before_context
    assert set(plugin._context_tokens) == {"__invocation_context__"}

    plugin.on_invocation_end(_invocation_end_info())
    assert plugin._context_tokens == {}


def test_suspension_releases_the_scope_on_the_originating_worker():
    """Verify the suspending worker releases its own scope.

    A suspended user function reports no outcome, so the SDK fires
    on_user_function_incomplete on the thread that ran it -- the only thread that
    can reset its context token. The worker is kept alive and probed to prove it
    is left clean even though the resume lands on a different thread.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()
    span_key = "step-1:attempt:1"

    with ThreadPoolExecutor(max_workers=1) as worker:

        def suspend_on_worker() -> tuple[int, bool]:
            plugin.on_user_function_start(_step_start_info("step-1"))
            attached_span_id = trace.get_current_span().get_span_context().span_id
            plugin.on_user_function_incomplete(_step_incomplete_info("step-1"))
            return (
                attached_span_id,
                trace.get_current_span().get_span_context().is_valid,
            )

        attached_span_id, span_still_current = worker.submit(suspend_on_worker).result()
        suspended_span = plugin._get_span(span_key)

        # The scope was released on the worker, and its span is left open.
        assert attached_span_id != 0
        assert span_still_current is False
        assert span_key not in plugin._context_tokens
        assert suspended_span is not None
        assert not exporter.get_finished_spans()

        # The timed resume lands on this thread, with nothing stale to unwind.
        plugin.on_user_function_start(_step_start_info("step-1"))
        assert plugin._context_tokens[span_key][0] == threading.get_ident()
        plugin.on_user_function_end(_step_end_info("step-1"))
        assert otel_context.get_current() == before_context

        # The originating worker is still clean.
        worker_span_valid = worker.submit(
            lambda: trace.get_current_span().get_span_context().is_valid
        ).result()
        assert worker_span_valid is False

    plugin.on_invocation_end(_invocation_end_info())


def test_nested_suspension_unwinds_scopes_in_reverse_order():
    """Verify nested suspends release inner-first and resume without stale scopes.

    The incomplete hook fires as the exception propagates outward, so the inner
    context's scope is released before its enclosing one. On resume, ending the
    inner operation restores the resumed outer scope rather than the one captured
    for the suspended run.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    plugin.on_user_function_start(_context_start_info("ctx-outer"))
    suspended_outer = plugin._get_span("ctx-outer")
    plugin.on_user_function_start(
        _context_start_info("ctx-inner", parent_id="ctx-outer")
    )
    assert suspended_outer is not None

    # Both contexts suspend: the inner one unwinds first.
    plugin.on_user_function_incomplete(
        _context_incomplete_info("ctx-inner", parent_id="ctx-outer")
    )
    assert trace.get_current_span() is suspended_outer

    plugin.on_user_function_incomplete(_context_incomplete_info("ctx-outer"))
    assert otel_context.get_current() == before_context
    assert set(plugin._context_tokens) == {"__invocation_context__"}
    # Neither span is ended: both operations are still in flight.
    assert not exporter.get_finished_spans()

    # The timed in-process resume replays both contexts, outer first.
    plugin.on_user_function_start(_context_start_info("ctx-outer"))
    resumed_outer = plugin._get_span("ctx-outer")
    plugin.on_user_function_start(
        _context_start_info("ctx-inner", parent_id="ctx-outer")
    )
    resumed_inner = plugin._get_span("ctx-inner")
    assert resumed_outer is not None
    assert resumed_outer is not suspended_outer
    assert trace.get_current_span() is resumed_inner

    plugin.on_user_function_end(_context_end_info("ctx-inner", parent_id="ctx-outer"))

    # The resumed outer scope is restored, not the one from the suspended run.
    assert trace.get_current_span() is resumed_outer

    plugin.on_user_function_end(_context_end_info("ctx-outer"))
    assert otel_context.get_current() == before_context
    assert set(plugin._context_tokens) == {"__invocation_context__"}

    plugin.on_invocation_end(_invocation_end_info())
    assert plugin._context_tokens == {}
