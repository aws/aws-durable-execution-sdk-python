"""Tests for the execution-view OpenTelemetry plugin (Workflow-rooted trace)."""

from __future__ import annotations

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
from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
    ProviderSource,
)


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _reset_otel_context():
    """Reset the OTel thread-local context around each test.

    The plugin attaches spans via context.attach() without detaching, so state
    would otherwise leak between tests running on the same thread.
    """
    token = otel_context.attach(Context())
    try:
        yield
    finally:
        otel_context.detach(token)


def _create_plugin() -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    """Create an ExecutionOtelPlugin wired to an in-memory exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            provider_source=ProviderSource.EXPLICIT,
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
def test_workflow_span_is_root_and_invocation_is_its_child():
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

    # Invocation is parented under the Workflow span.
    assert invocation.parent is not None
    assert invocation.parent.span_id == workflow.context.span_id


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


# ---------------------------------------------------------------------------
# Default-provider mode: invocation span
# ---------------------------------------------------------------------------
def _create_default_mode_plugin(
    monkeypatch,
) -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    """ExecutionOtelPlugin in GLOBAL (ADOT) mode wired to an in-memory exporter.

    The capture provider is installed as the global provider so
    ``provider_source=GLOBAL`` resolves to it, letting the test assert spans
    while exercising the ambient-parenting path.
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
