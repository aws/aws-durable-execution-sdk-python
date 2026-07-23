"""Tests for the execution-view OpenTelemetry plugin (Workflow-rooted trace)."""

from __future__ import annotations

from datetime import UTC, datetime

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
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
)
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    derive_workflow_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
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
    assert "invocation" in spans

    workflow = spans["Workflow"]
    invocation = spans["invocation"]

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
    assert "invocation" in names
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
    invocation = spans["invocation"]
    operation = spans["wait-for-signal"]

    # Parented under the Workflow span (no parentId => Workflow fallback).
    assert operation.parent is not None
    assert operation.parent.span_id == workflow.context.span_id

    # Linked (not parented) to the invocation span.
    linked_span_ids = {link.context.span_id for link in operation.links}
    assert invocation.context.span_id in linked_span_ids


def test_cross_invocation_operation_end_without_start_is_stitched():
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    # Operation end with no matching start (started in a prior invocation).
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id="step-earlier",
            operation_type=OperationType.STEP,
            sub_type=None,
            name="earlier-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=True,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "earlier-step" in spans
    stitched = spans["earlier-step"]
    # Stitched span links back to the deterministic logical-operation span.
    assert len(stitched.links) >= 1


# ---------------------------------------------------------------------------
# Default-provider mode: invocation span (JS PR #756 divergence)
# ---------------------------------------------------------------------------
def _create_default_mode_plugin() -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    """ExecutionOtelPlugin in default-provider mode wired to an in-memory exporter.

    Passing an explicit ``tracer_provider`` lets the test capture spans while
    ``use_default_tracer_provider=True`` still selects the default-mode code path.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            use_default_tracer_provider=True,
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )
    return plugin, exporter


def test_default_mode_creates_invocation_span():
    plugin, exporter = _create_default_mode_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    spans = {s.name: s for s in exporter.get_finished_spans()}
    # The invocation span is now created even in default-provider mode.
    assert "invocation" in spans
    invocation = spans["invocation"]
    assert invocation.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert invocation.attributes["durable.invocation.first"] is True


def test_default_mode_invocation_span_parented_to_ambient_span():
    plugin, exporter = _create_default_mode_plugin()

    # Simulate the ambient Lambda invocation span from the ADOT layer.
    ambient = plugin._provider.get_tracer("ambient").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        plugin.on_invocation_end(_invocation_end_info())
    finally:
        otel_context.detach(token)
        ambient.end()

    invocation = {s.name: s for s in exporter.get_finished_spans()}["invocation"]
    assert invocation.parent is not None
    assert invocation.parent.span_id == ambient.get_span_context().span_id
