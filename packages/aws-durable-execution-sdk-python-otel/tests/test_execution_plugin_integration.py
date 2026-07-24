"""In-process integration tests for ExecutionOtelPlugin.

Drives the full plugin lifecycle against a real TracerProvider +
InMemorySpanExporter for the two deployment shapes:

* Community collector layer: the plugin owns its provider
  (``use_default_tracer_provider=False``); the Workflow span is the trace root.
* ADOT layer: the ADOT Lambda layer supplies the global provider and the ambient
  Lambda invocation span (``use_default_tracer_provider=True``); the plugin's
  Invocation span parents to that ambient span.
"""

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
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"
OP_ID = "step-1"
OP_NAME = "fetch-user"


@pytest.fixture(autouse=True)
def _reset_otel_context():
    token = otel_context.attach(Context())
    try:
        yield
    finally:
        otel_context.detach(token)


def _provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def _invocation_start() -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )


def _invocation_end(
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


def _run_step_lifecycle(plugin: ExecutionOtelPlugin) -> None:
    """Drive a single completed STEP (operation + one attempt) through a plugin."""
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
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
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
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
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )


# ---------------------------------------------------------------------------
# Community collector layer (plugin owns the provider)
# ---------------------------------------------------------------------------
def test_community_layer_full_lifecycle_is_workflow_rooted():
    provider, exporter = _provider()
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            use_default_tracer_provider=False,
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )

    plugin.on_invocation_start(_invocation_start())
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())

    finished = exporter.get_finished_spans()
    spans = {s.name: s for s in finished}
    workflow = spans["Workflow"]
    invocation = spans["invocation"]
    operation = spans[OP_NAME]
    attempt = spans[f"{OP_NAME} attempt 1"]

    # Workflow is the trace root with the deterministic workflow span id.
    assert workflow.parent is None
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert (
        workflow.attributes["durable.execution.status"]
        == InvocationStatus.SUCCEEDED.value
    )

    # Invocation span is a child of the Workflow span.
    assert invocation.parent is not None
    assert invocation.parent.span_id == workflow.context.span_id

    # Operation span: deterministic id, parented under Workflow, linked to invocation.
    assert operation.context.span_id == operation_id_to_span_id(EXECUTION_ARN, OP_ID)
    assert operation.parent.span_id == workflow.context.span_id
    assert invocation.context.span_id in {
        link.context.span_id for link in operation.links
    }

    # Attempt span is a child of the operation span.
    assert attempt.parent.span_id == operation.context.span_id

    # The operation span is exported exactly once.
    assert len([s for s in finished if s.name == OP_NAME]) == 1


# ---------------------------------------------------------------------------
# ADOT layer (default provider; ambient invocation span)
# ---------------------------------------------------------------------------
def test_adot_layer_full_lifecycle_parents_to_ambient_span():
    provider, exporter = _provider()
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            use_default_tracer_provider=True,
            context_extractor=lambda _: Context(),
            enrich_logger=False,
        )
    )

    # Simulate the ambient Lambda invocation span the ADOT layer creates.
    ambient = provider.get_tracer("adot").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start())
        _run_step_lifecycle(plugin)
        plugin.on_invocation_end(_invocation_end())
    finally:
        otel_context.detach(token)
        ambient.end()

    finished = exporter.get_finished_spans()
    spans = {s.name: s for s in finished}
    invocation = spans["invocation"]
    operation = spans[OP_NAME]

    # Invocation span parents to the ambient ADOT span and carries the first flag.
    assert invocation.parent is not None
    assert invocation.parent.span_id == ambient.get_span_context().span_id
    assert invocation.attributes["durable.invocation.first"] is True

    # Operation span still uses the deterministic id and links to the ambient span.
    assert operation.context.span_id == operation_id_to_span_id(EXECUTION_ARN, OP_ID)
    assert ambient.get_span_context().span_id in {
        link.context.span_id for link in operation.links
    }

    # The operation span is exported exactly once.
    assert len([s for s in finished if s.name == OP_NAME]) == 1
