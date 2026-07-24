"""In-process integration tests for InvocationOtelPlugin.

Drives the full plugin lifecycle against a real TracerProvider +
InMemorySpanExporter for the two deployment shapes:

* Community collector layer: the caller supplies its own provider (configured to
  export to a community OTel collector) via ``trace_provider=...``.
* ADOT layer: the ADOT Lambda layer configures the global provider and the plugin
  picks it up from ``trace.get_tracer_provider()`` (no explicit provider passed).

Both paths run identical plugin logic; each test asserts the
invocation -> operation -> attempt hierarchy is produced against whichever
provider is in use.
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
from opentelemetry.trace import SpanKind

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin


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


def _run_step_lifecycle(plugin: InvocationOtelPlugin) -> None:
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


def _assert_hierarchy(exporter: InMemorySpanExporter) -> None:
    spans = {s.name: s for s in exporter.get_finished_spans()}
    invocation = spans["invocation"]
    operation = spans[OP_NAME]
    attempt = spans[f"{OP_NAME} attempt 1"]

    # Invocation span is a root (empty extracted context) and records status.
    assert invocation.parent is None
    assert invocation.kind is SpanKind.INTERNAL
    assert invocation.attributes is not None
    assert (
        invocation.attributes["durable.invocation.status"]
        == InvocationStatus.SUCCEEDED.value
    )

    # Operation span: deterministic id, child of the invocation span.
    assert operation.context.span_id == operation_id_to_span_id(EXECUTION_ARN, OP_ID)
    assert operation.parent is not None
    assert operation.parent.span_id == invocation.context.span_id

    # Attempt span is a child of the operation span.
    assert attempt.parent is not None
    assert attempt.parent.span_id == operation.context.span_id


# ---------------------------------------------------------------------------
# Community collector layer (caller-supplied provider)
# ---------------------------------------------------------------------------
def test_community_layer_full_lifecycle_uses_supplied_provider():
    provider, exporter = _provider()
    plugin = InvocationOtelPlugin(
        trace_provider=provider,
        context_extractor=lambda _: Context(),
        enrich_logger=False,
    )

    plugin.on_invocation_start(_invocation_start())
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())

    _assert_hierarchy(exporter)


# ---------------------------------------------------------------------------
# ADOT layer (global provider picked up from trace.get_tracer_provider)
# ---------------------------------------------------------------------------
def test_adot_layer_full_lifecycle_uses_global_provider(monkeypatch):
    provider, exporter = _provider()
    # Simulate the ADOT layer having configured the global TracerProvider.
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)

    plugin = InvocationOtelPlugin(
        context_extractor=lambda _: Context(),
        enrich_logger=False,
    )

    plugin.on_invocation_start(_invocation_start())
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())

    # Spans were produced against the (monkeypatched) global provider's exporter.
    _assert_hierarchy(exporter)
