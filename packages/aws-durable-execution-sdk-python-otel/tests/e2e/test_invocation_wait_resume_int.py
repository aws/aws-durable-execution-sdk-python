"""End-to-end invocation-view OTel coverage for wait/resume."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    derive_workflow_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


EXECUTION_ARN = "test-arn/execution-otel-wait-resume"
EXECUTION_START = datetime(2026, 8, 27, 5, 11, 47, tzinfo=UTC)
XRAY_TRACE_HEADER = (
    "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
)
XRAY_TRACE_ID = int("5759e988bd862e3fe1be46a994272793", 16)
XRAY_PARENT_SPAN_ID = int("53995c3f42cd8ad8", 16)


def _lambda_context() -> Mock:
    context = Mock()
    context.aws_request_id = "test-request-id"
    context.client_context = None
    context.identity = None
    context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    context.invoked_function_arn = "test-arn"
    context.tenant_id = None
    return context


def _event(
    operations: list[Operation],
    updated_operation_ids: list[str] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "DurableExecutionArn": EXECUTION_ARN,
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [operation.to_json_dict() for operation in operations],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }
    if updated_operation_ids is not None:
        event["UpdatedOperationIds"] = updated_operation_ids
    return event


def _execution_operation() -> Operation:
    return Operation(
        operation_id="execution-otel-wait-resume",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=EXECUTION_START,
        execution_details=ExecutionDetails(input_payload="{}"),
    )


def _checkpoint_store(initial_operations: list[Operation]):
    operations = {operation.operation_id: operation for operation in initial_operations}

    def checkpoint(
        durable_execution_arn,  # noqa: ARG001
        checkpoint_token,  # noqa: ARG001
        updates,
        client_token="token",  # noqa: S107, ARG001
    ) -> CheckpointOutput:
        for update in updates:
            now = datetime.now(UTC)
            previous = operations.get(update.operation_id)
            if update.action is OperationAction.START:
                operations[update.operation_id] = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    sub_type=update.sub_type,
                    start_timestamp=now,
                )
            elif update.action is OperationAction.SUCCEED:
                base = previous or Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    sub_type=update.sub_type,
                    start_timestamp=now,
                )
                operations[update.operation_id] = replace(
                    base,
                    status=OperationStatus.SUCCEEDED,
                    end_timestamp=now,
                    step_details=(
                        StepDetails(result=update.payload, attempt=1)
                        if update.operation_type is OperationType.STEP
                        else base.step_details
                    ),
                )

        return CheckpointOutput(
            checkpoint_token="new-token",
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=list(operations.values())
            ),
        )

    return checkpoint, operations


@pytest.mark.parametrize(
    "plugin_type",
    [InvocationOtelPlugin, ExecutionOtelPlugin],
)
def test_otel_wait_resume_spans_share_default_xray_execution_trace(
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[InvocationOtelPlugin] | type[ExecutionOtelPlugin],
) -> None:
    monkeypatch.setenv("_X_AMZN_TRACE_ID", XRAY_TRACE_HEADER)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
        )
    )

    @durable_step
    def complete_after_resume(_step_context) -> str:
        return "resumed"

    def handler_impl(_event: Any, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(1), name="otel-wait")
        return context.step(complete_after_resume(), name="otel-after-resume")

    handler = durable_execution(handler_impl, plugins=[plugin])

    initial_operations = [_execution_operation()]
    first_checkpoint, first_operations = _checkpoint_store(initial_operations)

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = first_checkpoint
        mock_client_class.initialize_client.return_value = mock_client

        first_result = handler(_event(initial_operations), _lambda_context())

    assert first_result["Status"] == InvocationStatus.PENDING.value
    wait_operation = next(
        operation
        for operation in first_operations.values()
        if operation.name == "otel-wait"
    )
    completed_wait = replace(
        wait_operation,
        status=OperationStatus.SUCCEEDED,
        end_timestamp=datetime.now(UTC),
        sub_type=OperationSubType.WAIT,
    )

    second_operations = [_execution_operation(), completed_wait]
    second_checkpoint, _ = _checkpoint_store(second_operations)

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = second_checkpoint
        mock_client_class.initialize_client.return_value = mock_client

        second_result = handler(
            _event(
                second_operations,
                updated_operation_ids=[completed_wait.operation_id],
            ),
            _lambda_context(),
        )

    assert second_result["Status"] == InvocationStatus.SUCCEEDED.value

    spans = exporter.get_finished_spans()
    durable_spans = [
        span
        for span in spans
        if span.name in {"Workflow", "Invocation", "otel-wait", "otel-after-resume"}
    ]
    trace_ids = {span.context.trace_id for span in durable_spans}
    assert trace_ids == {XRAY_TRACE_ID}

    workflow = next(span for span in spans if span.name == "Workflow")
    invocations = [span for span in spans if span.name == "Invocation"]
    waits = [span for span in spans if span.name == "otel-wait"]
    after_resume = next(span for span in spans if span.name == "otel-after-resume")

    assert len(invocations) >= 2
    if plugin_type is InvocationOtelPlugin:
        assert len(waits) >= 2  # one segment per invocation
    else:
        assert len(waits) == 1  # one span per operation
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert workflow.parent is not None
    assert workflow.parent.span_id == XRAY_PARENT_SPAN_ID
    assert {span.parent.span_id for span in invocations if span.parent} == {
        XRAY_PARENT_SPAN_ID
    }

    assert after_resume.parent is not None
    if plugin_type is InvocationOtelPlugin:
        assert after_resume.parent.span_id in {
            span.context.span_id for span in invocations
        }
    else:
        assert after_resume.parent.span_id == workflow.context.span_id
    completed_wait_span = next(
        span
        for span in waits
        if span.parent is not None
        and span.parent.span_id == after_resume.parent.span_id
    )
    assert completed_wait_span.end_time is not None
    assert after_resume.start_time is not None
    assert completed_wait_span.end_time <= after_resume.start_time
