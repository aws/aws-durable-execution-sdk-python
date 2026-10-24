"""Unvisited callback updates survive the public handler's invocation lifecycle."""

from datetime import UTC, datetime, timedelta
import logging
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from opentelemetry import context as otel_context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)


EXECUTION_ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:completion:1"
    "/durable-execution/unvisited-callback/execution-id"
)
CALLBACK_ID = "unvisited-callback"


@pytest.mark.parametrize(
    "factory_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("status", [OperationStatus.SUCCEEDED, OperationStatus.FAILED])
@pytest.mark.parametrize("missing_parent", [False, True])
def test_unvisited_input_completion_exports_before_invocation_closes(
    factory_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    status: OperationStatus,
    missing_parent: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Use service-shaped resume input without fabricating plugin callbacks.

    A handler can finish without revisiting an updated callback, such as a
    callback in a sibling branch after another branch determines the result.
    A later invocation may still contain its terminal checkpoint without
    naming it as newly updated. Only the first invocation should export it.
    """
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    caplog.set_level(logging.WARNING)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = factory_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    start = datetime.now(UTC) - timedelta(seconds=10)
    error = (
        ErrorObject(
            type="CallbackFailed",
            message="callback failed while suspended",
            data=None,
            stack_trace=None,
        )
        if status is OperationStatus.FAILED
        else None
    )
    operations = [
        Operation(
            operation_id="execution-id",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
            start_timestamp=start,
            execution_details=ExecutionDetails(input_payload='{"result": "winner"}'),
        ),
        Operation(
            operation_id=CALLBACK_ID,
            operation_type=OperationType.CALLBACK,
            status=status,
            name="unvisited callback",
            parent_id="missing-context" if missing_parent else None,
            start_timestamp=start + timedelta(seconds=1),
            end_timestamp=start + timedelta(seconds=2),
            callback_details=CallbackDetails(
                callback_id="service-callback-id",
                result='"callback-result"' if error is None else None,
                error=error,
            ),
        ),
    ]
    invocation: dict[str, Any] = {
        "DurableExecutionArn": EXECUTION_ARN,
        "CheckpointToken": "resume-token",
        "InitialExecutionState": {
            "Operations": [operation.to_json_dict() for operation in operations],
            "NextMarker": "",
        },
        "UpdatedOperationIds": [CALLBACK_ID],
    }
    lambda_context = Mock(
        aws_request_id="first-request",
        client_context=None,
        identity=None,
        _epoch_deadline_time_in_ms=0,
        invoked_function_arn="completion:1",
        tenant_id=None,
    )
    counts_during_handler: list[int] = []

    def body(event: dict[str, str], _context: DurableContext) -> str:
        counts_during_handler.append(
            sum(
                (span.attributes or {}).get("durable.operation.id") == CALLBACK_ID
                for span in exporter.get_finished_spans()
            )
        )
        return event["result"]

    handler = durable_execution(body, plugins=[factory])
    service = Mock()
    before = otel_context.get_current()
    try:
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
            return_value=service,
        ):
            result = handler(invocation, lambda_context)
            assert result == {"Status": "SUCCEEDED", "Result": '"winner"'}
            spans = exporter.get_finished_spans()
            (callback_span,) = [
                span
                for span in spans
                if (span.attributes or {}).get("durable.operation.id") == CALLBACK_ID
            ]
            (invocation_span,) = [span for span in spans if span.name == "Invocation"]
            assert spans.index(callback_span) < spans.index(invocation_span)
            assert callback_span.attributes is not None
            assert callback_span.attributes["durable.operation.status"] == status.value
            assert callback_span.status.status_code is (
                trace.StatusCode.ERROR if error else trace.StatusCode.OK
            )
            assert len(callback_span.events) == int(error is not None)
            assert callback_span.context is not None
            assert callback_span.parent is not None
            assert invocation_span.context is not None
            assert callback_span.context.trace_id == invocation_span.context.trace_id

            if factory_type is InvocationOtelPluginFactory:
                expected_parent_id = invocation_span.context.span_id
            elif missing_parent:
                expected_parent_id = operation_id_to_span_id(
                    EXECUTION_ARN, "missing-context"
                )
            else:
                (workflow_span,) = [span for span in spans if span.name == "Workflow"]
                assert workflow_span.context is not None
                expected_parent_id = workflow_span.context.span_id
            assert callback_span.parent.span_id == expected_parent_id

            invocation["UpdatedOperationIds"] = []
            lambda_context.aws_request_id = "second-request"
            assert handler(invocation, lambda_context) == result

        service.checkpoint.assert_not_called()
        assert counts_during_handler == [0, 1]
        assert [
            span
            for span in exporter.get_finished_spans()
            if (span.attributes or {}).get("durable.operation.id") == CALLBACK_ID
        ] == [callback_span]
        fallback_warnings = [
            record
            for record in caplog.records
            if "fallback parent" in record.getMessage()
            and CALLBACK_ID in record.getMessage()
        ]
        assert len(fallback_warnings) == int(missing_parent)
        assert otel_context.get_current() == before
    finally:
        provider.shutdown()
