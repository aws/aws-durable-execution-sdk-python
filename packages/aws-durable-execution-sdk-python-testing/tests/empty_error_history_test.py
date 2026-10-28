"""History preserves empty payload presence without inventing SDK diagnostics."""

from datetime import UTC, datetime
from typing import Any

import pytest

from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python_testing.model import (
    CallbackFailedDetails,
    ChainedInvokeFailedDetails,
    ContextFailedDetails,
    Event,
    EventCreationContext,
    EventError,
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionResponse,
    StartDurableExecutionInput,
    StepFailedDetails,
    events_to_operations,
)
from aws_durable_execution_sdk_python_testing.runner import (
    ContextOperation,
    DurableFunctionTestResult,
    InvokeOperation,
)


RICH_ERROR = {
    "ErrorType": "RejectedError",
    "ErrorMessage": "rejected",
    "ErrorData": '{"reason":"test"}',
    "StackTrace": ["original frame"],
}
PAYLOADS = [
    {},
    {"ErrorMessage": ""},
    {"ErrorType": ""},
    {"ErrorData": ""},
    {"StackTrace": []},
    RICH_ERROR,
]


@pytest.mark.parametrize("payload", PAYLOADS)
@pytest.mark.parametrize("truncated", [False, True])
def test_event_error_roundtrip_preserves_present_payload(
    payload: dict[str, Any], truncated: bool
) -> None:
    wire = {"Payload": payload, "Truncated": truncated}
    assert EventError.from_dict(wire).to_dict() == wire


@pytest.mark.parametrize("payload_field", [{}, {"Payload": None}])
@pytest.mark.parametrize("truncated", [False, True])
def test_event_error_omitted_and_null_payload_remain_absent(
    payload_field: dict[str, Any], truncated: bool
) -> None:
    parsed = EventError.from_dict({**payload_field, "Truncated": truncated})
    assert parsed.payload is None
    assert parsed.to_dict() == {"Truncated": truncated}


@pytest.mark.parametrize(
    "event_type,details_field,details_type,operation_field",
    [
        (
            "CallbackFailed",
            "callback_failed_details",
            CallbackFailedDetails,
            "callback_details",
        ),
        ("StepFailed", "step_failed_details", StepFailedDetails, "step_details"),
        (
            "ContextFailed",
            "context_failed_details",
            ContextFailedDetails,
            "context_details",
        ),
        (
            "ChainedInvokeFailed",
            "chained_invoke_failed_details",
            ChainedInvokeFailedDetails,
            "chained_invoke_details",
        ),
    ],
)
@pytest.mark.parametrize("payload", [None, *PAYLOADS])
@pytest.mark.parametrize("decode_wire", [False, True])
def test_failure_history_reconstruction_normalizes_only_empty_errors(
    event_type: str,
    details_field: str,
    details_type: type[
        CallbackFailedDetails
        | StepFailedDetails
        | ContextFailedDetails
        | ChainedInvokeFailedDetails
    ],
    operation_field: str,
    payload: dict[str, Any] | None,
    decode_wire: bool,
) -> None:
    timestamp = datetime(2026, 10, 27, tzinfo=UTC)
    error = ErrorObject.from_dict(payload) if payload is not None else None
    event_details: dict[str, Any] = {
        details_field: details_type(error=EventError(payload=error, truncated=False))
    }
    history = GetDurableExecutionHistoryResponse(
        events=[
            Event(
                event_type=event_type,
                event_id=1,
                event_timestamp=timestamp,
                operation_id="operation",
                name="failed-operation",
                **event_details,
            )
        ]
    )
    expected_wire = history.to_dict()
    if decode_wire:
        history = GetDurableExecutionHistoryResponse.from_dict(expected_wire)
    assert history.to_dict() == expected_wire
    expected_error = error if payload else None
    (operation,) = events_to_operations(history.events)
    assert operation.status is OperationStatus.FAILED
    assert getattr(operation, operation_field).error == expected_error

    execution = GetDurableExecutionResponse(
        durable_execution_arn="arn:execution",
        durable_execution_name="execution",
        function_arn="arn:function",
        status="FAILED",
        start_timestamp=timestamp,
    )
    result = DurableFunctionTestResult.from_execution_history(execution, history)
    result_operation = result.get_operation_by_name("failed-operation")
    assert isinstance(result_operation, (ContextOperation, InvokeOperation))
    assert result_operation.error == expected_error
    assert history.to_dict() == expected_wire


@pytest.mark.parametrize("include_data", [False, True])
@pytest.mark.parametrize("status", [OperationStatus.FAILED, OperationStatus.TIMED_OUT])
@pytest.mark.parametrize("payload", [None, {}, RICH_ERROR])
def test_callback_history_projection_preserves_state_and_data_inclusion(
    include_data: bool, status: OperationStatus, payload: dict[str, Any] | None
) -> None:
    error = ErrorObject.from_dict(payload) if payload is not None else None
    operation = Operation(
        operation_id="callback",
        operation_type=OperationType.CALLBACK,
        status=status,
        callback_details=CallbackDetails(callback_id="callback-id", error=error),
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=30,
            execution_retention_period_days=1,
        ),
        include_execution_data=include_data,
    )
    event = Event.create_callback_event(context)
    details = (
        event.callback_failed_details
        if status is OperationStatus.FAILED
        else event.callback_timed_out_details
    )
    assert details is not None and details.error is not None
    expected: dict[str, Any] = {"Truncated": not include_data}
    if include_data and (payload is not None or status is OperationStatus.FAILED):
        expected["Payload"] = payload if payload is not None else {}
    assert details.error.to_dict() == expected
    assert operation.callback_details is not None
    assert operation.callback_details.error is error


def test_missing_callback_details_do_not_invent_error_payload() -> None:
    operation = Operation(
        operation_id="callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=30,
            execution_retention_period_days=1,
        ),
        include_execution_data=True,
    )
    event = Event.create_callback_event_failed(context)
    assert event.callback_failed_details is not None
    assert event.callback_failed_details.error is None
