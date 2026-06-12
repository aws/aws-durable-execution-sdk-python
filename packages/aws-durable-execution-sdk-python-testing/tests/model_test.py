"""Tests for model serialization dataclasses."""

from __future__ import annotations

import datetime
import json

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.model import (
    CallbackFailedDetails,
    CallbackStartedDetails,
    CallbackSucceededDetails,
    CallbackTimedOutDetails,
    ChainedInvokeFailedDetails,
    ChainedInvokeStartedDetails,
    ChainedInvokeStoppedDetails,
    ChainedInvokeSucceededDetails,
    ChainedInvokeTimedOutDetails,
    CheckpointDurableExecutionRequest,
    CheckpointDurableExecutionResponse,
    CheckpointUpdatedExecutionState,
    ContextFailedDetails,
    ContextStartedDetails,
    ContextSucceededDetails,
    ErrorResponse,
    Event,
    EventError,
    EventInput,
    EventResult,
    Execution,
    ExecutionFailedDetails,
    ExecutionStartedDetails,
    ExecutionStoppedDetails,
    ExecutionSucceededDetails,
    ExecutionTimedOutDetails,
    GetDurableExecutionHistoryRequest,
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionRequest,
    GetDurableExecutionResponse,
    GetDurableExecutionStateRequest,
    GetDurableExecutionStateResponse,
    InvocationCompletedDetails,
    ListDurableExecutionsByFunctionRequest,
    ListDurableExecutionsByFunctionResponse,
    ListDurableExecutionsRequest,
    ListDurableExecutionsResponse,
    RetryDetails,
    SendDurableExecutionCallbackFailureRequest,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatRequest,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessRequest,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    StepFailedDetails,
    StepStartedDetails,
    StepSucceededDetails,
    StopDurableExecutionRequest,
    StopDurableExecutionResponse,
    WaitCancelledDetails,
    WaitStartedDetails,
    WaitSucceededDetails,
    events_to_operations,
)


# Test timestamp constants
TIMESTAMP_2023_01_01_00_00 = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
TIMESTAMP_2023_01_01_00_01 = datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC)
TIMESTAMP_2023_01_01_00_02 = datetime.datetime(2023, 1, 1, 0, 2, 0, tzinfo=datetime.UTC)
TIMESTAMP_2023_01_02_00_00 = datetime.datetime(2023, 1, 2, 0, 0, 0, tzinfo=datetime.UTC)

DEFAULT_START_DURABLE_EXECUTION_INPUT_DATA = {
    "AccountId": "123456789012",
    "FunctionName": "my-function",
    "FunctionQualifier": "$LATEST",
    "ExecutionName": "test-execution",
    "ExecutionTimeoutSeconds": 300,
    "ExecutionRetentionPeriodDays": 7,
    "InvocationId": "invocation-123",
    "TraceFields": {"key": "value"},
    "TenantId": "tenant-123",
    "Input": "test-input",
}


def test_start_durable_execution_input_serialization():
    """Test StartDurableExecutionInput from_dict/to_dict round-trip."""
    data = DEFAULT_START_DURABLE_EXECUTION_INPUT_DATA

    # Test from_dict
    input_obj = StartDurableExecutionInput.from_dict(data)
    assert input_obj.account_id == "123456789012"
    assert input_obj.function_name == "my-function"
    assert input_obj.function_qualifier == "$LATEST"
    assert input_obj.execution_name == "test-execution"
    assert input_obj.execution_timeout_seconds == 300
    assert input_obj.execution_retention_period_days == 7
    assert input_obj.invocation_id == "invocation-123"
    assert input_obj.trace_fields == {"key": "value"}
    assert input_obj.tenant_id == "tenant-123"
    assert input_obj.input == "test-input"

    # Test to_dict
    result_data = input_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = StartDurableExecutionInput.from_dict(result_data)
    assert round_trip == input_obj


def test_start_durable_execution_input_get_input_json_input():
    """Test StartDurableExecutionInput from_dict/to_dict round-trip."""
    data = DEFAULT_START_DURABLE_EXECUTION_INPUT_DATA
    data["Input"] = '{"message": "hello"}'

    input_obj = StartDurableExecutionInput.from_dict(data)
    assert '{"message": "hello"}' == input_obj.get_normalized_input()


def test_start_durable_execution_input_get_input_str_non_json_input():
    """Test StartDurableExecutionInput from_dict/to_dict round-trip."""
    data = DEFAULT_START_DURABLE_EXECUTION_INPUT_DATA
    data["Input"] = "hello"

    input_obj = StartDurableExecutionInput.from_dict(data)
    assert '"hello"' == input_obj.get_normalized_input()


def test_start_durable_execution_input_get_input_str_json_input():
    """Test StartDurableExecutionInput from_dict/to_dict round-trip."""
    data = DEFAULT_START_DURABLE_EXECUTION_INPUT_DATA
    data["Input"] = '"hello"'

    input_obj = StartDurableExecutionInput.from_dict(data)
    assert '"hello"' == input_obj.get_normalized_input()


def test_start_durable_execution_input_get_input_list_json_input():
    """Test StartDurableExecutionInput from_dict/to_dict round-trip."""
    data = DEFAULT_START_DURABLE_EXECUTION_INPUT_DATA
    data["Input"] = "[1,2,3]"

    input_obj = StartDurableExecutionInput.from_dict(data)
    assert "[1,2,3]" == input_obj.get_normalized_input()


def test_start_durable_execution_input_minimal():
    """Test StartDurableExecutionInput with only required fields."""
    data = {
        "AccountId": "123456789012",
        "FunctionName": "my-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    input_obj = StartDurableExecutionInput.from_dict(data)
    assert input_obj.invocation_id is None
    assert input_obj.trace_fields is None
    assert input_obj.tenant_id is None
    assert input_obj.input is None

    result_data = input_obj.to_dict()
    assert result_data == data


def test_start_durable_execution_output_serialization():
    """Test StartDurableExecutionOutput from_dict/to_dict round-trip."""
    data = {
        "ExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    }

    output_obj = StartDurableExecutionOutput.from_dict(data)
    assert (
        output_obj.execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )

    result_data = output_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = StartDurableExecutionOutput.from_dict(result_data)
    assert round_trip == output_obj


def test_start_durable_execution_output_empty():
    """Test StartDurableExecutionOutput with empty data."""
    data = {}

    output_obj = StartDurableExecutionOutput.from_dict(data)
    assert output_obj.execution_arn is None

    result_data = output_obj.to_dict()
    assert result_data == {}


def test_get_durable_execution_request_serialization():
    """Test GetDurableExecutionRequest from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    }

    request_obj = GetDurableExecutionRequest.from_dict(data)
    assert (
        request_obj.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = GetDurableExecutionRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_get_durable_execution_response_serialization():
    """Test GetDurableExecutionResponse from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function",
        "Status": "SUCCEEDED",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
        "InputPayload": "test-input",
        "Result": "test-result",
        "Error": {"ErrorMessage": "test error"},
        "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
        "Version": "1.0",
    }

    response_obj = GetDurableExecutionResponse.from_dict(data)
    assert (
        response_obj.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    assert response_obj.durable_execution_name == "test-execution"
    assert (
        response_obj.function_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function"
    )
    assert response_obj.status == "SUCCEEDED"
    assert response_obj.start_timestamp == TIMESTAMP_2023_01_01_00_00
    assert response_obj.input_payload == "test-input"
    assert response_obj.result == "test-result"
    assert response_obj.error.message == "test error"
    assert response_obj.end_timestamp == TIMESTAMP_2023_01_01_00_01
    assert response_obj.version == "1.0"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = GetDurableExecutionResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_get_durable_execution_response_minimal():
    """Test GetDurableExecutionResponse with only required fields."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function",
        "Status": "RUNNING",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
    }

    response_obj = GetDurableExecutionResponse.from_dict(data)
    assert response_obj.input_payload is None
    assert response_obj.result is None
    assert response_obj.error is None
    assert response_obj.end_timestamp is None
    assert response_obj.version is None

    result_data = response_obj.to_dict()
    assert result_data == data


def test_list_durable_executions_request_serialization():
    """Test ListDurableExecutionsRequest from_dict/to_dict round-trip."""
    data = {
        "FunctionName": "my-function",
        "FunctionVersion": "$LATEST",
        "DurableExecutionName": "test-execution",
        "StatusFilter": ["RUNNING", "SUCCEEDED"],
        "StartedAfter": TIMESTAMP_2023_01_01_00_00,
        "StartedBefore": TIMESTAMP_2023_01_02_00_00,
        "Marker": "marker-123",
        "MaxItems": 10,
        "ReverseOrder": True,
    }

    request_obj = ListDurableExecutionsRequest.from_dict(data)
    assert request_obj.function_name == "my-function"
    assert request_obj.function_version == "$LATEST"
    assert request_obj.durable_execution_name == "test-execution"
    assert request_obj.status_filter == ["RUNNING", "SUCCEEDED"]
    assert request_obj.started_after == TIMESTAMP_2023_01_01_00_00
    assert request_obj.started_before == TIMESTAMP_2023_01_02_00_00
    assert request_obj.marker == "marker-123"
    assert request_obj.max_items == 10
    assert request_obj.reverse_order is True

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = ListDurableExecutionsRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_list_durable_executions_request_empty():
    """Test ListDurableExecutionsRequest with empty data."""
    data = {}

    request_obj = ListDurableExecutionsRequest.from_dict(data)
    assert request_obj.function_name is None
    assert request_obj.function_version is None
    assert request_obj.durable_execution_name is None
    assert request_obj.status_filter is None
    assert request_obj.started_after is None
    assert request_obj.started_before is None
    assert request_obj.marker is None
    assert request_obj.max_items == 0  # Default value from Smithy
    assert request_obj.reverse_order is None

    result_data = request_obj.to_dict()
    # The result should include the default MaxItems
    expected_data = {"MaxItems": 0}
    assert result_data == expected_data


def test_durable_execution_summary_serialization():
    """Test Execution from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "Status": "SUCCEEDED",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
        "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
    }

    summary_obj = Execution.from_dict(data)
    assert (
        summary_obj.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    assert summary_obj.durable_execution_name == "test-execution"
    assert summary_obj.status == "SUCCEEDED"
    assert summary_obj.start_timestamp == TIMESTAMP_2023_01_01_00_00
    assert summary_obj.end_timestamp == TIMESTAMP_2023_01_01_00_01

    result_data = summary_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = Execution.from_dict(result_data)
    assert round_trip == summary_obj


def test_durable_execution_summary_no_end_timestamp():
    """Test Execution without end timestamp."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "Status": "RUNNING",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
    }

    summary_obj = Execution.from_dict(data)
    assert summary_obj.end_timestamp is None

    result_data = summary_obj.to_dict()
    assert result_data == data


def test_list_durable_executions_response_serialization():
    """Test ListDurableExecutionsResponse from_dict/to_dict round-trip."""
    data = {
        "DurableExecutions": [
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test1",
                "DurableExecutionName": "test-execution-1",
                "Status": "SUCCEEDED",
                "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
                "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
            },
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test2",
                "DurableExecutionName": "test-execution-2",
                "Status": "RUNNING",
                "StartTimestamp": TIMESTAMP_2023_01_01_00_02,
            },
        ],
        "NextMarker": "next-marker-123",
    }

    response_obj = ListDurableExecutionsResponse.from_dict(data)
    assert len(response_obj.durable_executions) == 2
    assert (
        response_obj.durable_executions[0].durable_execution_name == "test-execution-1"
    )
    assert (
        response_obj.durable_executions[1].durable_execution_name == "test-execution-2"
    )
    assert response_obj.next_marker == "next-marker-123"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = ListDurableExecutionsResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_list_durable_executions_response_empty():
    """Test ListDurableExecutionsResponse with empty executions."""
    data = {"DurableExecutions": []}

    response_obj = ListDurableExecutionsResponse.from_dict(data)
    assert len(response_obj.durable_executions) == 0
    assert response_obj.next_marker is None

    result_data = response_obj.to_dict()
    assert result_data == {"DurableExecutions": []}


def test_stop_durable_execution_request_serialization():
    """Test StopDurableExecutionRequest from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "Error": {"ErrorMessage": "Stopped by user"},
    }

    request_obj = StopDurableExecutionRequest.from_dict(data)
    assert (
        request_obj.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    assert request_obj.error.message == "Stopped by user"

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = StopDurableExecutionRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_stop_durable_execution_request_minimal():
    """Test StopDurableExecutionRequest with only required fields."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    }

    request_obj = StopDurableExecutionRequest.from_dict(data)
    assert request_obj.error is None

    result_data = request_obj.to_dict()
    assert result_data == data


def test_stop_durable_execution_response_serialization():
    """Test StopDurableExecutionResponse from_dict/to_dict round-trip."""
    data = {"StopTimestamp": "2023-01-01T00:01:00Z"}

    response_obj = StopDurableExecutionResponse.from_dict(data)
    assert response_obj.stop_timestamp == "2023-01-01T00:01:00Z"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = StopDurableExecutionResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_get_durable_execution_state_request_serialization():
    """Test GetDurableExecutionStateRequest from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "CheckpointToken": "checkpoint-123",
        "Marker": "marker-123",
        "MaxItems": 10,
    }

    request_obj = GetDurableExecutionStateRequest.from_dict(data)
    assert (
        request_obj.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    assert request_obj.checkpoint_token == "checkpoint-123"  # noqa: S105
    assert request_obj.marker == "marker-123"
    assert request_obj.max_items == 10

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = GetDurableExecutionStateRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_get_durable_execution_state_request_minimal():
    """Test GetDurableExecutionStateRequest with only required fields."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "CheckpointToken": "checkpoint-123",
    }

    request_obj = GetDurableExecutionStateRequest.from_dict(data)
    assert request_obj.marker is None
    assert request_obj.max_items == 0  # Default value from Smithy

    result_data = request_obj.to_dict()
    # The result should include the default MaxItems
    expected_data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "CheckpointToken": "checkpoint-123",
        "MaxItems": 0,
    }
    assert result_data == expected_data


def test_get_durable_execution_state_response_serialization():
    """Test GetDurableExecutionStateResponse from_dict/to_dict round-trip."""
    data = {
        "Operations": [
            {"Id": "op-1", "Type": "STEP", "Status": "SUCCEEDED"},
            {"Id": "op-2", "Type": "CONTEXT", "Status": "STARTED"},
        ],
        "NextMarker": "next-marker-123",
    }

    response_obj = GetDurableExecutionStateResponse.from_dict(data)
    assert len(response_obj.operations) == 2
    assert response_obj.operations[0].operation_id == "op-1"
    assert response_obj.operations[0].operation_type.value == "STEP"
    assert response_obj.operations[1].operation_id == "op-2"
    assert response_obj.operations[1].operation_type.value == "CONTEXT"
    assert response_obj.next_marker == "next-marker-123"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = GetDurableExecutionStateResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_get_durable_execution_state_response_empty():
    """Test GetDurableExecutionStateResponse with empty operations."""
    data = {"Operations": []}

    response_obj = GetDurableExecutionStateResponse.from_dict(data)
    assert len(response_obj.operations) == 0
    assert response_obj.next_marker is None

    result_data = response_obj.to_dict()
    assert result_data == {"Operations": []}


def test_get_durable_execution_history_request_serialization():
    """Test GetDurableExecutionHistoryRequest from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "IncludeExecutionData": True,
        "ReverseOrder": False,
        "Marker": "marker-123",
        "MaxItems": 20,
    }

    request_obj = GetDurableExecutionHistoryRequest.from_dict(data)
    assert (
        request_obj.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    assert request_obj.include_execution_data is True
    assert request_obj.reverse_order is False
    assert request_obj.marker == "marker-123"
    assert request_obj.max_items == 20

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = GetDurableExecutionHistoryRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_get_durable_execution_history_request_minimal():
    """Test GetDurableExecutionHistoryRequest with only required fields."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    }

    request_obj = GetDurableExecutionHistoryRequest.from_dict(data)
    assert request_obj.include_execution_data is None
    assert request_obj.reverse_order is None
    assert request_obj.marker is None
    assert request_obj.max_items == 0  # Default value from Smithy

    result_data = request_obj.to_dict()
    # The result should include the default MaxItems
    expected_data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "MaxItems": 0,
    }
    assert result_data == expected_data


def test_execution_event_serialization():
    """Test Event from_dict/to_dict round-trip."""
    data = {
        "EventType": "ExecutionStarted",
        "EventId": 123,
        "EventTimestamp": TIMESTAMP_2023_01_01_00_00,
        "SubType": "UserInitiated",
        "Id": "op-123",
        "Name": "test-operation",
        "ParentId": "parent-op-123",
        "ExecutionStartedDetails": {
            "Input": {"Payload": "test-input", "Truncated": False},
            "ExecutionTimeout": 300,
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ExecutionStarted"
    assert event_obj.event_id == 123
    assert event_obj.event_timestamp == TIMESTAMP_2023_01_01_00_00
    assert event_obj.sub_type == "UserInitiated"
    assert event_obj.operation_id == "op-123"
    assert event_obj.name == "test-operation"
    assert event_obj.parent_id == "parent-op-123"
    assert event_obj.execution_started_details is not None
    assert event_obj.execution_started_details.input.payload == "test-input"
    assert event_obj.execution_started_details.execution_timeout == 300

    result_data = event_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = Event.from_dict(result_data)
    assert round_trip == event_obj


def test_execution_event_minimal():
    """Test Event with only required fields."""
    data = {
        "EventType": "ExecutionStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_00,
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_id == 1  # Default value from Smithy
    assert event_obj.sub_type is None
    assert event_obj.operation_id is None
    assert event_obj.name is None
    assert event_obj.parent_id is None
    assert event_obj.execution_started_details is None

    result_data = event_obj.to_dict()
    # The result should include the default EventId
    expected_data = {
        "EventType": "ExecutionStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_00,
        "EventId": 1,
    }
    assert result_data == expected_data


def test_get_durable_execution_history_response_serialization():
    """Test GetDurableExecutionHistoryResponse from_dict/to_dict round-trip."""
    data = {
        "Events": [
            {
                "EventType": "ExecutionStarted",
                "EventId": 1,
                "EventTimestamp": TIMESTAMP_2023_01_01_00_00,
            },
            {
                "EventType": "ExecutionSucceeded",
                "EventId": 2,
                "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
                "ExecutionSucceededDetails": {
                    "Result": {"Payload": "success", "Truncated": False}
                },
            },
        ],
        "NextMarker": "next-marker-123",
    }

    response_obj = GetDurableExecutionHistoryResponse.from_dict(data)
    assert len(response_obj.events) == 2
    assert response_obj.events[0].event_type == "ExecutionStarted"
    assert response_obj.events[1].event_type == "ExecutionSucceeded"
    assert response_obj.events[1].execution_succeeded_details is not None
    assert (
        response_obj.events[1].execution_succeeded_details.result.payload == "success"
    )
    assert response_obj.next_marker == "next-marker-123"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = GetDurableExecutionHistoryResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_get_durable_execution_history_response_empty():
    """Test GetDurableExecutionHistoryResponse with empty events."""
    data = {"Events": []}

    response_obj = GetDurableExecutionHistoryResponse.from_dict(data)
    assert len(response_obj.events) == 0
    assert response_obj.next_marker is None

    result_data = response_obj.to_dict()
    assert result_data == {"Events": []}


def test_list_durable_executions_by_function_request_serialization():
    """Test ListDurableExecutionsByFunctionRequest from_dict/to_dict round-trip."""
    data = {
        "FunctionName": "my-function",
        "Qualifier": "$LATEST",
        "StatusFilter": ["RUNNING", "SUCCEEDED"],
        "StartedAfter": TIMESTAMP_2023_01_01_00_00,
        "StartedBefore": TIMESTAMP_2023_01_02_00_00,
        "Marker": "marker-123",
        "MaxItems": 10,
        "ReverseOrder": True,
    }

    request_obj = ListDurableExecutionsByFunctionRequest.from_dict(data)
    assert request_obj.function_name == "my-function"
    assert request_obj.qualifier == "$LATEST"
    assert request_obj.status_filter == ["RUNNING", "SUCCEEDED"]
    assert request_obj.started_after == TIMESTAMP_2023_01_01_00_00
    assert request_obj.started_before == TIMESTAMP_2023_01_02_00_00
    assert request_obj.marker == "marker-123"
    assert request_obj.max_items == 10
    assert request_obj.reverse_order is True

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = ListDurableExecutionsByFunctionRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_list_durable_executions_by_function_request_minimal():
    """Test ListDurableExecutionsByFunctionRequest with only required fields."""
    data = {"FunctionName": "my-function"}

    request_obj = ListDurableExecutionsByFunctionRequest.from_dict(data)
    assert request_obj.qualifier is None
    assert request_obj.status_filter is None
    assert request_obj.started_after is None
    assert request_obj.started_before is None
    assert request_obj.marker is None
    assert request_obj.max_items == 0  # Default value from Smithy
    assert request_obj.reverse_order is None

    result_data = request_obj.to_dict()
    # The result should include the default MaxItems
    expected_data = {"FunctionName": "my-function", "MaxItems": 0}
    assert result_data == expected_data


def test_list_durable_executions_by_function_response_serialization():
    """Test ListDurableExecutionsByFunctionResponse from_dict/to_dict round-trip."""
    data = {
        "DurableExecutions": [
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test1",
                "DurableExecutionName": "test-execution-1",
                "Status": "SUCCEEDED",
                "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
                "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
            }
        ],
        "NextMarker": "next-marker-123",
    }

    response_obj = ListDurableExecutionsByFunctionResponse.from_dict(data)
    assert len(response_obj.durable_executions) == 1
    assert (
        response_obj.durable_executions[0].durable_execution_name == "test-execution-1"
    )
    assert response_obj.next_marker == "next-marker-123"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = ListDurableExecutionsByFunctionResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_send_durable_execution_callback_success_request_serialization():
    """Test SendDurableExecutionCallbackSuccessRequest from_dict/to_dict round-trip."""
    data = {
        "CallbackId": "callback-123",
        "Result": "success-result",
    }

    request_obj = SendDurableExecutionCallbackSuccessRequest.from_dict(data)
    assert request_obj.callback_id == "callback-123"
    assert request_obj.result == "success-result"

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = SendDurableExecutionCallbackSuccessRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_send_durable_execution_callback_success_request_minimal():
    """Test SendDurableExecutionCallbackSuccessRequest with only required fields."""
    data = {"CallbackId": "callback-123"}

    request_obj = SendDurableExecutionCallbackSuccessRequest.from_dict(data)
    assert request_obj.result is None

    result_data = request_obj.to_dict()
    assert result_data == data


def test_send_durable_execution_callback_success_response_creation():
    """Test SendDurableExecutionCallbackSuccessResponse creation."""
    response_obj = SendDurableExecutionCallbackSuccessResponse()
    assert isinstance(response_obj, SendDurableExecutionCallbackSuccessResponse)


def test_send_durable_execution_callback_failure_request_serialization():
    """Test SendDurableExecutionCallbackFailureRequest from_dict/to_dict round-trip."""
    data = {"ErrorMessage": "callback failed"}

    request_obj = SendDurableExecutionCallbackFailureRequest.from_dict(
        data, "callback-123"
    )
    assert request_obj.callback_id == "callback-123"
    assert request_obj.error.message == "callback failed"

    result_data = request_obj.to_dict()
    expected_data = {
        "CallbackId": "callback-123",
        "Error": {"ErrorMessage": "callback failed"},
    }
    assert result_data == expected_data

    # Test round-trip
    round_trip = SendDurableExecutionCallbackFailureRequest.from_dict(
        result_data.get("Error", {}), result_data["CallbackId"]
    )
    assert round_trip == request_obj


def test_send_durable_execution_callback_failure_request_minimal():
    """Test SendDurableExecutionCallbackFailureRequest with only required fields."""

    request_obj = SendDurableExecutionCallbackFailureRequest.from_dict(
        {}, "callback-123"
    )
    assert request_obj.error is None

    result_data = request_obj.to_dict()
    assert result_data == {"CallbackId": "callback-123"}


def test_send_durable_execution_callback_failure_response_creation():
    """Test SendDurableExecutionCallbackFailureResponse creation."""
    response_obj = SendDurableExecutionCallbackFailureResponse()
    assert isinstance(response_obj, SendDurableExecutionCallbackFailureResponse)


def test_send_durable_execution_callback_heartbeat_request_serialization():
    """Test SendDurableExecutionCallbackHeartbeatRequest from_dict/to_dict round-trip."""
    data = {"CallbackId": "callback-123"}

    request_obj = SendDurableExecutionCallbackHeartbeatRequest.from_dict(data)
    assert request_obj.callback_id == "callback-123"

    result_data = request_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = SendDurableExecutionCallbackHeartbeatRequest.from_dict(result_data)
    assert round_trip == request_obj


def test_send_durable_execution_callback_heartbeat_response_creation():
    """Test SendDurableExecutionCallbackHeartbeatResponse creation."""
    response_obj = SendDurableExecutionCallbackHeartbeatResponse()
    assert isinstance(response_obj, SendDurableExecutionCallbackHeartbeatResponse)


def test_checkpoint_durable_execution_request_serialization():
    """Test CheckpointDurableExecutionRequest from_dict/to_dict round-trip."""
    execution_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    data = {
        "CheckpointToken": "checkpoint-123",
        "Updates": [
            {"Id": "op-1", "Type": "STEP", "Action": "SUCCEED"},
            {"Id": "op-2", "Type": "CONTEXT", "Action": "START"},
        ],
        "ClientToken": "client-token-123",
    }

    request_obj = CheckpointDurableExecutionRequest.from_dict(data, execution_arn)
    assert request_obj.durable_execution_arn == execution_arn
    assert request_obj.checkpoint_token == "checkpoint-123"  # noqa: S105
    assert len(request_obj.updates) == 2
    assert request_obj.updates[0].operation_id == "op-1"
    assert request_obj.updates[0].operation_type.value == "STEP"
    assert request_obj.updates[0].action.value == "SUCCEED"
    assert request_obj.updates[1].operation_id == "op-2"
    assert request_obj.updates[1].operation_type.value == "CONTEXT"
    assert request_obj.updates[1].action.value == "START"
    assert request_obj.client_token == "client-token-123"  # noqa: S105

    result_data = request_obj.to_dict()
    expected_data = {"DurableExecutionArn": execution_arn, **data}
    assert result_data == expected_data

    # Test round-trip
    round_trip = CheckpointDurableExecutionRequest.from_dict(result_data, execution_arn)
    assert round_trip == request_obj


def test_checkpoint_durable_execution_request_minimal():
    """Test CheckpointDurableExecutionRequest with only required fields."""
    execution_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )
    data = {
        "CheckpointToken": "checkpoint-123",
    }

    request_obj = CheckpointDurableExecutionRequest.from_dict(data, execution_arn)
    assert request_obj.updates is None
    assert request_obj.client_token is None

    result_data = request_obj.to_dict()
    expected_data = {"DurableExecutionArn": execution_arn, **data}
    assert result_data == expected_data


def test_checkpoint_durable_execution_response_serialization():
    """Test CheckpointDurableExecutionResponse from_dict/to_dict round-trip."""
    data = {
        "CheckpointToken": "new-checkpoint-123",
        "NewExecutionState": {
            "Operations": [{"Id": "op-1", "Type": "STEP", "Status": "SUCCEEDED"}],
            "NextMarker": "marker-123",
        },
    }

    response_obj = CheckpointDurableExecutionResponse.from_dict(data)
    assert response_obj.checkpoint_token == "new-checkpoint-123"  # noqa: S105
    assert response_obj.new_execution_state is not None
    assert len(response_obj.new_execution_state.operations) == 1
    assert response_obj.new_execution_state.operations[0].operation_id == "op-1"
    assert response_obj.new_execution_state.next_marker == "marker-123"

    result_data = response_obj.to_dict()
    assert result_data == data

    # Test round-trip
    round_trip = CheckpointDurableExecutionResponse.from_dict(result_data)
    assert round_trip == response_obj


def test_checkpoint_durable_execution_response_minimal():
    """Test CheckpointDurableExecutionResponse with only required fields."""
    data = {"CheckpointToken": "new-checkpoint-123"}

    response_obj = CheckpointDurableExecutionResponse.from_dict(data)
    assert response_obj.new_execution_state is None

    result_data = response_obj.to_dict()
    assert result_data == data


def test_error_response_creation():
    """Test ErrorResponse creation with all fields."""
    error_response = ErrorResponse(
        error_type="InvalidParameterValueException",
        error_message="Invalid parameter value",
        error_code="INVALID_PARAMETER",
        request_id="req-123",
    )

    assert error_response.error_type == "InvalidParameterValueException"
    assert error_response.error_message == "Invalid parameter value"
    assert error_response.error_code == "INVALID_PARAMETER"
    assert error_response.request_id == "req-123"


def test_error_response_creation_minimal():
    """Test ErrorResponse creation with minimal fields."""
    error_response = ErrorResponse(
        error_type="ServiceException",
        error_message="Internal server error",
    )

    assert error_response.error_type == "ServiceException"
    assert error_response.error_message == "Internal server error"
    assert error_response.error_code is None
    assert error_response.request_id is None


def test_error_response_to_dict_complete():
    """Test ErrorResponse.to_dict() with all fields."""
    error_response = ErrorResponse(
        error_type="ResourceNotFoundException",
        error_message="Resource not found",
        error_code="RESOURCE_NOT_FOUND",
        request_id="req-456",
    )

    result = error_response.to_dict()

    expected = {
        "error": {
            "type": "ResourceNotFoundException",
            "message": "Resource not found",
            "code": "RESOURCE_NOT_FOUND",
            "requestId": "req-456",
        }
    }

    assert result == expected


def test_error_response_to_dict_minimal():
    """Test ErrorResponse.to_dict() with minimal fields."""
    error_response = ErrorResponse(
        error_type="ConflictException",
        error_message="Resource conflict",
    )

    result = error_response.to_dict()

    expected = {
        "error": {
            "type": "ConflictException",
            "message": "Resource conflict",
        }
    }

    assert result == expected


def test_error_response_from_dict_nested():
    """Test ErrorResponse.from_dict() with nested error structure."""
    data = {
        "error": {
            "type": "InvalidParameterValueException",
            "message": "Invalid input",
            "code": "INVALID_INPUT",
            "requestId": "req-789",
        }
    }

    error_response = ErrorResponse.from_dict(data)

    assert error_response.error_type == "InvalidParameterValueException"
    assert error_response.error_message == "Invalid input"
    assert error_response.error_code == "INVALID_INPUT"
    assert error_response.request_id == "req-789"


def test_error_response_from_dict_flat():
    """Test ErrorResponse.from_dict() with flat error structure."""
    data = {
        "type": "ServiceException",
        "message": "Internal error",
        "code": "INTERNAL_ERROR",
    }

    error_response = ErrorResponse.from_dict(data)

    assert error_response.error_type == "ServiceException"
    assert error_response.error_message == "Internal error"
    assert error_response.error_code == "INTERNAL_ERROR"
    assert error_response.request_id is None


def test_error_response_from_dict_minimal():
    """Test ErrorResponse.from_dict() with minimal fields."""
    data = {
        "error": {
            "type": "TooManyRequestsException",
            "message": "Rate limit exceeded",
        }
    }

    error_response = ErrorResponse.from_dict(data)

    assert error_response.error_type == "TooManyRequestsException"
    assert error_response.error_message == "Rate limit exceeded"
    assert error_response.error_code is None
    assert error_response.request_id is None


def test_error_response_round_trip():
    """Test ErrorResponse round-trip serialization."""
    original = ErrorResponse(
        error_type="ExecutionAlreadyStartedException",
        error_message="Execution already exists",
        error_code="EXECUTION_ALREADY_STARTED",
        request_id="req-round-trip",
    )

    # Convert to dict and back
    data = original.to_dict()
    restored = ErrorResponse.from_dict(data)

    assert restored.error_type == original.error_type
    assert restored.error_message == original.error_message
    assert restored.error_code == original.error_code
    assert restored.request_id == original.request_id


def test_error_response_immutable():
    """Test that ErrorResponse is immutable (frozen dataclass)."""
    error_response = ErrorResponse(
        error_type="TestException",
        error_message="Test message",
    )

    with pytest.raises(AttributeError):
        error_response.error_type = "ModifiedException"  # type: ignore


# Tests for missing coverage in StartDurableExecutionInput
def test_start_durable_execution_input_missing_required_fields():
    """Test StartDurableExecutionInput validation with missing required fields."""
    # Test missing AccountId
    data = {
        "FunctionName": "my-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    with pytest.raises(InvalidParameterValueException) as exc_info:
        StartDurableExecutionInput.from_dict(data)
    assert "Missing required field: AccountId" in str(exc_info.value)

    # Test missing FunctionName
    data = {
        "AccountId": "123456789012",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    with pytest.raises(InvalidParameterValueException) as exc_info:
        StartDurableExecutionInput.from_dict(data)
    assert "Missing required field: FunctionName" in str(exc_info.value)

    # Test missing FunctionQualifier
    data = {
        "AccountId": "123456789012",
        "FunctionName": "my-function",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    with pytest.raises(InvalidParameterValueException) as exc_info:
        StartDurableExecutionInput.from_dict(data)
    assert "Missing required field: FunctionQualifier" in str(exc_info.value)

    # Test missing ExecutionName
    data = {
        "AccountId": "123456789012",
        "FunctionName": "my-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    with pytest.raises(InvalidParameterValueException) as exc_info:
        StartDurableExecutionInput.from_dict(data)
    assert "Missing required field: ExecutionName" in str(exc_info.value)

    # Test missing ExecutionTimeoutSeconds
    data = {
        "AccountId": "123456789012",
        "FunctionName": "my-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionRetentionPeriodDays": 7,
    }

    with pytest.raises(InvalidParameterValueException) as exc_info:
        StartDurableExecutionInput.from_dict(data)
    assert "Missing required field: ExecutionTimeoutSeconds" in str(exc_info.value)

    # Test missing ExecutionRetentionPeriodDays
    data = {
        "AccountId": "123456789012",
        "FunctionName": "my-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
    }

    with pytest.raises(InvalidParameterValueException) as exc_info:
        StartDurableExecutionInput.from_dict(data)
    assert "Missing required field: ExecutionRetentionPeriodDays" in str(exc_info.value)


# Tests for Execution backward compatibility
def test_execution_backward_compatibility_empty_function_arn():
    """Test Execution with empty FunctionArn for backward compatibility."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "Status": "SUCCEEDED",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
        "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
    }

    execution_obj = Execution.from_dict(data)
    assert (
        execution_obj.function_arn == ""
    )  # Default empty string for backward compatibility

    result_data = execution_obj.to_dict()
    # Empty function_arn should not be included in output
    expected_data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "Status": "SUCCEEDED",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
        "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
    }
    assert result_data == expected_data


def test_execution_with_function_arn():
    """Test Execution with non-empty FunctionArn."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function",
        "Status": "SUCCEEDED",
        "StartTimestamp": TIMESTAMP_2023_01_01_00_00,
        "EndTimestamp": TIMESTAMP_2023_01_01_00_01,
    }

    execution_obj = Execution.from_dict(data)
    assert (
        execution_obj.function_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function"
    )

    result_data = execution_obj.to_dict()
    assert result_data == data


# Tests for ListDurableExecutionsRequest with all optional fields
def test_list_durable_executions_request_all_optional_fields():
    """Test ListDurableExecutionsRequest to_dict with all optional fields as None."""
    request_obj = ListDurableExecutionsRequest(
        function_name=None,
        function_version=None,
        durable_execution_name=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=None,
        reverse_order=None,
    )

    result_data = request_obj.to_dict()
    # Only non-None fields should be included
    expected_data = {}
    assert result_data == expected_data


def test_list_durable_executions_request_partial_fields():
    """Test ListDurableExecutionsRequest to_dict with some optional fields."""
    request_obj = ListDurableExecutionsRequest(
        function_name="my-function",
        function_version=None,
        durable_execution_name="test-execution",
        status_filter=None,
        started_after=TIMESTAMP_2023_01_01_00_00,
        started_before=None,
        marker="marker-123",
        max_items=10,
        reverse_order=None,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "FunctionName": "my-function",
        "DurableExecutionName": "test-execution",
        "StartedAfter": TIMESTAMP_2023_01_01_00_00,
        "Marker": "marker-123",
        "MaxItems": 10,
    }
    assert result_data == expected_data


# Tests for GetDurableExecutionStateRequest with all optional fields
def test_get_durable_execution_state_request_all_optional_fields():
    """Test GetDurableExecutionStateRequest to_dict with all optional fields as None."""
    request_obj = GetDurableExecutionStateRequest(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        checkpoint_token="checkpoint-123",  # noqa: S106
        marker=None,
        max_items=None,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "CheckpointToken": "checkpoint-123",
    }
    assert result_data == expected_data


# Tests for EventInput
def test_event_input_serialization():
    """Test EventInput from_dict/to_dict round-trip."""
    data = {
        "Payload": "test-payload",
        "Truncated": True,
    }

    event_input = EventInput.from_dict(data)
    assert event_input.payload == "test-payload"
    assert event_input.truncated is True

    result_data = event_input.to_dict()
    assert result_data == data


def test_event_input_minimal():
    """Test EventInput with minimal data."""
    data = {}

    event_input = EventInput.from_dict(data)
    assert event_input.payload is None
    assert event_input.truncated is False

    result_data = event_input.to_dict()
    assert result_data == {"Truncated": False}


def test_event_input_with_payload_only():
    """Test EventInput with payload but default truncated."""
    data = {"Payload": "test-payload"}

    event_input = EventInput.from_dict(data)
    assert event_input.payload == "test-payload"
    assert event_input.truncated is False

    result_data = event_input.to_dict()
    assert result_data == {"Payload": "test-payload", "Truncated": False}


# Tests for EventResult
def test_event_result_serialization():
    """Test EventResult from_dict/to_dict round-trip."""
    data = {
        "Payload": "test-result",
        "Truncated": True,
    }

    event_result = EventResult.from_dict(data)
    assert event_result.payload == "test-result"
    assert event_result.truncated is True

    result_data = event_result.to_dict()
    assert result_data == data


def test_event_result_minimal():
    """Test EventResult with minimal data."""
    data = {}

    event_result = EventResult.from_dict(data)
    assert event_result.payload is None
    assert event_result.truncated is False

    result_data = event_result.to_dict()
    assert result_data == {"Truncated": False}


# Tests for EventError
def test_event_error_serialization():
    """Test EventError from_dict/to_dict round-trip."""
    data = {
        "Payload": {"ErrorMessage": "test error"},
        "Truncated": True,
    }

    event_error = EventError.from_dict(data)
    assert event_error.payload.message == "test error"
    assert event_error.truncated is True

    result_data = event_error.to_dict()
    assert result_data == data


def test_event_error_minimal():
    """Test EventError with minimal data."""
    data = {}

    event_error = EventError.from_dict(data)
    assert event_error.payload is None
    assert event_error.truncated is False

    result_data = event_error.to_dict()
    assert result_data == {"Truncated": False}


def test_event_error_with_payload_only():
    """Test EventError with payload but default truncated."""
    data = {"Payload": {"ErrorMessage": "test error"}}

    event_error = EventError.from_dict(data)
    assert event_error.payload.message == "test error"
    assert event_error.truncated is False

    result_data = event_error.to_dict()
    assert result_data == {
        "Payload": {"ErrorMessage": "test error"},
        "Truncated": False,
    }


# Tests for RetryDetails
def test_retry_details_serialization():
    """Test RetryDetails from_dict/to_dict round-trip."""
    data = {
        "CurrentAttempt": 3,
        "NextAttemptDelaySeconds": 60,
    }

    retry_details = RetryDetails.from_dict(data)
    assert retry_details.current_attempt == 3
    assert retry_details.next_attempt_delay_seconds == 60

    result_data = retry_details.to_dict()
    assert result_data == data


def test_retry_details_minimal():
    """Test RetryDetails with minimal data."""
    data = {}

    retry_details = RetryDetails.from_dict(data)
    assert retry_details.current_attempt == 0
    assert retry_details.next_attempt_delay_seconds is None

    result_data = retry_details.to_dict()
    assert result_data == {"CurrentAttempt": 0}


def test_retry_details_with_current_attempt_only():
    """Test RetryDetails with current attempt but no delay."""
    data = {"CurrentAttempt": 2}

    retry_details = RetryDetails.from_dict(data)
    assert retry_details.current_attempt == 2
    assert retry_details.next_attempt_delay_seconds is None

    result_data = retry_details.to_dict()
    assert result_data == {"CurrentAttempt": 2}


# Tests for ExecutionStartedDetails
def test_execution_started_details_serialization():
    """Test ExecutionStartedDetails from_dict/to_dict round-trip."""
    data = {
        "Input": {"Payload": "test-input", "Truncated": False},
        "ExecutionTimeout": 300,
    }

    details = ExecutionStartedDetails.from_dict(data)
    assert details.input.payload == "test-input"
    assert details.execution_timeout == 300

    result_data = details.to_dict()
    assert result_data == data


def test_execution_started_details_minimal():
    """Test ExecutionStartedDetails with minimal data."""
    data = {}

    details = ExecutionStartedDetails.from_dict(data)
    assert details.input is None
    assert details.execution_timeout is None

    result_data = details.to_dict()
    assert result_data == {}


def test_execution_started_details_with_input_only():
    """Test ExecutionStartedDetails with input but no timeout."""
    data = {"Input": {"Payload": "test-input", "Truncated": False}}

    details = ExecutionStartedDetails.from_dict(data)
    assert details.input.payload == "test-input"
    assert details.execution_timeout is None

    result_data = details.to_dict()
    assert result_data == {"Input": {"Payload": "test-input", "Truncated": False}}


# Tests for ExecutionSucceededDetails
def test_execution_succeeded_details_serialization():
    """Test ExecutionSucceededDetails from_dict/to_dict round-trip."""
    data = {
        "Result": {"Payload": "success-result", "Truncated": False},
    }

    details = ExecutionSucceededDetails.from_dict(data)
    assert details.result.payload == "success-result"

    result_data = details.to_dict()
    assert result_data == data


def test_execution_succeeded_details_minimal():
    """Test ExecutionSucceededDetails with minimal data."""
    data = {}

    details = ExecutionSucceededDetails.from_dict(data)
    assert details.result is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ExecutionFailedDetails
def test_execution_failed_details_serialization():
    """Test ExecutionFailedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "execution failed"}, "Truncated": False},
    }

    details = ExecutionFailedDetails.from_dict(data)
    assert details.error.payload.message == "execution failed"

    result_data = details.to_dict()
    assert result_data == data


def test_execution_failed_details_minimal():
    """Test ExecutionFailedDetails with minimal data."""
    data = {}

    details = ExecutionFailedDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ExecutionTimedOutDetails
def test_execution_timed_out_details_serialization():
    """Test ExecutionTimedOutDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {
            "Payload": {"ErrorMessage": "execution timed out"},
            "Truncated": False,
        },
    }

    details = ExecutionTimedOutDetails.from_dict(data)
    assert details.error.payload.message == "execution timed out"

    result_data = details.to_dict()
    assert result_data == data


def test_execution_timed_out_details_minimal():
    """Test ExecutionTimedOutDetails with minimal data."""
    data = {}

    details = ExecutionTimedOutDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ExecutionStoppedDetails
def test_execution_stopped_details_serialization():
    """Test ExecutionStoppedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "execution stopped"}, "Truncated": False},
    }

    details = ExecutionStoppedDetails.from_dict(data)
    assert details.error.payload.message == "execution stopped"

    result_data = details.to_dict()
    assert result_data == data


def test_execution_stopped_details_minimal():
    """Test ExecutionStoppedDetails with minimal data."""
    data = {}

    details = ExecutionStoppedDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ContextStartedDetails
def test_context_started_details_serialization():
    """Test ContextStartedDetails from_dict/to_dict round-trip."""
    # ContextStartedDetails ignores input data and always returns empty dict
    data = {"dummy": "value"}  # Can provide any data

    details = ContextStartedDetails.from_dict(data)
    assert isinstance(details, ContextStartedDetails)

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ContextSucceededDetails
def test_context_succeeded_details_serialization():
    """Test ContextSucceededDetails from_dict/to_dict round-trip."""
    data = {
        "Result": {"Payload": "context-result", "Truncated": False},
    }

    details = ContextSucceededDetails.from_dict(data)
    assert details.result.payload == "context-result"

    result_data = details.to_dict()
    assert result_data == data


def test_context_succeeded_details_minimal():
    """Test ContextSucceededDetails with minimal data."""
    data = {}

    details = ContextSucceededDetails.from_dict(data)
    assert details.result is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ContextFailedDetails
def test_context_failed_details_serialization():
    """Test ContextFailedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "context failed"}, "Truncated": False},
    }

    details = ContextFailedDetails.from_dict(data)
    assert details.error.payload.message == "context failed"

    result_data = details.to_dict()
    assert result_data == data


def test_context_failed_details_minimal():
    """Test ContextFailedDetails with minimal data."""
    data = {}

    details = ContextFailedDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for WaitStartedDetails
def test_wait_started_details_serialization():
    """Test WaitStartedDetails from_dict/to_dict round-trip."""
    data = {
        "Duration": 60,
        "ScheduledEndTimestamp": TIMESTAMP_2023_01_01_00_01,
    }

    details = WaitStartedDetails.from_dict(data)
    assert details.duration == 60
    assert details.scheduled_end_timestamp == TIMESTAMP_2023_01_01_00_01

    result_data = details.to_dict()
    assert result_data == data


def test_wait_started_details_minimal():
    """Test WaitStartedDetails with minimal data."""
    data = {}

    details = WaitStartedDetails.from_dict(data)
    assert details.duration is None
    assert details.scheduled_end_timestamp is None

    result_data = details.to_dict()
    assert result_data == {}


def test_wait_started_details_with_duration_only():
    """Test WaitStartedDetails with duration but no timestamp."""
    data = {"Duration": 30}

    details = WaitStartedDetails.from_dict(data)
    assert details.duration == 30
    assert details.scheduled_end_timestamp is None

    result_data = details.to_dict()
    assert result_data == {"Duration": 30}


# Tests for WaitSucceededDetails
def test_wait_succeeded_details_serialization():
    """Test WaitSucceededDetails from_dict/to_dict round-trip."""
    data = {"Duration": 60}

    details = WaitSucceededDetails.from_dict(data)
    assert details.duration == 60

    result_data = details.to_dict()
    assert result_data == data


def test_wait_succeeded_details_minimal():
    """Test WaitSucceededDetails with minimal data."""
    data = {}

    details = WaitSucceededDetails.from_dict(data)
    assert details.duration is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for WaitCancelledDetails
def test_wait_cancelled_details_serialization():
    """Test WaitCancelledDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "wait cancelled"}, "Truncated": False},
    }

    details = WaitCancelledDetails.from_dict(data)
    assert details.error.payload.message == "wait cancelled"

    result_data = details.to_dict()
    assert result_data == data


def test_wait_cancelled_details_minimal():
    """Test WaitCancelledDetails with minimal data."""
    data = {}

    details = WaitCancelledDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for StepStartedDetails
def test_step_started_details_serialization():
    """Test StepStartedDetails from_dict/to_dict round-trip."""
    # StepStartedDetails ignores input data and always returns empty dict
    data = {"dummy": "value"}  # Can provide any data

    details = StepStartedDetails.from_dict(data)
    assert isinstance(details, StepStartedDetails)

    result_data = details.to_dict()
    assert result_data == {}


# Tests for StepSucceededDetails
def test_step_succeeded_details_serialization():
    """Test StepSucceededDetails from_dict/to_dict round-trip."""
    data = {
        "Result": {"Payload": "step-result", "Truncated": False},
        "RetryDetails": {"CurrentAttempt": 2, "NextAttemptDelaySeconds": 30},
    }

    details = StepSucceededDetails.from_dict(data)
    assert details.result.payload == "step-result"
    assert details.retry_details.current_attempt == 2

    result_data = details.to_dict()
    assert result_data == data


def test_step_succeeded_details_minimal():
    """Test StepSucceededDetails with minimal data."""
    data = {}

    details = StepSucceededDetails.from_dict(data)
    assert details.result is None
    assert details.retry_details is None

    result_data = details.to_dict()
    assert result_data == {}


def test_step_succeeded_details_with_result_only():
    """Test StepSucceededDetails with result but no retry details."""
    data = {"Result": {"Payload": "step-result", "Truncated": False}}

    details = StepSucceededDetails.from_dict(data)
    assert details.result.payload == "step-result"
    assert details.retry_details is None

    result_data = details.to_dict()
    assert result_data == {"Result": {"Payload": "step-result", "Truncated": False}}


# Tests for StepFailedDetails
def test_step_failed_details_serialization():
    """Test StepFailedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "step failed"}, "Truncated": False},
        "RetryDetails": {"CurrentAttempt": 1, "NextAttemptDelaySeconds": 15},
    }

    details = StepFailedDetails.from_dict(data)
    assert details.error.payload.message == "step failed"
    assert details.retry_details.current_attempt == 1

    result_data = details.to_dict()
    assert result_data == data


def test_step_failed_details_minimal():
    """Test StepFailedDetails with minimal data."""
    data = {}

    details = StepFailedDetails.from_dict(data)
    assert details.error is None
    assert details.retry_details is None

    result_data = details.to_dict()
    assert result_data == {}


def test_step_failed_details_with_error_only():
    """Test StepFailedDetails with error but no retry details."""
    data = {"Error": {"Payload": {"ErrorMessage": "step failed"}, "Truncated": False}}

    details = StepFailedDetails.from_dict(data)
    assert details.error.payload.message == "step failed"
    assert details.retry_details is None

    result_data = details.to_dict()
    assert result_data == {
        "Error": {"Payload": {"ErrorMessage": "step failed"}, "Truncated": False}
    }


# Tests for ChainedInvokeStartedDetails
def test_invoke_started_details_serialization():
    """Test ChainedInvokeStartedDetails from_dict/to_dict round-trip."""
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
    }

    details = ChainedInvokeStartedDetails.from_dict(data)
    assert (
        details.durable_execution_arn
        == "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test"
    )

    result_data = details.to_dict()
    assert result_data == data


def test_invoke_started_details_minimal():
    """Test ChainedInvokeStartedDetails with minimal data."""
    data = {}

    details = ChainedInvokeStartedDetails.from_dict(data)
    assert details.durable_execution_arn is None

    result_data = details.to_dict()
    assert result_data == {}


def test_invoke_started_details_partial():
    """Test ChainedInvokeStartedDetails with partial data."""
    data = {}
    details = ChainedInvokeStartedDetails.from_dict(data)
    assert details.durable_execution_arn is None


# Tests for ChainedInvokeSucceededDetails
def test_invoke_succeeded_details_serialization():
    """Test ChainedInvokeSucceededDetails from_dict/to_dict round-trip."""
    data = {
        "Result": {"Payload": "invoke-result", "Truncated": False},
    }

    details = ChainedInvokeSucceededDetails.from_dict(data)
    assert details.result.payload == "invoke-result"

    result_data = details.to_dict()
    assert result_data == data


def test_invoke_succeeded_details_minimal():
    """Test ChainedInvokeSucceededDetails with minimal data."""
    data = {}

    details = ChainedInvokeSucceededDetails.from_dict(data)
    assert details.result is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ChainedInvokeFailedDetails
def test_invoke_failed_details_serialization():
    """Test ChainedInvokeFailedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "invoke failed"}, "Truncated": False},
    }

    details = ChainedInvokeFailedDetails.from_dict(data)
    assert details.error.payload.message == "invoke failed"

    result_data = details.to_dict()
    assert result_data == data


def test_invoke_failed_details_minimal():
    """Test ChainedInvokeFailedDetails with minimal data."""
    data = {}

    details = ChainedInvokeFailedDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ChainedInvokeTimedOutDetails
def test_invoke_timed_out_details_serialization():
    """Test ChainedInvokeTimedOutDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "invoke timed out"}, "Truncated": False},
    }

    details = ChainedInvokeTimedOutDetails.from_dict(data)
    assert details.error.payload.message == "invoke timed out"

    result_data = details.to_dict()
    assert result_data == data


def test_invoke_timed_out_details_minimal():
    """Test ChainedInvokeTimedOutDetails with minimal data."""
    data = {}

    details = ChainedInvokeTimedOutDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for ChainedInvokeStoppedDetails
def test_invoke_stopped_details_serialization():
    """Test ChainedInvokeStoppedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "invoke stopped"}, "Truncated": False},
    }

    details = ChainedInvokeStoppedDetails.from_dict(data)
    assert details.error.payload.message == "invoke stopped"

    result_data = details.to_dict()
    assert result_data == data


def test_invoke_stopped_details_minimal():
    """Test ChainedInvokeStoppedDetails with minimal data."""
    data = {}

    details = ChainedInvokeStoppedDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for CallbackStartedDetails
def test_callback_started_details_serialization():
    """Test CallbackStartedDetails from_dict/to_dict round-trip."""
    data = {
        "CallbackId": "callback-123",
        "HeartbeatTimeout": 60,
        "Timeout": 300,
    }

    details = CallbackStartedDetails.from_dict(data)
    assert details.callback_id == "callback-123"
    assert details.heartbeat_timeout == 60
    assert details.timeout == 300

    result_data = details.to_dict()
    assert result_data == data


def test_callback_started_details_minimal():
    """Test CallbackStartedDetails with minimal data."""
    data = {}

    details = CallbackStartedDetails.from_dict(data)
    assert details.callback_id is None
    assert details.heartbeat_timeout is None
    assert details.timeout is None

    result_data = details.to_dict()
    assert result_data == {}


def test_callback_started_details_partial():
    """Test CallbackStartedDetails with partial data."""
    data = {
        "CallbackId": "callback-123",
        "Timeout": 300,
    }

    details = CallbackStartedDetails.from_dict(data)
    assert details.callback_id == "callback-123"
    assert details.heartbeat_timeout is None
    assert details.timeout == 300

    result_data = details.to_dict()
    assert result_data == data


# Tests for CallbackSucceededDetails
def test_callback_succeeded_details_serialization():
    """Test CallbackSucceededDetails from_dict/to_dict round-trip."""
    data = {
        "Result": {"Payload": "callback-result", "Truncated": False},
    }

    details = CallbackSucceededDetails.from_dict(data)
    assert details.result.payload == "callback-result"

    result_data = details.to_dict()
    assert result_data == data


def test_callback_succeeded_details_minimal():
    """Test CallbackSucceededDetails with minimal data."""
    data = {}

    details = CallbackSucceededDetails.from_dict(data)
    assert details.result is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for CallbackFailedDetails
def test_callback_failed_details_serialization():
    """Test CallbackFailedDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {"Payload": {"ErrorMessage": "callback failed"}, "Truncated": False},
    }

    details = CallbackFailedDetails.from_dict(data)
    assert details.error.payload.message == "callback failed"

    result_data = details.to_dict()
    assert result_data == data


def test_callback_failed_details_minimal():
    """Test CallbackFailedDetails with minimal data."""
    data = {}

    details = CallbackFailedDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for CallbackTimedOutDetails
def test_callback_timed_out_details_serialization():
    """Test CallbackTimedOutDetails from_dict/to_dict round-trip."""
    data = {
        "Error": {
            "Payload": {"ErrorMessage": "callback timed out"},
            "Truncated": False,
        },
    }

    details = CallbackTimedOutDetails.from_dict(data)
    assert details.error.payload.message == "callback timed out"

    result_data = details.to_dict()
    assert result_data == data


def test_callback_timed_out_details_minimal():
    """Test CallbackTimedOutDetails with minimal data."""
    data = {}

    details = CallbackTimedOutDetails.from_dict(data)
    assert details.error is None

    result_data = details.to_dict()
    assert result_data == {}


# Tests for Event class with all detail types
def test_event_with_execution_succeeded_details():
    """Test Event with ExecutionSucceededDetails."""
    data = {
        "EventType": "ExecutionSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ExecutionSucceededDetails": {
            "Result": {"Payload": "success", "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ExecutionSucceeded"
    assert event_obj.execution_succeeded_details is not None
    assert event_obj.execution_succeeded_details.result.payload == "success"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ExecutionSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,  # Default value
        "ExecutionSucceededDetails": {
            "Result": {"Payload": "success", "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_execution_failed_details():
    """Test Event with ExecutionFailedDetails."""
    data = {
        "EventType": "ExecutionFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ExecutionFailedDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "execution failed"},
                "Truncated": False,
            }
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ExecutionFailed"
    assert event_obj.execution_failed_details is not None
    assert (
        event_obj.execution_failed_details.error.payload.message == "execution failed"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ExecutionFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ExecutionFailedDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "execution failed"},
                "Truncated": False,
            }
        },
    }
    assert result_data == expected_data


def test_event_with_execution_timed_out_details():
    """Test Event with ExecutionTimedOutDetails."""
    data = {
        "EventType": "ExecutionTimedOut",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ExecutionTimedOutDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "execution timed out"},
                "Truncated": False,
            }
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ExecutionTimedOut"
    assert event_obj.execution_timed_out_details is not None
    assert (
        event_obj.execution_timed_out_details.error.payload.message
        == "execution timed out"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ExecutionTimedOut",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ExecutionTimedOutDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "execution timed out"},
                "Truncated": False,
            }
        },
    }
    assert result_data == expected_data


def test_event_with_execution_stopped_details():
    """Test Event with ExecutionStoppedDetails."""
    data = {
        "EventType": "ExecutionStopped",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ExecutionStoppedDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "execution stopped"},
                "Truncated": False,
            }
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ExecutionStopped"
    assert event_obj.execution_stopped_details is not None
    assert (
        event_obj.execution_stopped_details.error.payload.message == "execution stopped"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ExecutionStopped",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ExecutionStoppedDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "execution stopped"},
                "Truncated": False,
            }
        },
    }
    assert result_data == expected_data


def test_event_with_context_started_details():
    """Test Event with ContextStartedDetails."""
    # Since ContextStartedDetails has no fields and empty dict is falsy,
    # we need to provide a non-empty dict or test without the key
    data = {
        "EventType": "ContextStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ContextStartedDetails": {"dummy": "value"},  # Non-empty to be truthy
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ContextStarted"
    assert event_obj.context_started_details is not None

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ContextStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ContextStartedDetails": {},  # to_dict() returns empty dict
    }
    assert result_data == expected_data


def test_event_with_context_succeeded_details():
    """Test Event with ContextSucceededDetails."""
    data = {
        "EventType": "ContextSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ContextSucceededDetails": {
            "Result": {"Payload": "context result", "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ContextSucceeded"
    assert event_obj.context_succeeded_details is not None
    assert event_obj.context_succeeded_details.result.payload == "context result"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ContextSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ContextSucceededDetails": {
            "Result": {"Payload": "context result", "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_context_failed_details():
    """Test Event with ContextFailedDetails."""
    data = {
        "EventType": "ContextFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ContextFailedDetails": {
            "Error": {"Payload": {"ErrorMessage": "context failed"}, "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ContextFailed"
    assert event_obj.context_failed_details is not None
    assert event_obj.context_failed_details.error.payload.message == "context failed"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ContextFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ContextFailedDetails": {
            "Error": {"Payload": {"ErrorMessage": "context failed"}, "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_wait_started_details():
    """Test Event with WaitStartedDetails."""
    data = {
        "EventType": "WaitStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "WaitStartedDetails": {
            "Duration": 60,
            "ScheduledEndTimestamp": TIMESTAMP_2023_01_01_00_02,
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "WaitStarted"
    assert event_obj.wait_started_details is not None
    assert event_obj.wait_started_details.duration == 60

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "WaitStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "WaitStartedDetails": {
            "Duration": 60,
            "ScheduledEndTimestamp": TIMESTAMP_2023_01_01_00_02,
        },
    }
    assert result_data == expected_data


def test_event_with_wait_succeeded_details():
    """Test Event with WaitSucceededDetails."""
    data = {
        "EventType": "WaitSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "WaitSucceededDetails": {"Duration": 60},
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "WaitSucceeded"
    assert event_obj.wait_succeeded_details is not None
    assert event_obj.wait_succeeded_details.duration == 60

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "WaitSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "WaitSucceededDetails": {"Duration": 60},
    }
    assert result_data == expected_data


def test_event_with_wait_cancelled_details():
    """Test Event with WaitCancelledDetails."""
    data = {
        "EventType": "WaitCancelled",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "WaitCancelledDetails": {
            "Error": {"Payload": {"ErrorMessage": "wait cancelled"}, "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "WaitCancelled"
    assert event_obj.wait_cancelled_details is not None
    assert event_obj.wait_cancelled_details.error.payload.message == "wait cancelled"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "WaitCancelled",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "WaitCancelledDetails": {
            "Error": {"Payload": {"ErrorMessage": "wait cancelled"}, "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_step_started_details():
    """Test Event with StepStartedDetails."""
    # Since StepStartedDetails has no fields and empty dict is falsy,
    # we need to provide a non-empty dict or test without the key
    data = {
        "EventType": "StepStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "StepStartedDetails": {"dummy": "value"},  # Non-empty to be truthy
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "StepStarted"
    assert event_obj.step_started_details is not None

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "StepStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "StepStartedDetails": {},  # to_dict() returns empty dict
    }
    assert result_data == expected_data


def test_event_with_step_succeeded_details():
    """Test Event with StepSucceededDetails."""
    data = {
        "EventType": "StepSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "StepSucceededDetails": {
            "Result": {"Payload": "step result", "Truncated": False},
            "RetryDetails": {"CurrentAttempt": 1},
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "StepSucceeded"
    assert event_obj.step_succeeded_details is not None
    assert event_obj.step_succeeded_details.result.payload == "step result"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "StepSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "StepSucceededDetails": {
            "Result": {"Payload": "step result", "Truncated": False},
            "RetryDetails": {"CurrentAttempt": 1},
        },
    }
    assert result_data == expected_data


def test_event_with_step_failed_details():
    """Test Event with StepFailedDetails."""
    data = {
        "EventType": "StepFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "StepFailedDetails": {
            "Error": {"Payload": {"ErrorMessage": "step failed"}, "Truncated": False},
            "RetryDetails": {"CurrentAttempt": 2, "NextAttemptDelaySeconds": 30},
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "StepFailed"
    assert event_obj.step_failed_details is not None
    assert event_obj.step_failed_details.error.payload.message == "step failed"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "StepFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "StepFailedDetails": {
            "Error": {"Payload": {"ErrorMessage": "step failed"}, "Truncated": False},
            "RetryDetails": {"CurrentAttempt": 2, "NextAttemptDelaySeconds": 30},
        },
    }
    assert result_data == expected_data


def test_event_with_invoke_started_details():
    """Test Event with ChainedInvokeStartedDetails."""
    data = {
        "EventType": "ChainedInvokeStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ChainedInvokeStartedDetails": {
            "DurableExecutionArn": "arn:aws:durable-execution:us-east-1:123456789012:execution:my-execution:1234567890",
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ChainedInvokeStarted"
    assert event_obj.chained_invoke_started_details is not None

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ChainedInvokeStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ChainedInvokeStartedDetails": {
            "DurableExecutionArn": "arn:aws:durable-execution:us-east-1:123456789012:execution:my-execution:1234567890",
        },
    }
    assert result_data == expected_data


def test_event_with_invoke_succeeded_details():
    """Test Event with ChainedInvokeSucceededDetails."""
    data = {
        "EventType": "ChainedInvokeSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ChainedInvokeSucceededDetails": {
            "Result": {"Payload": "invoke result", "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ChainedInvokeSucceeded"
    assert event_obj.chained_invoke_succeeded_details is not None
    assert event_obj.chained_invoke_succeeded_details.result.payload == "invoke result"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ChainedInvokeSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ChainedInvokeSucceededDetails": {
            "Result": {"Payload": "invoke result", "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_invoke_failed_details():
    """Test Event with ChainedInvokeFailedDetails."""
    data = {
        "EventType": "ChainedInvokeFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ChainedInvokeFailedDetails": {
            "Error": {"Payload": {"ErrorMessage": "invoke failed"}, "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ChainedInvokeFailed"
    assert event_obj.chained_invoke_failed_details is not None
    assert (
        event_obj.chained_invoke_failed_details.error.payload.message == "invoke failed"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ChainedInvokeFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ChainedInvokeFailedDetails": {
            "Error": {"Payload": {"ErrorMessage": "invoke failed"}, "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_invoke_timed_out_details():
    """Test Event with ChainedInvokeTimedOutDetails."""
    data = {
        "EventType": "ChainedInvokeTimedOut",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ChainedInvokeTimedOutDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "invoke timed out"},
                "Truncated": False,
            }
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ChainedInvokeTimedOut"
    assert event_obj.chained_invoke_timed_out_details is not None
    assert (
        event_obj.chained_invoke_timed_out_details.error.payload.message
        == "invoke timed out"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ChainedInvokeTimedOut",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ChainedInvokeTimedOutDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "invoke timed out"},
                "Truncated": False,
            }
        },
    }
    assert result_data == expected_data


def test_event_with_invoke_stopped_details():
    """Test Event with ChainedInvokeStoppedDetails."""
    data = {
        "EventType": "ChainedInvokeStopped",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "ChainedInvokeStoppedDetails": {
            "Error": {"Payload": {"ErrorMessage": "invoke stopped"}, "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "ChainedInvokeStopped"
    assert event_obj.chained_invoke_stopped_details is not None
    assert (
        event_obj.chained_invoke_stopped_details.error.payload.message
        == "invoke stopped"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "ChainedInvokeStopped",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "ChainedInvokeStoppedDetails": {
            "Error": {"Payload": {"ErrorMessage": "invoke stopped"}, "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_callback_started_details():
    """Test Event with CallbackStartedDetails."""
    data = {
        "EventType": "CallbackStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "CallbackStartedDetails": {
            "CallbackId": "callback-123",
            "HeartbeatTimeout": 60,
            "Timeout": 300,
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "CallbackStarted"
    assert event_obj.callback_started_details is not None
    assert event_obj.callback_started_details.callback_id == "callback-123"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "CallbackStarted",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "CallbackStartedDetails": {
            "CallbackId": "callback-123",
            "HeartbeatTimeout": 60,
            "Timeout": 300,
        },
    }
    assert result_data == expected_data


def test_event_with_callback_succeeded_details():
    """Test Event with CallbackSucceededDetails."""
    data = {
        "EventType": "CallbackSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "CallbackSucceededDetails": {
            "Result": {"Payload": "callback result", "Truncated": False}
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "CallbackSucceeded"
    assert event_obj.callback_succeeded_details is not None
    assert event_obj.callback_succeeded_details.result.payload == "callback result"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "CallbackSucceeded",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "CallbackSucceededDetails": {
            "Result": {"Payload": "callback result", "Truncated": False}
        },
    }
    assert result_data == expected_data


def test_event_with_callback_failed_details():
    """Test Event with CallbackFailedDetails."""
    data = {
        "EventType": "CallbackFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "CallbackFailedDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "callback failed"},
                "Truncated": False,
            }
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "CallbackFailed"
    assert event_obj.callback_failed_details is not None
    assert event_obj.callback_failed_details.error.payload.message == "callback failed"

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "CallbackFailed",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "CallbackFailedDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "callback failed"},
                "Truncated": False,
            }
        },
    }
    assert result_data == expected_data


def test_event_with_callback_timed_out_details():
    """Test Event with CallbackTimedOutDetails."""
    data = {
        "EventType": "CallbackTimedOut",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "CallbackTimedOutDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "callback timed out"},
                "Truncated": False,
            }
        },
    }

    event_obj = Event.from_dict(data)
    assert event_obj.event_type == "CallbackTimedOut"
    assert event_obj.callback_timed_out_details is not None
    assert (
        event_obj.callback_timed_out_details.error.payload.message
        == "callback timed out"
    )

    result_data = event_obj.to_dict()
    expected_data = {
        "EventType": "CallbackTimedOut",
        "EventTimestamp": TIMESTAMP_2023_01_01_00_01,
        "EventId": 1,
        "CallbackTimedOutDetails": {
            "Error": {
                "Payload": {"ErrorMessage": "callback timed out"},
                "Truncated": False,
            }
        },
    }
    assert result_data == expected_data


# Tests for GetDurableExecutionHistoryRequest with all optional fields
def test_get_durable_execution_history_request_all_optional_fields():
    """Test GetDurableExecutionHistoryRequest to_dict with all optional fields as None."""
    request_obj = GetDurableExecutionHistoryRequest(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        include_execution_data=None,
        reverse_order=None,
        marker=None,
        max_items=None,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
    }
    assert result_data == expected_data


def test_get_durable_execution_history_request_partial_fields():
    """Test GetDurableExecutionHistoryRequest to_dict with some optional fields."""
    request_obj = GetDurableExecutionHistoryRequest(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        include_execution_data=True,
        reverse_order=None,
        marker="marker-123",
        max_items=20,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution:test",
        "IncludeExecutionData": True,
        "Marker": "marker-123",
        "MaxItems": 20,
    }
    assert result_data == expected_data


# Tests for ListDurableExecutionsByFunctionRequest with all optional fields
def test_list_durable_executions_by_function_request_all_optional_fields():
    """Test ListDurableExecutionsByFunctionRequest to_dict with all optional fields as None."""
    request_obj = ListDurableExecutionsByFunctionRequest(
        function_name="my-function",
        qualifier=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=None,
        reverse_order=None,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "FunctionName": "my-function",
    }
    assert result_data == expected_data


def test_list_durable_executions_by_function_request_partial_fields():
    """Test ListDurableExecutionsByFunctionRequest to_dict with some optional fields."""
    request_obj = ListDurableExecutionsByFunctionRequest(
        function_name="my-function",
        qualifier="$LATEST",
        status_filter=["RUNNING"],
        started_after=None,
        started_before=TIMESTAMP_2023_01_02_00_00,
        marker=None,
        max_items=15,
        reverse_order=True,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "FunctionName": "my-function",
        "Qualifier": "$LATEST",
        "StatusFilter": ["RUNNING"],
        "StartedBefore": TIMESTAMP_2023_01_02_00_00,
        "MaxItems": 15,
        "ReverseOrder": True,
    }
    assert result_data == expected_data


# Tests for SendDurableExecutionCallbackSuccessRequest with optional result
def test_send_durable_execution_callback_success_request_with_result():
    """Test SendDurableExecutionCallbackSuccessRequest to_dict with result."""
    request_obj = SendDurableExecutionCallbackSuccessRequest(
        callback_id="callback-123",
        result="success-result",
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "CallbackId": "callback-123",
        "Result": "success-result",
    }
    assert result_data == expected_data


# Tests for SendDurableExecutionCallbackFailureRequest with optional error
def test_send_durable_execution_callback_failure_request_with_error():
    """Test SendDurableExecutionCallbackFailureRequest to_dict with error."""
    request_obj = SendDurableExecutionCallbackFailureRequest(
        callback_id="callback-123",
        error=None,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "CallbackId": "callback-123",
    }
    assert result_data == expected_data


# Test for missing coverage in ListDurableExecutionsByFunctionRequest
def test_list_durable_executions_by_function_request_with_durable_execution_name():
    """Test ListDurableExecutionsByFunctionRequest to_dict with durable_execution_name."""
    request_obj = ListDurableExecutionsByFunctionRequest(
        function_name="my-function",
        qualifier=None,
        durable_execution_name="specific-execution",
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=None,
        reverse_order=None,
    )

    result_data = request_obj.to_dict()
    expected_data = {
        "FunctionName": "my-function",
        "DurableExecutionName": "specific-execution",
    }
    assert result_data == expected_data


# Test for missing branch coverage in CheckpointDurableExecutionResponse
def test_checkpoint_updated_execution_state_with_next_marker():
    """Test CheckpointUpdatedExecutionState to_dict with next_marker."""
    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )

    operation = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )

    state_obj = CheckpointUpdatedExecutionState(
        operations=[operation],
        next_marker="next-marker-123",
    )

    result_data = state_obj.to_dict()
    expected_data = {
        "Operations": [{"Id": "op-1", "Type": "STEP", "Status": "SUCCEEDED"}],
        "NextMarker": "next-marker-123",
    }
    assert result_data == expected_data


# Tests for events_to_operations function


def test_events_to_operations_empty_list():
    """Test events_to_operations with empty event list."""
    from aws_durable_execution_sdk_python_testing.model import events_to_operations

    operations = events_to_operations([])
    assert operations == []


def test_events_to_operations_execution_started():
    """Test events_to_operations with ExecutionStarted event."""
    event = Event(
        event_type="ExecutionStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="exec-1",
        execution_started_details=ExecutionStartedDetails(
            input=EventInput(payload="test-input", truncated=False),
            execution_timeout=300,
        ),
    )

    operations = events_to_operations([event])

    assert len(operations) == 1
    assert operations[0].operation_id == "exec-1"
    assert operations[0].operation_type == OperationType.EXECUTION
    assert operations[0].status == OperationStatus.STARTED
    assert operations[0].execution_details.input_payload == "test-input"


def test_events_to_operations_callback_lifecycle():
    """Test events_to_operations with complete callback lifecycle."""

    started_event = Event(
        event_type="CallbackStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="cb-1",
        name="test-callback",
        callback_started_details=CallbackStartedDetails(callback_id="callback-123"),
    )

    succeeded_event = Event(
        event_type="CallbackSucceeded",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="cb-1",
        callback_succeeded_details=CallbackSucceededDetails(
            result=EventResult(payload="callback-result", truncated=False)
        ),
    )

    operations = events_to_operations([started_event, succeeded_event])

    assert len(operations) == 1
    assert operations[0].operation_id == "cb-1"
    assert operations[0].operation_type == OperationType.CALLBACK
    assert operations[0].status == OperationStatus.SUCCEEDED
    assert operations[0].name == "test-callback"
    assert operations[0].callback_details.callback_id == "callback-123"
    assert operations[0].callback_details.result == "callback-result"
    assert operations[0].callback_details.error is None


def test_events_to_operations_missing_event_type():
    """Test events_to_operations raises error for missing event_type."""
    event = Event(
        event_type=None,
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
    )

    with pytest.raises(
        InvalidParameterValueException, match="Missing required 'event_type' field"
    ):
        events_to_operations([event])


def test_events_to_operations_unknown_event_type():
    """Test events_to_operations raises error for unknown event type."""
    event = Event(
        event_type="UnknownEventType",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="op-1",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Unknown event type: UnknownEventType"
    ):
        events_to_operations([event])


def test_events_to_operations_missing_operation_id():
    """Test events_to_operations raises error for missing operation_id."""
    event = Event(
        event_type="StepStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id=None,
    )

    with pytest.raises(
        InvalidParameterValueException, match="Missing required 'operation_id' field"
    ):
        events_to_operations([event])


def test_events_to_operations_step_with_retry():
    """Test events_to_operations with step retry details."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationStatus,
        OperationType,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        EventResult,
        RetryDetails,
        StepSucceededDetails,
        events_to_operations,
    )

    succeeded_event = Event(
        event_type="StepSucceeded",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="step-1",
        name="test-step",
        step_succeeded_details=StepSucceededDetails(
            result=EventResult(payload="step-result", truncated=False),
            retry_details=RetryDetails(current_attempt=2),
        ),
    )

    operations = events_to_operations([succeeded_event])

    assert len(operations) == 1
    assert operations[0].operation_type == OperationType.STEP
    assert operations[0].status == OperationStatus.SUCCEEDED
    assert operations[0].step_details.result == "step-result"
    assert operations[0].step_details.attempt == 2


def test_events_to_operations_step_failed_with_next_attempt():
    """Test events_to_operations with failed step and next attempt timestamp."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        ErrorObject,
        OperationStatus,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        EventError,
        RetryDetails,
        StepFailedDetails,
        events_to_operations,
    )

    event_time = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    failed_event = Event(
        event_type="StepFailed",
        event_timestamp=event_time,
        operation_id="step-1",
        step_failed_details=StepFailedDetails(
            error=EventError(
                payload=ErrorObject(
                    message="step failed", type=None, data=None, stack_trace=None
                )
            ),
            retry_details=RetryDetails(
                current_attempt=1, next_attempt_delay_seconds=10
            ),
        ),
    )

    operations = events_to_operations([failed_event])

    assert len(operations) == 1
    assert operations[0].status == OperationStatus.FAILED
    assert operations[0].step_details.error.message == "step failed"
    assert operations[0].step_details.attempt == 1
    expected_next_attempt = event_time + datetime.timedelta(seconds=10)
    assert operations[0].step_details.next_attempt_timestamp == expected_next_attempt


def test_events_to_operations_context_succeeded():
    """Test events_to_operations with successful context."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationStatus,
        OperationType,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        ContextSucceededDetails,
        Event,
        EventResult,
        events_to_operations,
    )

    succeeded_event = Event(
        event_type="ContextSucceeded",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="ctx-1",
        name="test-context",
        context_succeeded_details=ContextSucceededDetails(
            result=EventResult(payload="context-result", truncated=False)
        ),
    )

    operations = events_to_operations([succeeded_event])

    assert len(operations) == 1
    assert operations[0].operation_type == OperationType.CONTEXT
    assert operations[0].status == OperationStatus.SUCCEEDED
    assert operations[0].context_details.result == "context-result"
    assert operations[0].context_details.error is None


def test_events_to_operations_chained_invoke_succeeded():
    """Test events_to_operations with successful chained invoke."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationStatus,
        OperationType,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        ChainedInvokeSucceededDetails,
        Event,
        EventResult,
        events_to_operations,
    )

    succeeded_event = Event(
        event_type="ChainedInvokeSucceeded",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="invoke-1",
        name="test-invoke",
        chained_invoke_succeeded_details=ChainedInvokeSucceededDetails(
            result=EventResult(payload="invoke-result", truncated=False)
        ),
    )

    operations = events_to_operations([succeeded_event])

    assert len(operations) == 1
    assert operations[0].operation_type == OperationType.CHAINED_INVOKE
    assert operations[0].status == OperationStatus.SUCCEEDED
    assert operations[0].chained_invoke_details.result == "invoke-result"
    assert operations[0].chained_invoke_details.error is None


def test_events_to_operations_skips_invocation_completed():
    """Test events_to_operations skips InvocationCompleted events."""
    invocation_event = Event(
        event_type="InvocationCompleted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="invocation-1",
    )

    operations = events_to_operations([invocation_event])
    assert len(operations) == 0


def test_events_to_operations_callback_failed():
    """Test events_to_operations with failed callback."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        ErrorObject,
        OperationStatus,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        CallbackFailedDetails,
        CallbackStartedDetails,
        Event,
        EventError,
        events_to_operations,
    )

    started_event = Event(
        event_type="CallbackStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="cb-1",
        callback_started_details=CallbackStartedDetails(callback_id="callback-123"),
    )

    failed_event = Event(
        event_type="CallbackFailed",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="cb-1",
        callback_failed_details=CallbackFailedDetails(
            error=EventError(
                payload=ErrorObject(
                    message="callback failed", type=None, data=None, stack_trace=None
                )
            )
        ),
    )

    operations = events_to_operations([started_event, failed_event])

    assert len(operations) == 1
    assert operations[0].status == OperationStatus.FAILED
    assert operations[0].callback_details.error.message == "callback failed"
    assert operations[0].callback_details.result is None


def test_events_to_operations_callback_timed_out():
    """Test events_to_operations with timed out callback."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        ErrorObject,
        OperationStatus,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        CallbackStartedDetails,
        CallbackTimedOutDetails,
        Event,
        EventError,
        events_to_operations,
    )

    started_event = Event(
        event_type="CallbackStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="cb-1",
        callback_started_details=CallbackStartedDetails(callback_id="callback-123"),
    )

    timed_out_event = Event(
        event_type="CallbackTimedOut",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="cb-1",
        callback_timed_out_details=CallbackTimedOutDetails(
            error=EventError(
                payload=ErrorObject(
                    message="callback timed out", type=None, data=None, stack_trace=None
                )
            )
        ),
    )

    operations = events_to_operations([started_event, timed_out_event])

    assert len(operations) == 1
    assert operations[0].status == OperationStatus.TIMED_OUT
    assert operations[0].callback_details.error.message == "callback timed out"


def test_events_to_operations_wait_started():
    """Test events_to_operations with wait operation."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationStatus,
        OperationType,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        WaitStartedDetails,
        events_to_operations,
    )

    scheduled_time = datetime.datetime(2023, 1, 1, 1, 0, 0, tzinfo=datetime.UTC)
    wait_event = Event(
        event_type="WaitStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="wait-1",
        name="test-wait",
        wait_started_details=WaitStartedDetails(
            duration=3600, scheduled_end_timestamp=scheduled_time
        ),
    )

    operations = events_to_operations([wait_event])

    assert len(operations) == 1
    assert operations[0].operation_type == OperationType.WAIT
    assert operations[0].status == OperationStatus.STARTED
    assert operations[0].wait_details.scheduled_end_timestamp == scheduled_time


def test_events_to_operations_context_failed():
    """Test events_to_operations with failed context."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        ErrorObject,
        OperationStatus,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        ContextFailedDetails,
        Event,
        EventError,
        events_to_operations,
    )

    failed_event = Event(
        event_type="ContextFailed",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="ctx-1",
        context_failed_details=ContextFailedDetails(
            error=EventError(
                payload=ErrorObject(
                    message="context failed", type=None, data=None, stack_trace=None
                )
            )
        ),
    )

    operations = events_to_operations([failed_event])

    assert len(operations) == 1
    assert operations[0].status == OperationStatus.FAILED
    assert operations[0].context_details.error.message == "context failed"
    assert operations[0].context_details.result is None


def test_events_to_operations_chained_invoke_failed():
    """Test events_to_operations with failed chained invoke."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        ErrorObject,
        OperationStatus,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        ChainedInvokeFailedDetails,
        Event,
        EventError,
        events_to_operations,
    )

    failed_event = Event(
        event_type="ChainedInvokeFailed",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
        operation_id="invoke-1",
        chained_invoke_failed_details=ChainedInvokeFailedDetails(
            error=EventError(
                payload=ErrorObject(
                    message="invoke failed", type=None, data=None, stack_trace=None
                )
            )
        ),
    )

    operations = events_to_operations([failed_event])

    assert len(operations) == 1
    assert operations[0].status == OperationStatus.FAILED
    assert operations[0].chained_invoke_details.error.message == "invoke failed"
    assert operations[0].chained_invoke_details.result is None


def test_events_to_operations_multiple_operations():
    """Test events_to_operations with multiple different operations."""
    import datetime

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationStatus,
        OperationType,
    )

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        EventResult,
        StepSucceededDetails,
        events_to_operations,
    )

    events = [
        Event(
            event_type="StepStarted",
            event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
            operation_id="step-1",
            name="step-one",
        ),
        Event(
            event_type="StepSucceeded",
            event_timestamp=datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC),
            operation_id="step-1",
            step_succeeded_details=StepSucceededDetails(
                result=EventResult(payload="result-1", truncated=False)
            ),
        ),
        Event(
            event_type="WaitStarted",
            event_timestamp=datetime.datetime(2023, 1, 1, 0, 2, 0, tzinfo=datetime.UTC),
            operation_id="wait-1",
            name="wait-one",
        ),
    ]

    operations = events_to_operations(events)

    assert len(operations) == 2
    step_op = next(op for op in operations if op.operation_id == "step-1")
    wait_op = next(op for op in operations if op.operation_id == "wait-1")

    assert step_op.operation_type == OperationType.STEP
    assert step_op.status == OperationStatus.SUCCEEDED
    assert step_op.name == "step-one"

    assert wait_op.operation_type == OperationType.WAIT
    assert wait_op.status == OperationStatus.STARTED
    assert wait_op.name == "wait-one"


def test_events_to_operations_merges_timestamps():
    """Test events_to_operations correctly merges start and end timestamps."""
    import datetime

    from aws_durable_execution_sdk_python_testing.model import (
        Event,
        EventResult,
        StepSucceededDetails,
        events_to_operations,
    )

    start_time = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC)

    events = [
        Event(
            event_type="StepStarted",
            event_timestamp=start_time,
            operation_id="step-1",
        ),
        Event(
            event_type="StepSucceeded",
            event_timestamp=end_time,
            operation_id="step-1",
            step_succeeded_details=StepSucceededDetails(
                result=EventResult(payload="result", truncated=False)
            ),
        ),
    ]

    operations = events_to_operations(events)

    assert len(operations) == 1
    assert operations[0].start_timestamp == start_time
    assert operations[0].end_timestamp == end_time


def test_events_to_operations_preserves_parent_id():
    """Test events_to_operations preserves parent_id from events."""
    event = Event(
        event_type="StepStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="step-1",
        parent_id="parent-ctx",
        name="child-step",
    )

    operations = events_to_operations([event])

    assert len(operations) == 1
    assert operations[0].parent_id == "parent-ctx"


def test_events_to_operations_preserves_sub_type():
    """Test events_to_operations preserves sub_type from events."""
    event = Event(
        event_type="StepStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="step-1",
        sub_type="Step",
    )

    operations = events_to_operations([event])

    assert len(operations) == 1
    assert operations[0].sub_type is not None
    assert operations[0].sub_type.value == "Step"


def test_events_to_operations_invalid_sub_type():
    """Test events_to_operations raises InvalidParameterValueException when sub_type is invalid."""
    invalid_sub_type: str = "INVALID_SUB_TYPE"
    event = Event(
        event_type="StepStarted",
        event_timestamp=datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        operation_id="step-1",
        sub_type=invalid_sub_type,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match=f"'{invalid_sub_type}' is not a valid OperationSubType",
    ):
        events_to_operations([event])


def test_invocation_completed_details_to_json_dict():
    """Test InvocationCompletedDetails.to_json_dict() converts datetime to Unix milliseconds."""
    start_time = datetime.datetime(2023, 1, 1, 0, 0, 0, 123456, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 0, 1, 0, 456789, tzinfo=datetime.UTC)

    details = InvocationCompletedDetails(
        start_timestamp=start_time, end_timestamp=end_time, request_id="req-123"
    )

    json_dict = details.to_json_dict()

    # Verify timestamps are converted to Unix milliseconds (integers)
    assert json_dict["StartTimestamp"] == 1672531200123
    assert json_dict["EndTimestamp"] == 1672531260456
    assert json_dict["RequestId"] == "req-123"

    # Verify all values are JSON-serializable
    json_str = json.dumps(json_dict)
    assert json_str is not None


def test_invocation_completed_details_from_json_dict():
    """Test InvocationCompletedDetails.from_json_dict() converts Unix milliseconds to datetime."""
    json_dict = {
        "StartTimestamp": 1672531200123,
        "EndTimestamp": 1672531260456,
        "RequestId": "req-456",
    }

    details = InvocationCompletedDetails.from_json_dict(json_dict)

    # Verify timestamps are converted to datetime objects
    assert details.start_timestamp == datetime.datetime(
        2023, 1, 1, 0, 0, 0, 123000, tzinfo=datetime.UTC
    )
    assert details.end_timestamp == datetime.datetime(
        2023, 1, 1, 0, 1, 0, 456000, tzinfo=datetime.UTC
    )
    assert details.request_id == "req-456"


def test_invocation_completed_details_json_round_trip():
    """Test InvocationCompletedDetails to_json_dict/from_json_dict round-trip."""
    original = InvocationCompletedDetails(
        start_timestamp=datetime.datetime(
            2023, 6, 15, 12, 30, 45, 678000, tzinfo=datetime.UTC
        ),
        end_timestamp=datetime.datetime(
            2023, 6, 15, 12, 31, 50, 123000, tzinfo=datetime.UTC
        ),
        request_id="round-trip-test",
    )

    # Serialize to JSON dict
    json_dict = original.to_json_dict()

    # Deserialize back
    restored = InvocationCompletedDetails.from_json_dict(json_dict)

    # Verify round-trip preserves data
    assert restored.start_timestamp == original.start_timestamp
    assert restored.end_timestamp == original.end_timestamp
    assert restored.request_id == original.request_id


def test_invocation_completed_details_to_dict_preserves_datetime():
    """Test InvocationCompletedDetails.to_dict() preserves datetime objects (not converted)."""
    start_time = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 0, 1, 0, tzinfo=datetime.UTC)

    details = InvocationCompletedDetails(
        start_timestamp=start_time, end_timestamp=end_time, request_id="req-789"
    )

    regular_dict = details.to_dict()

    # Verify to_dict() preserves datetime objects (not converted to Unix milliseconds)
    assert regular_dict["StartTimestamp"] == start_time
    assert regular_dict["EndTimestamp"] == end_time
    assert isinstance(regular_dict["StartTimestamp"], datetime.datetime)
    assert isinstance(regular_dict["EndTimestamp"], datetime.datetime)


def test_invocation_completed_details_from_json_dict_invalid_timestamp():
    """Test InvocationCompletedDetails.from_json_dict() raises error for invalid timestamps."""
    # Test with invalid timestamp that would return None
    json_dict = {
        "StartTimestamp": None,
        "EndTimestamp": 1672531260456,
        "RequestId": "req-error",
    }

    with pytest.raises(
        InvalidParameterValueException,
        match="StartTimestamp and EndTimestamp cannot be null",
    ):
        InvocationCompletedDetails.from_json_dict(json_dict)
