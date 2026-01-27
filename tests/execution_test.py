"""Tests for execution."""

import datetime
import json
import time
from typing import Any
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    BotoClientError,
    CheckpointError,
    CheckpointErrorCategory,
    ExecutionError,
    InvocationError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
    InvocationStatus,
    durable_execution,
)

# LambdaContext no longer needed - using duck typing
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    DurableServiceClient,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepDetails,
    WaitDetails,
)

LARGE_RESULT = "large_success" * 1024 * 1024

# region Models


def test_durable_execution_invocation_input_from_dict():
    """Test that DurableExecutionInvocationInput.from_dict works correctly"""
    input_dict = {
        "DurableExecutionArn": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
        "CheckpointToken": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
                    "ParentId": None,
                    "Name": None,
                    "Type": "EXECUTION",
                    "StartTimestamp": 1751414445.691,
                    "Status": "STARTED",
                    "ExecutionDetails": {"inputPayload": "{}"},
                }
            ],
            "NextMarker": "",
        },
    }

    result = DurableExecutionInvocationInput.from_dict(input_dict)

    assert result.durable_execution_arn == "9692ca80-399d-4f52-8d0a-41acc9cd0492"
    assert result.checkpoint_token == "9692ca80-399d-4f52-8d0a-41acc9cd0492"  # noqa: S105
    assert isinstance(result.initial_execution_state, InitialExecutionState)
    assert len(result.initial_execution_state.operations) == 1
    assert not result.initial_execution_state.next_marker
    assert (
        result.initial_execution_state.operations[0].operation_id
        == "9692ca80-399d-4f52-8d0a-41acc9cd0492"
    )


def test_initial_execution_state_from_dict_minimal():
    """Test that InitialExecutionState.from_dict works correctly"""
    input_dict = {
        "Operations": [
            {
                "Id": "9692ca80-399d-4f52-8d0a-41acc9cd0492",
                "Type": "EXECUTION",
                "Status": "STARTED",
            }
        ],
        "NextMarker": "test-marker",
    }

    result = InitialExecutionState.from_dict(input_dict)

    assert len(result.operations) == 1
    assert result.next_marker == "test-marker"
    assert result.operations[0].operation_id == "9692ca80-399d-4f52-8d0a-41acc9cd0492"


def test_initial_execution_state_from_dict_no_operations():
    """Test that InitialExecutionState.from_dict handles missing Operations key."""
    input_dict = {"NextMarker": "test-marker"}

    result = InitialExecutionState.from_dict(input_dict)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


def test_initial_execution_state_from_dict_empty_operations():
    """Test that InitialExecutionState.from_dict handles empty Operations list."""
    input_dict = {"Operations": [], "NextMarker": "test-marker"}

    result = InitialExecutionState.from_dict(input_dict)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


def test_initial_execution_state_to_dict():
    """Test InitialExecutionState.to_dict method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    state = InitialExecutionState(operations=[operation], next_marker="marker123")

    result = state.to_dict()
    expected = {"Operations": [operation.to_dict()], "NextMarker": "marker123"}

    assert result == expected


def test_initial_execution_state_to_dict_empty():
    """Test InitialExecutionState.to_dict with empty operations."""
    state = InitialExecutionState(operations=[], next_marker="")

    result = state.to_dict()
    expected = {"Operations": [], "NextMarker": ""}

    assert result == expected


def test_durable_execution_invocation_input_to_dict():
    """Test DurableExecutionInvocationInput.to_dict method."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="test_marker"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
    }

    assert result == expected


def test_durable_execution_invocation_input_to_dict_not_local():
    initial_state = InitialExecutionState(operations=[], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
    }

    assert result == expected


def test_durable_execution_invocation_input_with_client_inheritance():
    """Test DurableExecutionInvocationInputWithClient inherits to_dict from parent."""
    mock_client = Mock(spec=DurableServiceClient)
    initial_state = InitialExecutionState(operations=[], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    # Should inherit to_dict from parent class
    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
    }

    assert result == expected
    assert invocation_input.service_client == mock_client


def test_durable_execution_invocation_input_with_client_from_parent():
    """Test DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input."""
    mock_client = Mock(spec=DurableServiceClient)
    initial_state = InitialExecutionState(operations=[], next_marker="")

    parent_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    with_client = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        parent_input, mock_client
    )

    assert with_client.durable_execution_arn == parent_input.durable_execution_arn
    assert with_client.checkpoint_token == parent_input.checkpoint_token
    assert with_client.initial_execution_state == parent_input.initial_execution_state
    assert with_client.service_client == mock_client


def test_operation_to_dict_complete():
    """Test Operation.to_dict with all fields populated."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.UTC)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test_step",
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="exec_payload"),
    )

    result = operation.to_dict()
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ParentId": "parent1",
        "Name": "test_step",
        "StartTimestamp": start_time,
        "EndTimestamp": end_time,
        "ExecutionDetails": {"InputPayload": "exec_payload"},
    }

    assert result == expected


def test_operation_to_dict_minimal():
    """Test Operation.to_dict with minimal required fields."""
    operation = Operation(
        operation_id="minimal_op",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    result = operation.to_dict()
    expected = {
        "Id": "minimal_op",
        "Type": "EXECUTION",
        "Status": "STARTED",
    }

    assert result == expected


def test_durable_execution_invocation_output_from_dict():
    """Test DurableExecutionInvocationOutput.from_dict method."""
    data = {
        "Status": "SUCCEEDED",
        "Result": '{"key": "value"}',
        "Error": {"ErrorType": "ValueError", "ErrorMessage": "Test error"},
    }

    result = DurableExecutionInvocationOutput.from_dict(data)

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == '{"key": "value"}'
    assert result.error is not None
    assert result.error.type == "ValueError"
    assert result.error.message == "Test error"


def test_durable_execution_invocation_output_from_dict_no_error():
    """Test DurableExecutionInvocationOutput.from_dict without error."""
    data = {"Status": "SUCCEEDED", "Result": '{"key": "value"}'}

    result = DurableExecutionInvocationOutput.from_dict(data)

    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == '{"key": "value"}'
    assert result.error is None


def test_durable_execution_invocation_output_from_dict_no_result():
    """Test DurableExecutionInvocationOutput.from_dict without result."""
    data = {"Status": "PENDING"}

    result = DurableExecutionInvocationOutput.from_dict(data)

    assert result.status == InvocationStatus.PENDING
    assert result.result is None
    assert result.error is None


# endregion Models

# region durable_execution


def test_durable_execution_client_selection_env_normal_result():
    """Test durable_execution selects correct client from environment."""
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_lambda_client:
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.initialize_client.return_value = mock_client

        # Mock successful checkpoint
        mock_output = CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )
        mock_client.checkpoint.return_value = mock_output

        @durable_execution
        def test_handler(event: Any, context: DurableContext) -> dict:
            return {"result": "success"}

        # Create regular event with LocalRunner=False
        event = {
            "DurableExecutionArn": "arn:test:execution",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        result = test_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '{"result": "success"}'
        mock_lambda_client.initialize_client.assert_called_once()
        mock_client.checkpoint.assert_not_called()


def test_durable_execution_client_selection_env_large_result():
    """Test durable_execution selects correct client from environment."""
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_lambda_client:
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.initialize_client.return_value = mock_client

        # Mock successful checkpoint
        mock_output = CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )
        mock_client.checkpoint.return_value = mock_output

        @durable_execution
        def test_handler(event: Any, context: DurableContext) -> dict:
            return {"result": LARGE_RESULT}

        # Create regular event with LocalRunner=False
        event = {
            "DurableExecutionArn": "arn:test:execution",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        result = test_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert not result["Result"]
        mock_lambda_client.initialize_client.assert_called_once()
        mock_client.checkpoint.assert_called_once()


def test_durable_execution_with_injected_client_success_normal_result():
    """Test durable_execution uses injected DurableServiceClient for successful execution."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock successful checkpoint
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    # Create execution input with injected client
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload='{"input": "test"}'),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'
    mock_client.checkpoint.assert_not_called()


def test_durable_execution_with_injected_client_success_large_result():
    """Test durable_execution uses injected DurableServiceClient for successful execution."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock successful checkpoint
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": LARGE_RESULT}

    # Create execution input with injected client
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload='{"input": "test"}'),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert not result.get("Result")
    mock_client.checkpoint.assert_called_once()

    # Verify the checkpoint call was for execution success
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1
    assert updates[0].operation_type == OperationType.EXECUTION
    assert updates[0].action.value == "SUCCEED"
    assert json.loads(updates[0].payload) == {"result": LARGE_RESULT}


def test_durable_execution_with_injected_client_failure():
    """Test durable_execution uses injected DurableServiceClient for failed execution."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock successful checkpoint for failure
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        msg = "Test error"
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    # small error, should not call checkpoint
    assert result["Status"] == InvocationStatus.FAILED.value
    assert result["Error"] == {"ErrorMessage": "Test error", "ErrorType": "ValueError"}

    assert not mock_client.checkpoint.called


def test_durable_execution_with_large_error_payload():
    """Test that large error payloads trigger checkpoint."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        raise ValueError(LARGE_RESULT)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    assert result["Status"] == InvocationStatus.FAILED.value
    assert "Error" not in result
    mock_client.checkpoint.assert_called_once()

    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1
    assert updates[0].operation_type == OperationType.EXECUTION
    assert updates[0].action.value == "FAIL"
    assert updates[0].error.message == LARGE_RESULT


def test_durable_execution_fatal_error_handling():
    """Test durable_execution handles FatalError correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        msg = "Retriable invocation error occurred"
        raise InvocationError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # expect raise; backend will retry
    with pytest.raises(InvocationError, match="Retriable invocation error occurred"):
        test_handler(invocation_input, lambda_context)


def test_durable_execution_execution_error_handling():
    """Test durable_execution handles InvocationError correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        msg = "Retriable invocation error occurred"
        raise ExecutionError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # ExecutionError should return FAILED status with ErrorObject in result field
    result = test_handler(invocation_input, lambda_context)
    assert result["Status"] == InvocationStatus.FAILED.value

    # Parse the ErrorObject from the result field
    error_data = result["Error"]

    assert error_data["ErrorMessage"] == "Retriable invocation error occurred"
    assert error_data["ErrorType"] == "ExecutionError"


def test_durable_execution_client_selection_default():
    """Test durable_execution selects correct client using default initialization."""
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_lambda_client:
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.initialize_client.return_value = mock_client

        # Mock successful checkpoint
        mock_output = CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )
        mock_client.checkpoint.return_value = mock_output

        @durable_execution
        def test_handler(event: Any, context: DurableContext) -> dict:
            return {"result": "success"}

        # Create regular event dict instead of DurableExecutionInvocationInputWithClient
        event = {
            "DurableExecutionArn": "arn:test:execution",
            "CheckpointToken": "token123",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "exec1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
        lambda_context.invoked_function_arn = None
        lambda_context.tenant_id = None

        result = test_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        mock_lambda_client.initialize_client.assert_called_once()


def test_initial_execution_state_get_execution_operation_no_operations():
    """Test get_execution_operation logs debug and returns None when no operations exist."""
    state = InitialExecutionState(operations=[], next_marker="")

    with patch("aws_durable_execution_sdk_python.execution.logger") as mock_logger:
        result = state.get_execution_operation()

        assert result is None
        mock_logger.debug.assert_called_once_with(
            "No durable operations found in initial execution state."
        )


def test_initial_execution_state_get_execution_operation_wrong_type():
    """Test get_execution_operation raises error when first operation is not EXECUTION."""
    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )

    state = InitialExecutionState(operations=[operation], next_marker="")

    with pytest.raises(
        Exception,
        match="First operation in initial execution state is not an execution operation",
    ):
        state.get_execution_operation()


def test_initial_execution_state_get_input_payload_none():
    """Test get_input_payload returns None when execution_details is None."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=None,
    )

    state = InitialExecutionState(operations=[operation], next_marker="")

    result = state.get_input_payload()
    assert result is None


def test_durable_handler_empty_input_payload():
    """Test durable_handler handles empty input payload correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    # Create execution input with empty input payload
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload=""),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


def test_durable_handler_whitespace_input_payload():
    """Test durable_handler handles whitespace-only input payload correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    # Create execution input with whitespace-only input payload
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="   "),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


def test_durable_handler_invalid_json_input_payload():
    """Test durable_handler raises JSONDecodeError for invalid JSON input payload."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    # Create execution input with invalid JSON
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{invalid json}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    with pytest.raises(json.JSONDecodeError):
        test_handler(invocation_input, lambda_context)


def test_durable_handler_background_thread_failure():
    """Test durable_handler handles background thread failure correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    # Make checkpoint_batches_forever raise an error immediately
    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise RuntimeError(msg)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Call a checkpoint operation so background thread error can propagate
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail
    mock_client.checkpoint.side_effect = failing_checkpoint

    with pytest.raises(RuntimeError, match="Background checkpoint failed"):
        test_handler(invocation_input, lambda_context)


def test_durable_execution_suspend_execution():
    """Test durable_execution handles SuspendExecution correctly."""
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        msg = "Suspending for callback"
        raise SuspendExecution(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    result = test_handler(invocation_input, lambda_context)

    assert result["Status"] == InvocationStatus.PENDING.value
    assert "Result" not in result
    assert "Error" not in result


def test_durable_execution_checkpoint_error_in_background_thread():
    """Test durable_execution propagates CheckpointError from background thread.

    This test simulates a CheckpointError occurring in the background checkpointing
    thread, which should interrupt user code execution and propagate the error.
    """
    mock_client = Mock(spec=DurableServiceClient)

    # Make the background checkpoint thread fail immediately
    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise CheckpointError(msg, error_category=CheckpointErrorCategory.EXECUTION)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Call a checkpoint operation so background thread error can propagate
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail with CheckpointError
    mock_client.checkpoint.side_effect = failing_checkpoint

    with pytest.raises(CheckpointError, match="Background checkpoint failed"):
        test_handler(invocation_input, lambda_context)


# endregion durable_execution


def test_durable_execution_checkpoint_execution_error_stops_background():
    """Test that CheckpointError handler stops background checkpointing.

    When user code raises CheckpointError, the handler should stop the background
    thread before re-raising to terminate the Lambda.
    """
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Directly raise CheckpointError to simulate checkpoint failure
        msg = "Checkpoint system failed"
        raise CheckpointError(msg, CheckpointErrorCategory.EXECUTION)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make background thread sleep so user code completes first
    def slow_background():
        time.sleep(1)

    # Mock checkpoint_batches_forever to sleep (simulates background thread running)
    with patch(
        "aws_durable_execution_sdk_python.state.ExecutionState.checkpoint_batches_forever",
        side_effect=slow_background,
    ):
        with pytest.raises(CheckpointError, match="Checkpoint system failed"):
            test_handler(invocation_input, lambda_context)


def test_durable_execution_checkpoint_invocation_error_stops_background():
    """Test that CheckpointError handler stops background checkpointing.

    When user code raises CheckpointError, the handler should stop the background
    thread before re-raising to terminate the Lambda.
    """
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Directly raise CheckpointError to simulate checkpoint failure
        msg = "Checkpoint system failed"
        raise CheckpointError(msg, CheckpointErrorCategory.INVOCATION)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make background thread sleep so user code completes first
    def slow_background():
        time.sleep(1)

    # Mock checkpoint_batches_forever to sleep (simulates background thread running)
    with patch(
        "aws_durable_execution_sdk_python.state.ExecutionState.checkpoint_batches_forever",
        side_effect=slow_background,
    ):
        response = test_handler(invocation_input, lambda_context)
        assert response["Status"] == InvocationStatus.FAILED.value
        assert response["Error"]["ErrorType"] == "CheckpointError"


def test_durable_execution_background_thread_execution_error_retries():
    """Test that background thread Execution errors are retried (re-raised)."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise CheckpointError(msg, error_category=CheckpointErrorCategory.EXECUTION)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    with pytest.raises(CheckpointError, match="Background checkpoint failed"):
        test_handler(invocation_input, lambda_context)


def test_durable_execution_background_thread_invocation_error_returns_failed():
    """Test that background thread Invocation errors return FAILED status."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(*args, **kwargs):
        msg = "Background checkpoint failed"
        raise CheckpointError(msg, error_category=CheckpointErrorCategory.INVOCATION)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    response = test_handler(invocation_input, lambda_context)
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"


def test_durable_execution_final_success_checkpoint_execution_error_retries():
    """Test that execution errors on final success checkpoint trigger retry."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Return large result to trigger final checkpoint (>6MB)
        return {"result": "x" * (7 * 1024 * 1024)}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    with pytest.raises(CheckpointError, match="Final checkpoint failed"):
        test_handler(invocation_input, lambda_context)


def test_durable_execution_final_success_checkpoint_invocation_error_returns_failed():
    """Test that invocation errors on final success checkpoint return FAILED."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.INVOCATION,
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Return large result to trigger final checkpoint (>6MB)
        return {"result": "x" * (7 * 1024 * 1024)}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    response = test_handler(invocation_input, lambda_context)
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"
    assert response["Error"]["ErrorMessage"] == "Final checkpoint failed"


def test_durable_execution_final_failure_checkpoint_execution_error_retries():
    """Test that execution errors on final failure checkpoint trigger retry."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Raise error with large message to trigger final checkpoint (>6MB)
        msg = "x" * (7 * 1024 * 1024)
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    with pytest.raises(CheckpointError, match="Final checkpoint failed"):
        test_handler(invocation_input, lambda_context)


def test_durable_execution_final_failure_checkpoint_invocation_error_returns_failed():
    """Test that invocation errors on final failure checkpoint return FAILED."""
    mock_client = Mock(spec=DurableServiceClient)

    def failing_final_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Final checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.INVOCATION,
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Raise error with large message to trigger final checkpoint (>6MB)
        msg = "x" * (7 * 1024 * 1024)
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_final_checkpoint

    response = test_handler(invocation_input, lambda_context)
    assert response["Status"] == InvocationStatus.FAILED.value
    assert response["Error"]["ErrorType"] == "CheckpointError"
    assert response["Error"]["ErrorMessage"] == "Final checkpoint failed"


def test_durable_handler_background_thread_failure_on_succeed_checkpoint():
    """Test durable_handler handles background thread failure on SUCCEED checkpoint.

    This test allows the START checkpoint to succeed but fails on the SUCCEED checkpoint,
    which is the second checkpoint that occurs at the end of the step operation.
    """
    mock_client = Mock(spec=DurableServiceClient)

    def selective_failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a SUCCEED action for a STEP operation
        # The batch will contain both START and SUCCEED updates
        for update in updates:
            if (
                update.operation_type is OperationType.STEP
                and update.action is OperationAction.SUCCEED
            ):
                msg = "Background checkpoint failed on SUCCEED"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Call a step operation which will trigger START and SUCCEED checkpoints
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail selectively
    mock_client.checkpoint.side_effect = selective_failing_checkpoint

    with pytest.raises(RuntimeError, match="Background checkpoint failed on SUCCEED"):
        test_handler(invocation_input, lambda_context)

    # Verify that checkpoint was called exactly once with a batch containing both updates:
    # The batch contains: STEP START and STEP SUCCEED (fails on SUCCEED)
    assert mock_client.checkpoint.call_count == 1

    # Verify the checkpoint call contained both START and SUCCEED updates
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 2

    # First update should be STEP START
    start_update = updates[0]
    assert start_update.operation_type is OperationType.STEP
    assert start_update.action is OperationAction.START

    # Second update should be STEP SUCCEED (the one that failed)
    succeed_update = updates[1]
    assert succeed_update.operation_type is OperationType.STEP
    assert succeed_update.action is OperationAction.SUCCEED


def test_durable_handler_background_thread_failure_on_start_checkpoint():
    """Test durable_handler handles background thread failure on START checkpoint.

    This test fails on the START checkpoint, which should prevent the step from executing
    and therefore no SUCCEED checkpoint should be attempted.
    """
    mock_client = Mock(spec=DurableServiceClient)

    def selective_failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a START action for a STEP operation
        for update in updates:
            if (
                update.operation_type is OperationType.STEP
                and update.action is OperationAction.START
            ):
                msg = "Background checkpoint failed on START"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # First step with AT_MOST_ONCE_PER_RETRY (synchronous START checkpoint)
        # This should fail on START checkpoint and prevent execution
        step_config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
        context.step(lambda ctx: "first_step_result", config=step_config)

        # Second step should never be reached if first step's START checkpoint fails
        context.step(lambda ctx: "second_step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail selectively
    mock_client.checkpoint.side_effect = selective_failing_checkpoint

    with pytest.raises(RuntimeError, match="Background checkpoint failed on START"):
        test_handler(invocation_input, lambda_context)

    # Verify that checkpoint was called exactly once with only the START update:
    # With AT_MOST_ONCE_PER_RETRY, START checkpoint is synchronous and blocks execution
    assert mock_client.checkpoint.call_count == 1

    # Verify the checkpoint call contained only the first step's START update
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1

    # The single update should be STEP START (the one that fails)
    start_update = updates[0]
    assert start_update.operation_type is OperationType.STEP
    assert start_update.action is OperationAction.START

    # Verify no SUCCEED update was created (step execution was blocked)
    succeed_updates = [u for u in updates if u.action is OperationAction.SUCCEED]
    assert len(succeed_updates) == 0


def test_durable_handler_background_thread_failure_on_large_result_checkpoint():
    """Test durable_handler handles background thread failure on large result checkpoint.

    This test verifies that when a large result checkpoint fails due to background thread
    error, the original error is properly unwrapped and raised.
    """
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a SUCCEED action for EXECUTION operation (large result)
        for update in updates:
            if (
                update.operation_type is OperationType.EXECUTION
                and update.action is OperationAction.SUCCEED
            ):
                msg = "Background checkpoint failed on large result"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> str:
        # Return a large result that will trigger checkpoint
        return LARGE_RESULT

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail on large result
    mock_client.checkpoint.side_effect = failing_checkpoint

    # Verify that the original RuntimeError is raised (not BackgroundThreadError)
    with pytest.raises(
        RuntimeError, match="Background checkpoint failed on large result"
    ):
        test_handler(invocation_input, lambda_context)


def test_durable_handler_background_thread_failure_on_error_checkpoint():
    """Test durable_handler handles background thread failure on error checkpoint.

    This test verifies that when an error checkpoint fails due to background thread
    error, the original checkpoint error is properly unwrapped and raised (not the
    user error that triggered the checkpoint).
    """
    mock_client = Mock(spec=DurableServiceClient)

    def failing_checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # Check if any update is a FAIL action for EXECUTION operation (error handling)
        for update in updates:
            if (
                update.operation_type is OperationType.EXECUTION
                and update.action is OperationAction.FAIL
            ):
                msg = "Background checkpoint failed on error handling"
                raise RuntimeError(msg)

        # Allow other checkpoints to succeed
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> str:
        # Raise an error that will trigger error checkpoint
        msg = "User function error"
        raise ValueError(msg)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # Make the service client checkpoint call fail on error handling
    mock_client.checkpoint.side_effect = failing_checkpoint

    # Verify that errors are not raised, but returned because response is small
    resp = test_handler(invocation_input, lambda_context)
    assert resp["Error"]["ErrorMessage"] == "User function error"
    assert resp["Error"]["ErrorType"] == "ValueError"
    assert resp["Status"] == InvocationStatus.FAILED.value


def test_durable_execution_logs_checkpoint_error_extras_from_background_thread():
    """Test that CheckpointError extras are logged when raised from background thread."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_logger = Mock()

    error_obj = {"Code": "TestError", "Message": "Test checkpoint error"}
    metadata_obj = {"RequestId": "test-request-id"}

    def failing_checkpoint(*args, **kwargs):
        raise CheckpointError(  # noqa TRY003
            "Checkpoint failed",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
            error=error_obj,
            response_metadata=metadata_obj,  # EM101
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    with patch("aws_durable_execution_sdk_python.execution.logger", mock_logger):
        with pytest.raises(CheckpointError):
            test_handler(invocation_input, lambda_context)

    mock_logger.exception.assert_called_once()
    call_args = mock_logger.exception.call_args
    assert "Checkpoint processing failed" in call_args[0][0]
    assert call_args[1]["extra"]["Error"] == error_obj
    assert call_args[1]["extra"]["ResponseMetadata"] == metadata_obj


def test_durable_execution_logs_boto_client_error_extras_from_background_thread():
    """Test that BotoClientError extras are logged when raised from background thread."""

    mock_client = Mock(spec=DurableServiceClient)
    mock_logger = Mock()

    error_obj = {"Code": "ServiceError", "Message": "Boto3 service error"}
    metadata_obj = {"RequestId": "boto-request-id"}

    def failing_checkpoint(*args, **kwargs):
        raise BotoClientError(  # noqa TRY003
            "Boto3 error",  # noqa EM101
            error=error_obj,
            response_metadata=metadata_obj,  # EM101
        )

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        context.step(lambda ctx: "step_result")
        return {"result": "success"}

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    mock_client.checkpoint.side_effect = failing_checkpoint

    with patch("aws_durable_execution_sdk_python.execution.logger", mock_logger):
        with pytest.raises(BotoClientError):
            test_handler(invocation_input, lambda_context)

    mock_logger.exception.assert_called_once()
    call_args = mock_logger.exception.call_args
    assert "Checkpoint processing failed" in call_args[0][0]
    assert call_args[1]["extra"]["Error"] == error_obj
    assert call_args[1]["extra"]["ResponseMetadata"] == metadata_obj


def test_durable_execution_logs_checkpoint_error_extras_from_user_code():
    """Test that CheckpointError extras are logged when raised directly from user code."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_logger = Mock()

    error_obj = {
        "Code": "UserCheckpointError",
        "Message": "User raised checkpoint error",
    }
    metadata_obj = {"RequestId": "user-request-id"}

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        raise CheckpointError(  # noqa TRY003
            "User checkpoint error",  # noqa EM101
            error_category=CheckpointErrorCategory.EXECUTION,
            error=error_obj,
            response_metadata=metadata_obj,  # EM101
        )

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )

    initial_state = InitialExecutionState(operations=[operation], next_marker="")

    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        service_client=mock_client,
    )

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    with patch("aws_durable_execution_sdk_python.execution.logger", mock_logger):
        with pytest.raises(CheckpointError):
            test_handler(invocation_input, lambda_context)

    mock_logger.exception.assert_called_once()
    call_args = mock_logger.exception.call_args
    assert call_args[0][0] == "Checkpoint system failed"
    assert call_args[1]["extra"]["Error"] == error_obj
    assert call_args[1]["extra"]["ResponseMetadata"] == metadata_obj


def test_durable_execution_with_boto3_client_parameter():
    """Test durable_execution decorator accepts boto3_client parameter."""
    # GIVEN a custom boto3 Lambda client
    mock_boto3_client = Mock()
    mock_boto3_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": [], "NextMarker": ""},
    }
    mock_boto3_client.get_durable_execution_state.return_value = {
        "Operations": [],
        "NextMarker": "",
    }

    # GIVEN a durable function decorated with the custom client
    @durable_execution(boto3_client=mock_boto3_client)
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    event = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": '{"input": "test"}'},
                }
            ],
            "NextMarker": "",
        },
    }

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # WHEN the handler is invoked
    result = test_handler(event, lambda_context)

    # THEN the execution succeeds using the custom client
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert result["Result"] == '{"result": "success"}'


def test_durable_execution_with_non_durable_payload_raises_error():
    """Test that invoking a durable function with a regular event raises a helpful error."""

    # GIVEN a durable function
    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    # GIVEN a regular Lambda event (not a durable execution payload)
    regular_event = {"key": "value", "data": "test"}

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # WHEN the handler is invoked with a non-durable payload
    # THEN it raises a ValueError with a helpful message
    with pytest.raises(
        ExecutionError,
        match=(
            "Unexpected payload provided to start the durable execution. "
            "Check your resource configurations to confirm the durability is set."
        ),
    ):
        test_handler(regular_event, lambda_context)


def test_durable_execution_with_non_dict_event_raises_error():
    """Test that invoking a durable function with a non-dict event raises a helpful error."""

    # GIVEN a durable function
    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": "success"}

    # GIVEN a non-dict event
    non_dict_event = "not a dict"

    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1000000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None

    # WHEN the handler is invoked with a non-dict event
    # THEN it raises a ValueError with a helpful message
    with pytest.raises(
        ExecutionError,
        match=(
            "Unexpected payload provided to start the durable execution. "
            "Check your resource configurations to confirm the durability is set."
        ),
    ):
        test_handler(non_dict_event, lambda_context)


# =============================================================================
# Tests for JSON Serialization Methods
# =============================================================================


def test_initial_execution_state_to_json_dict_minimal():
    """Test InitialExecutionState.to_json_dict with minimal data."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    state = InitialExecutionState(operations=[operation], next_marker="marker123")

    result = state.to_json_dict()
    expected = {"Operations": [operation.to_json_dict()], "NextMarker": "marker123"}

    assert result == expected


def test_initial_execution_state_to_json_dict_with_timestamps():
    """Test InitialExecutionState.to_json_dict converts datetime objects to millisecond timestamps."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.UTC)

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    state = InitialExecutionState(operations=[operation], next_marker="marker123")

    result = state.to_json_dict()

    # Verify that timestamps are converted to milliseconds in the operation
    operation_result = result["Operations"][0]
    expected_start_ms = int(start_time.timestamp() * 1000)
    expected_end_ms = int(end_time.timestamp() * 1000)

    assert operation_result["StartTimestamp"] == expected_start_ms
    assert operation_result["EndTimestamp"] == expected_end_ms
    assert result["NextMarker"] == "marker123"


def test_initial_execution_state_to_json_dict_empty():
    """Test InitialExecutionState.to_json_dict with empty operations."""
    state = InitialExecutionState(operations=[], next_marker="")

    result = state.to_json_dict()
    expected = {"Operations": [], "NextMarker": ""}

    assert result == expected


def test_initial_execution_state_from_json_dict_minimal():
    """Test InitialExecutionState.from_json_dict with minimal data."""
    data = {
        "Operations": [
            {
                "Id": "op1",
                "Type": "EXECUTION",
                "Status": "STARTED",
            }
        ],
        "NextMarker": "test-marker",
    }

    result = InitialExecutionState.from_json_dict(data)

    assert len(result.operations) == 1
    assert result.next_marker == "test-marker"
    assert result.operations[0].operation_id == "op1"
    assert result.operations[0].operation_type is OperationType.EXECUTION
    assert result.operations[0].status is OperationStatus.STARTED


def test_initial_execution_state_from_json_dict_with_timestamps():
    """Test InitialExecutionState.from_json_dict converts millisecond timestamps to datetime objects."""
    start_ms = 1672574400000  # 2023-01-01 12:00:00 UTC
    end_ms = 1672578000000  # 2023-01-01 13:00:00 UTC

    data = {
        "Operations": [
            {
                "Id": "op1",
                "Type": "EXECUTION",
                "Status": "STARTED",
                "StartTimestamp": start_ms,
                "EndTimestamp": end_ms,
                "ExecutionDetails": {"InputPayload": "test_payload"},
            }
        ],
        "NextMarker": "test-marker",
    }

    result = InitialExecutionState.from_json_dict(data)

    expected_start = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    expected_end = datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.UTC)

    assert len(result.operations) == 1
    operation = result.operations[0]
    assert operation.start_timestamp == expected_start
    assert operation.end_timestamp == expected_end
    assert operation.execution_details.input_payload == "test_payload"


def test_initial_execution_state_from_json_dict_no_operations():
    """Test InitialExecutionState.from_json_dict handles missing Operations key."""
    data = {"NextMarker": "test-marker"}

    result = InitialExecutionState.from_json_dict(data)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


def test_initial_execution_state_from_json_dict_empty_operations():
    """Test InitialExecutionState.from_json_dict handles empty Operations list."""
    data = {"Operations": [], "NextMarker": "test-marker"}

    result = InitialExecutionState.from_json_dict(data)

    assert len(result.operations) == 0
    assert result.next_marker == "test-marker"


def test_initial_execution_state_json_roundtrip():
    """Test InitialExecutionState to_json_dict -> from_json_dict roundtrip preserves all data."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    next_attempt_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)

    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="error_data",
        stack_trace=["line1", "line2"],
    )

    step_details = StepDetails(
        attempt=2,
        next_attempt_timestamp=next_attempt_time,
        result="step_result",
        error=error,
    )

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        parent_id="parent1",
        name="test_step",
        start_timestamp=start_time,
        step_details=step_details,
    )

    original = InitialExecutionState(operations=[operation], next_marker="marker123")

    # Convert to JSON dict and back
    json_data = original.to_json_dict()
    restored = InitialExecutionState.from_json_dict(json_data)

    # Verify all fields are preserved
    assert len(restored.operations) == len(original.operations)
    assert restored.next_marker == original.next_marker

    restored_op = restored.operations[0]
    original_op = original.operations[0]

    assert restored_op.operation_id == original_op.operation_id
    assert restored_op.operation_type == original_op.operation_type
    assert restored_op.status == original_op.status
    assert restored_op.parent_id == original_op.parent_id
    assert restored_op.name == original_op.name
    assert restored_op.start_timestamp == original_op.start_timestamp
    assert restored_op.step_details.attempt == original_op.step_details.attempt
    assert (
        restored_op.step_details.next_attempt_timestamp
        == original_op.step_details.next_attempt_timestamp
    )
    assert restored_op.step_details.result == original_op.step_details.result
    assert (
        restored_op.step_details.error.message == original_op.step_details.error.message
    )


def test_durable_execution_invocation_input_to_json_dict_minimal():
    """Test DurableExecutionInvocationInput.to_json_dict with minimal data."""
    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="test_marker"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_json_dict(),
    }

    assert result == expected


def test_durable_execution_invocation_input_to_json_dict_with_timestamps():
    """Test DurableExecutionInvocationInput.to_json_dict converts datetime objects to millisecond timestamps."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.UTC)

    operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="test_marker"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()

    # Verify that timestamps are converted to milliseconds in nested operations
    operation_result = result["InitialExecutionState"]["Operations"][0]
    expected_start_ms = int(start_time.timestamp() * 1000)
    expected_end_ms = int(end_time.timestamp() * 1000)

    assert operation_result["StartTimestamp"] == expected_start_ms
    assert operation_result["EndTimestamp"] == expected_end_ms
    assert result["DurableExecutionArn"] == "arn:test:execution"
    assert result["CheckpointToken"] == "token123"


def test_durable_execution_invocation_input_to_json_dict_empty_operations():
    """Test DurableExecutionInvocationInput.to_json_dict with empty operations."""
    initial_state = InitialExecutionState(operations=[], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": {"Operations": [], "NextMarker": ""},
    }

    assert result == expected


def test_durable_execution_invocation_input_from_json_dict_minimal():
    """Test DurableExecutionInvocationInput.from_json_dict with minimal data."""
    data = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                }
            ],
            "NextMarker": "test_marker",
        },
    }

    result = DurableExecutionInvocationInput.from_json_dict(data)

    assert result.durable_execution_arn == "arn:test:execution"
    assert result.checkpoint_token == "token123"  # noqa: S105
    assert isinstance(result.initial_execution_state, InitialExecutionState)
    assert len(result.initial_execution_state.operations) == 1
    assert result.initial_execution_state.next_marker == "test_marker"
    assert result.initial_execution_state.operations[0].operation_id == "exec1"


def test_durable_execution_invocation_input_from_json_dict_with_timestamps():
    """Test DurableExecutionInvocationInput.from_json_dict converts millisecond timestamps to datetime objects."""
    start_ms = 1672574400000  # 2023-01-01 12:00:00 UTC
    end_ms = 1672578000000  # 2023-01-01 13:00:00 UTC

    data = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "StartTimestamp": start_ms,
                    "EndTimestamp": end_ms,
                    "ExecutionDetails": {"InputPayload": "test_payload"},
                }
            ],
            "NextMarker": "test_marker",
        },
    }

    result = DurableExecutionInvocationInput.from_json_dict(data)

    expected_start = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    expected_end = datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.UTC)

    operation = result.initial_execution_state.operations[0]
    assert operation.start_timestamp == expected_start
    assert operation.end_timestamp == expected_end
    assert operation.execution_details.input_payload == "test_payload"


def test_durable_execution_invocation_input_from_json_dict_empty_initial_state():
    """Test DurableExecutionInvocationInput.from_json_dict handles missing InitialExecutionState."""
    data = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
    }

    result = DurableExecutionInvocationInput.from_json_dict(data)

    assert result.durable_execution_arn == "arn:test:execution"
    assert result.checkpoint_token == "token123"  # noqa: S105
    assert isinstance(result.initial_execution_state, InitialExecutionState)
    assert len(result.initial_execution_state.operations) == 0
    assert not result.initial_execution_state.next_marker


def test_durable_execution_invocation_input_json_roundtrip():
    """Test DurableExecutionInvocationInput to_json_dict -> from_json_dict roundtrip preserves all data."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.UTC)
    next_attempt_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)

    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="error_data",
        stack_trace=["line1", "line2"],
    )

    step_details = StepDetails(
        attempt=2,
        next_attempt_timestamp=next_attempt_time,
        result="step_result",
        error=error,
    )

    wait_details = WaitDetails(scheduled_end_timestamp=next_attempt_time)

    execution_operation = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        execution_details=ExecutionDetails(input_payload="test_payload"),
    )

    step_operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        parent_id="exec1",
        name="test_step",
        start_timestamp=start_time,
        step_details=step_details,
        wait_details=wait_details,
    )

    initial_state = InitialExecutionState(
        operations=[execution_operation, step_operation], next_marker="marker123"
    )

    original = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution:12345",
        checkpoint_token="token123456",  # noqa: S106
        initial_execution_state=initial_state,
    )

    # Convert to JSON dict and back
    json_data = original.to_json_dict()
    restored = DurableExecutionInvocationInput.from_json_dict(json_data)

    # Verify all top-level fields are preserved
    assert restored.durable_execution_arn == original.durable_execution_arn
    assert restored.checkpoint_token == original.checkpoint_token

    # Verify initial execution state is preserved
    assert len(restored.initial_execution_state.operations) == len(
        original.initial_execution_state.operations
    )
    assert (
        restored.initial_execution_state.next_marker
        == original.initial_execution_state.next_marker
    )

    # Verify execution operation is preserved
    restored_exec_op = restored.initial_execution_state.operations[0]
    original_exec_op = original.initial_execution_state.operations[0]

    assert restored_exec_op.operation_id == original_exec_op.operation_id
    assert restored_exec_op.operation_type == original_exec_op.operation_type
    assert restored_exec_op.status == original_exec_op.status
    assert restored_exec_op.start_timestamp == original_exec_op.start_timestamp
    assert restored_exec_op.end_timestamp == original_exec_op.end_timestamp
    assert (
        restored_exec_op.execution_details.input_payload
        == original_exec_op.execution_details.input_payload
    )

    # Verify step operation is preserved
    restored_step_op = restored.initial_execution_state.operations[1]
    original_step_op = original.initial_execution_state.operations[1]

    assert restored_step_op.operation_id == original_step_op.operation_id
    assert restored_step_op.operation_type == original_step_op.operation_type
    assert restored_step_op.status == original_step_op.status
    assert restored_step_op.parent_id == original_step_op.parent_id
    assert restored_step_op.name == original_step_op.name
    assert restored_step_op.start_timestamp == original_step_op.start_timestamp
    assert (
        restored_step_op.step_details.attempt == original_step_op.step_details.attempt
    )
    assert (
        restored_step_op.step_details.next_attempt_timestamp
        == original_step_op.step_details.next_attempt_timestamp
    )
    assert restored_step_op.step_details.result == original_step_op.step_details.result
    assert (
        restored_step_op.step_details.error.message
        == original_step_op.step_details.error.message
    )
    assert (
        restored_step_op.wait_details.scheduled_end_timestamp
        == original_step_op.wait_details.scheduled_end_timestamp
    )


def test_durable_execution_invocation_input_json_dict_preserves_non_timestamp_fields():
    """Test that to_json_dict preserves all non-timestamp fields unchanged."""

    context_details = ContextDetails(replay_children=True, result="context_result")

    callback_details = CallbackDetails(callback_id="cb123", result="callback_result")

    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test_context",
        context_details=context_details,
        callback_details=callback_details,
    )

    initial_state = InitialExecutionState(
        operations=[operation], next_marker="marker123"
    )

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
    )

    result = invocation_input.to_json_dict()

    # Verify non-timestamp fields are unchanged
    operation_result = result["InitialExecutionState"]["Operations"][0]
    assert operation_result["Id"] == "op1"
    assert operation_result["Type"] == "CONTEXT"
    assert operation_result["Status"] == "SUCCEEDED"
    assert operation_result["ParentId"] == "parent1"
    assert operation_result["Name"] == "test_context"
    assert operation_result["ContextDetails"]["Result"] == "context_result"
    assert operation_result["CallbackDetails"]["CallbackId"] == "cb123"
    assert operation_result["CallbackDetails"]["Result"] == "callback_result"

    assert result["DurableExecutionArn"] == "arn:test:execution"
    assert result["CheckpointToken"] == "token123"
    assert result["InitialExecutionState"]["NextMarker"] == "marker123"


def test_event_parsing_with_unix_millis_timestamps():
    """Test that event parsing converts Unix millis timestamps to datetime objects.

    This reproduces the production bug where NextAttemptTimestamp was sent as
    Unix milliseconds (integer) and caused TypeError when comparing with datetime.now().

    Regression test for: TypeError: '<' not supported between instances of 'int' and 'datetime.datetime'

    Tests all timestamp fields handled by from_json_dict:
    - StartTimestamp
    - EndTimestamp
    - StepDetails.NextAttemptTimestamp
    - WaitDetails.ScheduledEndTimestamp
    """
    # Real event structure from Lambda backend with Unix millis timestamps
    event = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789:function:test:$LATEST/durable-execution/e/o",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "exec-op",
                    "Type": "EXECUTION",
                    "StartTimestamp": 1769481309631,  # Unix millis (int)
                    "EndTimestamp": 1769481319631,  # Unix millis (int)
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                },
                {
                    "Id": "step-with-retry",
                    "Type": "STEP",
                    "SubType": "WaitForCondition",
                    "StartTimestamp": 1769481309631,  # Unix millis (int)
                    "Status": "PENDING",
                    "StepDetails": {
                        "Attempt": 1,
                        "NextAttemptTimestamp": 1769481369631,  # Unix millis (int) - THE BUG!
                    },
                },
                {
                    "Id": "wait-op",
                    "Type": "WAIT",
                    "StartTimestamp": 1769481309631,  # Unix millis (int)
                    "Status": "PENDING",
                    "WaitDetails": {
                        "ScheduledEndTimestamp": 1769481399631  # Unix millis (int)
                    },
                },
            ]
        },
    }

    # Parse using from_json_dict (the fix)
    invocation_input = DurableExecutionInvocationInput.from_json_dict(event)
    operations = invocation_input.initial_execution_state.operations

    # Verify EXECUTION operation timestamps
    assert isinstance(operations[0].start_timestamp, datetime.datetime)
    assert isinstance(operations[0].end_timestamp, datetime.datetime)
    assert operations[0].start_timestamp.tzinfo == datetime.UTC
    assert operations[0].end_timestamp.tzinfo == datetime.UTC

    # Verify STEP operation with NextAttemptTimestamp (the critical one!)
    assert operations[1].step_details is not None
    next_attempt = operations[1].step_details.next_attempt_timestamp
    assert isinstance(next_attempt, datetime.datetime)
    assert next_attempt.tzinfo == datetime.UTC

    # Verify WAIT operation with ScheduledEndTimestamp
    assert operations[2].wait_details is not None
    scheduled_end = operations[2].wait_details.scheduled_end_timestamp
    assert isinstance(scheduled_end, datetime.datetime)
    assert scheduled_end.tzinfo == datetime.UTC

    # Verify timestamps can be compared with datetime.now() without TypeError
    now = datetime.datetime.now(tz=datetime.UTC)
    assert isinstance(next_attempt < now or next_attempt >= now, bool)
    assert isinstance(scheduled_end < now or scheduled_end >= now, bool)


def test_from_dict_leaves_timestamps_as_integers():
    """Test that from_dict (the bug) leaves timestamps as integers.

    This demonstrates the bug behavior for documentation purposes.
    """
    event = {
        "DurableExecutionArn": "arn:test",
        "CheckpointToken": "token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "step-id",
                    "Type": "STEP",
                    "SubType": "WaitForCondition",
                    "StartTimestamp": 1769481309631,
                    "EndTimestamp": 1769481319631,
                    "Status": "PENDING",
                    "StepDetails": {
                        "Attempt": 1,
                        "NextAttemptTimestamp": 1769481369631,  # Unix millis (int)
                    },
                },
                {
                    "Id": "wait-id",
                    "Type": "WAIT",
                    "StartTimestamp": 1769481309631,
                    "Status": "PENDING",
                    "WaitDetails": {
                        "ScheduledEndTimestamp": 1769481399631  # Unix millis (int)
                    },
                },
            ]
        },
    }

    # Using from_dict leaves timestamps as integers
    invocation_input = DurableExecutionInvocationInput.from_dict(event)
    operations = invocation_input.initial_execution_state.operations

    # All timestamps remain as integers (the bug)
    assert isinstance(operations[0].start_timestamp, int)
    assert isinstance(operations[0].end_timestamp, int)
    assert isinstance(operations[0].step_details.next_attempt_timestamp, int)
    assert isinstance(operations[1].wait_details.scheduled_end_timestamp, int)

    # These comparisons would cause TypeError
    with pytest.raises(
        TypeError,
        match="'<' not supported between instances of 'int' and 'datetime.datetime'",
    ):
        _ = operations[0].step_details.next_attempt_timestamp < datetime.datetime.now(
            tz=datetime.UTC
        )

    with pytest.raises(
        TypeError,
        match="'<' not supported between instances of 'int' and 'datetime.datetime'",
    ):
        _ = operations[1].wait_details.scheduled_end_timestamp < datetime.datetime.now(
            tz=datetime.UTC
        )
