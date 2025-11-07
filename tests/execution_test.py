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
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    DurableServiceClient,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
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
        is_local_runner=True,
    )

    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
        "LocalRunner": True,
    }

    assert result == expected


def test_durable_execution_invocation_input_to_dict_not_local():
    """Test DurableExecutionInvocationInput.to_dict with is_local_runner=False."""
    initial_state = InitialExecutionState(operations=[], next_marker="")

    invocation_input = DurableExecutionInvocationInput(
        durable_execution_arn="arn:test:execution",
        checkpoint_token="token123",  # noqa: S106
        initial_execution_state=initial_state,
        is_local_runner=False,
    )

    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
        "LocalRunner": False,
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
        is_local_runner=True,
        service_client=mock_client,
    )

    # Should inherit to_dict from parent class
    result = invocation_input.to_dict()
    expected = {
        "DurableExecutionArn": "arn:test:execution",
        "CheckpointToken": "token123",
        "InitialExecutionState": initial_state.to_dict(),
        "LocalRunner": True,
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
        is_local_runner=False,
    )

    with_client = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        parent_input, mock_client
    )

    assert with_client.durable_execution_arn == parent_input.durable_execution_arn
    assert with_client.checkpoint_token == parent_input.checkpoint_token
    assert with_client.initial_execution_state == parent_input.initial_execution_state
    assert with_client.is_local_runner == parent_input.is_local_runner
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
        mock_lambda_client.initialize_from_env.return_value = mock_client

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
            "LocalRunner": False,
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
        mock_lambda_client.initialize_from_env.assert_called_once()
        mock_client.checkpoint.assert_not_called()


def test_durable_execution_client_selection_env_large_result():
    """Test durable_execution selects correct client from environment."""
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_lambda_client:
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.initialize_from_env.return_value = mock_client

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
            "LocalRunner": False,
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
        mock_lambda_client.initialize_from_env.assert_called_once()
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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


def test_durable_execution_client_selection_local_runner():
    """Test durable_execution selects correct client for local runner."""
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_lambda_client:
        mock_client = Mock(spec=DurableServiceClient)
        mock_lambda_client.initialize_local_runner_client.return_value = mock_client

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
            "LocalRunner": True,
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
        mock_lambda_client.initialize_local_runner_client.assert_called_once()


def test_initial_execution_state_get_execution_operation_no_operations():
    """Test get_execution_operation raises error when no operations exist."""
    state = InitialExecutionState(operations=[], next_marker="")

    with pytest.raises(
        Exception, match="No durable operations found in initial execution state"
    ):
        state.get_execution_operation()


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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        raise CheckpointError(msg)

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
        is_local_runner=False,
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


def test_durable_execution_checkpoint_error_stops_background():
    """Test that CheckpointError handler stops background checkpointing.

    When user code raises CheckpointError, the handler should stop the background
    thread before re-raising to terminate the Lambda.
    """
    mock_client = Mock(spec=DurableServiceClient)

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        # Directly raise CheckpointError to simulate checkpoint failure
        msg = "Checkpoint system failed"
        raise CheckpointError(msg)

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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
        is_local_runner=False,
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
