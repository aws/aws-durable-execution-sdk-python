"""Tests for execution."""

import datetime
import json
from typing import Any
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    CheckpointError,
    ExecutionError,
    InvocationError,
)
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
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
    OperationStatus,
    OperationType,
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

    assert result["Status"] == InvocationStatus.FAILED.value
    mock_client.checkpoint.assert_called_once()

    # Verify the checkpoint call was for execution failure
    call_args = mock_client.checkpoint.call_args
    updates = call_args[1]["updates"]
    assert len(updates) == 1
    assert updates[0].operation_type == OperationType.EXECUTION
    assert updates[0].action.value == "FAIL"
    assert updates[0].error.message == "Test error"
    assert updates[0].error.type == "ValueError"


def test_durable_execution_checkpoint_error_propagation():
    """Test durable_execution propagates CheckpointError from DurableServiceClient."""
    mock_client = Mock(spec=DurableServiceClient)

    # Mock checkpoint to raise CheckpointError
    mock_client.checkpoint.side_effect = CheckpointError("Checkpoint failed")

    @durable_execution
    def test_handler(event: Any, context: DurableContext) -> dict:
        return {"result": LARGE_RESULT}

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

    with pytest.raises(CheckpointError, match="Checkpoint failed"):
        test_handler(invocation_input, lambda_context)


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


# endregion durable_execution
