"""Unit tests for execution state."""

from unittest.mock import Mock, call

import pytest

from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ChainedInvokeDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    ErrorObject,
    LambdaClient,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StateOutput,
    StepDetails,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState


def test_checkpointed_result_create_from_operation_step():
    """Test CheckpointedResult.create_from_operation with STEP operation."""
    step_details = StepDetails(result="test_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=step_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "test_result"
    assert result.error is None


def test_checkpointed_result_create_from_operation_callback():
    """Test CheckpointedResult.create_from_operation with CALLBACK operation."""
    callback_details = CallbackDetails(callback_id="cb1", result="callback_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "callback_result"
    assert result.error is None


def test_checkpointed_result_create_from_operation_invoke():
    """Test CheckpointedResult.create_from_operation with INVOKE operation."""
    chained_invoke_details = ChainedInvokeDetails(result="invoke_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=chained_invoke_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "invoke_result"
    assert result.error is None


def test_checkpointed_result_create_from_operation_invoke_with_error():
    """Test CheckpointedResult.create_from_operation with INVOKE operation and error."""
    error = ErrorObject(
        message="Invoke error", type="InvokeError", data=None, stack_trace=None
    )
    chained_invoke_details = ChainedInvokeDetails(error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.FAILED,
        chained_invoke_details=chained_invoke_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result is None
    assert result.error == error


def test_checkpointed_result_create_from_operation_invoke_no_details():
    """Test CheckpointedResult.create_from_operation with INVOKE operation but no chained_invoke_details."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


def test_checkpointed_result_create_from_operation_invoke_with_both_result_and_error():
    """Test CheckpointedResult.create_from_operation with INVOKE operation having both result and error."""
    error = ErrorObject(
        message="Invoke error", type="InvokeError", data=None, stack_trace=None
    )
    chained_invoke_details = ChainedInvokeDetails(result="invoke_result", error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.FAILED,
        chained_invoke_details=chained_invoke_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result == "invoke_result"
    assert result.error == error


def test_checkpointed_result_create_from_operation_context():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation."""
    context_details = ContextDetails(result="context_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.SUCCEEDED
    assert result.result == "context_result"
    assert result.error is None


def test_checkpointed_result_create_from_operation_context_with_error():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation and error."""
    error = ErrorObject(
        message="Context error", type="ContextError", data=None, stack_trace=None
    )
    context_details = ContextDetails(error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result is None
    assert result.error == error


def test_checkpointed_result_create_from_operation_context_no_details():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation but no context_details."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


def test_checkpointed_result_create_from_operation_context_with_both_result_and_error():
    """Test CheckpointedResult.create_from_operation with CONTEXT operation having both result and error."""
    error = ErrorObject(
        message="Context error", type="ContextError", data=None, stack_trace=None
    )
    context_details = ContextDetails(result="context_result", error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result == "context_result"
    assert result.error == error


def test_checkpointed_result_create_from_operation_unknown_type():
    """Test CheckpointedResult.create_from_operation with unknown operation type."""
    # Create operation with a mock operation type that doesn't match any case
    operation = Operation(
        operation_id="op1",
        operation_type="UNKNOWN_TYPE",  # This will not match any case
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


def test_checkpointed_result_create_from_operation_with_error():
    """Test CheckpointedResult.create_from_operation with error."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    step_details = StepDetails(error=error)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=step_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.FAILED
    assert result.result is None
    assert result.error == error


def test_checkpointed_result_create_from_operation_no_details():
    """Test CheckpointedResult.create_from_operation with no details."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.operation == operation
    assert result.status == OperationStatus.STARTED
    assert result.result is None
    assert result.error is None


def test_checkpointed_result_create_not_found():
    """Test CheckpointedResult.create_not_found class method."""
    result = CheckpointedResult.create_not_found()
    assert result.operation is None
    assert result.status is None
    assert result.result is None
    assert result.error is None


def test_checkpointed_result_is_succeeded():
    """Test CheckpointedResult.is_succeeded method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_succeeded() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_succeeded() is False


def test_checkpointed_result_is_failed():
    """Test CheckpointedResult.is_failed method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_failed() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_failed() is False


def test_checkpointerd_result_is_pending():
    """Test CheckpointedResult.is_pending method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_pending() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_pending() is False


def test_checkpointed_result_is_started():
    """Test CheckpointedResult.is_started method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_started() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_started() is False


def test_checkpointed_result_raise_callable_error():
    """Test CheckpointedResult.raise_callable_error method."""
    error = Mock(spec=ErrorObject)
    error.to_callable_runtime_error.return_value = RuntimeError("Test error")
    result = CheckpointedResult(error=error)

    with pytest.raises(RuntimeError, match="Test error"):
        result.raise_callable_error()

    error.to_callable_runtime_error.assert_called_once()


def test_checkpointed_result_raise_callable_error_no_error():
    """Test CheckpointedResult.raise_callable_error with no error."""
    result = CheckpointedResult()

    with pytest.raises(CallableRuntimeError, match="Unknown error"):
        result.raise_callable_error()


def test_checkpointed_result_raise_callable_error_no_error_with_message():
    """Test CheckpointedResult.raise_callable_error with no error and custom message."""
    result = CheckpointedResult()

    with pytest.raises(CallableRuntimeError, match="Custom error message"):
        result.raise_callable_error("Custom error message")


def test_checkpointed_result_immutable():
    """Test that CheckpointedResult is immutable."""
    result = CheckpointedResult(status=OperationStatus.SUCCEEDED)
    with pytest.raises(AttributeError):
        result.status = OperationStatus.FAILED


def test_execution_state_creation():
    """Test ExecutionState creation."""
    mock_lambda_client = Mock(spec=LambdaClient)
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )
    assert state.durable_execution_arn == "test_arn"
    assert state.operations == {}


def test_get_checkpoint_result_success_with_result():
    """Test get_checkpoint_result with successful operation and result."""
    mock_lambda_client = Mock(spec=LambdaClient)
    step_details = StepDetails(result="test_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=step_details,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
    )

    result = state.get_checkpoint_result("op1")
    assert result.is_succeeded() is True
    assert result.result == "test_result"
    assert result.operation == operation


def test_get_checkpoint_result_success_without_step_details():
    """Test get_checkpoint_result with successful operation but no step details."""
    mock_lambda_client = Mock(spec=LambdaClient)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
    )

    result = state.get_checkpoint_result("op1")
    assert result.is_succeeded() is True
    assert result.result is None
    assert result.operation == operation


def test_get_checkpoint_result_operation_not_succeeded():
    """Test get_checkpoint_result with failed operation."""
    mock_lambda_client = Mock(spec=LambdaClient)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
    )

    result = state.get_checkpoint_result("op1")
    assert result.is_failed() is True
    assert result.result is None
    assert result.operation == operation


def test_get_checkpoint_result_operation_not_found():
    """Test get_checkpoint_result with nonexistent operation."""
    mock_lambda_client = Mock(spec=LambdaClient)
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    result = state.get_checkpoint_result("nonexistent")
    assert result.is_succeeded() is False
    assert result.result is None
    assert result.operation is None


def test_create_checkpoint():
    """Test create_checkpoint method."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Mock the checkpoint response
    new_operation = Operation(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    mock_execution_state = CheckpointUpdatedExecutionState(operations=[new_operation])
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=mock_execution_state,
    )
    mock_lambda_client.checkpoint.return_value = mock_output

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    state.create_checkpoint(operation_update)

    # Verify the checkpoint was called
    mock_lambda_client.checkpoint.assert_called_once_with(
        durable_execution_arn="test_arn",
        checkpoint_token="token123",  # noqa: S106
        updates=[operation_update],
        client_token=None,
    )

    # Verify the operation was added to state
    assert "test_op" in state.operations
    assert state.operations["test_op"] == new_operation


def test_create_checkpoint_with_none():
    """Test create_checkpoint method with None operation_update."""
    mock_lambda_client = Mock(spec=LambdaClient)

    mock_execution_state = CheckpointUpdatedExecutionState(operations=[])
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=mock_execution_state,
    )
    mock_lambda_client.checkpoint.return_value = mock_output

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    state.create_checkpoint(None)

    # Verify the checkpoint was called with empty updates
    mock_lambda_client.checkpoint.assert_called_once_with(
        durable_execution_arn="test_arn",
        checkpoint_token="token123",  # noqa: S106
        updates=[],
        client_token=None,
    )


def test_create_checkpoint_with_no_args():
    """Test create_checkpoint method with no arguments (default None)."""
    mock_lambda_client = Mock(spec=LambdaClient)

    mock_execution_state = CheckpointUpdatedExecutionState(operations=[])
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=mock_execution_state,
    )
    mock_lambda_client.checkpoint.return_value = mock_output

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    state.create_checkpoint()

    # Verify the checkpoint was called with empty updates
    mock_lambda_client.checkpoint.assert_called_once_with(
        durable_execution_arn="test_arn",
        checkpoint_token="token123",  # noqa: S106
        updates=[],
        client_token=None,
    )


def test_get_checkpoint_result_started():
    """Test get_checkpoint_result with started operation."""
    mock_lambda_client = Mock(spec=LambdaClient)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={"op1": operation},
        service_client=mock_lambda_client,
    )

    result = state.get_checkpoint_result("op1")
    assert result.is_started() is True
    assert result.is_succeeded() is False
    assert result.is_failed() is False
    assert result.operation == operation


def test_checkpointed_result_is_timed_out():
    """Test CheckpointedResult.is_timed_out method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.TIMED_OUT,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_timed_out() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_timed_out() is False


def test_checkpointed_result_is_timed_out_false_for_other_statuses():
    """Test CheckpointedResult.is_timed_out returns False for non-timed-out statuses."""
    statuses = [
        OperationStatus.STARTED,
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.PENDING,
        OperationStatus.READY,
        OperationStatus.STOPPED,
    ]

    for status in statuses:
        operation = Operation(
            operation_id="op1",
            operation_type=OperationType.STEP,
            status=status,
        )
        result = CheckpointedResult.create_from_operation(operation)
        assert (
            result.is_timed_out() is False
        ), f"is_timed_out should be False for status {status}"


def test_fetch_paginated_operations_with_marker():
    mock_lambda_client = Mock(spec=LambdaClient)

    def mock_get_execution_state(durable_execution_arn, checkpoint_token, next_marker):
        resp = {
            "marker1": StateOutput(
                operations=[
                    Operation(
                        operation_id="1",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker="marker2",
            ),
            "marker2": StateOutput(
                operations=[
                    Operation(
                        operation_id="2",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker="marker3",
            ),
            "marker3": StateOutput(
                operations=[
                    Operation(
                        operation_id="3",
                        operation_type=OperationType.STEP,
                        status=OperationStatus.STARTED,
                    )
                ],
                next_marker=None,
            ),
        }
        return resp.get(next_marker)

    mock_lambda_client.get_execution_state.side_effect = mock_get_execution_state

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    state.fetch_paginated_operations(
        initial_operations=[
            Operation(
                operation_id="0",
                operation_type=OperationType.STEP,
                status=OperationStatus.STARTED,
            )
        ],
        checkpoint_token="test_token",  # noqa: S106
        next_marker="marker1",
    )

    assert mock_lambda_client.get_execution_state.call_count == 3
    mock_lambda_client.get_execution_state.assert_has_calls(
        [
            call(
                durable_execution_arn="test_arn",
                checkpoint_token="test_token",  # noqa: S106
                next_marker="marker1",
            ),
            call(
                durable_execution_arn="test_arn",
                checkpoint_token="test_token",  # noqa: S106
                next_marker="marker2",
            ),
            call(
                durable_execution_arn="test_arn",
                checkpoint_token="test_token",  # noqa: S106
                next_marker="marker3",
            ),
        ]
    )

    expected_operations = {
        "0": Operation(
            operation_id="0",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
        "1": Operation(
            operation_id="1",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
        "2": Operation(
            operation_id="2",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
        "3": Operation(
            operation_id="3",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        ),
    }

    assert len(state.operations) == len(expected_operations)

    for op_id, operation in state.operations.items():
        assert op_id in expected_operations
        expected_op = expected_operations[op_id]
        assert operation.operation_id == expected_op.operation_id
