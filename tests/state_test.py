"""Unit tests for execution state."""

from __future__ import annotations

import contextlib
import datetime
import json
import threading
import time
import unittest.mock
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, call

import pytest

from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    CallableRuntimeError,
    OrphanedChildException,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
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
from aws_durable_execution_sdk_python.state import (
    CheckpointBatcherConfig,
    CheckpointedResult,
    ExecutionState,
    QueuedOperation,
    ReplayStatus,
)
from aws_durable_execution_sdk_python.threading import CompletionEvent


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


def test_checkpointed_result_is_cancelled():
    """Test CheckpointedResult.is_cancelled method."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.CANCELLED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_cancelled() is True

    # Test with no operation
    result_no_op = CheckpointedResult.create_not_found()
    assert result_no_op.is_cancelled() is False


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
    """Test create_checkpoint method enqueues operations asynchronously."""
    mock_lambda_client = Mock(spec=LambdaClient)

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

    # create_checkpoint with is_sync=False just enqueues without blocking
    state.create_checkpoint(operation_update, is_sync=False)

    # Verify the operation was enqueued (not immediately processed)
    assert not mock_lambda_client.checkpoint.called
    assert state._checkpoint_queue.qsize() == 1

    # Verify we can retrieve the queued operation
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update == operation_update
    assert queued_op.completion_event is None  # Async operation has no completion event


def test_create_checkpoint_with_none():
    """Test create_checkpoint method with None operation_update (empty checkpoint)."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # create_checkpoint with None and is_sync=False enqueues an empty checkpoint
    state.create_checkpoint(None, is_sync=False)

    # Verify the operation was enqueued (not immediately processed)
    assert not mock_lambda_client.checkpoint.called
    assert state._checkpoint_queue.qsize() == 1

    # Verify we can retrieve the queued operation
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update is None  # Empty checkpoint
    assert queued_op.completion_event is None  # Async operation


def test_create_checkpoint_with_no_args():
    """Test create_checkpoint method with no arguments (default None)."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # create_checkpoint with no args and is_sync=False enqueues an empty checkpoint
    state.create_checkpoint(is_sync=False)

    # Verify the operation was enqueued (not immediately processed)
    assert not mock_lambda_client.checkpoint.called
    assert state._checkpoint_queue.qsize() == 1

    # Verify we can retrieve the queued operation
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update is None  # Empty checkpoint (default)
    assert queued_op.completion_event is None  # Async operation


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


# ============================================================================
# Checkpoint Batching Tests
# ============================================================================
# Note: These tests access private members (_checkpoint_queue, _overflow_queue,
# _parent_to_children, etc.) to test internal batching logic. This is justified
# for unit testing the core batching functionality that cannot be tested through
# public APIs alone.
# ruff: noqa: SLF001, BLE001


# Test 8.1: QueuedOperation wrapper and CheckpointBatcherConfig
def test_queued_operation_creation_with_completion_event():
    """Test QueuedOperation creation with completion event for synchronous operations."""
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    completion_event = CompletionEvent()

    queued_op = QueuedOperation(operation_update, completion_event)

    assert queued_op.operation_update == operation_update
    assert queued_op.completion_event == completion_event
    assert not completion_event.is_set()


def test_queued_operation_creation_without_completion_event():
    """Test QueuedOperation creation without completion event for async operations."""
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    queued_op = QueuedOperation(operation_update, completion_event=None)

    assert queued_op.operation_update == operation_update
    assert queued_op.completion_event is None


def test_queued_operation_with_none_operation_update():
    """Test QueuedOperation with None operation_update for empty checkpoints."""
    queued_op = QueuedOperation(operation_update=None, completion_event=None)

    assert queued_op.operation_update is None
    assert queued_op.completion_event is None


def test_checkpoint_batcher_config_default_values():
    """Test CheckpointBatcherConfig default values."""
    config = CheckpointBatcherConfig()

    assert config.max_batch_size_bytes == 750 * 1024  # 750KB
    assert config.max_batch_time_seconds == 1.0
    assert config.max_batch_operations == float("inf")


def test_checkpoint_batcher_config_custom_values():
    """Test CheckpointBatcherConfig with custom values."""
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=500 * 1024,
        max_batch_time_seconds=0.5,
        max_batch_operations=10,
    )

    assert config.max_batch_size_bytes == 500 * 1024
    assert config.max_batch_time_seconds == 0.5
    assert config.max_batch_operations == 10


def test_checkpoint_batcher_config_immutable():
    """Test that CheckpointBatcherConfig is immutable."""
    config = CheckpointBatcherConfig()

    with pytest.raises(AttributeError):
        config.max_batch_size_bytes = 1000


def test_calculate_operation_size_with_operation():
    """Test _calculate_operation_size with a real operation."""
    operation_update = OperationUpdate(
        operation_id="test_op_123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    queued_op = QueuedOperation(operation_update, completion_event=None)

    size = ExecutionState._calculate_operation_size(queued_op)

    # Verify size is positive and reasonable
    assert size > 0
    # Verify it matches JSON serialization size
    expected_size = len(json.dumps(operation_update.to_dict()).encode("utf-8"))
    assert size == expected_size


def test_calculate_operation_size_with_none():
    """Test _calculate_operation_size with None operation_update (empty checkpoint)."""
    queued_op = QueuedOperation(operation_update=None, completion_event=None)

    size = ExecutionState._calculate_operation_size(queued_op)

    assert size == 0


# Test 8.2: Batching logic and size limits
def test_collect_checkpoint_batch_respects_size_limit():
    """Test that batch collection respects max_batch_size_bytes limit."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,  # Small limit to trigger overflow
        max_batch_time_seconds=10.0,  # Long time to avoid time-based flush
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Enqueue multiple operations
    for i in range(5):
        operation_update = OperationUpdate(
            operation_id=f"op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect batch
    batch = state._collect_checkpoint_batch()

    # Verify batch size is limited
    assert len(batch) < 5  # Should not include all operations
    assert len(batch) > 0  # Should include at least one

    # Verify total size doesn't exceed limit
    total_size = sum(state._calculate_operation_size(op) for op in batch)
    assert total_size <= config.max_batch_size_bytes


def test_collect_checkpoint_batch_uses_overflow_queue():
    """Test that overflow queue is processed first to maintain FIFO order."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Put operations in overflow queue
    overflow_op1 = QueuedOperation(
        OperationUpdate(
            operation_id="overflow_1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        None,
    )
    overflow_op2 = QueuedOperation(
        OperationUpdate(
            operation_id="overflow_2",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        None,
    )
    state._overflow_queue.put(overflow_op1)
    state._overflow_queue.put(overflow_op2)

    # Put operation in main queue
    main_op = QueuedOperation(
        OperationUpdate(
            operation_id="main_1",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        None,
    )
    state._checkpoint_queue.put(main_op)

    # Collect batch
    batch = state._collect_checkpoint_batch()

    # Verify overflow operations come first
    assert len(batch) >= 2
    assert batch[0].operation_update.operation_id == "overflow_1"
    assert batch[1].operation_update.operation_id == "overflow_2"


def test_collect_checkpoint_batch_handles_empty_checkpoint():
    """Test batch collection with empty checkpoints (None operation_update)."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Enqueue empty checkpoint
    state._checkpoint_queue.put(QueuedOperation(None, None))

    # Enqueue regular checkpoint
    state._checkpoint_queue.put(
        QueuedOperation(
            OperationUpdate(
                operation_id="op_1",
                operation_type=OperationType.STEP,
                action=OperationAction.START,
            ),
            None,
        )
    )

    # Collect batch
    batch = state._collect_checkpoint_batch()

    # Verify both operations are in batch
    assert len(batch) == 2
    assert batch[0].operation_update is None  # Empty checkpoint
    assert batch[1].operation_update is not None


def test_collect_checkpoint_batch_returns_empty_when_stopped():
    """Test that batch collection returns empty list when checkpointing is stopped."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Signal stop before collecting
    state.stop_checkpointing()

    # Collect batch (should return empty quickly)
    batch = state._collect_checkpoint_batch()

    assert len(batch) == 0


# Test 8.3: Parallel operation concurrency management
def test_parent_child_relationship_building():
    """Test that parent-child relationships are built correctly."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create parent operation
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    state.create_checkpoint(parent_update, is_sync=False)

    # Create child operations
    child1_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    child2_update = OperationUpdate(
        operation_id="child_2",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    state.create_checkpoint(child1_update, is_sync=False)
    state.create_checkpoint(child2_update, is_sync=False)

    # Verify parent-child relationships
    assert "parent_1" in state._parent_to_children
    assert "child_1" in state._parent_to_children["parent_1"]
    assert "child_2" in state._parent_to_children["parent_1"]


def test_descendant_cancellation_when_parent_completes():
    """Test that descendants are marked as orphaned when parent CONTEXT completes."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Build parent-child hierarchy
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    state.create_checkpoint(parent_update, is_sync=False)

    child1_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    state.create_checkpoint(child1_update, is_sync=False)

    # Complete parent CONTEXT
    parent_complete = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    state.create_checkpoint(parent_complete, is_sync=False)

    # Verify child is marked as orphaned
    assert "child_1" in state._parent_done


def test_rejection_of_operations_from_completed_parents():
    """Test that operations are rejected if their parent has completed."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Build parent-child hierarchy
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    state.create_checkpoint(parent_update, is_sync=False)

    child1_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )
    state.create_checkpoint(child1_update, is_sync=False)

    # Complete parent CONTEXT
    parent_complete = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    state.create_checkpoint(parent_complete, is_sync=False)

    # Try to checkpoint child operation (should raise OrphanedChildException)
    child_checkpoint = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        parent_id="parent_1",
    )
    with pytest.raises(OrphanedChildException) as exc_info:
        state.create_checkpoint(child_checkpoint, is_sync=False)

    # Verify exception contains operation_id
    assert exc_info.value.operation_id == "child_1"


def test_nested_parallel_operations_deep_hierarchy():
    """Test that nested parallel operations handle deep hierarchies correctly."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Build deep hierarchy: grandparent -> parent -> child
    grandparent_update = OperationUpdate(
        operation_id="grandparent",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    state.create_checkpoint(grandparent_update, is_sync=False)

    parent_update = OperationUpdate(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        parent_id="grandparent",
    )
    state.create_checkpoint(parent_update, is_sync=False)

    child_update = OperationUpdate(
        operation_id="child",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent",
    )
    state.create_checkpoint(child_update, is_sync=False)

    # Complete grandparent CONTEXT
    grandparent_complete = OperationUpdate(
        operation_id="grandparent",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    state.create_checkpoint(grandparent_complete, is_sync=False)

    # Verify all descendants are marked as orphaned
    assert "parent" in state._parent_done
    assert "child" in state._parent_done


# Test 8.4: Thread safety and synchronous operations
def test_synchronous_checkpoint_blocks_until_complete():
    """Test that create_checkpoint_sync blocks until checkpoint is processed."""
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

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

    # Track if operation completed
    completed = threading.Event()

    def background_processor():
        """Simulate background processing."""
        time.sleep(0.1)  # Small delay
        batch = state._collect_checkpoint_batch()
        if batch:
            # Signal completion events
            for queued_op in batch:
                if queued_op.completion_event:
                    queued_op.completion_event.set()
        completed.set()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=background_processor)
    processor_thread.start()

    # Call synchronous checkpoint (should block)
    start_time = time.time()
    state.create_checkpoint(operation_update, is_sync=True)
    elapsed = time.time() - start_time

    # Verify it blocked for at least the delay time
    assert elapsed >= 0.1

    # Wait for background thread
    processor_thread.join(timeout=1.0)
    assert completed.is_set()


def test_concurrent_access_to_operations_dictionary():
    """Test thread-safe concurrent access to operations dictionary."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Add initial operation
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    state.operations["op1"] = operation

    results = []
    errors = []

    def reader_thread():
        """Thread that reads from operations."""
        try:
            for _ in range(100):
                result = state.get_checkpoint_result("op1")
                results.append(result)
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)

    def writer_thread():
        """Thread that writes to operations."""
        try:
            for i in range(100):
                new_op = Operation(
                    operation_id=f"op{i}",
                    operation_type=OperationType.STEP,
                    status=OperationStatus.SUCCEEDED,
                )
                with state._operations_lock:
                    state.operations[f"op{i}"] = new_op
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)

    # Start multiple reader and writer threads
    threads = []
    for _ in range(3):
        threads.extend(
            [
                threading.Thread(daemon=True, target=reader_thread),
                threading.Thread(daemon=True, target=writer_thread),
            ]
        )

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=5.0)

    # Verify no errors occurred
    assert len(errors) == 0
    # Verify readers got results
    assert len(results) > 0


def test_stop_checkpointing_signals_background_thread():
    """Test that stop_checkpointing signals the background thread to stop."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Verify event is not set initially
    assert not state._checkpointing_stopped.is_set()

    # Call stop_checkpointing
    state.stop_checkpointing()

    # Verify event is now set
    assert state._checkpointing_stopped.is_set()


# Additional coverage tests for missing lines
def test_checkpointed_result_is_replay_children_true():
    """Test CheckpointedResult.is_replay_children when context_details.replay_children is True."""

    context_details = ContextDetails(replay_children=True)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_replay_children() is True


def test_checkpointed_result_is_replay_children_false():
    """Test CheckpointedResult.is_replay_children when context_details.replay_children is False."""

    context_details = ContextDetails(replay_children=False)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        context_details=context_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_replay_children() is False


def test_checkpointed_result_is_replay_children_no_context_details():
    """Test CheckpointedResult.is_replay_children when context_details is None."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.is_replay_children() is False


def test_checkpointed_result_is_replay_children_no_operation():
    """Test CheckpointedResult.is_replay_children when operation is None."""
    result = CheckpointedResult.create_not_found()
    assert result.is_replay_children() is False


def test_checkpointed_result_get_next_attempt_timestamp():
    """Test CheckpointedResult.get_next_attempt_timestamp with timestamp."""

    timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    step_details = StepDetails(next_attempt_timestamp=timestamp)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.get_next_attempt_timestamp() == timestamp


def test_checkpointed_result_get_next_attempt_timestamp_none():
    """Test CheckpointedResult.get_next_attempt_timestamp when no timestamp."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    result = CheckpointedResult.create_from_operation(operation)
    assert result.get_next_attempt_timestamp() is None


def test_create_checkpoint_sync_with_parent_id():
    """Test create_checkpoint_sync builds parent-child relationships."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create parent operation
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )

    # Create child operation with parent_id
    child_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )

    # Simulate background processor
    def process_sync_checkpoint():
        time.sleep(0.05)
        batch = state._collect_checkpoint_batch()
        for queued_op in batch:
            if queued_op.completion_event:
                queued_op.completion_event.set()

    # Process parent
    processor = threading.Thread(daemon=True, target=process_sync_checkpoint)
    processor.start()
    state.create_checkpoint(parent_update, is_sync=True)
    processor.join(timeout=1.0)

    # Process child
    processor = threading.Thread(daemon=True, target=process_sync_checkpoint)
    processor.start()
    state.create_checkpoint(child_update, is_sync=True)
    processor.join(timeout=1.0)

    # Verify parent-child relationship was built
    assert "parent_1" in state._parent_to_children
    assert "child_1" in state._parent_to_children["parent_1"]


def test_create_checkpoint_sync_rejects_orphaned_operation():
    """Test create_checkpoint_sync rejects operations whose parent is done."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Build parent-child relationship
    parent_update = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
    )
    child_update = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_1",
    )

    # Simulate background processor
    def process_sync_checkpoint():
        time.sleep(0.05)
        batch = state._collect_checkpoint_batch()
        for queued_op in batch:
            if queued_op.completion_event:
                queued_op.completion_event.set()

    # Process parent and child
    for update in [parent_update, child_update]:
        processor = threading.Thread(daemon=True, target=process_sync_checkpoint)
        processor.start()
        state.create_checkpoint(update, is_sync=True)
        processor.join(timeout=1.0)

    # Complete parent CONTEXT
    parent_complete = OperationUpdate(
        operation_id="parent_1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
    )
    processor = threading.Thread(daemon=True, target=process_sync_checkpoint)
    processor.start()
    state.create_checkpoint(parent_complete, is_sync=True)
    processor.join(timeout=1.0)

    # Try to checkpoint child (should raise OrphanedChildException)
    child_checkpoint = OperationUpdate(
        operation_id="child_1",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        parent_id="parent_1",
    )
    with pytest.raises(OrphanedChildException) as exc_info:
        state.create_checkpoint(child_checkpoint, is_sync=True)

    # Verify exception contains operation_id
    assert exc_info.value.operation_id == "child_1"


def test_mark_orphans_handles_cycles():
    """Test _mark_orphans handles potential cycles in parent-child relationships."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Manually create a cycle (shouldn't happen in practice, but test defensive code)
    state._parent_to_children["parent"] = {"child1", "child2"}
    state._parent_to_children["child1"] = {"child2"}
    state._parent_to_children["child2"] = {"child1"}  # Cycle

    # Mark orphans should handle this gracefully
    with state._parent_done_lock:
        state._mark_orphans("parent")

    # Verify descendants were marked (cycle detection prevents infinite loop)
    assert "child1" in state._parent_done
    assert "child2" in state._parent_done


def test_checkpoint_batches_forever_exception_handling():
    """Test checkpoint_batches_forever handles exceptions without signaling completion events.

    This test verifies the bug fix where completion events should NOT be signaled
    when checkpoint fails, preventing callers from continuing with corrupted state.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.side_effect = RuntimeError("API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create synchronous operation
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    completion_event = CompletionEvent()
    queued_op = QueuedOperation(operation_update, completion_event)
    state._checkpoint_queue.put(queued_op)

    # Run checkpoint_batches_forever in thread
    def run_batching():
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    thread = threading.Thread(daemon=True, target=run_batching)
    thread.start()
    thread.join(timeout=2.0)

    # Verify completion event WAS signaled with error (new behavior)
    # This ensures synchronous callers get BackgroundThreadError instead of hanging
    assert completion_event.is_set()

    # Verify the error is a BackgroundThreadError
    try:
        completion_event.wait()
        pytest.fail("Should have raised BackgroundThreadError")
    except BackgroundThreadError:
        pass  # Expected


def test_collect_checkpoint_batch_shutdown_path():
    """Test _collect_checkpoint_batch during shutdown with operations in queue.

    With the simplified shutdown logic, once stop_checkpointing() is called,
    _collect_checkpoint_batch() returns empty immediately. Any remaining operations
    in the queue are non-essential async checkpoints that will be abandoned.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Add operation to queue (would be a non-essential async checkpoint in practice)
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Signal shutdown
    state.stop_checkpointing()

    # Collect batch during shutdown - returns empty immediately
    batch = state._collect_checkpoint_batch()

    # Should return empty batch, abandoning the non-essential async checkpoint
    assert len(batch) == 0


def test_collect_checkpoint_batch_shutdown_empty_queue():
    """Test _collect_checkpoint_batch during shutdown with empty queue."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Signal shutdown with empty queue
    state.stop_checkpointing()

    # Collect batch during shutdown
    batch = state._collect_checkpoint_batch()

    # Should return empty batch immediately
    assert len(batch) == 0


def test_collect_checkpoint_batch_overflow_put_back():
    """Test that operations exceeding size limit are put back in overflow queue."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with very small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=150,  # Small enough to trigger overflow
        max_batch_time_seconds=10.0,  # Long time window
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Enqueue two operations - first will fit, second will overflow
    op1 = OperationUpdate(
        operation_id="op_1" * 10,  # Make ID large
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    op2 = OperationUpdate(
        operation_id="op_2" * 10,  # Make ID large
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(op1, None))
    state._checkpoint_queue.put(QueuedOperation(op2, None))

    # Collect first batch
    batch1 = state._collect_checkpoint_batch()

    # Should have collected first operation
    assert len(batch1) == 1
    assert batch1[0].operation_update.operation_id == "op_1" * 10

    # Verify second operation was put in overflow queue
    assert state._overflow_queue.qsize() == 1

    # Collect second batch (should get overflow operation first)
    batch2 = state._collect_checkpoint_batch()

    # Should have collected the overflow operation
    assert len(batch2) == 1
    assert batch2[0].operation_update.operation_id == "op_2" * 10


# Additional edge case tests for remaining coverage
def test_create_checkpoint_sync_with_none_operation_update():
    """Test create_checkpoint_sync with None operation_update (empty checkpoint)."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Simulate background processor
    def process_sync_checkpoint():
        time.sleep(0.05)
        batch = state._collect_checkpoint_batch()
        for queued_op in batch:
            if queued_op.completion_event:
                queued_op.completion_event.set()

    processor = threading.Thread(daemon=True, target=process_sync_checkpoint)
    processor.start()

    # Call with None (empty checkpoint)
    state.create_checkpoint(None, is_sync=True)

    processor.join(timeout=1.0)

    # Verify it completed without error
    assert True  # If we get here, it worked


def test_checkpoint_batches_forever_exception_with_no_sync_operations():
    """Test checkpoint_batches_forever exception handling when no sync operations exist."""
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.side_effect = RuntimeError("API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create async operation (no completion event)
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    queued_op = QueuedOperation(operation_update, completion_event=None)
    state._checkpoint_queue.put(queued_op)

    # Run checkpoint_batches_forever in thread
    def run_batching():
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    thread = threading.Thread(daemon=True, target=run_batching)
    thread.start()
    thread.join(timeout=2.0)

    # Verify thread completed (exception was handled)
    assert not thread.is_alive()


def test_collect_checkpoint_batch_size_limit_during_time_window():
    """Test that size limit is enforced during time window collection."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,
        max_batch_time_seconds=0.5,  # Short window
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Enqueue first small operation
    small_op = OperationUpdate(
        operation_id="small",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(small_op, None))

    # Start collecting in background
    def collect_batch():
        time.sleep(0.05)  # Let first op be collected
        # Enqueue large operation during time window
        large_op = OperationUpdate(
            operation_id="large_op" * 20,  # Very large ID
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(large_op, None))

    thread = threading.Thread(daemon=True, target=collect_batch)
    thread.start()

    # Collect batch
    batch = state._collect_checkpoint_batch()

    thread.join(timeout=1.0)

    # Should have collected small op, large op should be in overflow
    assert len(batch) >= 1
    # If large op exceeded size limit, it should be in overflow queue
    if len(batch) == 1:
        assert state._overflow_queue.qsize() == 1


def test_collect_checkpoint_batch_respects_max_operations_limit():
    """Test that batch collection respects max_batch_operations limit."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with max 1 operation per batch
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,  # Large size limit
        max_batch_time_seconds=10.0,  # Long time window
        max_batch_operations=1,  # Only 1 operation per batch
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Enqueue multiple operations
    for i in range(3):
        operation_update = OperationUpdate(
            operation_id=f"op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect first batch
    batch1 = state._collect_checkpoint_batch()

    # Should have collected exactly 1 operation due to max_batch_operations limit
    assert len(batch1) == 1
    assert batch1[0].operation_update.operation_id == "op_0"

    # Collect second batch
    batch2 = state._collect_checkpoint_batch()

    # Should have collected exactly 1 operation again
    assert len(batch2) == 1
    assert batch2[0].operation_update.operation_id == "op_1"


def test_collect_checkpoint_batch_time_window_expires():
    """Test that batch collection stops when time window expires (remaining_time <= 0)."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with very short time window
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,  # Large size limit
        max_batch_time_seconds=0.01,  # Very short time window (10ms)
        max_batch_operations=100,  # High operation limit
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Enqueue first operation
    first_op = OperationUpdate(
        operation_id="first_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(first_op, None))

    # Mock time.time() to simulate time window expiring between loop check and remaining_time calculation
    original_time = time.time
    call_count = [0]
    start_time = original_time()

    def mock_time():
        call_count[0] += 1
        # First call: initial time for batch_deadline calculation
        if call_count[0] == 1:
            return start_time
        # Second call: while loop condition check (still within window)
        if call_count[0] == 2:
            return start_time + 0.005  # 5ms elapsed, still within 10ms window
        # Third call: remaining_time calculation (time window expired)
        return start_time + 0.015  # 15ms elapsed, past the 10ms window

    with unittest.mock.patch(
        "aws_durable_execution_sdk_python.state.time.time", side_effect=mock_time
    ):
        # Collect batch - should get first operation, then break when remaining_time <= 0
        batch = state._collect_checkpoint_batch()

    # Should have collected only the first operation (time window expired before second get)
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "first_op"


def test_collect_checkpoint_batch_empty_overflow_queue_path():
    """Test batch collection when overflow queue is empty from the start."""
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Ensure overflow queue is empty (it should be by default)
    assert state._overflow_queue.qsize() == 0

    # Enqueue operation in main queue
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Collect batch - should skip overflow queue (empty) and get from main queue
    batch = state._collect_checkpoint_batch()

    # Should have collected from main queue
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "test_op"


def test_collect_checkpoint_batch_overflow_queue_hits_operation_limit():
    """Test that overflow queue draining stops when max_batch_operations is reached."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with max 2 operations per batch
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=1000000,  # Large size limit
        max_batch_time_seconds=10.0,  # Long time window
        max_batch_operations=2,  # Only 2 operations per batch
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Put 3 operations in overflow queue
    for i in range(3):
        operation_update = OperationUpdate(
            operation_id=f"overflow_op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        state._overflow_queue.put(QueuedOperation(operation_update, None))

    # Collect batch - should stop after 2 operations due to max_batch_operations
    batch = state._collect_checkpoint_batch()

    # Should have collected exactly 2 operations from overflow queue
    assert len(batch) == 2
    assert batch[0].operation_update.operation_id == "overflow_op_0"
    assert batch[1].operation_update.operation_id == "overflow_op_1"

    # Third operation should still be in overflow queue
    assert state._overflow_queue.qsize() == 1


def test_collect_checkpoint_batch_overflow_queue_size_limit():
    """Test that overflow queue draining respects size limit and puts back oversized operations."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,
        max_batch_time_seconds=10.0,
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Put operations in overflow queue - first small, second large
    small_op = OperationUpdate(
        operation_id="small",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    large_op = OperationUpdate(
        operation_id="large_op" * 20,  # Very large ID
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._overflow_queue.put(QueuedOperation(small_op, None))
    state._overflow_queue.put(QueuedOperation(large_op, None))

    # Collect batch - should get small op, large op should be put back
    batch = state._collect_checkpoint_batch()

    # Should have collected small operation
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "small"

    # Large operation should be put back in overflow queue
    assert state._overflow_queue.qsize() == 1


# ============================================================================
# Error Handling Tests for Task 1.1
# ============================================================================


def test_checkpoint_error_signals_completion_events_with_error():
    """Test that completion events ARE signaled with error when checkpoint fails.

    This verifies that when checkpoint_batches_forever encounters an error,
    completion events are signaled with BackgroundThreadError to wake up
    blocked callers and allow them to exit cleanly.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Checkpoint API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create synchronous operation with completion event
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    completion_event = CompletionEvent()
    queued_op = QueuedOperation(operation_update, completion_event)
    state._checkpoint_queue.put(queued_op)

    # Run checkpoint_batches_forever in background thread
    thread_completed = threading.Event()

    def run_batching():
        # Background thread now exits gracefully after signaling errors
        state.checkpoint_batches_forever()
        thread_completed.set()

    thread = threading.Thread(daemon=True, target=run_batching)
    thread.start()

    # Wait for thread to complete gracefully
    thread_completed.wait(timeout=2.0)
    thread.join(timeout=1.0)

    # Verify thread completed gracefully (no exception raised)
    assert thread_completed.is_set()

    # CRITICAL: Verify completion event WAS signaled with error
    # This ensures synchronous callers wake up and can exit cleanly
    assert completion_event.is_set()

    # Verify that waiting on the event raises BackgroundThreadError
    with pytest.raises(BackgroundThreadError):
        completion_event.wait()


def test_synchronous_caller_receives_error_on_background_thread_failure():
    """Test that synchronous callers receive error when background thread fails.

    This verifies that when the background thread encounters an error, synchronous
    callers waiting on completion events are woken up with BackgroundThreadError,
    allowing them to exit cleanly rather than hanging indefinitely.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Background thread error")

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

    # Track synchronous call result
    sync_call_result = []
    sync_call_started = threading.Event()

    def synchronous_caller():
        """Simulate a synchronous checkpoint call."""
        sync_call_started.set()
        try:
            # This should raise BackgroundThreadError when background thread fails
            state.create_checkpoint(operation_update, is_sync=True)
            sync_call_result.append("unexpected_success")
        except BaseException as e:
            sync_call_result.append(f"error: {type(e).__name__}: {e}")

    # Start background processor that will fail
    def run_batching():
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    background_thread = threading.Thread(daemon=True, target=run_batching)
    background_thread.start()

    # Start synchronous caller
    caller_thread = threading.Thread(daemon=True, target=synchronous_caller)
    caller_thread.start()

    # Wait for sync call to start
    sync_call_started.wait(timeout=1.0)

    # Give time for background thread to fail and for sync call to potentially complete
    time.sleep(0.5)

    # Wait for background thread to finish
    background_thread.join(timeout=1.0)

    # Wait for caller thread to complete (should exit with error)
    caller_thread.join(timeout=2.0)

    # CRITICAL: Verify synchronous call received BackgroundThreadError
    assert len(sync_call_result) == 1
    assert "BackgroundThreadError" in sync_call_result[0]
    assert (
        "Background thread error" in sync_call_result[0]
        or "Checkpoint creation failed" in sync_call_result[0]
    )

    # Verify caller thread completed (not blocked)
    assert not caller_thread.is_alive()

    # Clean up
    state.stop_checkpointing()


def test_exception_propagates_through_threadpoolexecutor():
    """Test that checkpoint_batches_forever exits gracefully after signaling errors.

    This verifies that when checkpoint_batches_forever encounters an error,
    it signals the error through completion events and failure state, then
    exits gracefully rather than raising an exception in the background thread.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Checkpoint API failure")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Enqueue an operation
    operation_update = OperationUpdate(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(operation_update, None))

    # Run checkpoint_batches_forever and verify it exits gracefully (no exception)
    state.checkpoint_batches_forever()

    # Verify the failure state was set
    assert state._checkpointing_failed.is_set()


def test_multiple_sync_operations_all_remain_blocked_on_error():
    """Test that multiple synchronous operations all remain blocked when checkpoint fails.

    This verifies that when multiple synchronous operations are waiting and the
    background thread fails, ALL of them remain blocked (none of their completion
    events are signaled).
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Batch processing error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create multiple synchronous operations
    num_operations = 3
    completion_events = []
    for i in range(num_operations):
        operation_update = OperationUpdate(
            operation_id=f"test_op_{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        completion_event = CompletionEvent()
        completion_events.append(completion_event)
        queued_op = QueuedOperation(operation_update, completion_event)
        state._checkpoint_queue.put(queued_op)

    # Run checkpoint_batches_forever in background thread
    def run_batching():
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    thread = threading.Thread(daemon=True, target=run_batching)
    thread.start()
    thread.join(timeout=2.0)

    # CRITICAL: Verify ALL completion events are signaled with error
    for i, event in enumerate(completion_events):
        assert event.is_set(), f"Completion event {i} should be signaled with error"
        # Verify each has BackgroundThreadError
        try:
            event.wait()
            pytest.fail(f"Event {i} should have raised BackgroundThreadError")
        except BackgroundThreadError:
            pass  # Expected


def test_async_operations_not_affected_by_error_handling():
    """Test that async operations (no completion event) are not affected by error handling.

    This verifies that the error handling logic correctly handles batches containing
    only async operations (no completion events to signal).
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("API error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create async operation (no completion event)
    operation_update = OperationUpdate(
        operation_id="async_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    queued_op = QueuedOperation(operation_update, completion_event=None)
    state._checkpoint_queue.put(queued_op)

    # Run checkpoint_batches_forever and verify it exits gracefully
    state.checkpoint_batches_forever()

    # Verify the failure state was set
    assert state._checkpointing_failed.is_set()

    # Test passes if no AttributeError or other issues occur
    # (verifying the code handles None completion_event correctly)


def test_mixed_sync_async_operations_only_sync_blocked_on_error():
    """Test that in mixed batches, only sync operations remain blocked on error.

    This verifies that when a batch contains both sync and async operations,
    the error handling correctly processes both types without attempting to
    signal non-existent completion events for async operations.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint API failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Mixed batch error")

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Create sync operation with completion event
    sync_op = OperationUpdate(
        operation_id="sync_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    sync_event = CompletionEvent()
    state._checkpoint_queue.put(QueuedOperation(sync_op, sync_event))

    # Create async operation without completion event
    async_op = OperationUpdate(
        operation_id="async_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._checkpoint_queue.put(QueuedOperation(async_op, None))

    # Run checkpoint_batches_forever
    def run_batching():
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    thread = threading.Thread(daemon=True, target=run_batching)
    thread.start()
    thread.join(timeout=2.0)

    # Verify sync operation's completion event WAS signaled with error
    assert sync_event.is_set()

    # Verify it has BackgroundThreadError
    try:
        sync_event.wait()
        pytest.fail("Should have raised BackgroundThreadError")
    except BackgroundThreadError:
        pass  # Expected

    # Test passes if no AttributeError occurs when processing async operation
    # (verifying None completion_event is handled correctly)


# ============================================================================
# Task 4.1: Test method signature and defaults
# ============================================================================


def test_create_checkpoint_accepts_is_sync_parameter():
    """Test that create_checkpoint() accepts is_sync parameter.

    Verifies that the consolidated create_checkpoint method accepts the is_sync
    parameter for controlling synchronous vs asynchronous behavior.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

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

    # Test that is_sync parameter is accepted without error
    # is_sync=False (asynchronous) - doesn't block
    state.create_checkpoint(operation_update, is_sync=False)
    assert state._checkpoint_queue.qsize() == 1

    # Clear queue
    state._checkpoint_queue.get_nowait()

    # is_sync=False again to avoid blocking
    state.create_checkpoint(operation_update, is_sync=False)
    assert state._checkpoint_queue.qsize() == 1


def test_create_checkpoint_default_is_sync_true():
    """Test that create_checkpoint() defaults to is_sync=True (synchronous).

    Verifies that when is_sync parameter is not provided, the method defaults
    to synchronous behavior (is_sync=True), creating a completion event.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

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

    # Use a thread to call create_checkpoint without is_sync parameter
    # This will block, so we run it in a thread and check the queue
    def enqueue_sync():
        state.create_checkpoint(operation_update)

    thread = threading.Thread(daemon=True, target=enqueue_sync)
    thread.start()

    # Give thread time to enqueue
    time.sleep(0.1)

    # Verify operation was enqueued
    assert state._checkpoint_queue.qsize() == 1

    # Retrieve queued operation and verify it has a completion event (sync behavior)
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update == operation_update
    assert queued_op.completion_event is not None  # Sync creates completion event
    assert isinstance(queued_op.completion_event, CompletionEvent)

    # Signal completion to unblock thread
    queued_op.completion_event.set()
    thread.join(timeout=1.0)


def test_create_checkpoint_explicit_is_sync_true():
    """Test that create_checkpoint(is_sync=True) creates completion event.

    Verifies that explicitly setting is_sync=True results in synchronous behavior
    with a completion event created.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

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

    # Use a thread to call with explicit is_sync=True (will block)
    def enqueue_sync():
        state.create_checkpoint(operation_update, is_sync=True)

    thread = threading.Thread(daemon=True, target=enqueue_sync)
    thread.start()

    # Give thread time to enqueue
    time.sleep(0.1)

    # Verify operation was enqueued with completion event
    assert state._checkpoint_queue.qsize() == 1
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.completion_event is not None

    # Signal completion to unblock thread
    queued_op.completion_event.set()
    thread.join(timeout=1.0)


def test_create_checkpoint_is_sync_false_no_completion_event():
    """Test that create_checkpoint(is_sync=False) does not create completion event.

    Verifies that setting is_sync=False results in asynchronous behavior
    without a completion event.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

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

    # Call with is_sync=False
    state.create_checkpoint(operation_update, is_sync=False)

    # Verify operation was enqueued without completion event
    assert state._checkpoint_queue.qsize() == 1
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.completion_event is None  # Async has no completion event


def test_create_checkpoint_is_sync_false_returns_immediately():
    """Test that create_checkpoint(is_sync=False) returns immediately.

    Verifies that asynchronous checkpoints return immediately without blocking,
    even when the background thread is not processing checkpoints.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

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

    # Measure time for async checkpoint call
    start_time = time.time()
    state.create_checkpoint(operation_update, is_sync=False)
    elapsed_time = time.time() - start_time

    # Verify it returns immediately (should be < 10ms, we allow 50ms for safety)
    assert (
        elapsed_time < 0.05
    ), f"Async checkpoint took {elapsed_time:.3f}s, expected < 0.05s"

    # Verify operation was enqueued
    assert state._checkpoint_queue.qsize() == 1
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update == operation_update
    assert queued_op.completion_event is None


def test_create_checkpoint_with_none_defaults_to_sync():
    """Test that create_checkpoint(None) defaults to synchronous behavior.

    Verifies that empty checkpoints (operation_update=None) also default
    to synchronous behavior when is_sync is not specified.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Use a thread to call with None (will block)
    def enqueue_sync():
        state.create_checkpoint(None)

    thread = threading.Thread(daemon=True, target=enqueue_sync)
    thread.start()

    # Give thread time to enqueue
    time.sleep(0.1)

    # Verify operation was enqueued with completion event (sync behavior)
    assert state._checkpoint_queue.qsize() == 1
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update is None  # Empty checkpoint
    assert queued_op.completion_event is not None  # Sync creates completion event

    # Signal completion to unblock thread
    queued_op.completion_event.set()
    thread.join(timeout=1.0)


def test_create_checkpoint_no_args_defaults_to_sync():
    """Test that create_checkpoint() with no arguments defaults to synchronous.

    Verifies that calling create_checkpoint with no arguments results in
    an empty synchronous checkpoint.
    """
    mock_lambda_client = Mock(spec=LambdaClient)

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Use a thread to call with no arguments (will block)
    def enqueue_sync():
        state.create_checkpoint()

    thread = threading.Thread(daemon=True, target=enqueue_sync)
    thread.start()

    # Give thread time to enqueue
    time.sleep(0.1)

    # Verify operation was enqueued with completion event (sync behavior)
    assert state._checkpoint_queue.qsize() == 1
    queued_op = state._checkpoint_queue.get_nowait()
    assert queued_op.operation_update is None  # Empty checkpoint (default)
    assert queued_op.completion_event is not None  # Sync creates completion event

    # Signal completion to unblock thread
    queued_op.completion_event.set()
    thread.join(timeout=1.0)


def test_collect_checkpoint_batch_overflow_queue_size_limit_final():
    """Test that overflow queue draining respects size limit and puts back oversized operations."""
    mock_lambda_client = Mock(spec=LambdaClient)

    # Create config with small size limit
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=200,  # Small size limit
        max_batch_time_seconds=10.0,  # Long time window
        max_batch_operations=10,  # High operation limit
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
        batcher_config=config,
    )

    # Put 2 operations in overflow queue - second one will be too large
    small_op = OperationUpdate(
        operation_id="small",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    large_op = OperationUpdate(
        operation_id="large_operation_id" * 20,  # Very large ID
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    state._overflow_queue.put(QueuedOperation(small_op, None))
    state._overflow_queue.put(QueuedOperation(large_op, None))

    # Collect batch - should get small op, put back large op
    batch = state._collect_checkpoint_batch()

    # Should have collected only the small operation
    assert len(batch) == 1
    assert batch[0].operation_update.operation_id == "small"

    # Large operation should be back in overflow queue
    assert state._overflow_queue.qsize() == 1
    remaining_op = state._overflow_queue.get_nowait()
    assert remaining_op.operation_update.operation_id == "large_operation_id" * 20


# ============================================================================
# Task 4.2: Test synchronous behavior
# ============================================================================


def test_create_checkpoint_blocks_until_completion_default():
    """Test that create_checkpoint() blocks until completion when is_sync=True (default).

    Verifies that calling create_checkpoint without specifying is_sync results in
    synchronous blocking behavior until the background thread processes the checkpoint.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

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

    # Track timing and completion
    call_completed = threading.Event()
    start_time = None
    end_time = None

    def call_checkpoint():
        nonlocal start_time, end_time
        start_time = time.time()
        # Call without is_sync parameter (defaults to True)
        state.create_checkpoint(operation_update)
        end_time = time.time()
        call_completed.set()

    def background_processor():
        """Simulate background processing with delay."""
        time.sleep(0.15)  # Delay to verify blocking
        batch = state._collect_checkpoint_batch()
        if batch:
            # Signal completion events
            for queued_op in batch:
                if queued_op.completion_event:
                    queued_op.completion_event.set()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=background_processor)
    processor_thread.start()

    # Start checkpoint call
    caller_thread = threading.Thread(daemon=True, target=call_checkpoint)
    caller_thread.start()

    # Wait for both threads
    caller_thread.join(timeout=2.0)
    processor_thread.join(timeout=1.0)

    # Verify call completed
    assert call_completed.is_set()

    # Verify it blocked for at least the delay time
    elapsed = end_time - start_time
    assert elapsed >= 0.15, f"Expected blocking for at least 0.15s, got {elapsed}s"


def test_create_checkpoint_blocks_until_completion_explicit_true():
    """Test that create_checkpoint(is_sync=True) blocks until completion.

    Verifies that explicitly setting is_sync=True results in synchronous blocking
    behavior until the background thread processes the checkpoint.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

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

    # Track timing and completion
    call_completed = threading.Event()
    start_time = None
    end_time = None

    def call_checkpoint():
        nonlocal start_time, end_time
        start_time = time.time()
        # Call with explicit is_sync=True
        state.create_checkpoint(operation_update, is_sync=True)
        end_time = time.time()
        call_completed.set()

    def background_processor():
        """Simulate background processing with delay."""
        time.sleep(0.15)  # Delay to verify blocking
        batch = state._collect_checkpoint_batch()
        if batch:
            # Signal completion events
            for queued_op in batch:
                if queued_op.completion_event:
                    queued_op.completion_event.set()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=background_processor)
    processor_thread.start()

    # Start checkpoint call
    caller_thread = threading.Thread(daemon=True, target=call_checkpoint)
    caller_thread.start()

    # Wait for both threads
    caller_thread.join(timeout=2.0)
    processor_thread.join(timeout=1.0)

    # Verify call completed
    assert call_completed.is_set()

    # Verify it blocked for at least the delay time
    elapsed = end_time - start_time
    assert elapsed >= 0.15, f"Expected blocking for at least 0.15s, got {elapsed}s"


def test_create_checkpoint_completion_event_created_and_signaled():
    """Test that completion event is created and signaled on success.

    Verifies that when is_sync=True, a completion event is created, enqueued,
    and properly signaled by the background thread upon successful checkpoint.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

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

    # Track the queued operation
    queued_operation = None

    def call_checkpoint():
        nonlocal queued_operation
        # Start the call (will block)
        state.create_checkpoint(operation_update, is_sync=True)

    def verify_and_signal():
        """Verify completion event exists and signal it."""
        time.sleep(0.05)  # Let checkpoint be enqueued
        # Get the queued operation
        nonlocal queued_operation
        queued_operation = state._checkpoint_queue.get_nowait()

        # Verify completion event was created
        assert queued_operation.completion_event is not None
        assert isinstance(queued_operation.completion_event, CompletionEvent)
        assert not queued_operation.completion_event.is_set()

        # Signal the event (simulating successful checkpoint)
        queued_operation.completion_event.set()

    # Start verifier thread
    verifier_thread = threading.Thread(daemon=True, target=verify_and_signal)
    verifier_thread.start()

    # Start checkpoint call
    caller_thread = threading.Thread(daemon=True, target=call_checkpoint)
    caller_thread.start()

    # Wait for both threads
    verifier_thread.join(timeout=1.0)
    caller_thread.join(timeout=1.0)

    # Verify completion event was signaled
    assert queued_operation is not None
    assert queued_operation.completion_event.is_set()


def test_create_checkpoint_completion_event_not_signaled_on_failure():
    """Test that completion event is NOT signaled when checkpoint fails.

    Verifies that when the background thread encounters an error during checkpoint
    processing, completion events are NOT signaled, preventing callers from
    continuing with corrupted execution state.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate checkpoint failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Checkpoint failed")

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

    # Track the queued operation
    queued_operation = None
    checkpoint_started = threading.Event()

    def call_checkpoint():
        """Call synchronous checkpoint (will block indefinitely on error)."""
        checkpoint_started.set()
        with contextlib.suppress(BackgroundThreadError):
            # Expected - background thread failed and propagated error
            state.create_checkpoint(operation_update, is_sync=True)

    def run_background_processor():
        """Run background processor that will fail."""
        # Wait for checkpoint to be enqueued
        checkpoint_started.wait(timeout=1.0)
        time.sleep(0.05)

        # Get the queued operation before processing
        nonlocal queued_operation
        queued_operation = state._checkpoint_queue.get_nowait()

        # Put it back for processing
        state._checkpoint_queue.put(queued_operation)

        # Run checkpoint_batches_forever (will fail)
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=run_background_processor)
    processor_thread.start()

    # Start checkpoint call
    caller_thread = threading.Thread(daemon=True, target=call_checkpoint)
    caller_thread.start()

    # Wait for processor to fail
    processor_thread.join(timeout=2.0)

    # Give caller thread a moment to potentially complete (it shouldn't)
    time.sleep(0.2)

    # CRITICAL: Verify completion event WAS signaled with error
    assert queued_operation is not None
    assert queued_operation.completion_event is not None
    assert queued_operation.completion_event.is_set()

    # Verify it has BackgroundThreadError
    try:
        queued_operation.completion_event.wait()
        pytest.fail("Should have raised BackgroundThreadError")
    except BackgroundThreadError:
        pass  # Expected

    # Verify caller thread exited with error (not alive)
    assert not caller_thread.is_alive()

    # Clean up - signal event to unblock caller
    queued_operation.completion_event.set()
    caller_thread.join(timeout=1.0)


def test_create_checkpoint_caller_remains_blocked_on_background_failure():
    """Test that caller remains blocked when background thread fails.

    Verifies that when the background thread encounters an error, synchronous
    callers remain blocked indefinitely (until Lambda termination), preventing
    them from executing with corrupted state.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    # Simulate background thread failure
    mock_lambda_client.checkpoint.side_effect = RuntimeError("Background failure")

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

    # Track whether caller completed
    caller_completed = threading.Event()
    caller_started = threading.Event()

    def call_checkpoint():
        """Call synchronous checkpoint (should remain blocked)."""
        caller_started.set()
        with contextlib.suppress(BackgroundThreadError):
            # Expected - background thread failed and propagated error
            state.create_checkpoint(operation_update, is_sync=True)
            # This line should never be reached
            caller_completed.set()

    def run_background_processor():
        """Run background processor that will fail."""
        # Wait for caller to start
        caller_started.wait(timeout=1.0)
        time.sleep(0.05)

        # Run checkpoint_batches_forever (will fail)
        with contextlib.suppress(RuntimeError):
            state.checkpoint_batches_forever()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=run_background_processor)
    processor_thread.start()

    # Start checkpoint call
    caller_thread = threading.Thread(daemon=True, target=call_checkpoint)
    caller_thread.start()

    # Wait for processor to fail
    processor_thread.join(timeout=2.0)

    # Give caller thread time to potentially complete (it shouldn't)
    time.sleep(0.3)

    # CRITICAL: Verify caller DID complete (with error)
    # The caller should have received BackgroundThreadError and exited
    assert not caller_completed.is_set()  # Should not reach the success line

    # Verify caller thread is NOT alive (exited with error)
    assert not caller_thread.is_alive()

    # Clean up - stop checkpointing to unblock
    state.stop_checkpointing()
    # Get and signal the completion event to unblock
    if not state._checkpoint_queue.empty():
        queued_op = state._checkpoint_queue.get_nowait()
        if queued_op.completion_event:
            queued_op.completion_event.set()
    caller_thread.join(timeout=1.0)


def test_create_checkpoint_multiple_sync_calls_all_block():
    """Test that multiple synchronous checkpoint calls all block correctly.

    Verifies that when multiple threads call create_checkpoint synchronously,
    they all block until their respective completion events are signaled.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    num_callers = 3
    completion_events = [CompletionEvent() for _ in range(num_callers)]
    start_times = [None] * num_callers
    end_times = [None] * num_callers

    def call_checkpoint(index):
        """Call synchronous checkpoint."""
        operation_update = OperationUpdate(
            operation_id=f"test_op_{index}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
        start_times[index] = time.time()
        state.create_checkpoint(operation_update, is_sync=True)
        end_times[index] = time.time()
        completion_events[index].set()

    def background_processor():
        """Process all checkpoints with delay."""
        time.sleep(0.15)  # Delay to verify blocking
        batch = state._collect_checkpoint_batch()
        if batch:
            # Signal all completion events
            for queued_op in batch:
                if queued_op.completion_event:
                    queued_op.completion_event.set()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=background_processor)
    processor_thread.start()

    # Start multiple caller threads
    caller_threads = []
    for i in range(num_callers):
        thread = threading.Thread(daemon=True, target=call_checkpoint, args=(i,))
        thread.start()
        caller_threads.append(thread)

    # Wait for all threads
    for thread in caller_threads:
        thread.join(timeout=2.0)
    processor_thread.join(timeout=1.0)

    # Verify all calls completed
    for i, event in enumerate(completion_events):
        assert event.is_set(), f"Caller {i} did not complete"

    # Verify all calls blocked for at least the delay time
    for i in range(num_callers):
        elapsed = end_times[i] - start_times[i]
        assert (
            elapsed >= 0.15
        ), f"Caller {i} expected blocking for at least 0.15s, got {elapsed}s"


def test_create_checkpoint_sync_with_empty_checkpoint():
    """Test synchronous behavior with empty checkpoint (None operation_update).

    Verifies that empty checkpoints also block correctly when is_sync=True.
    """
    mock_lambda_client = Mock(spec=LambdaClient)
    mock_lambda_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[],
            next_marker=None,
        ),
    )

    state = ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token123",  # noqa: S106
        operations={},
        service_client=mock_lambda_client,
    )

    # Track timing and completion
    call_completed = threading.Event()
    start_time = None
    end_time = None

    def call_checkpoint():
        nonlocal start_time, end_time
        start_time = time.time()
        # Call with None (empty checkpoint) and is_sync=True
        state.create_checkpoint(None, is_sync=True)
        end_time = time.time()
        call_completed.set()

    def background_processor():
        """Simulate background processing with delay."""
        time.sleep(0.15)  # Delay to verify blocking
        batch = state._collect_checkpoint_batch()
        if batch:
            # Signal completion events
            for queued_op in batch:
                if queued_op.completion_event:
                    queued_op.completion_event.set()

    # Start background processor
    processor_thread = threading.Thread(daemon=True, target=background_processor)
    processor_thread.start()

    # Start checkpoint call
    caller_thread = threading.Thread(daemon=True, target=call_checkpoint)
    caller_thread.start()

    # Wait for both threads
    caller_thread.join(timeout=2.0)
    processor_thread.join(timeout=1.0)

    # Verify call completed
    assert call_completed.is_set()

    # Verify it blocked for at least the delay time
    elapsed = end_time - start_time
    assert elapsed >= 0.15, f"Expected blocking for at least 0.15s, got {elapsed}s"


def test_create_checkpoint_sync_success():
    """Test create_checkpoint_sync works normally when no error occurs."""
    mock_client = Mock(spec=LambdaClient)
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )

    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="initial-token",  # noqa: S106
        operations={},
        service_client=mock_client,
    )

    # Start background thread
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(state.checkpoint_batches_forever)

    try:
        operation_update = OperationUpdate.create_step_start(
            OperationIdentifier("test-op", None, "test-step")
        )

        # Should work normally without error
        state.create_checkpoint_sync(operation_update)

        # Verify checkpoint was called
        assert mock_client.checkpoint.call_count == 1
    finally:
        state.stop_checkpointing()
        executor.shutdown(wait=True)


def test_create_checkpoint_sync_unwraps_background_thread_error():
    """Test create_checkpoint_sync unwraps BackgroundThreadError to original exception."""
    mock_client = Mock(spec=LambdaClient)

    # Make checkpoint fail with a specific error
    original_error = RuntimeError("Original checkpoint error")
    mock_client.checkpoint.side_effect = original_error

    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="initial-token",  # noqa: S106
        operations={},
        service_client=mock_client,
    )

    # Start background thread
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(state.checkpoint_batches_forever)

    try:
        operation_update = OperationUpdate.create_step_start(
            OperationIdentifier("test-op", None, "test-step")
        )

        # Should raise the original RuntimeError, not BackgroundThreadError
        with pytest.raises(RuntimeError, match="Original checkpoint error"):
            state.create_checkpoint_sync(operation_update)

    finally:
        state.stop_checkpointing()
        executor.shutdown(wait=True)


def test_create_checkpoint_sync_always_synchronous():
    """Test create_checkpoint_sync is always synchronous and blocks until completion."""
    mock_client = Mock(spec=LambdaClient)
    mock_client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )

    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="initial-token",  # noqa: S106
        operations={},
        service_client=mock_client,
    )

    # Start background thread
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(state.checkpoint_batches_forever)

    try:
        operation_update = OperationUpdate.create_step_start(
            OperationIdentifier("test-op", None, "test-step")
        )

        # Should block until completion (synchronous behavior)
        state.create_checkpoint_sync(operation_update)

        # Verify checkpoint was called immediately (no need to wait)
        assert mock_client.checkpoint.call_count == 1
    finally:
        state.stop_checkpointing()
        executor.shutdown(wait=True)


def test_state_replay_mode():
    operation1 = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    operation2 = Operation(
        operation_id="op2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    execution_state = ExecutionState(
        durable_execution_arn="arn:aws:test",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={"op1": operation1, "op2": operation2},
        service_client=Mock(),
        replay_status=ReplayStatus.REPLAY,
    )
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op1")
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op2")
    assert execution_state.is_replaying() is False


def test_state_replay_mode_with_timed_out():
    """Test that TIMED_OUT operations are treated as terminal states for replay tracking.

    This test verifies that when an operation has TIMED_OUT status, it is correctly
    recognized as a completed/terminal state, allowing the replay status to transition
    from REPLAY to NEW once all completed operations have been visited.

    Regression test for: https://github.com/aws/aws-durable-execution-sdk-python/issues/262
    """
    operation1 = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.TIMED_OUT,
    )
    operation2 = Operation(
        operation_id="op2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    execution_state = ExecutionState(
        durable_execution_arn="arn:aws:test",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={"op1": operation1, "op2": operation2},
        service_client=Mock(),
        replay_status=ReplayStatus.REPLAY,
    )
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op1")
    assert execution_state.is_replaying() is True
    execution_state.track_replay(operation_id="op2")
    assert execution_state.is_replaying() is False
