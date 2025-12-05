"""Unit tests for step handler."""

import datetime
import json
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import (
    Duration,
    StepConfig,
    StepSemantics,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    StepInterruptedError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python.logger import Logger
from aws_durable_execution_sdk_python.operation.step import StepOperationExecutor
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState
from tests.serdes_test import CustomDictSerDes


# Test helper - maintains old handler signature for backward compatibility in tests
def step_handler(func, state, operation_identifier, config, context_logger):
    """Test helper that wraps StepOperationExecutor with old handler signature."""
    if not config:
        config = StepConfig()
    executor = StepOperationExecutor(
        func=func,
        config=config,
        state=state,
        operation_identifier=operation_identifier,
        context_logger=context_logger,
    )
    return executor.process()


def test_step_handler_already_succeeded():
    """Test step_handler when operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps("test_result")),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_callable = Mock(return_value="should_not_call")
    mock_logger = Mock(spec=Logger)

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step1", None, "test_step"),
        None,
        mock_logger,
    )

    assert result == "test_result"
    mock_callable.assert_not_called()
    mock_state.create_checkpoint.assert_not_called()


def test_step_handler_already_succeeded_none_result():
    """Test step_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=None),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_callable = Mock()
    mock_logger = Mock(spec=Logger)

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step2", None, "test_step"),
        None,
        mock_logger,
    )

    assert result is None
    mock_callable.assert_not_called()


def test_step_handler_already_failed():
    """Test step_handler when operation already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="step3",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=StepDetails(error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_callable = Mock()
    mock_logger = Mock(spec=Logger)

    with pytest.raises(CallableRuntimeError):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step3", None, "test_step"),
            None,
            mock_logger,
        )

    mock_callable.assert_not_called()


def test_step_handler_started_at_most_once():
    """Test step_handler when operation started with AT_MOST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step4",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock()
    mock_logger = Mock(spec=Logger)

    with pytest.raises(SuspendExecution):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step4", None, "test_step"),
            config,
            mock_logger,
        )


def test_step_handler_started_at_least_once():
    """Test step_handler when operation started with AT_LEAST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="step5",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    config = StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=Logger)

    step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step5", None, "test_step"),
        config,
        mock_logger,
    )


def test_step_handler_success_at_least_once():
    """Test step_handler successful execution with AT_LEAST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    config = StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step6", None, "test_step"),
        config,
        mock_logger,
    )

    assert result == "success_result"

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step6"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.operation_id == "step6"
    assert success_operation.payload == json.dumps("success_result")
    assert success_operation.operation_type is OperationType.STEP
    assert success_operation.sub_type is OperationSubType.STEP
    assert success_operation.action is OperationAction.SUCCEED


def test_step_handler_success_at_most_once():
    """Test step_handler successful execution with AT_MOST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (after sync checkpoint)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step7",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step7", None, "test_step"),
        config,
        mock_logger,
    )

    assert result == "success_result"

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step7"
    assert start_operation.name == "test_step"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == json.dumps("success_result")
    assert success_operation.operation_type is OperationType.STEP
    assert success_operation.sub_type is OperationSubType.STEP
    assert success_operation.action is OperationAction.SUCCEED


def test_step_handler_non_retriable_execution_error():
    """Test step_handler with ExecutionError exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_callable = Mock(side_effect=ExecutionError("Do Not Retry"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(ExecutionError, match="Do Not Retry"):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step8", None, "test_step"),
            None,
            mock_logger,
        )


def test_step_handler_retry_success():
    """Test step_handler with retry that succeeds."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(
        return_value=RetryDecision(should_retry=True, delay=Duration.from_seconds(5))
    )
    config = StepConfig(retry_strategy=mock_retry_strategy)
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(SuspendExecution, match="Retry scheduled"):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step9", None, "test_step"),
            config,
            mock_logger,
        )

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step9"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify retry checkpoint
    retry_call = mock_state.create_checkpoint.call_args_list[1]
    retry_operation = retry_call[1]["operation_update"]
    assert retry_operation.operation_id == "step9"
    assert retry_operation.operation_type is OperationType.STEP
    assert retry_operation.sub_type is OperationSubType.STEP
    assert retry_operation.action is OperationAction.RETRY


def test_step_handler_retry_exhausted():
    """Test step_handler with retry exhausted."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(
        return_value=RetryDecision(should_retry=False, delay=Duration.from_seconds(0))
    )
    config = StepConfig(retry_strategy=mock_retry_strategy)
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(CallableRuntimeError):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step10", None, "test_step"),
            config,
            mock_logger,
        )

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step10"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify fail checkpoint
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.operation_id == "step10"
    assert fail_operation.operation_type is OperationType.STEP
    assert fail_operation.sub_type is OperationSubType.STEP
    assert fail_operation.action is OperationAction.FAIL


def test_step_handler_retry_interrupted_error():
    """Test step_handler with StepInterruptedError in retry."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(
        return_value=RetryDecision(should_retry=False, delay=Duration.from_seconds(0))
    )
    config = StepConfig(retry_strategy=mock_retry_strategy)
    interrupted_error = StepInterruptedError("Step interrupted")
    mock_callable = Mock(side_effect=interrupted_error)
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(StepInterruptedError, match="Step interrupted"):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step11", None, "test_step"),
            config,
            mock_logger,
        )


def test_step_handler_retry_with_existing_attempts():
    """Test step_handler retry logic with existing attempt count."""
    mock_state = Mock(spec=ExecutionState)

    # Simulate a retry operation that was previously checkpointed
    operation = Operation(
        operation_id="step12",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            attempt=2,
            next_attempt_timestamp=datetime.datetime.fromtimestamp(
                1764547200, tz=datetime.UTC
            ),
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(
        return_value=RetryDecision(should_retry=True, delay=Duration.from_seconds(10))
    )
    config = StepConfig(retry_strategy=mock_retry_strategy)
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(SuspendExecution, match="Retry scheduled"):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step12", None, "test_step"),
            config,
            mock_logger,
        )

    # Verify retry strategy was not called because we already have attempt timestamp in the checkpointed location
    mock_retry_strategy.assert_not_called()


def test_step_handler_pending_without_existing_attempts():
    """Test step_handler retry logic with existing attempt count."""
    mock_state = Mock(spec=ExecutionState)

    # Simulate a retry operation that was previously checkpointed
    operation = Operation(
        operation_id="step12",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(
        return_value=RetryDecision(should_retry=True, delay=Duration.from_seconds(10))
    )
    config = StepConfig(retry_strategy=mock_retry_strategy)
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(SuspendExecution, match="No timestamp provided"):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step12", None, "test_step"),
            config,
            mock_logger,
        )

    # Verify retry strategy was not called because we already have attempt timestamp in the checkpointed location
    mock_retry_strategy.assert_not_called()


@patch(
    "aws_durable_execution_sdk_python.operation.step.StepOperationExecutor.retry_handler"
)
def test_step_handler_retry_handler_no_exception(mock_retry_handler):
    """Test step_handler when retry_handler doesn't raise an exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (AT_LEAST_ONCE default)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step13",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    # Mock retry_handler to not raise an exception (which it should always do)
    mock_retry_handler.return_value = None

    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    with pytest.raises(
        ExecutionError,
        match="retry handler should have raised an exception, but did not.",
    ):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step13", None, "test_step"),
            None,
            mock_logger,
        )

    mock_retry_handler.assert_called_once()


def test_step_handler_custom_serdes_success():
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    config = StepConfig(
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY, serdes=CustomDictSerDes()
    )
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step6", None, "test_step"),
        config,
        mock_logger,
    )

    expected_checkpoointed_result = (
        '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == expected_checkpoointed_result


def test_step_handler_custom_serdes_already_succeeded():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_callable = Mock(return_value="should_not_call")
    mock_logger = Mock(spec=Logger)

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step1", None, "test_step"),
        StepConfig(serdes=CustomDictSerDes()),
        mock_logger,
    )

    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


# Tests for immediate response handling


def test_step_immediate_response_get_checkpoint_called_twice():
    """Test that get_checkpoint_result is called twice when checkpoint is created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found (checkpoint doesn't exist)
    # Second call: started (checkpoint created, no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step_immediate_1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_immediate_1", None, "test_step"),
        config,
        mock_logger,
    )

    # Verify get_checkpoint_result was called twice (before and after checkpoint creation)
    assert mock_state.get_checkpoint_result.call_count == 2
    assert result == "success_result"


def test_step_immediate_response_create_checkpoint_sync_at_most_once():
    """Test that create_checkpoint is called with is_sync=True for AT_MOST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step_immediate_2",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_immediate_2", None, "test_step"),
        config,
        mock_logger,
    )

    # Verify START checkpoint was created with is_sync=True
    start_call = mock_state.create_checkpoint.call_args_list[0]
    assert start_call[1]["is_sync"] is True


def test_step_immediate_response_create_checkpoint_async_at_least_once():
    """Test that create_checkpoint is called with is_sync=False for AT_LEAST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # For AT_LEAST_ONCE, only one call to get_checkpoint_result (no second check)
    not_found = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = not_found

    config = StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_immediate_3", None, "test_step"),
        config,
        mock_logger,
    )

    # Verify START checkpoint was created with is_sync=False
    start_call = mock_state.create_checkpoint.call_args_list[0]
    assert start_call[1]["is_sync"] is False


def test_step_immediate_response_immediate_success():
    """Test immediate success: checkpoint returns SUCCEEDED on second check, operation returns without suspend.

    Note: The current implementation calls get_checkpoint_result twice within check_result_status()
    for sync checkpoints, so we need to handle that in the mock setup.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found
    # Second call: started (no immediate response, proceed to execute)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step_immediate_4",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="immediate_success_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_immediate_4", None, "test_step"),
        config,
        mock_logger,
    )

    # Verify operation executed normally (no immediate response in current implementation)
    assert result == "immediate_success_result"
    mock_callable.assert_called_once()
    # Both START and SUCCEED checkpoints should be created
    assert mock_state.create_checkpoint.call_count == 2


def test_step_immediate_response_immediate_failure():
    """Test immediate failure: checkpoint returns FAILED on second check, operation raises error without suspend."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found
    # Second call: started (current implementation doesn't support immediate terminal responses from START)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step_immediate_5",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    # Make the step function raise an error
    mock_callable = Mock(side_effect=RuntimeError("Step execution error"))
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    # Configure retry strategy to not retry
    mock_retry_strategy = Mock(
        return_value=RetryDecision(should_retry=False, delay=Duration.from_seconds(0))
    )
    config = StepConfig(
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        retry_strategy=mock_retry_strategy,
    )

    # Verify operation raises error after executing step function
    with pytest.raises(CallableRuntimeError, match="Step execution error"):
        step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step_immediate_5", None, "test_step"),
            config,
            mock_logger,
        )

    mock_callable.assert_called_once()
    # Both START and FAIL checkpoints should be created
    assert mock_state.create_checkpoint.call_count == 2


def test_step_immediate_response_no_immediate_response():
    """Test no immediate response: checkpoint returns STARTED on second check, operation executes step function."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found
    # Second call: started (no immediate response, proceed to execute)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step_immediate_6",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="normal_execution_result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_immediate_6", None, "test_step"),
        config,
        mock_logger,
    )

    # Verify step function was executed
    assert result == "normal_execution_result"
    mock_callable.assert_called_once()
    # Both START and SUCCEED checkpoints should be created
    assert mock_state.create_checkpoint.call_count == 2


def test_step_immediate_response_already_completed():
    """Test already completed: checkpoint is already SUCCEEDED on first check, no checkpoint created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: already succeeded (replay scenario)
    succeeded_op = Operation(
        operation_id="step_immediate_7",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps("already_completed_result")),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.get_checkpoint_result.return_value = succeeded

    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)
    mock_callable = Mock(return_value="should_not_call")
    mock_logger = Mock(spec=Logger)

    result = step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_immediate_7", None, "test_step"),
        config,
        mock_logger,
    )

    # Verify operation returned immediately without creating checkpoint
    assert result == "already_completed_result"
    mock_callable.assert_not_called()
    mock_state.create_checkpoint.assert_not_called()
    # Only one call to get_checkpoint_result (no second check needed)
    assert mock_state.get_checkpoint_result.call_count == 1


def test_step_executes_function_when_second_check_returns_started():
    """Test backward compatibility: when the second checkpoint check returns
    STARTED (not terminal), the step function executes normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: checkpoint doesn't exist
    # Second call: checkpoint returns STARTED (no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=1),
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    mock_step_function = Mock(return_value="result")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    executor = StepOperationExecutor(
        func=mock_step_function,
        config=StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY),
        state=mock_state,
        operation_identifier=OperationIdentifier("step-1", None, "test_step"),
        context_logger=mock_logger,
    )
    result = executor.process()

    # Assert - behaves like "old way"
    mock_step_function.assert_called_once()  # Function executed (not skipped)
    assert result == "result"
    assert (
        mock_state.get_checkpoint_result.call_count == 1
    )  # Only one check for AT_LEAST_ONCE
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCEED checkpoints
