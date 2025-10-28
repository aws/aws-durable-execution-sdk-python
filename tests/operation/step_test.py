"""Unit tests for step handler."""

import datetime
import json
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import (
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
from aws_durable_execution_sdk_python.operation.step import step_handler
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState
from tests.serdes_test import CustomDictSerDes


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
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

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
        return_value=RetryDecision(should_retry=True, delay_seconds=5)
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
        return_value=RetryDecision(should_retry=False, delay_seconds=0)
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
        return_value=RetryDecision(should_retry=False, delay_seconds=0)
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
        return_value=RetryDecision(should_retry=True, delay_seconds=10)
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
        return_value=RetryDecision(should_retry=True, delay_seconds=10)
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


@patch("aws_durable_execution_sdk_python.operation.step.retry_handler")
def test_step_handler_retry_handler_no_exception(mock_retry_handler):
    """Test step_handler when retry_handler doesn't raise an exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

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
