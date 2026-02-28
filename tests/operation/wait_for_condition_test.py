"""Unit tests for wait_for_condition operation."""

import datetime
import json
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    InvocationError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python.logger import Logger, LogInfo
from aws_durable_execution_sdk_python.operation.wait_for_condition import (
    WaitForConditionOperationExecutor,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from tests.serdes_test import CustomDictSerDes


# Test helper - maintains old handler signature for backward compatibility in tests
def wait_for_condition_handler(
    check, config, state, operation_identifier, context_logger
):
    """Test helper that wraps WaitForConditionOperationExecutor with old handler signature."""
    executor = WaitForConditionOperationExecutor(
        check=check,
        config=config,
        state=state,
        operation_identifier=operation_identifier,
        context_logger=context_logger,
    )
    return executor.process()


def test_wait_for_condition_first_execution_condition_met():
    """Test wait_for_condition on first execution when condition is met."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 6
    assert mock_state.create_checkpoint.call_count == 2  # START and SUCCESS


def test_wait_for_condition_first_execution_condition_not_met():
    """Test wait_for_condition on first execution when condition is not met."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(30))

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 30 seconds"):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )

    assert mock_state.create_checkpoint.call_count == 2  # START and RETRY


def test_wait_for_condition_already_succeeded():
    """Test wait_for_condition when already completed successfully."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps(42)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 42
    assert mock_state.create_checkpoint.call_count == 0  # No new checkpoints


def test_wait_for_condition_already_succeeded_none_result():
    """Test wait_for_condition when already completed with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=None),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result is None


def test_wait_for_condition_already_failed():
    """Test wait_for_condition when already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=StepDetails(
            error=ErrorObject("Test error", "TestError", None, None)
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    with pytest.raises(CallableRuntimeError):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )


def test_wait_for_condition_retry_with_state():
    """Test wait_for_condition on retry with previous state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 11  # 10 (from checkpoint) + 1
    assert mock_state.create_checkpoint.call_count == 1  # Only SUCCESS


def test_wait_for_condition_retry_without_state():
    """Test wait_for_condition on retry without previous state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=None, attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 6  # 5 (initial) + 1


def test_wait_for_condition_retry_invalid_json_state():
    """Test wait_for_condition on retry with invalid JSON state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result="invalid json", attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 6  # Falls back to initial state


def test_wait_for_condition_check_function_exception():
    """Test wait_for_condition when check function raises exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        msg = "Test error"
        raise ValueError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    with pytest.raises(ValueError, match="Test error"):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )

    assert mock_state.create_checkpoint.call_count == 2  # START and FAIL


def test_wait_for_condition_check_context():
    """Test that check function receives proper context."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    captured_context = None

    def check_func(state, context):
        nonlocal captured_context
        captured_context = context
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert isinstance(captured_context, WaitForConditionCheckContext)
    assert captured_context.logger is mock_logger


def test_wait_for_condition_delay_seconds_none():
    """Test wait_for_condition with None delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    def wait_strategy(state, attempt):
        return WaitForConditionDecision(should_continue=True, delay=Duration())

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 0 seconds"):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )


def test_wait_for_condition_no_operation_in_checkpoint():
    """Test wait_for_condition when checkpoint has no operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"

    # Create a mock result that is started but has no operation
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_pending.return_value = False
    mock_result.is_started_or_ready.return_value = True
    mock_result.is_existent.return_value = True
    mock_result.result = json.dumps(10)
    mock_result.operation = None

    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 11  # Uses attempt=1 by default


def test_wait_for_condition_operation_no_step_details():
    """Test wait_for_condition when operation has no step_details."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"

    # Create operation without step_details
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    # Mock the result property since CheckpointedResult is frozen
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_pending.return_value = False
    mock_result.is_started_or_ready.return_value = True
    mock_result.is_existent.return_value = True
    mock_result.result = json.dumps(10)
    mock_result.operation = operation

    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == 11  # Uses attempt=1 by default


def test_wait_for_condition_custom_delay_seconds():
    """Test wait_for_condition with custom delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    def wait_strategy(state, attempt):
        return WaitForConditionDecision(
            should_continue=True, delay=Duration.from_minutes(1)
        )

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 60 seconds"):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )


def test_wait_for_condition_attempt_number_passed_to_strategy():
    """Test that attempt number is correctly passed to wait strategy."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=3),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    captured_attempt = None

    def wait_strategy(state, attempt):
        nonlocal captured_attempt
        captured_attempt = attempt
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert captured_attempt == 4


def test_wait_for_condition_attempt_sequence_is_monotonic():
    """Test that attempt numbers form a monotonically increasing sequence: 1, 2, 3, 4...

    This test validates the fix for the attempt counting bug where:
    - First execution (no checkpoint): attempt = 1
    - After first retry (checkpoint.attempt = 1): attempt = 2
    - After second retry (checkpoint.attempt = 2): attempt = 3
    - After third retry (checkpoint.attempt = 3): attempt = 4

    The current attempt should always be: checkpointed_attempts + 1
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    captured_attempts = []

    def wait_strategy(state, attempt):
        captured_attempts.append(attempt)
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    # Test 1: First execution (no checkpoint exists)
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert captured_attempts[-1] == 1, "First execution should have attempt=1"

    # Test 2: After first retry (checkpoint has attempt=1)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=1),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert captured_attempts[-1] == 2, (
        "After first retry (checkpoint.attempt=1), current attempt should be 2"
    )

    # Test 3: After second retry (checkpoint has attempt=2)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert captured_attempts[-1] == 3, (
        "After second retry (checkpoint.attempt=2), current attempt should be 3"
    )

    # Test 4: After third retry (checkpoint has attempt=3)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=3),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert captured_attempts[-1] == 4, (
        "After third retry (checkpoint.attempt=3), current attempt should be 4"
    )

    # Verify the complete sequence is monotonically increasing
    assert captured_attempts == [
        1,
        2,
        3,
        4,
    ], f"Expected [1, 2, 3, 4] but got {captured_attempts}"


def test_wait_for_condition_state_passed_to_strategy():
    """Test that new state is correctly passed to wait strategy."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state * 2

    captured_state = None

    def wait_strategy(state, attempt):
        nonlocal captured_state
        captured_state = state
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert captured_state == 10  # 5 * 2


def test_wait_for_condition_logger_with_log_info():
    """Test that logger is properly configured with log info."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test:execution:123"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    # Verify logger.with_log_info was called
    mock_logger.with_log_info.assert_called_once()
    call_args = mock_logger.with_log_info.call_args[0][0]
    assert isinstance(call_args, LogInfo)


def test_wait_for_condition_zero_delay_seconds():
    """Test wait_for_condition with zero delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    def wait_strategy(state, attempt):
        return WaitForConditionDecision(
            should_continue=True, delay=Duration.from_seconds(0)
        )

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 0 seconds"):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )


def test_wait_for_condition_custom_serdes_first_execution_condition_met():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    def check_func(state, context):
        return complex_result

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(
        initial_state=5, wait_strategy=wait_strategy, serdes=CustomDictSerDes()
    )

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )
    expected_checkpoointed_result = (
        '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == expected_checkpoointed_result


def test_wait_for_condition_custom_serdes_already_succeeded():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
        serdes=CustomDictSerDes(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


def test_wait_for_condition_pending():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="XXX",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
            next_attempt_timestamp=datetime.datetime.fromtimestamp(
                1764547200, tz=datetime.UTC
            ),
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        msg = "Should not be called"
        raise InvocationError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
        serdes=CustomDictSerDes(),
    )

    with pytest.raises(
        SuspendExecution, match="wait_for_condition test_wait will retry at timestamp"
    ):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )


def test_wait_for_condition_pending_without_next_attempt():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="XXX",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        msg = "Should not be called"
        raise InvocationError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
        serdes=CustomDictSerDes(),
    )

    with pytest.raises(
        SuspendExecution,
        match="No timestamp provided. Suspending without retry timestamp.",
    ):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )


# Immediate Response Handling Tests


def test_wait_for_condition_checkpoint_called_once_with_is_sync_false():
    """Test that get_checkpoint_result is called once when checkpoint is created (is_sync=False)."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    # Verify get_checkpoint_result called only once (no second check for async checkpoint)
    assert mock_state.get_checkpoint_result.call_count == 1

    # Verify create_checkpoint called with is_sync=False
    assert mock_state.create_checkpoint.call_count == 2  # START and SUCCESS
    start_call = mock_state.create_checkpoint.call_args_list[0]
    assert start_call[1]["is_sync"] is False


def test_wait_for_condition_immediate_success_without_executing_check():
    """Test immediate success: checkpoint returns SUCCEEDED on first check, returns result without executing check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps(42)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    # Check function should NOT be called
    def check_func(state, context):
        msg = "Check function should not be called for immediate success"
        raise AssertionError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    # Verify result returned without executing check function
    assert result == 42
    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


def test_wait_for_condition_immediate_failure_without_executing_check():
    """Test immediate failure: checkpoint returns FAILED on first check, raises error without executing check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=StepDetails(
            error=ErrorObject("Test error", "TestError", None, None)
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    # Check function should NOT be called
    def check_func(state, context):
        msg = "Check function should not be called for immediate failure"
        raise AssertionError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    # Verify error raised without executing check function
    with pytest.raises(CallableRuntimeError):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )

    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


def test_wait_for_condition_pending_suspends_without_executing_check():
    """Test pending handling: checkpoint returns PENDING on first check, suspends without executing check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            result=json.dumps(10),
            next_attempt_timestamp=datetime.datetime.fromtimestamp(
                1764547200, tz=datetime.UTC
            ),
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    # Check function should NOT be called
    def check_func(state, context):
        msg = "Check function should not be called for pending status"
        raise AssertionError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    # Verify suspend occurs without executing check function
    with pytest.raises(
        SuspendExecution, match="wait_for_condition test_wait will retry at timestamp"
    ):
        wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
            context_logger=mock_logger,
        )

    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


def test_wait_for_condition_no_checkpoint_executes_check_function():
    """Test no immediate response: when checkpoint doesn't exist, operation executes check function."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    op_id = OperationIdentifier("op1", None, "test_wait")

    check_called = False

    def check_func(state, context):
        nonlocal check_called
        check_called = True
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    # Verify check function was executed
    assert check_called is True
    assert result == 6

    # Verify checkpoints created (START and SUCCESS)
    assert mock_state.create_checkpoint.call_count == 2


def test_wait_for_condition_already_completed_no_checkpoint_created():
    """Test already completed: when checkpoint is SUCCEEDED on first check, no checkpoint created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps(42)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    mock_logger = Mock(spec=Logger)
    op_id = OperationIdentifier("op1", None, "test_wait")

    def check_func(state, context):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
        context_logger=mock_logger,
    )

    # Verify result returned
    assert result == 42

    # Verify NO checkpoints created (already completed)
    assert mock_state.create_checkpoint.call_count == 0


def test_wait_for_condition_executes_check_when_checkpoint_not_terminal():
    """Test backward compatibility: when checkpoint is not terminal (STARTED),
    the wait_for_condition operation executes the check function normally.

    Note: wait_for_condition uses async checkpoints (is_sync=False), so there's
    only one check, not two.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Single call: checkpoint doesn't exist (async checkpoint, no second check)
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_check_function = Mock(return_value="final_state")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    def mock_wait_strategy(state, attempt):
        return WaitForConditionDecision(
            should_continue=False, delay=Duration.from_seconds(0)
        )

    executor = WaitForConditionOperationExecutor(
        check=mock_check_function,
        config=WaitForConditionConfig(
            initial_state="initial",
            wait_strategy=mock_wait_strategy,
        ),
        state=mock_state,
        operation_identifier=OperationIdentifier("wfc-1", None, "test_wfc"),
        context_logger=mock_logger,
    )
    result = executor.process()

    # Assert - behaves like "old way"
    mock_check_function.assert_called_once()  # Check function executed
    assert result == "final_state"
    assert mock_state.get_checkpoint_result.call_count == 1  # Single check (async)
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCESS checkpoints


def test_wait_for_condition_executes_check_when_checkpoint_not_terminal_duplicate():
    """Test backward compatibility: when checkpoint is not terminal (STARTED),
    the wait_for_condition operation executes the check function normally.

    Note: wait_for_condition uses async checkpoints (is_sync=False), so there's
    only one check, not two.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Single call: checkpoint doesn't exist (async checkpoint, no second check)
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_not_found()
    )

    mock_check_function = Mock(return_value="final_state")
    mock_logger = Mock(spec=Logger)
    mock_logger.with_log_info.return_value = mock_logger

    def mock_wait_strategy(state, attempt):
        return WaitForConditionDecision(should_continue=False, delay=None)

    executor = WaitForConditionOperationExecutor(
        check=mock_check_function,
        config=WaitForConditionConfig(
            initial_state="initial",
            wait_strategy=mock_wait_strategy,
        ),
        state=mock_state,
        operation_identifier=OperationIdentifier("wfc-1", None, "test_wfc"),
        context_logger=mock_logger,
    )
    result = executor.process()

    # Assert - behaves like "old way"
    mock_check_function.assert_called_once()  # Check function executed
    assert result == "final_state"
    assert mock_state.get_checkpoint_result.call_count == 1  # Single check (async)
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCESS checkpoints
