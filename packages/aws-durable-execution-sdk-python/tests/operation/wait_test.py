"""Unit tests for wait handler."""

from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    WaitOptions,
)
from aws_durable_execution_sdk_python.operation.wait import WaitOperationExecutor
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState


# Test helper function - maintains old handler signature for backward compatibility
def wait_handler(seconds: int, state, operation_identifier) -> None:
    """Test helper that wraps WaitOperationExecutor with old handler signature."""
    executor = WaitOperationExecutor(
        seconds=seconds,
        state=state,
        operation_identifier=operation_identifier,
    )
    return executor.process()


def test_wait_handler_already_completed():
    """Test wait_handler when operation is already completed."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_succeeded.return_value = True
    mock_state.get_checkpoint_result.return_value = mock_result

    wait_handler(
        seconds=10,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait1", None),
    )

    mock_state.get_checkpoint_result.assert_called_once_with("wait1")
    mock_state.create_checkpoint.assert_not_called()


def test_wait_handler_not_completed():
    """Test wait_handler when operation is not completed."""
    mock_state = Mock(spec=ExecutionState)

    # First call: checkpoint doesn't exist
    not_found_result = Mock(spec=CheckpointedResult)
    not_found_result.is_succeeded.return_value = False
    not_found_result.is_existent.return_value = False

    # Second call: checkpoint exists but not completed (no immediate response)
    started_result = Mock(spec=CheckpointedResult)
    started_result.is_succeeded.return_value = False
    started_result.is_existent.return_value = True

    mock_state.get_checkpoint_result.side_effect = [not_found_result, started_result]

    with pytest.raises(SuspendExecution, match="Wait for 30 seconds"):
        wait_handler(
            seconds=30,
            state=mock_state,
            operation_identifier=OperationIdentifier("wait2", None),
        )

    # Should be called twice: once before checkpoint, once after to check for immediate response
    assert mock_state.get_checkpoint_result.call_count == 2

    expected_operation = OperationUpdate(
        operation_id="wait2",
        parent_id=None,
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(wait_seconds=30),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation, is_sync=True
    )


def test_wait_handler_with_none_name():
    """Test wait_handler with None name."""
    mock_state = Mock(spec=ExecutionState)

    # First call: checkpoint doesn't exist
    not_found_result = Mock(spec=CheckpointedResult)
    not_found_result.is_succeeded.return_value = False
    not_found_result.is_existent.return_value = False

    # Second call: checkpoint exists but not completed (no immediate response)
    started_result = Mock(spec=CheckpointedResult)
    started_result.is_succeeded.return_value = False
    started_result.is_existent.return_value = True

    mock_state.get_checkpoint_result.side_effect = [not_found_result, started_result]

    with pytest.raises(SuspendExecution, match="Wait for 5 seconds"):
        wait_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("wait3", None),
            seconds=5,
        )

    # Should be called twice: once before checkpoint, once after to check for immediate response
    assert mock_state.get_checkpoint_result.call_count == 2

    expected_operation = OperationUpdate(
        operation_id="wait3",
        parent_id=None,
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(wait_seconds=5),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation, is_sync=True
    )


def test_wait_handler_with_existent():
    """Test wait_handler with existent operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_succeeded.return_value = False
    mock_result.is_existent.return_value = True
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution, match="Wait for 5 seconds"):
        wait_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("wait4", None),
            seconds=5,
        )

    mock_state.get_checkpoint_result.assert_called_once_with("wait4")
    mock_state.create_checkpoint.assert_not_called()


# Immediate response handling tests


def test_wait_status_evaluation_after_checkpoint():
    """Test that status is evaluated twice: before and after checkpoint creation.

    This verifies the immediate response pattern:
    1. Check status (checkpoint doesn't exist)
    2. Create checkpoint with is_sync=True
    3. Check status again (catches immediate response)
    """
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    # First call: checkpoint doesn't exist
    not_found_result = Mock(spec=CheckpointedResult)
    not_found_result.is_succeeded.return_value = False
    not_found_result.is_existent.return_value = False

    # Second call: checkpoint exists but not completed (no immediate response)
    started_result = Mock(spec=CheckpointedResult)
    started_result.is_succeeded.return_value = False
    started_result.is_existent.return_value = True

    mock_state.get_checkpoint_result.side_effect = [not_found_result, started_result]

    executor = WaitOperationExecutor(
        seconds=30,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait_eval", None, "test_wait"),
    )

    # Act
    with pytest.raises(SuspendExecution):
        executor.process()

    # Assert - verify status checked twice
    assert mock_state.get_checkpoint_result.call_count == 2
    mock_state.get_checkpoint_result.assert_any_call("wait_eval")

    # Verify checkpoint created with is_sync=True
    expected_operation = OperationUpdate(
        operation_id="wait_eval",
        parent_id=None,
        name="test_wait",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(wait_seconds=30),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation, is_sync=True
    )


def test_wait_immediate_success_handling():
    """Test that immediate SUCCEEDED response returns without suspend.

    When the checkpoint returns SUCCEEDED on the second status check,
    the operation should return immediately without suspending.
    """
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    # First call: checkpoint doesn't exist
    not_found_result = Mock(spec=CheckpointedResult)
    not_found_result.is_succeeded.return_value = False
    not_found_result.is_existent.return_value = False

    # Second call: checkpoint succeeded immediately
    succeeded_result = Mock(spec=CheckpointedResult)
    succeeded_result.is_succeeded.return_value = True

    mock_state.get_checkpoint_result.side_effect = [not_found_result, succeeded_result]

    executor = WaitOperationExecutor(
        seconds=5,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wait_immediate", None, "immediate_wait"
        ),
    )

    # Act
    result = executor.process()

    # Assert - verify immediate return without suspend
    assert result is None  # Wait returns None

    # Verify checkpoint was created
    assert mock_state.create_checkpoint.call_count == 1

    # Verify status checked twice
    assert mock_state.get_checkpoint_result.call_count == 2


def test_wait_no_immediate_response_suspends():
    """Test that wait suspends when no immediate response received.

    When the checkpoint returns STARTED (not completed) on the second check,
    the operation should suspend to wait for timer completion.
    """
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    # First call: checkpoint doesn't exist
    not_found_result = Mock(spec=CheckpointedResult)
    not_found_result.is_succeeded.return_value = False
    not_found_result.is_existent.return_value = False

    # Second call: checkpoint exists but not completed
    started_result = Mock(spec=CheckpointedResult)
    started_result.is_succeeded.return_value = False
    started_result.is_existent.return_value = True

    mock_state.get_checkpoint_result.side_effect = [not_found_result, started_result]

    executor = WaitOperationExecutor(
        seconds=60,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait_suspend", None),
    )

    # Act & Assert - verify suspend occurs
    with pytest.raises(SuspendExecution) as exc_info:
        executor.process()

    # Verify suspend message
    assert "Wait for 60 seconds" in str(exc_info.value)

    # Verify checkpoint was created
    assert mock_state.create_checkpoint.call_count == 1

    # Verify status checked twice
    assert mock_state.get_checkpoint_result.call_count == 2


def test_wait_already_completed_no_checkpoint():
    """Test that already completed wait doesn't create checkpoint.

    When replaying and the wait is already completed, it should return
    immediately without creating a new checkpoint.
    """
    # Arrange
    mock_state = Mock(spec=ExecutionState)

    # Checkpoint already exists and succeeded
    succeeded_result = Mock(spec=CheckpointedResult)
    succeeded_result.is_succeeded.return_value = True

    mock_state.get_checkpoint_result.return_value = succeeded_result

    executor = WaitOperationExecutor(
        seconds=10,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait_replay", None, "completed_wait"),
    )

    # Act
    result = executor.process()

    # Assert - verify immediate return without checkpoint
    assert result is None

    # Verify no checkpoint created
    mock_state.create_checkpoint.assert_not_called()

    # Verify status checked only once
    mock_state.get_checkpoint_result.assert_called_once_with("wait_replay")


def test_wait_with_various_durations():
    """Test wait operations with different durations handle immediate response correctly."""
    for seconds in [1, 30, 300, 3600]:
        # Arrange
        mock_state = Mock(spec=ExecutionState)

        # First call: checkpoint doesn't exist
        not_found_result = Mock(spec=CheckpointedResult)
        not_found_result.is_succeeded.return_value = False
        not_found_result.is_existent.return_value = False

        # Second call: immediate success
        succeeded_result = Mock(spec=CheckpointedResult)
        succeeded_result.is_succeeded.return_value = True

        mock_state.get_checkpoint_result.side_effect = [
            not_found_result,
            succeeded_result,
        ]

        executor = WaitOperationExecutor(
            seconds=seconds,
            state=mock_state,
            operation_identifier=OperationIdentifier(f"wait_duration_{seconds}", None),
        )

        # Act
        result = executor.process()

        # Assert
        assert result is None
        assert mock_state.get_checkpoint_result.call_count == 2

        # Verify correct wait duration in checkpoint
        call_args = mock_state.create_checkpoint.call_args
        assert call_args[1]["operation_update"].wait_options.wait_seconds == seconds


def test_wait_suspends_when_second_check_returns_started():
    """Test backward compatibility: when the second checkpoint check returns
    STARTED (not terminal), the wait operation suspends normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: checkpoint doesn't exist
    # Second call: checkpoint returns STARTED (no immediate response)
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(
            Operation(
                operation_id="wait-1",
                operation_type=OperationType.WAIT,
                status=OperationStatus.STARTED,
            )
        ),
    ]

    executor = WaitOperationExecutor(
        seconds=5,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait-1", None, "test_wait"),
    )

    with pytest.raises(SuspendExecution):
        executor.process()

    # Assert - behaves like "old way"
    assert mock_state.get_checkpoint_result.call_count == 2  # Double-check happened
    mock_state.create_checkpoint.assert_called_once()  # START checkpoint created


def test_wait_suspends_when_second_check_returns_started_duplicate():
    """Test backward compatibility: when the second checkpoint check returns
    STARTED (not terminal), the wait operation suspends normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: checkpoint doesn't exist
    # Second call: checkpoint returns STARTED (no immediate response)
    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="wait-1",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    executor = WaitOperationExecutor(
        seconds=5,
        state=mock_state,
        operation_identifier=OperationIdentifier("wait-1", None, "test_wait"),
    )

    with pytest.raises(SuspendExecution):
        executor.process()

    # Assert - behaves like "old way"
    assert mock_state.get_checkpoint_result.call_count == 2  # Double-check happened
    mock_state.create_checkpoint.assert_called_once()  # START checkpoint created
