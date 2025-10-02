"""Unit tests for wait handler."""

from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationSubType,
    OperationType,
    OperationUpdate,
    WaitOptions,
)
from aws_durable_execution_sdk_python.operation.wait import wait_handler
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState


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
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_succeeded.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution, match="Wait for 30 seconds"):
        wait_handler(
            seconds=30,
            state=mock_state,
            operation_identifier=OperationIdentifier("wait2", None),
        )

    mock_state.get_checkpoint_result.assert_called_once_with("wait2")

    expected_operation = OperationUpdate(
        operation_id="wait2",
        parent_id=None,
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(seconds=30),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


def test_wait_handler_with_none_name():
    """Test wait_handler with None name."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_succeeded.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution, match="Wait for 5 seconds"):
        wait_handler(
            seconds=5,
            state=mock_state,
            operation_identifier=OperationIdentifier("wait3", None),
        )

    expected_operation = OperationUpdate(
        operation_id="wait3",
        parent_id=None,
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT,
        wait_options=WaitOptions(seconds=5),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
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
            seconds=5,
            state=mock_state,
            operation_identifier=OperationIdentifier("wait4", None),
        )

    mock_state.get_checkpoint_result.assert_called_once_with("wait4")
    mock_state.create_checkpoint.assert_not_called()
