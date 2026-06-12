"""Unit tests for wait operation validator."""

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.wait import (
    WaitOperationValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


def test_validate_start_action_with_no_current_state():
    """Test START action with no current state."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
    )
    WaitOperationValidator.validate(None, update)


def test_validate_start_action_with_existing_state():
    """Test START action with existing state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException, match="Cannot start a WAIT that already exist"
    ):
        WaitOperationValidator.validate(current_state, update)


def test_validate_cancel_action_with_started_state():
    """Test CANCEL action with STARTED state."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.CANCEL,
    )
    WaitOperationValidator.validate(current_state, update)


def test_validate_cancel_action_with_no_current_state():
    """Test CANCEL action with no current state raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot cancel a WAIT that does not exist or has already completed",
    ):
        WaitOperationValidator.validate(None, update)


def test_validate_cancel_action_with_completed_state():
    """Test CANCEL action with completed state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot cancel a WAIT that does not exist or has already completed",
    ):
        WaitOperationValidator.validate(current_state, update)


def test_validate_invalid_action():
    """Test invalid action raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.SUCCEED,
    )

    with pytest.raises(InvalidParameterValueException, match="Invalid WAIT action"):
        WaitOperationValidator.validate(None, update)
