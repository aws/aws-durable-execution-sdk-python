"""Unit tests for callback operation validator."""

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.callback import (
    CallbackOperationValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


def test_validate_start_action_with_no_current_state():
    """Test START action with no current state."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
    )
    CallbackOperationValidator.validate(None, update)


def test_validate_start_action_with_existing_state():
    """Test START action with existing state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Cannot start a CALLBACK that already exist",
    ):
        CallbackOperationValidator.validate(current_state, update)


def test_validate_cancel_action_with_no_current_state():
    """Test CANCEL action with no current state raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for CALLBACK operation.",
    ):
        CallbackOperationValidator.validate(None, update)


def test_validate_cancel_action_with_completed_state():
    """Test CANCEL action with completed state raises error."""
    current_state = Operation(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
    )
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.CANCEL,
    )

    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for CALLBACK operation.",
    ):
        CallbackOperationValidator.validate(current_state, update)


def test_validate_invalid_action():
    """Test invalid action raises error."""
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.SUCCEED,
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation."
    ):
        CallbackOperationValidator.validate(None, update)
