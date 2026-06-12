"""Unit tests for transitions validator."""

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.transitions import (
    ValidActionsByOperationTypeValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


def test_validate_step_valid_actions():
    """Test valid actions for STEP operations."""
    valid_actions = [
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.RETRY,
        OperationAction.SUCCEED,
    ]
    for action in valid_actions:
        ValidActionsByOperationTypeValidator.validate(OperationType.STEP, action)


def test_validate_context_valid_actions():
    """Test valid actions for CONTEXT operations."""
    valid_actions = [
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.SUCCEED,
    ]
    for action in valid_actions:
        ValidActionsByOperationTypeValidator.validate(OperationType.CONTEXT, action)


def test_validate_wait_valid_actions():
    """Test valid actions for WAIT operations."""
    valid_actions = [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
    for action in valid_actions:
        ValidActionsByOperationTypeValidator.validate(OperationType.WAIT, action)


def test_validate_callback_valid_actions():
    """Test valid actions for CALLBACK operations."""
    valid_actions = [
        OperationAction.START,
    ]
    for action in valid_actions:
        ValidActionsByOperationTypeValidator.validate(OperationType.CALLBACK, action)


def test_validate_invoke_valid_actions():
    """Test valid actions for INVOKE operations."""
    valid_actions = [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
    for action in valid_actions:
        ValidActionsByOperationTypeValidator.validate(
            OperationType.CHAINED_INVOKE, action
        )


def test_validate_execution_valid_actions():
    """Test valid actions for EXECUTION operations."""
    valid_actions = [
        OperationAction.SUCCEED,
        OperationAction.FAIL,
    ]
    for action in valid_actions:
        ValidActionsByOperationTypeValidator.validate(OperationType.EXECUTION, action)


def test_validate_invalid_action_for_step():
    """Test invalid action for STEP operation."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ValidActionsByOperationTypeValidator.validate(
            OperationType.STEP, OperationAction.CANCEL
        )


def test_validate_invalid_action_for_context():
    """Test invalid action for CONTEXT operation."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ValidActionsByOperationTypeValidator.validate(
            OperationType.CONTEXT, OperationAction.RETRY
        )


def test_validate_invalid_action_for_wait():
    """Test invalid action for WAIT operation."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ValidActionsByOperationTypeValidator.validate(
            OperationType.WAIT, OperationAction.SUCCEED
        )


def test_validate_invalid_action_for_callback():
    """Test invalid action for CALLBACK operation."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ValidActionsByOperationTypeValidator.validate(
            OperationType.CALLBACK, OperationAction.FAIL
        )


def test_validate_invalid_action_for_invoke():
    """Test invalid action for INVOKE operation."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ValidActionsByOperationTypeValidator.validate(
            OperationType.CHAINED_INVOKE, OperationAction.RETRY
        )


def test_validate_invalid_action_for_execution():
    """Test invalid action for EXECUTION operation."""
    with pytest.raises(
        InvalidParameterValueException,
        match="Invalid action for the given operation type",
    ):
        ValidActionsByOperationTypeValidator.validate(
            OperationType.EXECUTION, OperationAction.START
        )


def test_validate_unknown_operation_type():
    """Test validation with unknown operation type."""
    with pytest.raises(InvalidParameterValueException, match="Unknown operation type"):
        ValidActionsByOperationTypeValidator.validate(None, OperationAction.START)
