"""Unit tests for child handler."""

import json
from unittest.mock import Mock

import pytest

from aws_durable_functions_sdk_python.config import ChildConfig
from aws_durable_functions_sdk_python.exceptions import CallableRuntimeError, FatalError
from aws_durable_functions_sdk_python.identifier import OperationIdentifier
from aws_durable_functions_sdk_python.lambda_service import (
    ErrorObject,
    OperationAction,
    OperationSubType,
    OperationType,
)
from aws_durable_functions_sdk_python.operation.child import child_handler
from aws_durable_functions_sdk_python.state import ExecutionState
from tests.serdes_test import CustomDictSerDes


# region child_handler
@pytest.mark.parametrize(
    ("config", "expected_sub_type"),
    [
        (
            ChildConfig(sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT),
            OperationSubType.RUN_IN_CHILD_CONTEXT,
        ),
        (ChildConfig(sub_type=OperationSubType.STEP), OperationSubType.STEP),
        (None, OperationSubType.RUN_IN_CHILD_CONTEXT),
    ],
)
def test_child_handler_not_started(
    config: ChildConfig, expected_sub_type: OperationSubType
):
    """Test child_handler when operation not started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="fresh_result")

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op1", None, "test_name"), config
    )

    assert result == "fresh_result"
    mock_state.create_checkpoint.assert_called()
    assert mock_state.create_checkpoint.call_count == 2  # start and succeed

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "op1"
    assert start_operation.name == "test_name"
    assert start_operation.operation_type is OperationType.CONTEXT
    assert start_operation.sub_type is expected_sub_type
    assert start_operation.action is OperationAction.START

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.operation_id == "op1"
    assert success_operation.name == "test_name"
    assert success_operation.operation_type is OperationType.CONTEXT
    assert success_operation.sub_type is expected_sub_type
    assert success_operation.action is OperationAction.SUCCEED
    assert success_operation.payload == json.dumps("fresh_result")

    mock_callable.assert_called_once()


def test_child_handler_already_succeeded():
    """Test child_handler when operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.result = json.dumps("cached_result")
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op2", None, "test_name"), None
    )

    assert result == "cached_result"
    mock_callable.assert_not_called()
    mock_state.create_checkpoint.assert_not_called()


def test_child_handler_already_succeeded_none_result():
    """Test child_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.result = None
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op3", None, "test_name"), None
    )

    assert result is None
    mock_callable.assert_not_called()


def test_child_handler_already_failed():
    """Test child_handler when operation already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = True
    mock_result.raise_callable_error.side_effect = CallableRuntimeError(
        "Previous failure", "TestError", None, None
    )
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()

    with pytest.raises(CallableRuntimeError, match="Previous failure"):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op4", None, "test_name"),
            None,
        )

    mock_callable.assert_not_called()


@pytest.mark.parametrize(
    ("config", "expected_sub_type"),
    [
        (
            ChildConfig(sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT),
            OperationSubType.RUN_IN_CHILD_CONTEXT,
        ),
        (ChildConfig(sub_type=OperationSubType.STEP), OperationSubType.STEP),
        (None, OperationSubType.RUN_IN_CHILD_CONTEXT),
    ],
)
def test_child_handler_already_started(
    config: ChildConfig, expected_sub_type: OperationSubType
):
    """Test child_handler when operation already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = True
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="started_result")

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op5", None, "test_name"), config
    )

    assert result == "started_result"

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[0]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.operation_id == "op5"
    assert success_operation.name == "test_name"
    assert success_operation.operation_type is OperationType.CONTEXT
    assert success_operation.sub_type == expected_sub_type
    assert success_operation.action is OperationAction.SUCCEED
    assert success_operation.payload == json.dumps("started_result")

    mock_callable.assert_called_once()


@pytest.mark.parametrize(
    ("config", "expected_sub_type"),
    [
        (
            ChildConfig(sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT),
            OperationSubType.RUN_IN_CHILD_CONTEXT,
        ),
        (ChildConfig(sub_type=OperationSubType.STEP), OperationSubType.STEP),
        (None, OperationSubType.RUN_IN_CHILD_CONTEXT),
    ],
)
def test_child_handler_callable_exception(
    config: ChildConfig, expected_sub_type: OperationSubType
):
    """Test child_handler when callable raises exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(side_effect=ValueError("Test error"))

    with pytest.raises(CallableRuntimeError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op6", None, "test_name"),
            config,
        )

    mock_state.create_checkpoint.assert_called()
    assert mock_state.create_checkpoint.call_count == 2  # start and fail

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "op6"
    assert start_operation.name == "test_name"
    assert start_operation.operation_type is OperationType.CONTEXT
    assert start_operation.sub_type is expected_sub_type
    assert start_operation.action is OperationAction.START

    # Verify fail checkpoint
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.operation_id == "op6"
    assert fail_operation.name == "test_name"
    assert fail_operation.operation_type is OperationType.CONTEXT
    assert fail_operation.sub_type is expected_sub_type
    assert fail_operation.action is OperationAction.FAIL
    assert fail_operation.error == ErrorObject.from_exception(ValueError("Test error"))


def test_child_handler_fatal_error_propagated():
    """Test child_handler propagates FatalError without wrapping."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    fatal_error = FatalError("Fatal test error")
    mock_callable = Mock(side_effect=fatal_error)

    with pytest.raises(FatalError, match="Fatal test error"):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op7", None, "test_name"),
            None,
        )


def test_child_handler_with_config():
    """Test child_handler with config parameter."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="config_result")
    config = ChildConfig()

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op8", None, "test_name"), config
    )

    assert result == "config_result"
    mock_callable.assert_called_once()


def test_child_handler_default_serialization():
    """Test child_handler properly serializes complex result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op9", None, "test_name"), None
    )

    assert result == complex_result
    # Verify JSON serialization was used in checkpoint
    success_call = [
        call
        for call in mock_state.create_checkpoint.call_args_list
        if "SUCCEED" in str(call)
    ]
    assert len(success_call) == 1


def test_child_handler_custom_serdes_not_start():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    child_config: ChildConfig = ChildConfig(serdes=CustomDictSerDes())

    child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op9", None, "test_name"),
        child_config,
    )

    expected_checkpoointed_result = (
        '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == expected_checkpoointed_result


def test_child_handler_custom_serdes_already_succeeded():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.result = '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()
    child_config: ChildConfig = ChildConfig(serdes=CustomDictSerDes())

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op9", None, "test_name"),
        child_config,
    )

    expected_checkpoointed_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    assert actual_result == expected_checkpoointed_result


# endregion child_handler
