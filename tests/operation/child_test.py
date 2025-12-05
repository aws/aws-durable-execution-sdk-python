"""Unit tests for child handler."""

from __future__ import annotations

import json
from typing import cast
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    InvocationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationAction,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.operation.child import child_handler
from aws_durable_execution_sdk_python.state import ExecutionState
from aws_durable_execution_sdk_python.types import SummaryGenerator
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
    config: ChildConfig | None, expected_sub_type: OperationSubType
):
    """Test child_handler when operation not started.

    Verifies:
    - get_checkpoint_result is called once (async checkpoint, no second check)
    - create_checkpoint is called with is_sync=False for START
    - Operation executes and creates SUCCEED checkpoint
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="fresh_result")

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op1", None, "test_name"), config
    )

    assert result == "fresh_result"

    # Verify get_checkpoint_result called once (async checkpoint, no second check)
    assert mock_state.get_checkpoint_result.call_count == 1

    # Verify create_checkpoint called twice (start and succeed)
    mock_state.create_checkpoint.assert_called()
    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint with is_sync=False
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "op1"
    assert start_operation.name == "test_name"
    assert start_operation.operation_type is OperationType.CONTEXT
    assert start_operation.sub_type is expected_sub_type
    assert start_operation.action is OperationAction.START
    # CRITICAL: Verify is_sync=False for START checkpoint (async, no immediate response)
    assert start_call[1]["is_sync"] is False

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
    """Test child_handler when operation already succeeded without replay_children.

    Verifies:
    - Returns cached result without executing function
    - No checkpoint created
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.is_replay_children.return_value = False
    mock_result.result = json.dumps("cached_result")
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op2", None, "test_name"), None
    )

    assert result == "cached_result"
    # Verify function not executed
    mock_callable.assert_not_called()
    # Verify no checkpoint created
    mock_state.create_checkpoint.assert_not_called()
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1


def test_child_handler_already_succeeded_none_result():
    """Test child_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.is_replay_children.return_value = False
    mock_result.result = None
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op3", None, "test_name"), None
    )

    assert result is None
    mock_callable.assert_not_called()


def test_child_handler_already_failed():
    """Test child_handler when operation already failed.

    Verifies:
    - Already failed: raises error without executing function
    - No checkpoint created
    - get_checkpoint_result called once
    """
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

    # Verify function not executed
    mock_callable.assert_not_called()
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1


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
    config: ChildConfig | None, expected_sub_type: OperationSubType
):
    """Test child_handler when operation already started.

    Verifies:
    - Operation executes when already started
    - Only SUCCEED checkpoint created (no START)
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = True
    mock_result.is_replay_children.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="started_result")

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op5", None, "test_name"), config
    )

    assert result == "started_result"

    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1

    # Verify only success checkpoint (no START since already started)
    assert mock_state.create_checkpoint.call_count == 1
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
    config: ChildConfig | None, expected_sub_type: OperationSubType
):
    """Test child_handler when callable raises exception.

    Verifies:
    - Error handling: checkpoints FAIL and raises wrapped error
    - get_checkpoint_result called once
    - create_checkpoint called with is_sync=False for START
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(side_effect=ValueError("Test error"))

    with pytest.raises(CallableRuntimeError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op6", None, "test_name"),
            config,
        )

    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1

    # Verify create_checkpoint called twice (start and fail)
    mock_state.create_checkpoint.assert_called()
    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint with is_sync=False
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "op6"
    assert start_operation.name == "test_name"
    assert start_operation.operation_type is OperationType.CONTEXT
    assert start_operation.sub_type is expected_sub_type
    assert start_operation.action is OperationAction.START
    assert start_call[1]["is_sync"] is False

    # Verify fail checkpoint
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.operation_id == "op6"
    assert fail_operation.name == "test_name"
    assert fail_operation.operation_type is OperationType.CONTEXT
    assert fail_operation.sub_type is expected_sub_type
    assert fail_operation.action is OperationAction.FAIL
    assert fail_operation.error == ErrorObject.from_exception(ValueError("Test error"))


def test_child_handler_error_wrapped():
    """Test child_handler wraps regular errors as CallableRuntimeError.

    Verifies:
    - Regular exceptions are wrapped as CallableRuntimeError
    - FAIL checkpoint is created
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    test_error = RuntimeError("Test error")
    mock_callable = Mock(side_effect=test_error)

    with pytest.raises(CallableRuntimeError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op7", None, "test_name"),
            None,
        )

    # Verify FAIL checkpoint was created
    assert mock_state.create_checkpoint.call_count == 2  # start and fail


def test_child_handler_invocation_error_reraised():
    """Test child_handler re-raises InvocationError after checkpointing FAIL.

    Verifies:
    - InvocationError: checkpoints FAIL and re-raises (for retry)
    - FAIL checkpoint is created
    - Original InvocationError is re-raised (not wrapped)
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    test_error = InvocationError("Invocation failed")
    mock_callable = Mock(side_effect=test_error)

    with pytest.raises(InvocationError, match="Invocation failed"):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op7b", None, "test_name"),
            None,
        )

    # Verify FAIL checkpoint was created
    assert mock_state.create_checkpoint.call_count == 2  # start and fail

    # Verify fail checkpoint
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.action is OperationAction.FAIL


def test_child_handler_with_config():
    """Test child_handler with config parameter."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="config_result")
    config = ChildConfig()

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op8", None, "test_name"), config
    )

    assert result == "config_result"
    mock_callable.assert_called_once()
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1


def test_child_handler_default_serialization():
    """Test child_handler properly serializes complex result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)

    result = child_handler(
        mock_callable, mock_state, OperationIdentifier("op9", None, "test_name"), None
    )

    assert result == complex_result
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1
    # Verify JSON serialization was used in checkpoint
    success_call = [
        call
        for call in mock_state.create_checkpoint.call_args_list
        if "SUCCEED" in str(call)
    ]
    assert len(success_call) == 1


def test_child_handler_custom_serdes_not_start() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
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


def test_child_handler_custom_serdes_already_succeeded() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
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
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1


# endregion child_handler


# large payload with summary generator
def test_child_handler_large_payload_with_summary_generator() -> None:
    """Test child_handler with large payload and summary generator.

    Verifies:
    - Large payload: uses ReplayChildren mode with summary_generator
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    large_result = "large" * 256 * 1024
    mock_callable = Mock(return_value=large_result)

    def my_summary(result: str) -> str:
        return "summary"

    child_config: ChildConfig = ChildConfig[str](
        summary_generator=cast(SummaryGenerator, my_summary)
    )

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op9", None, "test_name"),
        child_config,
    )

    assert large_result == actual_result
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1
    # Verify replay_children mode with summary
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.context_options.replay_children
    expected_checkpoointed_result = "summary"
    assert success_operation.payload == expected_checkpoointed_result


# large payload without summary generator
def test_child_handler_large_payload_without_summary_generator() -> None:
    """Test child_handler with large payload and no summary generator.

    Verifies:
    - Large payload without summary_generator: uses ReplayChildren mode with empty string
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    large_result = "large" * 256 * 1024
    mock_callable = Mock(return_value=large_result)
    child_config: ChildConfig = ChildConfig()

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op9", None, "test_name"),
        child_config,
    )

    assert large_result == actual_result
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1
    # Verify replay_children mode with empty string
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.context_options.replay_children
    expected_checkpoointed_result = ""
    assert success_operation.payload == expected_checkpoointed_result


# mocked children replay mode execute the function again
def test_child_handler_replay_children_mode() -> None:
    """Test child_handler in ReplayChildren mode.

    Verifies:
    - Already succeeded with replay_children: re-executes function
    - No checkpoint created (returns without checkpointing)
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = True
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = True
    mock_result.is_replay_children.return_value = True
    mock_state.get_checkpoint_result.return_value = mock_result
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    child_config: ChildConfig = ChildConfig()

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op9", None, "test_name"),
        child_config,
    )

    assert actual_result == complex_result
    # Verify function was executed (replay_children mode)
    mock_callable.assert_called_once()
    # Verify no checkpoint created (returns without checkpointing in replay mode)
    mock_state.create_checkpoint.assert_not_called()
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1


def test_small_payload_with_summary_generator():
    """Test: Small payload with summary_generator -> replay_children = False

    Verifies:
    - Small payload does NOT trigger replay_children even with summary_generator
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result

    # Small payload (< 256KB)
    small_result = "small_payload"
    mock_callable = Mock(return_value=small_result)

    def my_summary(result: str) -> str:
        return "summary_of_small_payload"

    child_config = ChildConfig[str](summary_generator=my_summary)

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op1", None, "test_name"),
        child_config,
    )

    assert actual_result == small_result
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]

    # Small payload should NOT trigger replay_children, even with summary_generator
    assert not success_operation.context_options.replay_children
    # Should checkpoint the actual result, not the summary
    assert success_operation.payload == '"small_payload"'  # JSON serialized


def test_small_payload_without_summary_generator():
    """Test: Small payload without summary_generator -> replay_children = False

    Verifies:
    - Small payload does NOT trigger replay_children
    - get_checkpoint_result called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result

    # Small payload (< 256KB)
    small_result = "small_payload"
    mock_callable = Mock(return_value=small_result)

    child_config = ChildConfig[str]()  # No summary_generator

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op2", None, "test_name"),
        child_config,
    )

    assert actual_result == small_result
    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]

    # Small payload should NOT trigger replay_children
    assert not success_operation.context_options.replay_children
    # Should checkpoint the actual result
    assert success_operation.payload == '"small_payload"'  # JSON serialized
