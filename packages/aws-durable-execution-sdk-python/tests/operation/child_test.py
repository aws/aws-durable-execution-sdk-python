"""Unit tests for child handler."""

from __future__ import annotations

import json
from typing import cast
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import (
    BotoClientError,
    ChildContextError,
    DurableApiErrorCategory,
    ExecutionError,
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
    mock_state.wrap_user_function.return_value = mock_callable

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config,
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
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op2", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        None,
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
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op3", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        None,
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
    mock_result.raise_operation_error.side_effect = ChildContextError(
        "Previous failure"
    )
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock()

    with pytest.raises(ChildContextError, match="Previous failure"):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op4", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
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
    mock_state.wrap_user_function.return_value = mock_callable

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op5", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config,
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
    mock_state.wrap_user_function.return_value = mock_callable

    with pytest.raises(ChildContextError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op6", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
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
    # The checkpoint records the escaping error as-is (its own type); the
    # ChildContextError wrapper is raised and recorded one level up.
    assert fail_operation.error == ErrorObject(
        message="Test error", type="ValueError", data=None, stack_trace=None
    )


def test_child_handler_error_wrapped():
    """Test child_handler wraps regular errors as ChildContextError.

    Verifies:
    - Regular exceptions are wrapped as ChildContextError
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
    mock_state.wrap_user_function.return_value = mock_callable

    with pytest.raises(ChildContextError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op7", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            None,
        )

    # Verify FAIL checkpoint was created
    assert mock_state.create_checkpoint.call_count == 2  # start and fail


def test_child_handler_invocation_error_reraised():
    """InvocationError propagates unchanged and writes no FAIL checkpoint."""
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
    mock_state.wrap_user_function.return_value = mock_callable

    # Raises the original InvocationError, NOT ChildContextError.
    with pytest.raises(InvocationError, match="Invocation failed"):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op7b", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            None,
        )

    # Only the async START checkpoint is written - no FAIL.
    assert mock_state.create_checkpoint.call_count == 1  # start only
    actions = [
        call.kwargs["operation_update"].action
        for call in mock_state.create_checkpoint.call_args_list
    ]
    assert OperationAction.FAIL not in actions


def test_child_handler_execution_error_wrapped():
    """ExecutionError is wrapped as ChildContextError (replay-safe), not raised raw."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    test_error = ExecutionError("execution failed")
    mock_callable = Mock(side_effect=test_error)
    mock_state.wrap_user_function.return_value = mock_callable

    with pytest.raises(ChildContextError) as exc_info:
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op7c", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            None,
        )

    # Not the raw ExecutionError; the original type survives on error_type.
    assert not isinstance(exc_info.value, ExecutionError)
    assert exc_info.value.error_type == "ExecutionError"

    # Verify FAIL checkpoint was created
    assert mock_state.create_checkpoint.call_count == 2  # start and fail
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.action is OperationAction.FAIL


def test_child_handler_non_retryable_invocation_error_wrapped():
    """Non-retryable InvocationError is terminal: writes FAIL and wraps as ChildContextError."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    test_error = BotoClientError(
        "boto failure", error_category=DurableApiErrorCategory.EXECUTION
    )
    assert test_error.is_retryable() is False
    mock_callable = Mock(side_effect=test_error)
    mock_state.wrap_user_function.return_value = mock_callable

    with pytest.raises(ChildContextError) as exc_info:
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op7d", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            None,
        )

    # Not re-raised raw; the escaping type survives on error_type.
    assert not isinstance(exc_info.value, InvocationError)
    assert exc_info.value.error_type == "BotoClientError"

    # FAIL checkpoint IS written so the op is not left STARTED in a FAILED execution.
    assert mock_state.create_checkpoint.call_count == 2  # start and fail
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
    mock_state.wrap_user_function.return_value = mock_callable
    config = ChildConfig()

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op8", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config,
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
    mock_state.wrap_user_function.return_value = mock_callable

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        None,
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
    mock_state.wrap_user_function.return_value = mock_callable
    child_config: ChildConfig = ChildConfig(serdes=CustomDictSerDes())

    child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
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
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
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
    mock_state.wrap_user_function.return_value = mock_callable

    def my_summary(result: str) -> str:
        return "summary"

    child_config: ChildConfig = ChildConfig[str](
        summary_generator=cast(SummaryGenerator, my_summary)
    )

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
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
    mock_state.wrap_user_function.return_value = mock_callable
    child_config: ChildConfig = ChildConfig()

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
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
    mock_state.wrap_user_function.return_value = mock_callable
    child_config: ChildConfig = ChildConfig()

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
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
    mock_state.wrap_user_function.return_value = mock_callable

    def my_summary(result: str) -> str:
        return "summary_of_small_payload"

    child_config = ChildConfig[str](summary_generator=my_summary)

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
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
    """Test: small payload without summary_generator -> replay_children=False.

    Restored from pre-PR #351. For small payloads we always checkpoint
    the actual result (JSON-serialized); ReplayChildren mode exists only
    to handle payloads that exceed the size limit, so a small payload
    without a summary generator must still round-trip through a normal
    SUCCEED checkpoint.
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

    # Small payload (< 256KB); no summary_generator provided
    small_result = "small_payload"
    mock_callable = Mock(return_value=small_result)
    mock_state.wrap_user_function.return_value = mock_callable

    child_config: ChildConfig[str] = ChildConfig[str]()

    actual_result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        child_config,
    )

    assert actual_result == small_result
    assert mock_state.get_checkpoint_result.call_count == 1

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]

    # Small payload MUST NOT trigger replay_children.
    assert not success_operation.context_options.replay_children
    # Payload MUST be the JSON-serialized result, not a summary.
    assert success_operation.payload == '"small_payload"'


def test_child_handler_is_virtual_no_start():
    """Skip the START checkpoint when is_virtual=True.

    A virtual branch is a logical scope for step-id prefixing but does
    not appear in the execution history, so no START entry is emitted.
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
    mock_callable = Mock(return_value="no_checkpoint_result")
    mock_state.wrap_user_function.return_value = mock_callable

    config = ChildConfig(is_virtual=True)

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config,
    )

    assert result == "no_checkpoint_result"

    # Verify get_checkpoint_result called once
    assert mock_state.get_checkpoint_result.call_count == 1

    # Verify no checkpoints created (virtual context writes none)
    assert mock_state.create_checkpoint.call_count == 0

    mock_callable.assert_called_once()


def test_child_handler_is_virtual_no_succeed():
    """Skip the SUCCEED checkpoint when is_virtual=True.

    A virtual branch is not represented in the execution history; its
    successful completion is observable only via the values returned
    to the calling concurrency executor.
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
    mock_callable = Mock(return_value="no_checkpoint_result")
    mock_state.wrap_user_function.return_value = mock_callable

    config = ChildConfig(is_virtual=True)

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op2", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config,
    )

    assert result == "no_checkpoint_result"

    # Verify no checkpoints created
    mock_state.create_checkpoint.assert_not_called()

    mock_callable.assert_called_once()


def test_child_handler_not_is_virtual_finish_mode():
    """Create START + SUCCEED checkpoints when is_virtual=False."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(return_value="checkpoint_result")
    mock_state.wrap_user_function.return_value = mock_callable

    config = ChildConfig(is_virtual=False)

    result = child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op3", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config,
    )

    assert result == "checkpoint_result"

    # Verify both START and SUCCEED checkpoints created
    assert mock_state.create_checkpoint.call_count == 2

    # Verify START checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.action.value == "START"
    assert start_call[1]["is_sync"] is False

    # Verify SUCCEED checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.action.value == "SUCCEED"

    mock_callable.assert_called_once()


def test_child_handler_is_virtual_with_exception():
    """Skip the FAIL checkpoint when is_virtual=True and the user function raises.

    A virtual branch emits no lifecycle entries in the execution
    history, so a failure inside the branch does not get its own FAIL
    checkpoint. The exception still propagates (wrapped as
    ChildContextError for non-InvocationError exceptions) so the
    concurrency executor records the failure in the BatchResult and
    its completion-tolerance logic still applies.
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
    mock_callable = Mock(side_effect=ValueError("Test error"))
    mock_state.wrap_user_function.return_value = mock_callable

    config = ChildConfig(is_virtual=True)

    with pytest.raises(ChildContextError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op4", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            config,
        )

    # Verify NO FAIL checkpoint created (virtual contexts suppress all lifecycle checkpoints).
    assert mock_state.create_checkpoint.call_count == 0

    mock_callable.assert_called_once()


def test_child_handler_not_is_virtual_with_exception():
    """Create a FAIL checkpoint when is_virtual=False and the user function raises."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.get_checkpoint_result.return_value = mock_result
    mock_callable = Mock(side_effect=ValueError("Test error"))
    mock_state.wrap_user_function.return_value = mock_callable

    config = ChildConfig(is_virtual=False)

    with pytest.raises(ChildContextError):
        child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op5", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            config,
        )

    # Verify START + FAIL checkpoints created (non-virtual path).
    assert mock_state.create_checkpoint.call_count == 2
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.action.value == "START"
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.action.value == "FAIL"

    mock_callable.assert_called_once()


def test_child_handler_is_virtual_comparison():
    """Compare checkpoint counts between is_virtual=True and is_virtual=False for success.

    - is_virtual=False: 2 checkpoints (START + SUCCEED)
    - is_virtual=True:  0 checkpoints
    """

    # Setup common mocks
    def setup_mocks():
        mock_state = Mock(spec=ExecutionState)
        mock_state.durable_execution_arn = "test_arn"
        mock_result = Mock()
        mock_result.is_succeeded.return_value = False
        mock_result.is_failed.return_value = False
        mock_result.is_started.return_value = False
        mock_result.is_replay_children.return_value = False
        mock_result.is_existent.return_value = False
        mock_state.get_checkpoint_result.return_value = mock_result
        mock_callable = Mock(return_value="test_result")
        mock_state.wrap_user_function.return_value = mock_callable
        return mock_state, mock_callable

    # is_virtual=False: 2 checkpoints
    mock_state1, mock_callable1 = setup_mocks()
    config1 = ChildConfig(is_virtual=False)

    result1 = child_handler(
        mock_callable1,
        mock_state1,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config1,
    )

    assert result1 == "test_result"
    assert mock_state1.create_checkpoint.call_count == 2  # START + SUCCEED

    # is_virtual=True: 0 checkpoints
    mock_state2, mock_callable2 = setup_mocks()
    config2 = ChildConfig(is_virtual=True)

    result2 = child_handler(
        mock_callable2,
        mock_state2,
        OperationIdentifier(
            "op2", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        config2,
    )

    assert result2 == "test_result"
    assert mock_state2.create_checkpoint.call_count == 0  # No checkpoints
