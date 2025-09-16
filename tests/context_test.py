"""Unit tests for context."""

import json
from unittest.mock import ANY, Mock, patch

import pytest

from aws_durable_functions_sdk_python.config import (
    CallbackConfig,
    ChildConfig,
    MapConfig,
    ParallelConfig,
    StepConfig,
    WaitForConditionConfig,
)
from aws_durable_functions_sdk_python.context import Callback, DurableContext
from aws_durable_functions_sdk_python.exceptions import (
    CallableRuntimeError,
    FatalError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_functions_sdk_python.identifier import OperationIdentifier
from aws_durable_functions_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_functions_sdk_python.state import CheckpointedResult, ExecutionState


def test_durable_context():
    """Test the context module."""
    assert DurableContext is not None


# region Callback
def test_callback_init():
    """Test Callback initialization."""
    mock_state = Mock(spec=ExecutionState)
    callback = Callback("callback123", "op456", mock_state)

    assert callback.callback_id == "callback123"
    assert callback.operation_id == "op456"
    assert callback.state is mock_state


def test_callback_result_succeeded():
    """Test Callback.result() when operation succeeded."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(
            callback_id="callback1", result=json.dumps("success_result")
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback1", "op1", mock_state)
    result = callback.result()

    assert result == "success_result"
    mock_state.get_checkpoint_result.assert_called_once_with("op1")


def test_callback_result_succeeded_none():
    """Test Callback.result() when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op2",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(callback_id="callback2", result=None),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback2", "op2", mock_state)
    result = callback.result()

    assert result is None


def test_callback_result_started_no_timeout():
    """Test Callback.result() when operation started without timeout."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op3",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="callback3"),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback3", "op3", mock_state)

    with pytest.raises(SuspendExecution, match="Calback result not received yet"):
        callback.result()


def test_callback_result_started_with_timeout():
    """Test Callback.result() when operation started with timeout."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op4",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="callback4"),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback4", "op4", mock_state)

    with pytest.raises(SuspendExecution, match="Calback result not received yet"):
        callback.result()


def test_callback_result_failed():
    """Test Callback.result() when operation failed."""
    mock_state = Mock(spec=ExecutionState)
    error = ErrorObject(
        message="Callback failed", type="CallbackError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="op5",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=CallbackDetails(callback_id="callback5", error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback5", "op5", mock_state)

    with pytest.raises(CallableRuntimeError):
        callback.result()


def test_callback_result_not_started():
    """Test Callback.result() when operation not started."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback6", "op6", mock_state)

    with pytest.raises(FatalError, match="Callback must be started"):
        callback.result()


# endregion Callback


# region create_callback
@patch("aws_durable_functions_sdk_python.context.create_callback_handler")
def test_create_callback_basic(mock_handler):
    """Test create_callback with basic parameters."""
    mock_handler.return_value = "callback123"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state)

    callback = context.create_callback()

    assert isinstance(callback, Callback)
    assert callback.callback_id == "callback123"
    assert callback.operation_id == "1"
    assert callback.state is mock_state

    mock_handler.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier("1", None, None),
        config=None,
    )


@patch("aws_durable_functions_sdk_python.context.create_callback_handler")
def test_create_callback_with_name_and_config(mock_handler):
    """Test create_callback with name and config."""
    mock_handler.return_value = "callback456"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    config = CallbackConfig()

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    callback = context.create_callback(config=config)

    assert callback.callback_id == "callback456"
    assert callback.operation_id == "6"

    mock_handler.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier("6", None, None),
        config=config,
    )


@patch("aws_durable_functions_sdk_python.context.create_callback_handler")
def test_create_callback_with_parent_id(mock_handler):
    """Test create_callback with parent_id."""
    mock_handler.return_value = "callback789"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state, parent_id="parent123")
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    callback = context.create_callback()

    assert callback.operation_id == "parent123-3"

    mock_handler.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier("parent123-3", "parent123"),
        config=None,
    )


@patch("aws_durable_functions_sdk_python.context.create_callback_handler")
def test_create_callback_increments_counter(mock_handler):
    """Test create_callback increments step counter."""
    mock_handler.return_value = "callback_test"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    callback1 = context.create_callback()
    callback2 = context.create_callback()

    assert callback1.operation_id == "11"
    assert callback2.operation_id == "12"
    assert context._step_counter.get_current() == 12  # noqa: SLF001


# endregion create_callback


# region step
@patch("aws_durable_functions_sdk_python.context.step_handler")
def test_step_basic(mock_handler):
    """Test step with basic parameters."""
    mock_handler.return_value = "step_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock(return_value="test_result")
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = DurableContext(state=mock_state)

    result = context.step(mock_callable)

    assert result == "step_result"
    mock_handler.assert_called_once_with(
        func=mock_callable,
        config=None,
        state=mock_state,
        operation_identifier=OperationIdentifier("1", None, None),
        context_logger=ANY,
    )


@patch("aws_durable_functions_sdk_python.context.step_handler")
def test_step_with_name_and_config(mock_handler):
    """Test step with name and config."""
    mock_handler.return_value = "configured_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure Mock doesn't have _original_name
    config = StepConfig()

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    result = context.step(mock_callable, config=config)

    assert result == "configured_result"
    mock_handler.assert_called_once_with(
        func=mock_callable,
        config=config,
        state=mock_state,
        operation_identifier=OperationIdentifier("6", None, None),
        context_logger=ANY,
    )


@patch("aws_durable_functions_sdk_python.context.step_handler")
def test_step_with_parent_id(mock_handler):
    """Test step with parent_id."""
    mock_handler.return_value = "parent_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = DurableContext(state=mock_state, parent_id="parent123")
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    context.step(mock_callable)

    mock_handler.assert_called_once_with(
        func=mock_callable,
        config=None,
        state=mock_state,
        operation_identifier=OperationIdentifier("parent123-3", "parent123"),
        context_logger=ANY,
    )


@patch("aws_durable_functions_sdk_python.context.step_handler")
def test_step_increments_counter(mock_handler):
    """Test step increments step counter."""
    mock_handler.return_value = "result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    context.step(mock_callable)
    context.step(mock_callable)

    assert context._step_counter.get_current() == 12  # noqa: SLF001
    assert mock_handler.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier("11", None, None)
    assert mock_handler.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier("12", None, None)


@patch("aws_durable_functions_sdk_python.context.step_handler")
def test_step_with_original_name(mock_handler):
    """Test step with callable that has _original_name attribute."""
    mock_handler.return_value = "named_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    mock_callable._original_name = "original_function"  # noqa: SLF001

    context = DurableContext(state=mock_state)

    context.step(mock_callable, name="override_name")

    mock_handler.assert_called_once_with(
        func=mock_callable,
        config=None,
        state=mock_state,
        operation_identifier=OperationIdentifier("1", None, "override_name"),
        context_logger=ANY,
    )


# endregion step


# region wait
@patch("aws_durable_functions_sdk_python.context.wait_handler")
def test_wait_basic(mock_handler):
    """Test wait with basic parameters."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state)

    context.wait(30)

    mock_handler.assert_called_once_with(
        seconds=30,
        state=mock_state,
        operation_identifier=OperationIdentifier("1", None, None),
    )


@patch("aws_durable_functions_sdk_python.context.wait_handler")
def test_wait_with_name(mock_handler):
    """Test wait with name parameter."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    context.wait(60, name="test_wait")

    mock_handler.assert_called_once_with(
        seconds=60,
        state=mock_state,
        operation_identifier=OperationIdentifier("6", None, "test_wait"),
    )


@patch("aws_durable_functions_sdk_python.context.wait_handler")
def test_wait_with_parent_id(mock_handler):
    """Test wait with parent_id."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state, parent_id="parent123")
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    context.wait(45)

    mock_handler.assert_called_once_with(
        seconds=45,
        state=mock_state,
        operation_identifier=OperationIdentifier("parent123-3", "parent123"),
    )


@patch("aws_durable_functions_sdk_python.context.wait_handler")
def test_wait_increments_counter(mock_handler):
    """Test wait increments step counter."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    context.wait(15)
    context.wait(25)

    assert context._step_counter.get_current() == 12  # noqa: SLF001
    assert mock_handler.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier("11", None, None)
    assert mock_handler.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier("12", None, None)


@patch("aws_durable_functions_sdk_python.context.wait_handler")
def test_wait_returns_none(mock_handler):
    """Test wait returns None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = DurableContext(state=mock_state)

    result = context.wait(10)

    assert result is None


# endregion wait


# region run_in_child_context
@patch("aws_durable_functions_sdk_python.context.child_handler")
def test_run_in_child_context_basic(mock_handler):
    """Test run_in_child_context with basic parameters."""
    mock_handler.return_value = "child_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock(return_value="test_result")
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = DurableContext(state=mock_state)

    result = context.run_in_child_context(mock_callable)

    assert result == "child_result"
    assert mock_handler.call_count == 1

    # Verify the callable was wrapped with child context
    call_args = mock_handler.call_args
    assert call_args[1]["state"] is mock_state
    assert call_args[1]["operation_identifier"] == OperationIdentifier("1", None, None)
    assert call_args[1]["config"] is None


@patch("aws_durable_functions_sdk_python.context.child_handler")
def test_run_in_child_context_with_name_and_config(mock_handler):
    """Test run_in_child_context with name and config."""
    mock_handler.return_value = "configured_child_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    mock_callable._original_name = "original_function"  # noqa: SLF001

    config = ChildConfig()

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(3)]  # Set counter to 3 # noqa: SLF001

    result = context.run_in_child_context(mock_callable, config=config)

    assert result == "configured_child_result"
    call_args = mock_handler.call_args
    assert call_args[1]["operation_identifier"] == OperationIdentifier(
        "4", None, "original_function"
    )
    assert call_args[1]["config"] is config


@patch("aws_durable_functions_sdk_python.context.child_handler")
def test_run_in_child_context_with_parent_id(mock_handler):
    """Test run_in_child_context with parent_id."""
    mock_handler.return_value = "parent_child_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure Mock doesn't have _original_name

    context = DurableContext(state=mock_state, parent_id="parent456")
    [context._create_step_id() for _ in range(1)]  # Set counter to 1 # noqa: SLF001

    context.run_in_child_context(mock_callable)

    call_args = mock_handler.call_args
    assert call_args[1]["operation_identifier"] == OperationIdentifier(
        "parent456-2", "parent456", None
    )


@patch("aws_durable_functions_sdk_python.context.child_handler")
def test_run_in_child_context_creates_child_context(mock_handler):
    """Test run_in_child_context creates proper child context."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def capture_child_context(child_context):
        # Verify child context properties
        assert isinstance(child_context, DurableContext)
        assert child_context.state is mock_state
        assert child_context._parent_id == "1"  # noqa: SLF001
        return "child_executed"

    mock_callable = Mock(side_effect=capture_child_context)
    mock_handler.side_effect = lambda func, **kwargs: func()

    context = DurableContext(state=mock_state)

    result = context.run_in_child_context(mock_callable)

    assert result == "child_executed"
    mock_callable.assert_called_once()


@patch("aws_durable_functions_sdk_python.context.child_handler")
def test_run_in_child_context_increments_counter(mock_handler):
    """Test run_in_child_context increments step counter."""
    mock_handler.return_value = "result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = DurableContext(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    context.run_in_child_context(mock_callable)
    context.run_in_child_context(mock_callable)

    assert context._step_counter.get_current() == 7  # noqa: SLF001
    assert mock_handler.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier("6", None, None)
    assert mock_handler.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier("7", None, None)


@patch("aws_durable_functions_sdk_python.context.child_handler")
def test_run_in_child_context_resolves_name_from_callable(mock_handler):
    """Test run_in_child_context resolves name from callable._original_name."""
    mock_handler.return_value = "named_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    mock_callable._original_name = "original_function_name"  # noqa: SLF001

    context = DurableContext(state=mock_state)

    context.run_in_child_context(mock_callable)

    call_args = mock_handler.call_args
    assert call_args[1]["operation_identifier"].name == "original_function_name"


# endregion run_in_child_context


# region wait_for_callback
@patch("aws_durable_functions_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_basic(mock_handler):
    """Test wait_for_callback with basic parameters."""
    mock_handler.return_value = "callback_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = Mock()
    del (
        mock_submitter._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "callback_result"
        context = DurableContext(state=mock_state)

        result = context.wait_for_callback(mock_submitter)

        assert result == "callback_result"
        mock_run_in_child.assert_called_once()

        # Verify the child context callable
        call_args = mock_run_in_child.call_args
        assert call_args[0][1] is None  # name should be None


@patch("aws_durable_functions_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_with_name_and_config(mock_handler):
    """Test wait_for_callback with name and config."""
    mock_handler.return_value = "configured_callback_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = Mock()
    mock_submitter._original_name = "submit_function"  # noqa: SLF001
    config = CallbackConfig()

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "configured_callback_result"
        context = DurableContext(state=mock_state)

        result = context.wait_for_callback(mock_submitter, config=config)

        assert result == "configured_callback_result"
        call_args = mock_run_in_child.call_args
        assert (
            call_args[0][1] == "submit_function"
        )  # name should be from _original_name


@patch("aws_durable_functions_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_resolves_name_from_submitter(mock_handler):
    """Test wait_for_callback resolves name from submitter._original_name."""
    mock_handler.return_value = "named_callback_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = Mock()
    mock_submitter._original_name = "submit_task"  # noqa: SLF001

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "named_callback_result"
        context = DurableContext(state=mock_state)

        context.wait_for_callback(mock_submitter)

        call_args = mock_run_in_child.call_args
        assert call_args[0][1] == "submit_task"


@patch("aws_durable_functions_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_passes_child_context(mock_handler):
    """Test wait_for_callback passes child context to handler."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = Mock()

    def capture_handler_call(context, submitter, name, config):
        assert isinstance(context, DurableContext)
        assert submitter is mock_submitter
        return "handler_result"

    mock_handler.side_effect = capture_handler_call

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:

        def run_child_context(callable_func, name):
            # Execute the child context callable
            child_context = DurableContext(state=mock_state, parent_id="test")
            return callable_func(child_context)

        mock_run_in_child.side_effect = run_child_context
        context = DurableContext(state=mock_state)

        result = context.wait_for_callback(mock_submitter)

        assert result == "handler_result"
        mock_handler.assert_called_once()


# endregion wait_for_callback


# region map
@patch("aws_durable_functions_sdk_python.context.map_handler")
def test_map_basic(mock_handler):
    """Test map with basic parameters."""
    mock_handler.return_value = "map_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def test_function(context, item, index, items):
        return f"processed_{item}"

    inputs = [1, 2, 3]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "map_result"
        context = DurableContext(state=mock_state)

        result = context.map(inputs, test_function)

        assert result == "map_result"
        mock_run_in_child.assert_called_once()

        # Verify the child context callable
        call_args = mock_run_in_child.call_args
        assert call_args[1]["name"] is None  # name should be None
        assert call_args[1]["config"].sub_type.value == "Map"


@patch("aws_durable_functions_sdk_python.context.map_handler")
def test_map_with_name_and_config(mock_handler):
    """Test map with name and config."""
    from aws_durable_functions_sdk_python.config import MapConfig

    mock_handler.return_value = "configured_map_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def test_function(context, item, index, items):
        return f"processed_{item}"

    test_function._original_name = "test_map_function"  # noqa: SLF001

    inputs = ["a", "b", "c"]
    config = MapConfig()

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "configured_map_result"
        context = DurableContext(state=mock_state)

        result = context.map(inputs, test_function, name="custom_map", config=config)

        assert result == "configured_map_result"
        call_args = mock_run_in_child.call_args
        assert call_args[1]["name"] == "custom_map"  # name should be custom_map


@patch("aws_durable_functions_sdk_python.context.map_handler")
def test_map_calls_handler_correctly(mock_handler):
    """Test map calls map_handler with correct parameters."""
    mock_handler.return_value = "handler_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def test_function(context, item, index, items):
        return item.upper()

    inputs = ["hello", "world"]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "handler_result"
        context = DurableContext(state=mock_state)

        result = context.map(inputs, test_function)

        assert result == "handler_result"
        mock_run_in_child.assert_called_once()


@patch("aws_durable_functions_sdk_python.context.map_handler")
def test_map_with_empty_inputs(mock_handler):
    """Test map with empty inputs."""
    mock_handler.return_value = "empty_map_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def test_function(context, item, index, items):
        return item

    inputs = []

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "empty_map_result"
        context = DurableContext(state=mock_state)

        result = context.map(inputs, test_function)

        assert result == "empty_map_result"


@patch("aws_durable_functions_sdk_python.context.map_handler")
def test_map_with_different_input_types(mock_handler):
    """Test map with different input types."""
    mock_handler.return_value = "mixed_map_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def test_function(context, item, index, items):
        return str(item)

    inputs = [1, "hello", {"key": "value"}, [1, 2, 3]]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "mixed_map_result"
        context = DurableContext(state=mock_state)

        result = context.map(inputs, test_function)

        assert result == "mixed_map_result"


# endregion map


# region parallel
@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_basic(mock_handler):
    """Test parallel with basic parameters."""
    mock_handler.return_value = "parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def task1(context):
        return "result1"

    def task2(context):
        return "result2"

    callables = [task1, task2]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "parallel_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables)

        assert result == "parallel_result"
        mock_run_in_child.assert_called_once()

        # Verify the child context callable
        call_args = mock_run_in_child.call_args
        assert call_args[1]["name"] is None  # name should be None
        assert call_args[1]["config"].sub_type.value == "Parallel"


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_with_name_and_config(mock_handler):
    """Test parallel with name and config."""
    from aws_durable_functions_sdk_python.config import ParallelConfig

    mock_handler.return_value = "configured_parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def task1(context):
        return "result1"

    def task2(context):
        return "result2"

    callables = [task1, task2]
    config = ParallelConfig()

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "configured_parallel_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables, name="custom_parallel", config=config)

        assert result == "configured_parallel_result"
        call_args = mock_run_in_child.call_args
        assert (
            call_args[1]["name"] == "custom_parallel"
        )  # name should be custom_parallel


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_resolves_name_from_callable(mock_handler):
    """Test parallel resolves name from callable._original_name."""
    mock_handler.return_value = "named_parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def task1(context):
        return "result1"

    def task2(context):
        return "result2"

    # Mock callable with _original_name
    mock_callable = Mock()
    mock_callable._original_name = "parallel_tasks"  # noqa: SLF001

    callables = [task1, task2]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "named_parallel_result"
        context = DurableContext(state=mock_state)

        # Use _resolve_step_name to test name resolution
        resolved_name = context._resolve_step_name(None, mock_callable)  # noqa: SLF001
        assert resolved_name == "parallel_tasks"

        context.parallel(callables)

        call_args = mock_run_in_child.call_args
        assert (
            call_args[1]["name"] is None
        )  # name should be None since callables don't have _original_name


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_calls_handler_correctly(mock_handler):
    """Test parallel calls parallel_handler with correct parameters."""
    mock_handler.return_value = "handler_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def task1(context):
        return "result1"

    def task2(context):
        return "result2"

    callables = [task1, task2]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "handler_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables)

        assert result == "handler_result"
        mock_run_in_child.assert_called_once()


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_with_empty_callables(mock_handler):
    """Test parallel with empty callables."""
    mock_handler.return_value = "empty_parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    callables = []

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "empty_parallel_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables)

        assert result == "empty_parallel_result"


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_with_single_callable(mock_handler):
    """Test parallel with single callable."""
    mock_handler.return_value = "single_parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def single_task(context):
        return "single_result"

    callables = [single_task]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "single_parallel_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables)

        assert result == "single_parallel_result"


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_with_many_callables(mock_handler):
    """Test parallel with many callables."""
    mock_handler.return_value = "many_parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def create_task(i):
        def task(context):
            return f"result_{i}"

        return task

    callables = [create_task(i) for i in range(10)]

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "many_parallel_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables)

        assert result == "many_parallel_result"


# endregion parallel


def test_callback_result_timed_out():
    """Test Callback.result() when operation timed out."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    error = ErrorObject(
        message="Callback timed out", type="TimeoutError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="op_timeout",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=CallbackDetails(callback_id="callback_timeout", error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback_timeout", "op_timeout", mock_state)

    with pytest.raises(CallableRuntimeError):
        callback.result()


# region map
@patch("aws_durable_functions_sdk_python.context.map_handler")
def test_map_calls_handler(mock_handler):
    """Test map calls map_handler through run_in_child_context."""
    mock_handler.return_value = "map_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def test_function(context, item, index, items):
        return f"processed_{item}"

    inputs = ["a", "b", "c"]
    config = MapConfig()

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "map_result"
        context = DurableContext(state=mock_state)

        result = context.map(inputs, test_function, config=config)

        assert result == "map_result"
        mock_run_in_child.assert_called_once()


@patch("aws_durable_functions_sdk_python.context.parallel_handler")
def test_parallel_calls_handler(mock_handler):
    """Test parallel calls parallel_handler through run_in_child_context."""
    mock_handler.return_value = "parallel_result"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    def task1(context):
        return "result1"

    def task2(context):
        return "result2"

    callables = [task1, task2]
    config = ParallelConfig()

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "parallel_result"
        context = DurableContext(state=mock_state)

        result = context.parallel(callables, config=config)

        assert result == "parallel_result"
        mock_run_in_child.assert_called_once()


# region wait_for_condition
def test_wait_for_condition_validation_errors():
    """Test wait_for_condition raises ValidationError for invalid inputs."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = DurableContext(state=mock_state)

    def dummy_wait_strategy(state, attempt):
        return None

    config = WaitForConditionConfig(
        wait_strategy=dummy_wait_strategy, initial_state="test"
    )

    # Test None check function
    with pytest.raises(
        ValidationError, match="`check` is required for wait_for_condition"
    ):
        context.wait_for_condition(None, config)

    # Test None config
    def dummy_check(state, check_context):
        return state

    with pytest.raises(
        ValidationError, match="`config` is required for wait_for_condition"
    ):
        context.wait_for_condition(dummy_check, None)


def test_context_map_handler_call():
    """Test that map method calls through to map_handler (line 283)."""
    execution_calls = []

    def test_function(context, item, index, items):
        execution_calls.append(f"item_{index}")
        return f"result_{index}"

    # Create mock state and context
    state = Mock()
    state.durable_execution_arn = "test_arn"

    context = DurableContext(state=state)

    # Mock the handlers to track calls
    with patch(
        "aws_durable_functions_sdk_python.context.map_handler"
    ) as mock_map_handler:
        mock_map_handler.return_value = Mock()

        with patch.object(context, "run_in_child_context") as mock_run_in_child:
            # Set up the mock to call the nested function
            def mock_run_side_effect(func, name=None, config=None):
                child_context = Mock()
                child_context.run_in_child_context = Mock()
                return func(child_context)

            mock_run_in_child.side_effect = mock_run_side_effect

            # Call map method
            context.map([1, 2], test_function)

            # Verify map_handler was called (line 283)
            mock_map_handler.assert_called_once()


def test_context_parallel_handler_call():
    """Test that parallel method calls through to parallel_handler (line 306)."""
    execution_calls = []

    def test_callable_1(context):
        execution_calls.append("callable_1")
        return "result_1"

    def test_callable_2(context):
        execution_calls.append("callable_2")
        return "result_2"

    # Create mock state and context
    state = Mock()
    state.durable_execution_arn = "test_arn"

    context = DurableContext(state=state)

    # Mock the handlers to track calls
    with patch(
        "aws_durable_functions_sdk_python.context.parallel_handler"
    ) as mock_parallel_handler:
        mock_parallel_handler.return_value = Mock()

        with patch.object(context, "run_in_child_context") as mock_run_in_child:
            # Set up the mock to call the nested function
            def mock_run_side_effect(func, name=None, config=None):
                child_context = Mock()
                child_context.run_in_child_context = Mock()
                return func(child_context)

            mock_run_in_child.side_effect = mock_run_side_effect

            # Call parallel method
            context.parallel([test_callable_1, test_callable_2])

            # Verify parallel_handler was called (line 306)
            mock_parallel_handler.assert_called_once()


def test_context_wait_for_condition_handler_call():
    """Test that wait_for_condition method calls through to wait_for_condition_handler (line 425)."""
    execution_calls = []

    def test_check(state, check_context):
        execution_calls.append("check_called")
        return state

    def test_wait_strategy(state, attempt):
        from aws_durable_functions_sdk_python.config import WaitForConditionDecision

        return WaitForConditionDecision.STOP

    # Create mock state and context
    state = Mock()
    state.durable_execution_arn = "test_arn"

    context = DurableContext(state=state)

    # Create config
    config = WaitForConditionConfig(
        wait_strategy=test_wait_strategy, initial_state="test"
    )

    # Mock the handler to track calls
    with patch(
        "aws_durable_functions_sdk_python.context.wait_for_condition_handler"
    ) as mock_handler:
        mock_handler.return_value = "final_state"

        # Call wait_for_condition method
        result = context.wait_for_condition(test_check, config)

        # Verify wait_for_condition_handler was called (line 425)
        mock_handler.assert_called_once()
        assert result == "final_state"
