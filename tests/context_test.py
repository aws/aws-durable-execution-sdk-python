"""Unit tests for context."""

import hashlib
import json
import random
from itertools import islice
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    ChildConfig,
    Duration,
    InvokeConfig,
    MapConfig,
    ParallelBranch,
    ParallelConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import (
    Callback,
    DurableContext,
    ExecutionContext,
    durable_parallel_branch,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallbackError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from tests.serdes_test import CustomDictSerDes
from tests.test_helpers import operation_id_sequence


def create_test_context(
    state: ExecutionState | None = None, parent_id: str | None = None
) -> DurableContext:
    """Helper to create DurableContext for tests with required execution_context."""
    if state is None:
        state = Mock(spec=ExecutionState)
        state.durable_execution_arn = (
            "arn:aws:durable:us-east-1:123456789012:execution/test"
        )

    # Ensure mock state has _operations_lock and operations for child context creation
    if not hasattr(state, "_operations_lock") or isinstance(
        state._operations_lock, Mock
    ):
        from threading import Lock

        state._operations_lock = Lock()
    if not hasattr(state, "operations") or isinstance(state.operations, Mock):
        state.operations = {}

    execution_context = ExecutionContext(
        durable_execution_arn=state.durable_execution_arn
    )
    return DurableContext(
        state=state, execution_context=execution_context, parent_id=parent_id
    )


def test_durable_context():
    """Test the context module."""
    assert DurableContext is not None


# region Callback
def test_callback_init():
    """Test Callback initialization."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    callback = Callback("callback123", "op456", mock_state)

    assert callback.callback_id == "callback123"
    assert callback.operation_id == "op456"
    assert callback.state is mock_state


def test_callback_result_succeeded():
    """Test Callback.result() when operation succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
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

    assert result == '"success_result"'
    mock_state.get_checkpoint_result.assert_called_once_with("op1")


def test_callback_result_succeeded_with_plain_str():
    """Test Callback.result() when operation succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(
            callback_id="callback1", result="success_result"
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

    with pytest.raises(SuspendExecution, match="Callback result not received yet"):
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

    with pytest.raises(SuspendExecution, match="Callback result not received yet"):
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

    with pytest.raises(CallbackError):
        callback.result()


def test_callback_result_not_started():
    """Test Callback.result() when operation not started."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback6", "op6", mock_state)

    with pytest.raises(CallbackError, match="Callback operation must exist"):
        callback.result()


def test_callback_custom_serdes_result_succeeded():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(
            callback_id="callback1",
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    callback = Callback("callback1", "op1", mock_state, CustomDictSerDes())
    result = callback.result()

    expected_complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    assert result == expected_complex_result


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

    with pytest.raises(CallbackError):
        callback.result()


# endregion Callback


# region create_callback
@patch("aws_durable_execution_sdk_python.context.CallbackOperationExecutor")
def test_create_callback_basic(mock_executor_class):
    """Test create_callback with basic parameters."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = "callback123"
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    callback = context.create_callback()

    assert isinstance(callback, Callback)
    assert callback.callback_id == "callback123"
    assert callback.operation_id == expected_operation_id
    assert callback.state is mock_state

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_operation_id, None, None),
        config=CallbackConfig(),
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.CallbackOperationExecutor")
def test_create_callback_with_name_and_config(mock_executor_class):
    """Test create_callback with name and config."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = "callback456"
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    config = CallbackConfig()

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    [next(operation_ids) for _ in range(5)]  # Skip 5 IDs
    expected_operation_id = next(operation_ids)  # Get the 6th ID
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    callback = context.create_callback(config=config)

    assert callback.callback_id == "callback456"
    assert callback.operation_id == expected_operation_id

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_operation_id, None, None),
        config=config,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.CallbackOperationExecutor")
def test_create_callback_with_parent_id(mock_executor_class):
    """Test create_callback with parent_id."""

    mock_executor = MagicMock()

    mock_executor.process.return_value = "callback789"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state, parent_id="parent123")
    operation_ids = operation_id_sequence("parent123")
    [next(operation_ids) for _ in range(2)]  # Skip 2 IDs
    expected_operation_id = next(operation_ids)  # Get the 3rd ID
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    callback = context.create_callback()

    assert callback.operation_id == expected_operation_id

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_operation_id, "parent123"),
        config=CallbackConfig(),
    )


@patch("aws_durable_execution_sdk_python.context.CallbackOperationExecutor")
def test_create_callback_increments_counter(mock_executor_class):
    """Test create_callback increments step counter."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "callback_test"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    callback1 = context.create_callback()
    callback2 = context.create_callback()

    # Use operation_id_sequence to get expected IDs
    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]  # Skip first 10
    expected_id1 = next(seq)  # 11th
    expected_id2 = next(seq)  # 12th

    assert callback1.operation_id == expected_id1
    assert callback2.operation_id == expected_id2
    assert context._step_counter.get_current() == 12  # noqa: SLF001


# endregion create_callback


# region step
@patch("aws_durable_execution_sdk_python.context.StepOperationExecutor")
def test_step_basic(mock_executor_class):
    """Test step with basic parameters."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "step_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock(return_value="test_result")
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    result = context.step(mock_callable)

    assert result == "step_result"
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_operation_id, None, None),
        config=ANY,  # StepConfig() is created in context.step()
        func=mock_callable,
        context_logger=ANY,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.StepOperationExecutor")
def test_step_with_name_and_config(mock_executor_class):
    """Test step with name and config."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "configured_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure Mock doesn't have _original_name
    config = StepConfig()

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    result = context.step(mock_callable, config=config)

    # Get expected ID
    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]  # Skip first 5
    expected_id = next(seq)  # 6th

    assert result == "configured_result"
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, None, None),
        config=config,
        func=mock_callable,
        context_logger=ANY,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.StepOperationExecutor")
def test_step_with_parent_id(mock_executor_class):
    """Test step with parent_id."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "parent_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = create_test_context(state=mock_state, parent_id="parent123")
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    context.step(mock_callable)

    # Get expected ID with parent
    seq = operation_id_sequence("parent123")
    [next(seq) for _ in range(2)]  # Skip first 2
    expected_id = next(seq)  # 3rd

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, "parent123"),
        config=ANY,
        func=mock_callable,
        context_logger=ANY,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.StepOperationExecutor")
def test_step_increments_counter(mock_executor_class):
    """Test step increments step counter."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    context.step(mock_callable)
    context.step(mock_callable)

    # Get expected IDs
    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]  # Skip first 10
    expected_id1 = next(seq)  # 11th
    expected_id2 = next(seq)  # 12th

    assert context._step_counter.get_current() == 12  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, None, None)
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, None, None)


@patch("aws_durable_execution_sdk_python.context.StepOperationExecutor")
def test_step_with_original_name(mock_executor_class):
    """Test step with callable that has _original_name attribute."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "named_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    mock_callable._original_name = "original_function"  # noqa: SLF001

    context = create_test_context(state=mock_state)

    context.step(mock_callable, name="override_name")

    # Get expected ID
    seq = operation_id_sequence()
    expected_id = next(seq)  # 1st

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, None, "override_name"),
        config=ANY,
        func=mock_callable,
        context_logger=ANY,
    )
    mock_executor.process.assert_called_once()


# endregion step


# region invoke
@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_basic(mock_executor_class):
    """Test invoke with basic parameters."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "invoke_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    result = context.invoke("test_function", "test_payload")

    assert result == "invoke_result"

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_operation_id, None, None),
        function_name="test_function",
        payload="test_payload",
        config=ANY,  # InvokeConfig() is created in context.invoke()
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_with_name_and_config(mock_executor_class):
    """Test invoke with name and config."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "configured_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    config = InvokeConfig[str, str](timeout=Duration.from_seconds(30))

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    result = context.invoke(
        "test_function", {"key": "value"}, name="named_invoke", config=config
    )

    # Get expected ID
    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]  # Skip first 5
    expected_id = next(seq)  # 6th

    assert result == "configured_result"
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, None, "named_invoke"),
        function_name="test_function",
        payload={"key": "value"},
        config=config,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_with_parent_id(mock_executor_class):
    """Test invoke with parent_id."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "parent_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state, parent_id="parent123")
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    context.invoke("test_function", None)

    seq = operation_id_sequence("parent123")
    [next(seq) for _ in range(2)]
    expected_id = next(seq)

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, "parent123", None),
        function_name="test_function",
        payload=None,
        config=ANY,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_increments_counter(mock_executor_class):
    """Test invoke increments step counter."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    context.invoke("function1", "payload1")
    context.invoke("function2", "payload2")

    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]
    expected_id1 = next(seq)
    expected_id2 = next(seq)

    assert context._step_counter.get_current() == 12  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, None, None)
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, None, None)


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_with_none_payload(mock_executor_class):
    """Test invoke with None payload."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = None

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = context.invoke("test_function", None)

    seq = operation_id_sequence()
    expected_id = next(seq)

    assert result is None

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, None, None),
        function_name="test_function",
        payload=None,
        config=ANY,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_with_custom_serdes(mock_executor_class):
    """Test invoke with custom serialization config."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = {"transformed": "data"}

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    payload_serdes = CustomDictSerDes()
    result_serdes = CustomDictSerDes()
    config = InvokeConfig[dict, dict](
        serdes_payload=payload_serdes,
        serdes_result=result_serdes,
        timeout=Duration.from_minutes(1),
    )

    context = create_test_context(state=mock_state)

    result = context.invoke(
        "test_function",
        {"original": "data"},
        name="custom_serdes_invoke",
        config=config,
    )

    seq = operation_id_sequence()
    expected_id = next(seq)

    assert result == {"transformed": "data"}
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, None, "custom_serdes_invoke"
        ),
        function_name="test_function",
        payload={"original": "data"},
        config=config,
    )
    mock_executor.process.assert_called_once()


# endregion invoke


# region wait
@patch("aws_durable_execution_sdk_python.context.WaitOperationExecutor")
def test_wait_basic(mock_executor_class):
    """Test wait with basic parameters."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = None
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    context.wait(Duration.from_seconds(30))

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_operation_id, None, None),
        seconds=30,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.WaitOperationExecutor")
def test_wait_with_name(mock_executor_class):
    """Test wait with name parameter."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = None
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    context.wait(Duration.from_minutes(1), name="test_wait")

    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]
    expected_id = next(seq)

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, None, "test_wait"),
        seconds=60,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.WaitOperationExecutor")
def test_wait_with_parent_id(mock_executor_class):
    """Test wait with parent_id."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = None
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state, parent_id="parent123")
    [context._create_step_id() for _ in range(2)]  # Set counter to 2 # noqa: SLF001

    context.wait(Duration.from_seconds(45))

    seq = operation_id_sequence("parent123")
    [next(seq) for _ in range(2)]
    expected_id = next(seq)

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(expected_id, "parent123"),
        seconds=45,
    )
    mock_executor.process.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.WaitOperationExecutor")
def test_wait_increments_counter(mock_executor_class):
    """Test wait increments step counter."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = None
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(10)]  # Set counter to 10 # noqa: SLF001

    context.wait(Duration.from_seconds(15))
    context.wait(Duration.from_seconds(25))

    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]
    expected_id1 = next(seq)
    expected_id2 = next(seq)

    assert context._step_counter.get_current() == 12  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, None, None)
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, None, None)


@patch("aws_durable_execution_sdk_python.context.WaitOperationExecutor")
def test_wait_returns_none(mock_executor_class):
    """Test wait returns None."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = None
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = context.wait(Duration.from_seconds(10))

    assert result is None


@patch("aws_durable_execution_sdk_python.context.WaitOperationExecutor")
def test_wait_with_time_less_than_one(mock_executor_class):
    """Test wait with time less than one."""
    mock_executor = MagicMock()
    mock_executor.process.return_value = None
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    with pytest.raises(ValidationError):
        context.wait(Duration.from_seconds(0))


# endregion wait


# region run_in_child_context
@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    result = context.run_in_child_context(mock_callable)

    assert result == "child_result"
    assert mock_handler.call_count == 1

    # Verify the callable was wrapped with child context
    call_args = mock_handler.call_args
    assert call_args[1]["state"] is mock_state
    assert call_args[1]["operation_identifier"] == OperationIdentifier(
        expected_operation_id, None, None
    )
    assert call_args[1]["config"] is None


@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(3)]  # Set counter to 3 # noqa: SLF001

    result = context.run_in_child_context(mock_callable, config=config)

    seq = operation_id_sequence()
    [next(seq) for _ in range(3)]
    expected_id = next(seq)

    assert result == "configured_child_result"
    call_args = mock_handler.call_args
    assert call_args[1]["operation_identifier"] == OperationIdentifier(
        expected_id, None, "original_function"
    )
    assert call_args[1]["config"] is config


@patch("aws_durable_execution_sdk_python.context.child_handler")
def test_run_in_child_context_with_parent_id(mock_executor_class):
    """Test run_in_child_context with parent_id."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "parent_child_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure Mock doesn't have _original_name

    context = create_test_context(state=mock_state, parent_id="parent456")
    [context._create_step_id() for _ in range(1)]  # Set counter to 1 # noqa: SLF001

    context.run_in_child_context(mock_callable)

    seq = operation_id_sequence("parent456")
    [next(seq) for _ in range(1)]
    expected_id = next(seq)

    call_args = mock_executor_class.call_args
    assert call_args[1]["operation_identifier"] == OperationIdentifier(
        expected_id, "parent456", None
    )


@patch("aws_durable_execution_sdk_python.context.child_handler")
def test_run_in_child_context_creates_child_context(mock_executor_class):
    """Test run_in_child_context creates proper child context."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    seq = operation_id_sequence()
    expected_parent_id = next(seq)

    def capture_child_context(child_context):
        # Verify child context properties
        assert isinstance(child_context, DurableContext)
        assert child_context.state is mock_state
        assert child_context._parent_id == expected_parent_id  # noqa: SLF001
        return "child_executed"

    mock_callable = Mock(side_effect=capture_child_context)
    mock_executor_class.side_effect = lambda func, **kwargs: func()

    context = create_test_context(state=mock_state)

    result = context.run_in_child_context(mock_callable)

    assert result == "child_executed"
    mock_callable.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.child_handler")
def test_run_in_child_context_increments_counter(mock_executor_class):
    """Test run_in_child_context increments step counter."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = create_test_context(state=mock_state)
    [context._create_step_id() for _ in range(5)]  # Set counter to 5 # noqa: SLF001

    context.run_in_child_context(mock_callable)
    context.run_in_child_context(mock_callable)

    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]
    expected_id1 = next(seq)
    expected_id2 = next(seq)

    assert context._step_counter.get_current() == 7  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, None, None)
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, None, None)


@patch("aws_durable_execution_sdk_python.context.child_handler")
def test_run_in_child_context_resolves_name_from_callable(mock_executor_class):
    """Test run_in_child_context resolves name from callable._original_name."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "named_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = Mock()
    mock_callable._original_name = "original_function_name"  # noqa: SLF001

    context = create_test_context(state=mock_state)

    context.run_in_child_context(mock_callable)

    call_args = mock_executor_class.call_args
    assert call_args[1]["operation_identifier"].name == "original_function_name"


# endregion run_in_child_context


# region wait_for_callback
@patch("aws_durable_execution_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_basic(mock_executor_class):
    """Test wait_for_callback with basic parameters."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "callback_result"

    mock_executor_class.return_value = mock_executor
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
        context = create_test_context(state=mock_state)

        result = context.wait_for_callback(mock_submitter)

        assert result == "callback_result"
        mock_run_in_child.assert_called_once()

        # Verify the child context callable
        call_args = mock_run_in_child.call_args
        assert call_args[0][1] is None  # name should be None


@patch("aws_durable_execution_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_with_name_and_config(mock_executor_class):
    """Test wait_for_callback with name and config."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "configured_callback_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = Mock()
    mock_submitter._original_name = "submit_function"  # noqa: SLF001
    config = CallbackConfig()

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "configured_callback_result"
        context = create_test_context(state=mock_state)

        result = context.wait_for_callback(mock_submitter, config=config)

        assert result == "configured_callback_result"
        call_args = mock_run_in_child.call_args
        assert (
            call_args[0][1] == "submit_function"
        )  # name should be from _original_name


@patch("aws_durable_execution_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_resolves_name_from_submitter(mock_executor_class):
    """Test wait_for_callback resolves name from submitter._original_name."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "named_callback_result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = Mock()
    mock_submitter._original_name = "submit_task"  # noqa: SLF001

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:
        mock_run_in_child.return_value = "named_callback_result"
        context = create_test_context(state=mock_state)

        context.wait_for_callback(mock_submitter)

        call_args = mock_run_in_child.call_args
        assert call_args[0][1] == "submit_task"


@patch("aws_durable_execution_sdk_python.context.wait_for_callback_handler")
def test_wait_for_callback_passes_child_context(mock_executor_class):
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

    mock_executor_class.side_effect = capture_handler_call

    with patch.object(DurableContext, "run_in_child_context") as mock_run_in_child:

        def run_child_context(callable_func, name):
            # Execute the child context callable
            child_context = create_test_context(state=mock_state, parent_id="test")
            return callable_func(child_context)

        mock_run_in_child.side_effect = run_child_context
        context = create_test_context(state=mock_state)

        result = context.wait_for_callback(mock_submitter)

        assert result == "handler_result"
        mock_executor_class.assert_called_once()


# endregion wait_for_callback


# region map
@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    result = context.map(inputs, test_function)

    assert result == "map_result"
    mock_handler.assert_called_once()

    # Verify the child handler was called with correct parameters
    call_args = mock_handler.call_args
    assert call_args[1]["config"].sub_type.value == "Map"


@patch("aws_durable_execution_sdk_python.context.child_handler")
def test_map_with_name_and_config(mock_handler):
    """Test map with name and config."""
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

    context = create_test_context(state=mock_state)

    result = context.map(inputs, test_function, name="custom_map", config=config)

    assert result == "configured_map_result"
    call_args = mock_handler.call_args
    assert (
        call_args[1]["operation_identifier"].name == "custom_map"
    )  # name should be custom_map


@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    result = context.map(inputs, test_function)

    assert result == "handler_result"
    mock_handler.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.map_handler")
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
        context = create_test_context(state=mock_state)

        result = context.map(inputs, test_function)

        assert result == "empty_map_result"


@patch("aws_durable_execution_sdk_python.context.map_handler")
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
        context = create_test_context(state=mock_state)

        result = context.map(inputs, test_function)

        assert result == "mixed_map_result"


# endregion map


# region parallel
@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    result = context.parallel(callables)

    assert result == "parallel_result"
    mock_handler.assert_called_once()

    # Verify the child handler was called with correct parameters
    call_args = mock_handler.call_args
    assert call_args[1]["config"].sub_type.value == "Parallel"


@patch("aws_durable_execution_sdk_python.context.child_handler")
def test_parallel_with_name_and_config(mock_handler):
    """Test parallel with name and config."""
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

    context = create_test_context(state=mock_state)

    result = context.parallel(callables, name="custom_parallel", config=config)

    assert result == "configured_parallel_result"
    call_args = mock_handler.call_args
    assert (
        call_args[1]["operation_identifier"].name == "custom_parallel"
    )  # name should be custom_parallel


@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    # Use _resolve_step_name to test name resolution
    resolved_name = context._resolve_step_name(None, mock_callable)  # noqa: SLF001
    assert resolved_name == "parallel_tasks"

    context.parallel(callables)

    call_args = mock_handler.call_args
    assert (
        call_args[1]["operation_identifier"].name is None
    )  # name should be None since callables don't have _original_name


@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    result = context.parallel(callables)

    assert result == "handler_result"
    mock_handler.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.parallel_handler")
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
        context = create_test_context(state=mock_state)

        result = context.parallel(callables)

        assert result == "empty_parallel_result"


@patch("aws_durable_execution_sdk_python.context.parallel_handler")
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
        context = create_test_context(state=mock_state)

        result = context.parallel(callables)

        assert result == "single_parallel_result"


@patch("aws_durable_execution_sdk_python.context.parallel_handler")
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
        context = create_test_context(state=mock_state)

        result = context.parallel(callables)

        assert result == "many_parallel_result"


# endregion parallel


# region map
@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    result = context.map(inputs, test_function, config=config)

    assert result == "map_result"
    mock_handler.assert_called_once()


@patch("aws_durable_execution_sdk_python.context.child_handler")
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

    context = create_test_context(state=mock_state)

    result = context.parallel(callables, config=config)

    assert result == "parallel_result"
    mock_handler.assert_called_once()


# region wait_for_condition
def test_wait_for_condition_validation_errors():
    """Test wait_for_condition raises ValidationError for invalid inputs."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = create_test_context(state=mock_state)

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

    context = create_test_context(state=state)

    # Mock the handlers to track calls
    with patch(
        "aws_durable_execution_sdk_python.context.map_handler"
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

    context = create_test_context(state=state)

    # Mock the handlers to track calls
    with patch(
        "aws_durable_execution_sdk_python.context.parallel_handler"
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
        return WaitForConditionDecision.STOP

    # Create mock state and context
    state = Mock()
    state.durable_execution_arn = "test_arn"

    context = create_test_context(state=state)

    # Create config
    config = WaitForConditionConfig(
        wait_strategy=test_wait_strategy, initial_state="test"
    )

    # Mock the executor to track calls
    with patch(
        "aws_durable_execution_sdk_python.context.WaitForConditionOperationExecutor"
    ) as mock_executor_class:
        mock_executor = MagicMock()
        mock_executor.process.return_value = "final_state"
        mock_executor_class.return_value = mock_executor

        # Call wait_for_condition method
        result = context.wait_for_condition(test_check, config)

        # Verify executor was called
        mock_executor_class.assert_called_once()
        mock_executor.process.assert_called_once()
        assert result == "final_state"


# region operation_id generation
def test_operation_id_conditional_on_parent():
    """
    - ensure that for all unique parents we produce unique sequences for the children
    """
    all_sequences = set()

    for i in range(10):
        parent = f"parent_{i}"
        seq = operation_id_sequence(parent)
        sequence = tuple(islice(seq, 10))
        all_sequences.add(sequence)

    assert len(all_sequences) == 10


def test_operation_id_generation_conditional_on_name_and_parent():
    """
    ensure that for all given (name, parent), None included, we observe unique sequences
    """

    parents = [f"parent_{i}" for i in range(9)] + [None]
    random.shuffle(parents)
    all_sequences = set()

    for parent in parents:
        seq = operation_id_sequence(parent)
        sequence = tuple(islice(seq, 5))
        all_sequences.add(sequence)

    assert len(all_sequences) == 10


def test_operation_id_generation_deterministic():
    """
    ensure that any sequence with any seed name and parent is deterministic
    """

    random.seed(43)
    parents = [f"parent_{i}" for i in range(9)] + [None]
    random.shuffle(parents)

    for parent in parents:
        seq1 = operation_id_sequence(parent)
        sequence1 = tuple(islice(seq1, 10))

        seq2 = operation_id_sequence(parent)
        sequence2 = tuple(islice(seq2, 10))

        assert sequence1 == sequence2


def test_operation_id_generation_unique():
    """
    ensure that for any sequence, any two adjacent operation ids are unique
    """
    seq = operation_id_sequence()
    ids = [next(seq) for _ in range(100)]

    for i in range(len(ids) - 1):
        assert ids[i] != ids[i + 1]


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_with_explicit_tenant_id(mock_executor_class):
    """Test invoke with explicit tenant_id in config."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    config = InvokeConfig(tenant_id="explicit-tenant")
    context = create_test_context(state=mock_state)

    result = context.invoke("test_function", "payload", config=config)

    assert result == "result"
    call_args = mock_executor_class.call_args[1]
    assert call_args["config"].tenant_id == "explicit-tenant"


@patch("aws_durable_execution_sdk_python.context.InvokeOperationExecutor")
def test_invoke_without_tenant_id_defaults_to_none(mock_executor_class):
    """Test invoke without tenant_id defaults to None."""
    mock_executor = MagicMock()

    mock_executor.process.return_value = "result"

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = context.invoke("test_function", "payload")

    assert result == "result"
    # Config is created as InvokeConfig() when not provided
    call_args = mock_executor_class.call_args[1]
    assert isinstance(call_args["config"], InvokeConfig)
    assert call_args["config"].tenant_id is None


# region ExecutionContext tests


def test_execution_context_exists_on_durable_context():
    """Test that DurableContext has execution_context attribute."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test-execution"
    )

    context = create_test_context(state=mock_state)

    assert hasattr(context, "execution_context")
    assert context.execution_context is not None


def test_execution_context_has_correct_arn():
    """Test that ExecutionContext contains the correct durable_execution_arn."""
    expected_arn = "arn:aws:durable:us-west-2:987654321098:execution/my-execution"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = expected_arn

    context = create_test_context(state=mock_state)

    assert context.execution_context.durable_execution_arn == expected_arn


def test_execution_context_is_immutable():
    """Test that ExecutionContext is frozen and immutable."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    # Attempt to modify should raise FrozenInstanceError for frozen dataclass
    with pytest.raises(AttributeError, match="cannot assign to field"):
        context.execution_context.durable_execution_arn = "new-arn"


def test_execution_context_propagates_to_child_context():
    """Test that child contexts inherit the same execution_context."""
    parent_arn = "arn:aws:durable:eu-west-1:111222333444:execution/parent-exec"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = parent_arn

    parent_context = create_test_context(state=mock_state)
    child_context = parent_context.create_child_context("parent-op-123")

    assert child_context.execution_context is not None
    assert child_context.execution_context.durable_execution_arn == parent_arn
    # Should be the same instance (not a copy)
    assert child_context.execution_context is parent_context.execution_context


def test_from_lambda_context_creates_execution_context():
    """Test that from_lambda_context factory creates ExecutionContext."""
    expected_arn = "arn:aws:durable:ap-south-1:555666777888:execution/lambda-exec"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = expected_arn
    mock_lambda_context = Mock()

    context = DurableContext.from_lambda_context(
        state=mock_state, lambda_context=mock_lambda_context
    )

    assert context.execution_context is not None
    assert context.execution_context.durable_execution_arn == expected_arn


def test_execution_context_type():
    """Test that execution_context is of type ExecutionContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    assert isinstance(context.execution_context, ExecutionContext)


# endregion ExecutionContext tests

# region Virtual-context identity tests


def test_should_default_step_id_prefix_to_parent_id_when_not_specified():
    """A non-virtual context holds parent_id and step_id_prefix equal."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    ctx = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        parent_id="parent-op-1",
    )

    assert ctx._parent_id == "parent-op-1"  # noqa: SLF001
    assert ctx._step_id_prefix == "parent-op-1"  # noqa: SLF001
    assert ctx.is_virtual is False


def test_should_mark_context_virtual_when_parent_id_differs_from_step_prefix():
    """A virtual context holds parent_id and step_id_prefix with different values."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    ctx = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        parent_id="grandparent-op",
        step_id_prefix="branch-op",
    )

    assert ctx._parent_id == "grandparent-op"  # noqa: SLF001
    assert ctx._step_id_prefix == "branch-op"  # noqa: SLF001
    assert ctx.is_virtual is True


def test_should_use_step_id_prefix_when_generating_step_ids():
    """Step ids derive from the step_id_prefix, not parent_id.

    For virtual contexts this is load-bearing: step ids must stay stable
    across virtual/non-virtual construction so replay ids match.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    virtual = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        parent_id="grandparent-op",
        step_id_prefix="branch-op",
    )
    expected_prefixed = hashlib.blake2b(b"branch-op-1").hexdigest()[:64]

    assert virtual._create_step_id_for_logical_step(1) == expected_prefixed  # noqa: SLF001


def test_should_use_parent_id_as_step_prefix_when_non_virtual():
    """Non-virtual contexts prefix step ids with parent_id (default fallback).

    For the non-virtual case `step_id_prefix` is not passed explicitly;
    it defaults to `parent_id`. Replay stability for executions produced
    before the virtual-context refactor depends on this fallback
    matching the pre-refactor behaviour exactly.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    non_virtual = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        parent_id="parent-op",
    )
    expected = hashlib.blake2b(b"parent-op-1").hexdigest()[:64]

    assert non_virtual._create_step_id_for_logical_step(1) == expected  # noqa: SLF001
    assert non_virtual.is_virtual is False


def test_should_create_non_virtual_child_when_is_virtual_false():
    """create_child_context(op_id) returns a non-virtual child."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    parent = create_test_context(state=mock_state, parent_id="parent-op")

    child = parent.create_child_context("child-op")

    assert child._parent_id == "child-op"  # noqa: SLF001
    assert child._step_id_prefix == "child-op"  # noqa: SLF001
    assert child.is_virtual is False


def test_should_create_virtual_child_that_propagates_grandparent_id():
    """create_child_context(op_id, is_virtual=True) propagates the grandparent as parent_id."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    parent = create_test_context(state=mock_state, parent_id="grandparent-op")

    child = parent.create_child_context("child-op", is_virtual=True)

    assert child._parent_id == "grandparent-op"  # noqa: SLF001
    assert child._step_id_prefix == "child-op"  # noqa: SLF001
    assert child.is_virtual is True


def test_should_create_virtual_child_with_none_parent_when_parent_is_root():
    """Virtual child of a root context (parent_id=None) keeps parent_id=None.

    Inner operations then report at the top level; step ids still prefix
    on the child's own operation id.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    root_parent = create_test_context(state=mock_state, parent_id=None)

    child = root_parent.create_child_context("child-op", is_virtual=True)

    assert child._parent_id is None  # noqa: SLF001
    assert child._step_id_prefix == "child-op"  # noqa: SLF001
    assert child.is_virtual is True

    expected = hashlib.blake2b(b"child-op-1").hexdigest()[:64]
    assert child._create_step_id_for_logical_step(1) == expected  # noqa: SLF001


def test_should_propagate_outer_parent_id_when_virtual_is_nested_in_virtual():
    """A virtual child of a virtual parent still reports to the outer non-virtual ancestor.

    Nested concurrency is a real scenario: e.g. a FLAT `map` inside a
    FLAT `parallel`. Each layer creates a virtual child. The inner
    virtual child inherits `_parent_id` from its immediate (virtual)
    parent, which in turn inherited it from its non-virtual
    grandparent. The expected end result is that inner operations in
    the doubly-nested virtual branch still stamp the outer
    non-virtual ancestor's id — every virtual layer collapses out of
    the observable hierarchy without accumulating.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    # Non-virtual outer context (e.g. the top-level parallel operation's context).
    outer = create_test_context(state=mock_state, parent_id="outer-parallel-op")

    # First virtual layer: outer parallel is FLAT, so its branch is virtual.
    outer_branch = outer.create_child_context("outer-branch-op", is_virtual=True)
    assert outer_branch._parent_id == "outer-parallel-op"  # noqa: SLF001
    assert outer_branch._step_id_prefix == "outer-branch-op"  # noqa: SLF001
    assert outer_branch.is_virtual is True

    # Second virtual layer: an inner FLAT map inside the outer branch,
    # whose per-item branch is also virtual.
    inner_branch = outer_branch.create_child_context("inner-branch-op", is_virtual=True)
    # Inner branch's parent_id must be the outermost non-virtual
    # ancestor's id, not the outer virtual branch's id — otherwise the
    # inner operations would report to a logical layer that does not
    # appear in the execution history, breaking the hierarchy.
    assert inner_branch._parent_id == "outer-parallel-op"  # noqa: SLF001
    assert inner_branch._step_id_prefix == "inner-branch-op"  # noqa: SLF001
    assert inner_branch.is_virtual is True

    # Step ids inside the inner branch still prefix on the inner branch's
    # own operation id; they must not leak the outer ancestor into the
    # step-id namespace.
    expected = hashlib.blake2b(b"inner-branch-op-1").hexdigest()[:64]
    assert inner_branch._create_step_id_for_logical_step(1) == expected  # noqa: SLF001


# endregion Virtual-context identity tests


# region durable_parallel_branch


def test_durable_parallel_branch_returns_parallel_branch_with_name():
    """Test that the decorator produces a ParallelBranch with the given name."""

    @durable_parallel_branch(name="fetch-user-data")
    def fetch_user(ctx: DurableContext, user_id: str) -> dict:
        return {"id": user_id}

    result = fetch_user("user-123")

    assert isinstance(result, ParallelBranch)
    assert result.name == "fetch-user-data"


def test_durable_parallel_branch_with_no_name():
    """Test that when name is None, ParallelBranch.name is None."""

    @durable_parallel_branch()
    def fetch_orders(ctx: DurableContext) -> list:
        return ["order1"]

    result = fetch_orders()

    assert isinstance(result, ParallelBranch)
    assert result.name is None


def test_durable_parallel_branch_callable_delegates_to_func():
    """Test that calling the ParallelBranch delegates to the wrapped function."""

    @durable_parallel_branch(name="my-branch")
    def my_branch(ctx: DurableContext, value: int) -> int:
        return value * 2

    branch = my_branch(21)
    mock_ctx = Mock(spec=DurableContext)

    result = branch(mock_ctx)

    assert result == 42


def test_durable_parallel_branch_with_multiple_args_and_kwargs():
    """Test that positional and keyword arguments are correctly bound."""

    @durable_parallel_branch(name="compute")
    def compute(ctx: DurableContext, a: int, b: int, op: str = "add") -> str:
        if op == "add":
            return f"{a + b}"
        return f"{a * b}"

    branch = compute(3, 4, op="mul")
    mock_ctx = Mock(spec=DurableContext)

    result = branch(mock_ctx)

    assert result == "12"


def test_durable_parallel_branch_passes_context_as_first_arg():
    """Test that the DurableContext is passed as the first argument to the function."""
    received_ctx = None

    @durable_parallel_branch(name="capture-ctx")
    def capture(ctx: DurableContext) -> str:
        nonlocal received_ctx
        received_ctx = ctx
        return "done"

    branch = capture()
    mock_ctx = Mock(spec=DurableContext)
    branch(mock_ctx)

    assert received_ctx is mock_ctx


def test_durable_parallel_branch_multiple_invocations_are_independent():
    """Test that calling the wrapper multiple times produces independent branches."""

    @durable_parallel_branch(name="greet")
    def greet(ctx: DurableContext, name: str) -> str:
        return f"hello {name}"

    branch_a = greet("Alice")
    branch_b = greet("Bob")

    mock_ctx = Mock(spec=DurableContext)

    assert branch_a(mock_ctx) == "hello Alice"
    assert branch_b(mock_ctx) == "hello Bob"


def test_durable_parallel_branch_is_compatible_with_parallel_functions_arg():
    """Test that the result can be used in a functions list alongside plain callables."""

    @durable_parallel_branch(name="named-branch")
    def named(ctx: DurableContext) -> str:
        return "named"

    plain = lambda ctx: "plain"  # noqa: E731

    functions = [named(), plain]

    assert isinstance(functions[0], ParallelBranch)
    assert callable(functions[0])
    assert callable(functions[1])


# endregion durable_parallel_branch


# region per-context replay status


def test_is_replaying_returns_true_when_replay_status_is_replay():
    """Test is_replaying() returns True when context is in REPLAY status."""
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    context = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        replay_status=ReplayStatus.REPLAY,
    )

    assert context.is_replaying() is True


def test_is_replaying_returns_false_when_replay_status_is_new():
    """Test is_replaying() returns False when context is in NEW status."""
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    context = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        replay_status=ReplayStatus.NEW,
    )

    assert context.is_replaying() is False


def test_track_replay_adds_operation_id_to_visited_set():
    """Test track_replay adds operation IDs to the visited operations set."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    # Two completed operations scoped to this context (parent_id=None for root)
    mock_state.operations = {
        "op-1": Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            parent_id=None,
        ),
        "op-2": Operation(
            operation_id="op-2",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            parent_id=None,
        ),
    }

    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )
    context = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        replay_status=ReplayStatus.REPLAY,
    )

    context.track_replay("op-1")

    assert "op-1" in context._visited_operations  # noqa: SLF001


def test_track_replay_transitions_to_new_when_all_scoped_ops_visited():
    """Test track_replay transitions from REPLAY to NEW when all scoped completed ops are visited."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    mock_state.operations = {
        "op-1": Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            parent_id=None,
        ),
        "op-2": Operation(
            operation_id="op-2",
            operation_type=OperationType.WAIT,
            status=OperationStatus.SUCCEEDED,
            parent_id=None,
        ),
    }

    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )
    context = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        replay_status=ReplayStatus.REPLAY,
    )

    # After visiting first op, still replaying
    context.track_replay("op-1")
    assert context.is_replaying() is True

    # After visiting second op, transitions to NEW
    context.track_replay("op-2")
    assert context.is_replaying() is False


def test_track_replay_no_op_when_already_new():
    """Test track_replay does nothing when context is already in NEW status."""
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )

    context = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        replay_status=ReplayStatus.NEW,
    )

    # Should not raise or change state
    context.track_replay("op-1")

    assert context.is_replaying() is False
    # Visited set should remain empty since early return skips adding
    assert len(context._visited_operations) == 0  # noqa: SLF001


def test_track_replay_transitions_immediately_when_no_scoped_ops():
    """Test track_replay transitions to NEW immediately when no completed ops are scoped."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    # Operations exist but belong to a different context (different parent_id)
    mock_state.operations = {
        "op-other": Operation(
            operation_id="op-other",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            parent_id="other-parent",
        ),
    }

    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )
    context = DurableContext(
        state=mock_state,
        execution_context=execution_context,
        replay_status=ReplayStatus.REPLAY,
        parent_id=None,
    )

    # First track_replay call should transition immediately since no ops are scoped to root
    context.track_replay("op-1")
    assert context.is_replaying() is False


def test_child_context_starts_in_replay_when_completed_ops_exist():
    """Test create_child_context sets child to REPLAY when completed ops exist for child scope."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    # A completed operation scoped to the child (parent_id = "child-op-id")
    mock_state.operations = {
        "child-step-1": Operation(
            operation_id="child-step-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            parent_id="child-op-id",
        ),
    }

    parent_context = create_test_context(state=mock_state)
    child_context = parent_context.create_child_context("child-op-id")

    assert child_context.is_replaying() is True


def test_child_context_starts_in_new_when_no_completed_ops_exist():
    """Test create_child_context sets child to NEW when no completed ops exist for child scope."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    # No operations at all
    mock_state.operations = {}

    parent_context = create_test_context(state=mock_state)
    child_context = parent_context.create_child_context("child-op-id")

    assert child_context.is_replaying() is False


def test_child_context_ignores_execution_type_ops_for_replay_determination():
    """Test that EXECUTION type operations don't count for child replay status."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    # Only an EXECUTION type operation exists for the child scope
    mock_state.operations = {
        "exec-op": Operation(
            operation_id="exec-op",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.SUCCEEDED,
            parent_id="child-op-id",
        ),
    }

    parent_context = create_test_context(state=mock_state)
    child_context = parent_context.create_child_context("child-op-id")

    # Should be NEW because EXECUTION ops are excluded
    assert child_context.is_replaying() is False


def test_child_context_ignores_non_terminal_ops_for_replay_determination():
    """Test that non-terminal operations don't count for child replay status."""
    from threading import Lock

    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationStatus,
        OperationType,
    )
    from aws_durable_execution_sdk_python.state import ReplayStatus

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state._operations_lock = Lock()
    # Only a STARTED (non-terminal) operation exists for the child scope
    mock_state.operations = {
        "started-op": Operation(
            operation_id="started-op",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
            parent_id="child-op-id",
        ),
    }

    parent_context = create_test_context(state=mock_state)
    child_context = parent_context.create_child_context("child-op-id")

    # Should be NEW because STARTED is not a terminal status
    assert child_context.is_replaying() is False


# endregion per-context replay status
