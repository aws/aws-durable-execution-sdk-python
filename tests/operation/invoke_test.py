"""Unit tests for invoke handler."""

from __future__ import annotations

import json
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.operation.invoke import (
    invoke_handler,
    suspend_with_optional_resume_delay,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState
from tests.serdes_test import CustomDictSerDes


def test_invoke_handler_already_succeeded():
    """Test invoke_handler when operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke1",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps("test_result")),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier("invoke1", None, "test_invoke"),
        config=None,
    )

    assert result == "test_result"
    mock_state.create_checkpoint.assert_not_called()


def test_invoke_handler_already_succeeded_none_result():
    """Test invoke_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke2",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=None),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier("invoke2", None, "test_invoke"),
        config=None,
    )

    assert result is None


def test_invoke_handler_already_succeeded_no_chained_invoke_details():
    """Test invoke_handler when operation succeeded but has no chained_invoke_details."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke3",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = invoke_handler(
        function_name="test_function",
        payload="test_input",
        state=mock_state,
        operation_identifier=OperationIdentifier("invoke3", None, "test_invoke"),
        config=None,
    )

    assert result is None


@pytest.mark.parametrize(
    "kind", [OperationStatus.FAILED, OperationStatus.STOPPED, OperationStatus.TIMED_OUT]
)
def test_invoke_handler_already_terminated(kind: OperationStatus):
    """Test invoke_handler when operation already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="invoke4",
        operation_type=OperationType.CHAINED_INVOKE,
        status=kind,
        chained_invoke_details=ChainedInvokeDetails(error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(CallableRuntimeError):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke4", None, "test_invoke"),
            config=None,
        )


def test_invoke_handler_already_timed_out():
    """Test invoke_handler when operation already timed out."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    error = ErrorObject(
        message="Operation timed out", type="TimeoutError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="invoke5",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.TIMED_OUT,
        chained_invoke_details=ChainedInvokeDetails(error=error),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(CallableRuntimeError):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke5", None, "test_invoke"),
            config=None,
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
def test_invoke_handler_already_started(status):
    """Test invoke_handler when operation is already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke6",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution, match="Invoke invoke6 still in progress"):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke6", None, "test_invoke"),
            config=None,
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
def test_invoke_handler_already_started_with_timeout(status):
    """Test invoke_handler when operation is already started with timeout config."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke7",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    config = InvokeConfig[str, str](timeout_seconds=30)

    with pytest.raises(TimedSuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke7", None, "test_invoke"),
            config=config,
        )


def test_invoke_handler_new_operation():
    """Test invoke_handler when starting a new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    config = InvokeConfig[str, str](timeout_seconds=60)

    with pytest.raises(
        SuspendExecution, match="Invoke invoke8 started, suspending for completion"
    ):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke8", None, "test_invoke"),
            config=config,
        )

    # Verify checkpoint was created
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]

    assert operation_update.operation_id == "invoke8"
    assert operation_update.operation_type == OperationType.CHAINED_INVOKE
    assert operation_update.action == OperationAction.START
    assert operation_update.name == "test_invoke"
    assert operation_update.payload == json.dumps("test_input")
    assert operation_update.chained_invoke_options.function_name == "test_function"


def test_invoke_handler_new_operation_with_timeout():
    """Test invoke_handler when starting a new operation with timeout."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    config = InvokeConfig[str, str](timeout_seconds=30)

    with pytest.raises(TimedSuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke9", None, "test_invoke"),
            config=config,
        )


def test_invoke_handler_new_operation_no_timeout():
    """Test invoke_handler when starting a new operation without timeout."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    config = InvokeConfig[str, str](timeout_seconds=0)

    with pytest.raises(SuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke10", None, "test_invoke"),
            config=config,
        )


def test_invoke_handler_no_config():
    """Test invoke_handler when no config is provided."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke11", None, "test_invoke"),
            config=None,
        )

    # Verify default config was used
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert (
        operation_update.to_dict()["ChainedInvokeOptions"]["FunctionName"]
        == "test_function"
    )


def test_invoke_handler_custom_serdes():
    """Test invoke_handler with custom serialization."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke12",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    config = InvokeConfig[dict, dict](
        serdes_payload=CustomDictSerDes(), serdes_result=CustomDictSerDes()
    )

    result = invoke_handler(
        function_name="test_function",
        payload={"key": "value", "number": 42, "list": [1, 2, 3]},
        state=mock_state,
        operation_identifier=OperationIdentifier("invoke12", None, "test_invoke"),
        config=config,
    )

    # CustomDictSerDes transforms the result back
    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


def test_invoke_handler_custom_serdes_new_operation():
    """Test invoke_handler with custom serialization for new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    config = InvokeConfig[dict, dict](
        serdes_payload=CustomDictSerDes(), serdes_result=CustomDictSerDes()
    )
    complex_payload = {"key": "value", "number": 42, "list": [1, 2, 3]}

    with pytest.raises(SuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload=complex_payload,
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke13", None, "test_invoke"),
            config=config,
        )

    # Verify custom serialization was used
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    expected_serialized = '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    assert operation_update.payload == expected_serialized


def test_suspend_with_optional_resume_delay_with_timeout():
    """Test suspend_with_optional_resume_delay with timeout."""
    with pytest.raises(TimedSuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", 30)

    assert "test message" in str(exc_info.value)


def test_suspend_with_optional_resume_delay_no_timeout():
    """Test suspend_with_optional_resume_delay without timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", None)

    assert "test message" in str(exc_info.value)


def test_suspend_with_optional_resume_delay_zero_timeout():
    """Test suspend_with_optional_resume_delay with zero timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", 0)

    assert "test message" in str(exc_info.value)


def test_suspend_with_optional_resume_delay_negative_timeout():
    """Test suspend_with_optional_resume_delay with negative timeout."""
    with pytest.raises(SuspendExecution) as exc_info:
        suspend_with_optional_resume_delay("test message", -5)

    assert "test message" in str(exc_info.value)


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
def test_invoke_handler_with_operation_name(status: OperationStatus):
    """Test invoke_handler uses operation name in logs when available."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke14",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke14", None, "named_invoke"),
            config=None,
        )


@pytest.mark.parametrize("status", [OperationStatus.STARTED, OperationStatus.PENDING])
def test_invoke_handler_without_operation_name(status: OperationStatus):
    """Test invoke_handler uses function name in logs when no operation name."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke15",
        operation_type=OperationType.CHAINED_INVOKE,
        status=status,
        chained_invoke_details=ChainedInvokeDetails(),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke15", None, None),
            config=None,
        )


def test_invoke_handler_with_none_payload():
    """Test invoke_handler when payload is None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(SuspendExecution):
        invoke_handler(
            function_name="test_function",
            payload=None,
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke16", None, "test_invoke"),
            config=None,
        )

    # Verify checkpoint was created with None payload
    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert operation_update.payload == "null"  # JSON serialization of None


def test_invoke_handler_already_succeeded_with_none_payload():
    """Test invoke_handler when operation succeeded and original payload was None."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="invoke17",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        chained_invoke_details=ChainedInvokeDetails(result=json.dumps("test_result")),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = invoke_handler(
        function_name="test_function",
        payload=None,
        state=mock_state,
        operation_identifier=OperationIdentifier("invoke17", None, "test_invoke"),
        config=None,
    )

    assert result == "test_result"
    mock_state.create_checkpoint.assert_not_called()


@patch(
    "aws_durable_execution_sdk_python.operation.invoke.suspend_with_optional_resume_delay"
)
def test_invoke_handler_suspend_does_not_raise(mock_suspend):
    """Test invoke_handler when suspend_with_optional_resume_delay doesn't raise an exception."""

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    mock_result = CheckpointedResult.create_not_found()
    mock_state.get_checkpoint_result.return_value = mock_result

    # Mock suspend_with_optional_resume_delay to not raise an exception (which it should always do)
    mock_suspend.return_value = None

    with pytest.raises(
        ExecutionError,
        match="suspend_with_optional_resume_delay should have raised an exception, but did not.",
    ):
        invoke_handler(
            function_name="test_function",
            payload="test_input",
            state=mock_state,
            operation_identifier=OperationIdentifier("invoke18", None, "test_invoke"),
            config=None,
        )

    mock_suspend.assert_called_once()
