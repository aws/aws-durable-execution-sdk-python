"""Tests for the service module."""

import datetime
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    CheckpointError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    CallbackOptions,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    ContextOptions,
    DurableServiceClient,
    ErrorObject,
    ExecutionDetails,
    InvokeDetails,
    InvokeOptions,
    LambdaClient,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StateOutput,
    StepDetails,
    StepOptions,
    WaitDetails,
    WaitOptions,
)

# =============================================================================
# Tests for Data Classes (ExecutionDetails, ContextDetails, ErrorObject, etc.)
# =============================================================================


def test_execution_details_from_dict():
    """Test ExecutionDetails.from_dict method."""
    data = {"InputPayload": "test_payload"}
    details = ExecutionDetails.from_dict(data)
    assert details.input_payload == "test_payload"


def test_execution_details_empty():
    """Test ExecutionDetails.from_dict with empty data."""
    data = {}
    details = ExecutionDetails.from_dict(data)
    assert details.input_payload is None


def test_context_details_from_dict():
    """Test ContextDetails.from_dict method."""
    data = {"Result": "test_result"}
    details = ContextDetails.from_dict(data)
    assert details.result == "test_result"
    assert details.error is None


def test_context_details_with_error():
    """Test ContextDetails.from_dict with error."""
    error_data = {"ErrorMessage": "Context error", "ErrorType": "ContextError"}
    data = {"Result": "test_result", "Error": error_data}
    details = ContextDetails.from_dict(data)
    assert details.result == "test_result"
    assert details.error.message == "Context error"
    assert details.error.type == "ContextError"


def test_context_details_error_only():
    """Test ContextDetails.from_dict with only error."""
    error_data = {"ErrorMessage": "Context failed"}
    data = {"Error": error_data}
    details = ContextDetails.from_dict(data)
    assert details.result is None
    assert details.error.message == "Context failed"


def test_context_details_empty():
    """Test ContextDetails.from_dict with empty data."""
    data = {}
    details = ContextDetails.from_dict(data)
    assert details.replay_children is False
    assert details.result is None
    assert details.error is None


def test_context_details_with_replay_children():
    """Test ContextDetails.from_dict with replay_children field."""
    data = {"ReplayChildren": True, "Result": "test_result"}
    details = ContextDetails.from_dict(data)
    assert details.replay_children is True
    assert details.result == "test_result"
    assert details.error is None


def test_error_object_from_dict():
    """Test ErrorObject.from_dict method."""
    data = {
        "ErrorMessage": "Test error",
        "ErrorType": "TestError",
        "ErrorData": "test_data",
        "StackTrace": ["line1", "line2"],
    }
    error = ErrorObject.from_dict(data)
    assert error.message == "Test error"
    assert error.type == "TestError"
    assert error.data == "test_data"
    assert error.stack_trace == ["line1", "line2"]


def test_error_object_from_exception():
    """Test ErrorObject.from_exception method."""
    exception = ValueError("Test value error")
    error = ErrorObject.from_exception(exception)
    assert error.message == "Test value error"
    assert error.type == "ValueError"
    assert error.data is None
    assert error.stack_trace is None


def test_error_object_from_exception_runtime_error():
    """Test ErrorObject.from_exception with RuntimeError."""
    runtime_error = RuntimeError("Runtime issue")
    error = ErrorObject.from_exception(runtime_error)
    assert error.message == "Runtime issue"
    assert error.type == "RuntimeError"
    assert error.data is None
    assert error.stack_trace is None


def test_error_object_from_exception_custom_error():
    """Test ErrorObject.from_exception with custom exception."""

    class CustomError(Exception):
        pass

    custom_error = CustomError("Custom message")
    error = ErrorObject.from_exception(custom_error)
    assert error.message == "Custom message"
    assert error.type == "CustomError"
    assert error.data is None
    assert error.stack_trace is None


def test_error_object_from_exception_empty_message():
    """Test ErrorObject.from_exception with exception that has no message."""
    empty_error = ValueError()
    error = ErrorObject.from_exception(empty_error)
    assert error.message == ""
    assert error.type == "ValueError"
    assert error.data is None
    assert error.stack_trace is None


def test_error_object_from_message_regular():
    """Test ErrorObject.from_message with regular message."""
    error = ErrorObject.from_message("Test error message")
    assert error.message == "Test error message"
    assert error.type is None
    assert error.data is None
    assert error.stack_trace is None


def test_error_object_from_message_empty():
    """Test ErrorObject.from_message with empty message."""
    error = ErrorObject.from_message("")
    assert error.message == ""
    assert error.type is None
    assert error.data is None
    assert error.stack_trace is None


def test_error_object_to_dict():
    """Test ErrorObject.to_dict method."""
    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="test_data",
        stack_trace=["line1", "line2"],
    )
    result = error.to_dict()
    expected = {
        "ErrorMessage": "Test error",
        "ErrorType": "TestError",
        "ErrorData": "test_data",
        "StackTrace": ["line1", "line2"],
    }
    assert result == expected


def test_error_object_to_dict_partial():
    """Test ErrorObject.to_dict with None values."""
    error = ErrorObject(message="Test error", type=None, data=None, stack_trace=None)
    result = error.to_dict()
    assert result == {"ErrorMessage": "Test error"}


def test_error_object_to_dict_all_none():
    """Test ErrorObject.to_dict with all None values."""
    error = ErrorObject(message=None, type=None, data=None, stack_trace=None)
    result = error.to_dict()
    assert result == {}


def test_error_object_to_callable_runtime_error():
    """Test ErrorObject.to_callable_runtime_error method."""
    error = ErrorObject(
        message="Test error",
        type="TestError",
        data="test_data",
        stack_trace=["line1"],
    )
    runtime_error = error.to_callable_runtime_error()
    assert isinstance(runtime_error, CallableRuntimeError)
    assert runtime_error.message == "Test error"
    assert runtime_error.error_type == "TestError"
    assert runtime_error.data == "test_data"
    assert runtime_error.stack_trace == ["line1"]


def test_step_details_from_dict():
    """Test StepDetails.from_dict method."""
    error_data = {"ErrorMessage": "Step error"}
    data = {
        "Attempt": 2,
        "NextAttemptTimestamp": "2023-01-01T00:00:00Z",
        "Result": "step_result",
        "Error": error_data,
    }
    details = StepDetails.from_dict(data)
    assert details.attempt == 2
    assert details.next_attempt_timestamp == "2023-01-01T00:00:00Z"
    assert details.result == "step_result"
    assert details.error.message == "Step error"


def test_step_details_all_fields():
    """Test StepDetails.from_dict with all fields."""
    error_data = {"ErrorMessage": "Step failed", "ErrorType": "StepError"}
    data = {
        "Attempt": 3,
        "NextAttemptTimestamp": "2023-01-01T12:00:00Z",
        "Result": "step_success",
        "Error": error_data,
    }
    details = StepDetails.from_dict(data)
    assert details.attempt == 3
    assert details.next_attempt_timestamp == "2023-01-01T12:00:00Z"
    assert details.result == "step_success"
    assert details.error.message == "Step failed"
    assert details.error.type == "StepError"


def test_step_details_minimal():
    """Test StepDetails.from_dict with minimal data."""
    data = {}
    details = StepDetails.from_dict(data)
    assert details.attempt == 0
    assert details.next_attempt_timestamp is None
    assert details.result is None
    assert details.error is None


def test_wait_details_from_dict():
    """Test WaitDetails.from_dict method."""
    timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    data = {"ScheduledTimestamp": timestamp}
    details = WaitDetails.from_dict(data)
    assert details.scheduled_timestamp == timestamp


def test_wait_details_from_dict_empty():
    """Test WaitDetails.from_dict with empty data."""
    data = {}
    details = WaitDetails.from_dict(data)
    assert details.scheduled_timestamp is None


def test_callback_details_from_dict():
    """Test CallbackDetails.from_dict method."""
    error_data = {"ErrorMessage": "Callback error"}
    data = {
        "CallbackId": "cb123",
        "Result": "callback_result",
        "Error": error_data,
    }
    details = CallbackDetails.from_dict(data)
    assert details.callback_id == "cb123"
    assert details.result == "callback_result"
    assert details.error.message == "Callback error"


def test_callback_details_all_fields():
    """Test CallbackDetails.from_dict with all fields."""
    error_data = {"ErrorMessage": "Callback failed", "ErrorType": "CallbackError"}
    data = {
        "CallbackId": "cb456",
        "Result": "callback_success",
        "Error": error_data,
    }
    details = CallbackDetails.from_dict(data)
    assert details.callback_id == "cb456"
    assert details.result == "callback_success"
    assert details.error.message == "Callback failed"
    assert details.error.type == "CallbackError"


def test_callback_details_minimal():
    """Test CallbackDetails.from_dict with minimal required data."""
    data = {"CallbackId": "cb789"}
    details = CallbackDetails.from_dict(data)
    assert details.callback_id == "cb789"
    assert details.result is None
    assert details.error is None


def test_invoke_details_from_dict():
    """Test InvokeDetails.from_dict method."""
    error_data = {"ErrorMessage": "Invoke error"}
    data = {
        "DurableExecutionArn": "arn:test",
        "Result": "invoke_result",
        "Error": error_data,
    }
    details = InvokeDetails.from_dict(data)
    assert details.durable_execution_arn == "arn:test"
    assert details.result == "invoke_result"
    assert details.error.message == "Invoke error"


def test_invoke_details_all_fields():
    """Test InvokeDetails.from_dict with all fields."""
    error_data = {"ErrorMessage": "Invoke failed", "ErrorType": "InvokeError"}
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-west-2:123456789012:function:test",
        "Result": "invoke_success",
        "Error": error_data,
    }
    details = InvokeDetails.from_dict(data)
    assert (
        details.durable_execution_arn
        == "arn:aws:lambda:us-west-2:123456789012:function:test"
    )
    assert details.result == "invoke_success"
    assert details.error.message == "Invoke failed"
    assert details.error.type == "InvokeError"


def test_invoke_details_minimal():
    """Test InvokeDetails.from_dict with minimal required data."""
    data = {"DurableExecutionArn": "arn:minimal"}
    details = InvokeDetails.from_dict(data)
    assert details.durable_execution_arn == "arn:minimal"
    assert details.result is None
    assert details.error is None


# =============================================================================
# Tests for Options Classes (StepOptions, WaitOptions, etc.)
# =============================================================================


def test_step_options_from_dict():
    """Test StepOptions.from_dict method."""
    data = {"NextAttemptDelaySeconds": 30}
    options = StepOptions.from_dict(data)
    assert options.next_attempt_delay_seconds == 30


def test_step_options_from_dict_empty():
    """Test StepOptions.from_dict with empty dict."""
    options = StepOptions.from_dict({})
    assert options.next_attempt_delay_seconds == 0


def test_callback_options_from_dict():
    """Test CallbackOptions.from_dict method."""
    data = {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60}
    options = CallbackOptions.from_dict(data)
    assert options.timeout_seconds == 300
    assert options.heartbeat_timeout_seconds == 60


def test_callback_options_from_dict_partial():
    """Test CallbackOptions.from_dict with partial data."""
    data = {"TimeoutSeconds": 300}
    options = CallbackOptions.from_dict(data)
    assert options.timeout_seconds == 300
    assert options.heartbeat_timeout_seconds == 0


def test_invoke_options_from_dict():
    """Test InvokeOptions.from_dict method."""
    data = {"FunctionName": "test-function", "TimeoutSeconds": 120}
    options = InvokeOptions.from_dict(data)
    assert options.function_name == "test-function"
    assert options.timeout_seconds == 120


def test_invoke_options_from_dict_required_only():
    """Test InvokeOptions.from_dict with only required field."""
    data = {"FunctionName": "test-function"}
    options = InvokeOptions.from_dict(data)
    assert options.function_name == "test-function"
    assert options.timeout_seconds == 0


def test_context_options_from_dict():
    """Test ContextOptions.from_dict method."""
    data = {"ReplayChildren": True}
    options = ContextOptions.from_dict(data)
    assert options.replay_children is True


def test_context_options_from_dict_empty():
    """Test ContextOptions.from_dict with empty dict."""
    options = ContextOptions.from_dict({})
    assert options.replay_children is False


def test_step_options_roundtrip():
    """Test StepOptions to_dict -> from_dict roundtrip."""
    original = StepOptions(next_attempt_delay_seconds=45)
    data = original.to_dict()
    restored = StepOptions.from_dict(data)
    assert restored == original


def test_callback_options_roundtrip():
    """Test CallbackOptions to_dict -> from_dict roundtrip."""
    original = CallbackOptions(timeout_seconds=300, heartbeat_timeout_seconds=60)
    data = original.to_dict()
    restored = CallbackOptions.from_dict(data)
    assert restored == original


def test_invoke_options_roundtrip():
    """Test InvokeOptions to_dict -> from_dict roundtrip."""
    original = InvokeOptions(function_name="test-func", timeout_seconds=120)
    data = original.to_dict()
    restored = InvokeOptions.from_dict(data)
    assert restored == original


def test_context_options_roundtrip():
    """Test ContextOptions to_dict -> from_dict roundtrip."""
    original = ContextOptions(replay_children=True)
    data = original.to_dict()
    restored = ContextOptions.from_dict(data)
    assert restored == original


def test_wait_options_from_dict():
    """Test WaitOptions.from_dict method."""
    data = {"WaitSeconds": 30}
    options = WaitOptions.from_dict(data)
    assert options.wait_seconds == 30


def test_step_options_to_dict():
    """Test StepOptions.to_dict method."""
    options = StepOptions(next_attempt_delay_seconds=30)
    result = options.to_dict()
    assert result == {"NextAttemptDelaySeconds": 30}


def test_wait_options_to_dict():
    """Test WaitOptions.to_dict method."""
    options = WaitOptions(wait_seconds=60)
    result = options.to_dict()
    assert result == {"WaitSeconds": 60}


def test_callback_options_to_dict():
    """Test CallbackOptions.to_dict method."""
    options = CallbackOptions(timeout_seconds=300, heartbeat_timeout_seconds=60)
    result = options.to_dict()
    assert result == {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60}


def test_callback_options_all_fields():
    """Test CallbackOptions with all fields."""
    options = CallbackOptions(timeout_seconds=300, heartbeat_timeout_seconds=60)
    result = options.to_dict()
    assert result["TimeoutSeconds"] == 300
    assert result["HeartbeatTimeoutSeconds"] == 60


def test_invoke_options_to_dict():
    """Test InvokeOptions.to_dict method."""
    options = InvokeOptions(
        function_name="test_function",
        timeout_seconds=30,
    )
    result = options.to_dict()
    expected = {
        "FunctionName": "test_function",
        "TimeoutSeconds": 30,
    }
    assert result == expected


def test_invoke_options_to_dict_minimal():
    """Test InvokeOptions.to_dict with minimal fields."""
    options = InvokeOptions(function_name="test_function")
    result = options.to_dict()
    assert result == {"FunctionName": "test_function", "TimeoutSeconds": 0}


def test_context_options_to_dict():
    """Test ContextOptions.to_dict method."""
    options = ContextOptions(replay_children=True)
    result = options.to_dict()
    assert result == {"ReplayChildren": True}


def test_context_options_to_dict_default():
    """Test ContextOptions.to_dict with default value."""
    options = ContextOptions()
    result = options.to_dict()
    assert result == {"ReplayChildren": False}


def test_context_options_to_dict_false():
    """Test ContextOptions.to_dict with replay_children=False."""
    options = ContextOptions(replay_children=False)
    result = options.to_dict()
    assert result == {"ReplayChildren": False}


def test_invoke_options_from_dict_missing_function_name():
    """Test InvokeOptions.from_dict with missing required FunctionName."""
    data = {"TimeoutSeconds": 60}

    with pytest.raises(KeyError):
        InvokeOptions.from_dict(data)


def test_invoke_options_to_dict_complete():
    """Test InvokeOptions.to_dict with all fields."""
    options = InvokeOptions(function_name="test_func", timeout_seconds=120)

    result = options.to_dict()

    assert result["FunctionName"] == "test_func"
    assert result["TimeoutSeconds"] == 120


# =============================================================================
# Tests for OperationUpdate Class
# =============================================================================


def test_operation_update_create_invoke_start():
    """Test OperationUpdate.create_invoke_start method to cover line 545."""
    identifier = OperationIdentifier("test-id", "parent-id")
    invoke_options = InvokeOptions("test-func", 120)
    update = OperationUpdate.create_invoke_start(identifier, "payload", invoke_options)
    assert update.operation_id == "test-id"


def test_operation_update_to_dict():
    """Test OperationUpdate.to_dict method."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    step_options = StepOptions(next_attempt_delay_seconds=30)

    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        parent_id="parent1",
        name="test_step",
        payload="test_payload",
        error=error,
        step_options=step_options,
    )

    result = update.to_dict()
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Action": "RETRY",
        "ParentId": "parent1",
        "Name": "test_step",
        "Payload": "test_payload",
        "Error": {"ErrorMessage": "Test error", "ErrorType": "TestError"},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
    }
    assert result == expected


def test_operation_update_to_dict_complete():
    """Test OperationUpdate.to_dict with all optional fields."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    step_options = StepOptions(next_attempt_delay_seconds=30)
    wait_options = WaitOptions(wait_seconds=60)
    callback_options = CallbackOptions(
        timeout_seconds=300, heartbeat_timeout_seconds=60
    )
    invoke_options = InvokeOptions(function_name="test_func", timeout_seconds=60)

    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        parent_id="parent1",
        name="test_step",
        payload="test_payload",
        error=error,
        step_options=step_options,
        wait_options=wait_options,
        callback_options=callback_options,
        invoke_options=invoke_options,
    )

    result = update.to_dict()
    expected = {
        "Id": "op1",
        "Type": "STEP",
        "Action": "RETRY",
        "ParentId": "parent1",
        "Name": "test_step",
        "Payload": "test_payload",
        "Error": {"ErrorMessage": "Test error", "ErrorType": "TestError"},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "InvokeOptions": {"FunctionName": "test_func", "TimeoutSeconds": 60},
    }
    assert result == expected


def test_operation_update_minimal():
    """Test OperationUpdate.to_dict with minimal required fields."""
    update = OperationUpdate(
        operation_id="minimal_op",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.START,
    )
    result = update.to_dict()
    expected = {
        "Id": "minimal_op",
        "Type": "EXECUTION",
        "Action": "START",
    }
    assert result == expected


def test_operation_update_create_callback():
    """Test OperationUpdate.create_callback factory method."""
    callback_options = CallbackOptions(timeout_seconds=300)
    update = OperationUpdate.create_callback(
        OperationIdentifier("cb1", None, "test_callback"), callback_options
    )
    assert update.operation_id == "cb1"
    assert update.operation_type is OperationType.CALLBACK
    assert update.action is OperationAction.START
    assert update.name == "test_callback"
    assert update.callback_options == callback_options
    assert update.sub_type is OperationSubType.CALLBACK


def test_operation_update_create_wait_start():
    """Test OperationUpdate.create_wait_start factory method."""
    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate.create_wait_start(
        OperationIdentifier("wait1", "parent1", "test_wait"), wait_options
    )
    assert update.operation_id == "wait1"
    assert update.parent_id == "parent1"
    assert update.operation_type is OperationType.WAIT
    assert update.action is OperationAction.START
    assert update.name == "test_wait"
    assert update.wait_options == wait_options
    assert update.sub_type is OperationSubType.WAIT


@patch("aws_durable_execution_sdk_python.lambda_service.datetime")
def test_operation_update_create_execution_succeed(mock_datetime):
    """Test OperationUpdate.create_execution_succeed factory method."""
    mock_datetime.datetime.now.return_value = "2023-01-01"
    update = OperationUpdate.create_execution_succeed("success_payload")
    assert update.operation_id == "execution-result-2023-01-01"
    assert update.operation_type == OperationType.EXECUTION
    assert update.action == OperationAction.SUCCEED
    assert update.payload == "success_payload"


def test_operation_update_create_step_succeed():
    """Test OperationUpdate.create_step_succeed factory method."""
    update = OperationUpdate.create_step_succeed(
        OperationIdentifier("step1", None, "test_step"), "step_payload"
    )
    assert update.operation_id == "step1"
    assert update.operation_type is OperationType.STEP
    assert update.action is OperationAction.SUCCEED
    assert update.name == "test_step"
    assert update.payload == "step_payload"
    assert update.sub_type is OperationSubType.STEP


def test_operation_update_factory_methods():
    """Test all OperationUpdate factory methods."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )

    # Test create_context_start
    update = OperationUpdate.create_context_start(
        OperationIdentifier("ctx1", None, "test_context"),
        OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    assert update.operation_type is OperationType.CONTEXT
    assert update.action is OperationAction.START
    assert update.sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT

    # Test create_context_succeed
    update = OperationUpdate.create_context_succeed(
        OperationIdentifier("ctx1", None, "test_context"),
        "payload",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    assert update.action is OperationAction.SUCCEED
    assert update.payload == "payload"
    assert update.sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT

    # Test create_context_fail
    update = OperationUpdate.create_context_fail(
        OperationIdentifier("ctx1", None, "test_context"),
        error,
        OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    assert update.action is OperationAction.FAIL
    assert update.error == error
    assert update.sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT

    # Test create_execution_fail
    update = OperationUpdate.create_execution_fail(error)
    assert update.operation_type is OperationType.EXECUTION
    assert update.action is OperationAction.FAIL

    # Test create_step_fail
    update = OperationUpdate.create_step_fail(
        OperationIdentifier("step1", None, "test_step"), error
    )
    assert update.operation_type is OperationType.STEP
    assert update.action is OperationAction.FAIL
    assert update.sub_type is OperationSubType.STEP

    # Test create_step_start
    update = OperationUpdate.create_step_start(
        OperationIdentifier("step1", None, "test_step")
    )
    assert update.action is OperationAction.START
    assert update.sub_type is OperationSubType.STEP

    # Test create_step_retry
    update = OperationUpdate.create_step_retry(
        OperationIdentifier("step1", None, "test_step"), error, 30
    )
    assert update.action is OperationAction.RETRY
    assert update.step_options.next_attempt_delay_seconds == 30
    assert update.sub_type is OperationSubType.STEP


def test_operation_update_with_parent_id():
    """Test OperationUpdate with parent_id field."""
    update = OperationUpdate(
        operation_id="child_op",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent_op",
        name="child_step",
    )

    result = update.to_dict()
    assert result["ParentId"] == "parent_op"


def test_operation_update_wait_and_invoke_types():
    """Test OperationUpdate with WAIT and INVOKE operation types."""
    # Test WAIT operation
    wait_options = WaitOptions(wait_seconds=30)
    wait_update = OperationUpdate(
        operation_id="wait_op",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        wait_options=wait_options,
    )

    result = wait_update.to_dict()
    assert result["Type"] == "WAIT"
    assert result["WaitOptions"]["WaitSeconds"] == 30

    # Test INVOKE operation
    invoke_options = InvokeOptions(function_name="test_func")
    invoke_update = OperationUpdate(
        operation_id="invoke_op",
        operation_type=OperationType.INVOKE,
        action=OperationAction.START,
        invoke_options=invoke_options,
    )

    result = invoke_update.to_dict()
    assert result["Type"] == "INVOKE"
    assert result["InvokeOptions"]["FunctionName"] == "test_func"


def test_operation_update_create_wait():
    """Test OperationUpdate factory method for WAIT operations."""
    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate(
        operation_id="wait1",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        wait_options=wait_options,
    )

    assert update.operation_type == OperationType.WAIT
    assert update.wait_options == wait_options


def test_operation_update_create_invoke():
    """Test OperationUpdate factory method for INVOKE operations."""
    invoke_options = InvokeOptions(function_name="test-function")
    update = OperationUpdate(
        operation_id="invoke1",
        operation_type=OperationType.INVOKE,
        action=OperationAction.START,
        invoke_options=invoke_options,
    )

    assert update.operation_type == OperationType.INVOKE
    assert update.invoke_options == invoke_options


def test_operation_update_with_sub_type():
    """Test OperationUpdate with sub_type field."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        sub_type=OperationSubType.STEP,
    )
    result = update.to_dict()
    assert result["SubType"] == "Step"


def test_operation_update_with_context_options():
    """Test OperationUpdate with context_options field."""
    context_options = ContextOptions(replay_children=True)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        context_options=context_options,
    )
    result = update.to_dict()
    assert result["ContextOptions"] == {"ReplayChildren": True}


def test_operation_update_complete_with_new_fields():
    """Test OperationUpdate.to_dict with all fields including new ones."""
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    context_options = ContextOptions(replay_children=True)
    step_options = StepOptions(next_attempt_delay_seconds=30)
    wait_options = WaitOptions(wait_seconds=60)
    callback_options = CallbackOptions(
        timeout_seconds=300, heartbeat_timeout_seconds=60
    )
    invoke_options = InvokeOptions(function_name="test_func", timeout_seconds=60)

    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.RETRY,
        parent_id="parent1",
        name="test_context",
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        payload="test_payload",
        error=error,
        context_options=context_options,
        step_options=step_options,
        wait_options=wait_options,
        callback_options=callback_options,
        invoke_options=invoke_options,
    )

    result = update.to_dict()
    expected = {
        "Id": "op1",
        "Type": "CONTEXT",
        "Action": "RETRY",
        "ParentId": "parent1",
        "Name": "test_context",
        "SubType": "RunInChildContext",
        "Payload": "test_payload",
        "Error": {"ErrorMessage": "Test error", "ErrorType": "TestError"},
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "InvokeOptions": {"FunctionName": "test_func", "TimeoutSeconds": 60},
    }
    assert result == expected


# =============================================================================
# Tests for new wait-for-condition factory methods
# =============================================================================


def test_operation_update_create_wait_for_condition_start():
    """Test OperationUpdate.create_wait_for_condition_start factory method."""
    identifier = OperationIdentifier("wait_cond_1", "parent1", "test_wait_condition")
    update = OperationUpdate.create_wait_for_condition_start(identifier)

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.START
    assert update.name == "test_wait_condition"


def test_operation_update_create_wait_for_condition_succeed():
    """Test OperationUpdate.create_wait_for_condition_succeed factory method."""
    identifier = OperationIdentifier("wait_cond_1", "parent1", "test_wait_condition")
    update = OperationUpdate.create_wait_for_condition_succeed(
        identifier, "success_payload"
    )

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.SUCCEED
    assert update.name == "test_wait_condition"
    assert update.payload == "success_payload"


def test_operation_update_create_wait_for_condition_retry():
    """Test OperationUpdate.create_wait_for_condition_retry factory method."""
    identifier = OperationIdentifier("wait_cond_1", "parent1", "test_wait_condition")
    update = OperationUpdate.create_wait_for_condition_retry(
        identifier, "retry_payload", 45
    )

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.RETRY
    assert update.name == "test_wait_condition"
    assert update.payload == "retry_payload"
    assert update.step_options.next_attempt_delay_seconds == 45


def test_operation_update_create_wait_for_condition_fail():
    """Test OperationUpdate.create_wait_for_condition_fail factory method."""
    identifier = OperationIdentifier("wait_cond_1", "parent1", "test_wait_condition")
    error = ErrorObject(
        message="Condition failed", type="ConditionError", data=None, stack_trace=None
    )
    update = OperationUpdate.create_wait_for_condition_fail(identifier, error)

    assert update.operation_id == "wait_cond_1"
    assert update.parent_id == "parent1"
    assert update.operation_type == OperationType.STEP
    assert update.sub_type == OperationSubType.WAIT_FOR_CONDITION
    assert update.action == OperationAction.FAIL
    assert update.name == "test_wait_condition"
    assert update.error == error


# Tests for ContextOptions class


def test_operation_update_to_dict_with_sub_type():
    """Test OperationUpdate.to_dict includes sub_type field when present."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        sub_type=OperationSubType.WAIT_FOR_CONDITION,
    )
    result = update.to_dict()
    assert result["SubType"] == "WaitForCondition"


def test_operation_update_to_dict_without_sub_type():
    """Test OperationUpdate.to_dict excludes sub_type field when None."""
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    result = update.to_dict()
    assert "SubType" not in result


def test_operation_update_with_all_none_values():
    """Test OperationUpdate.to_dict with None values for optional fields."""
    update = OperationUpdate(
        operation_id="test",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    result = update.to_dict()

    # Should only contain required fields
    assert result["Id"] == "test"
    assert result["Type"] == "STEP"
    assert result["Action"] == "START"
    assert "ParentId" not in result
    assert "Name" not in result
    assert "Payload" not in result


def test_operation_update_from_dict_with_minimal_data():
    """Test OperationUpdate.from_dict with minimal required data."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
    }

    update = OperationUpdate.from_dict(data)
    assert update.operation_id == "test-id"
    assert update.operation_type == OperationType.STEP
    assert update.action == OperationAction.START
    assert update.parent_id is None
    assert update.name is None


def test_operation_update_from_dict_with_error_only():
    """Test OperationUpdate.from_dict with Error field only."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "FAIL",
        "Error": {"ErrorMessage": "Test error"},
    }

    update = OperationUpdate.from_dict(data)
    assert update.error is not None
    assert update.error.message == "Test error"


def test_operation_update_from_dict_with_all_options():
    """Test OperationUpdate.from_dict with all option types."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "InvokeOptions": {"FunctionName": "test_func", "TimeoutSeconds": 120},
    }

    update = OperationUpdate.from_dict(data)
    assert update.operation_id == "test-id"
    assert update.operation_type == OperationType.STEP
    assert update.action == OperationAction.START
    assert update.context_options is not None
    assert update.step_options is not None
    assert update.wait_options is not None
    assert update.callback_options is not None
    assert update.invoke_options is not None


# =============================================================================
# Tests for Operation Class
# =============================================================================


def test_operation_from_dict_with_all_options():
    """Test Operation.from_dict with all option types to cover lines 339-361."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "ParentId": "parent-id",
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "InvokeOptions": {"FunctionName": "test-func", "TimeoutSeconds": 120},
    }
    operation = Operation.from_dict(data)
    assert operation.operation_id == "test-id"


def test_operation_from_dict_no_options():
    """Test Operation.from_dict without options to cover None assignments."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
    }
    operation = Operation.from_dict(data)
    assert operation.operation_id == "test-id"


def test_operation_from_dict_individual_options():
    """Test Operation.from_dict with each option type individually."""
    # Test with just ContextOptions
    data1 = {
        "Id": "test1",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "ContextOptions": {"ReplayChildren": True},
    }
    op1 = Operation.from_dict(data1)
    assert op1.operation_id == "test1"

    # Test with just StepOptions
    data2 = {
        "Id": "test2",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "StepOptions": {"NextAttemptDelaySeconds": 30},
    }
    op2 = Operation.from_dict(data2)
    assert op2.operation_id == "test2"

    # Test with just WaitOptions
    data3 = {
        "Id": "test3",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "WaitOptions": {"WaitSeconds": 60},
    }
    op3 = Operation.from_dict(data3)
    assert op3.operation_id == "test3"

    # Test with just CallbackOptions
    data4 = {
        "Id": "test4",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "CallbackOptions": {"TimeoutSeconds": 300},
    }
    op4 = Operation.from_dict(data4)
    assert op4.operation_id == "test4"

    # Test with just InvokeOptions
    data5 = {
        "Id": "test5",
        "Type": "STEP",
        "Action": "START",
        "Status": "PENDING",
        "InvokeOptions": {"FunctionName": "test-func"},
    }
    op5 = Operation.from_dict(data5)
    assert op5.operation_id == "test5"


def test_operation_from_dict_with_all_option_types():
    """Test Operation.from_dict with all option types present."""
    data = {
        "Id": "test-id",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ContextOptions": {"ReplayChildren": True},
        "StepOptions": {"NextAttemptDelaySeconds": 30},
        "WaitOptions": {"WaitSeconds": 60},
        "CallbackOptions": {"TimeoutSeconds": 300, "HeartbeatTimeoutSeconds": 60},
        "InvokeOptions": {"FunctionName": "test_func", "TimeoutSeconds": 120},
    }

    operation = Operation.from_dict(data)
    assert operation.operation_id == "test-id"
    assert operation.operation_type == OperationType.STEP
    assert operation.status == OperationStatus.SUCCEEDED


def test_operation_to_dict_with_all_details():
    """Test Operation.to_dict with all detail types."""
    execution_details = ExecutionDetails(input_payload="exec_payload")
    context_details = ContextDetails(
        result="context_result", error=None, replay_children=True
    )
    step_details = StepDetails(
        attempt=2, next_attempt_timestamp="2023-01-01", result="step_result", error=None
    )
    wait_details = WaitDetails(
        scheduled_timestamp=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC)
    )
    callback_details = CallbackDetails(
        callback_id="cb123", result="callback_result", error=None
    )
    invoke_details = InvokeDetails(
        durable_execution_arn="arn:test", result="invoke_result", error=None
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent",
        name="test_op",
        start_timestamp=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        end_timestamp=datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC),
        execution_details=execution_details,
        context_details=context_details,
        step_details=step_details,
        wait_details=wait_details,
        callback_details=callback_details,
        invoke_details=invoke_details,
        sub_type=OperationSubType.STEP,
    )

    result = operation.to_dict()

    assert result["ExecutionDetails"]["InputPayload"] == "exec_payload"
    assert result["ContextDetails"]["Result"] == "context_result"
    assert result["StepDetails"]["Attempt"] == 2
    assert result["WaitDetails"]["ScheduledTimestamp"] == datetime.datetime(
        2023, 1, 1, tzinfo=datetime.UTC
    )
    assert result["CallbackDetails"]["CallbackId"] == "cb123"
    assert result["InvokeDetails"]["DurableExecutionArn"] == "arn:test"


def test_operation_to_dict_with_step_details_partial():
    """Test Operation.to_dict with step_details having some None fields."""
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=None, result=None, error=None
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )

    result = operation.to_dict()
    step_dict = result["StepDetails"]
    assert step_dict["Attempt"] == 1
    assert "NextAttemptTimestamp" not in step_dict
    assert "Result" not in step_dict
    assert "Error" not in step_dict


def test_operation_to_dict_with_callback_details_partial():
    """Test Operation.to_dict with callback_details having some None fields."""
    callback_details = CallbackDetails(callback_id="cb123", result=None, error=None)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.PENDING,
        callback_details=callback_details,
    )

    result = operation.to_dict()
    callback_dict = result["CallbackDetails"]
    assert callback_dict["CallbackId"] == "cb123"
    assert "Result" not in callback_dict
    assert "Error" not in callback_dict


def test_operation_to_dict_with_invoke_details_partial():
    """Test Operation.to_dict with invoke_details having some None fields."""
    invoke_details = InvokeDetails(
        durable_execution_arn="arn:test", result=None, error=None
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.INVOKE,
        status=OperationStatus.PENDING,
        invoke_details=invoke_details,
    )

    result = operation.to_dict()
    invoke_dict = result["InvokeDetails"]
    assert invoke_dict["DurableExecutionArn"] == "arn:test"
    assert "Result" not in invoke_dict
    assert "Error" not in invoke_dict


def test_operation_to_dict_with_context_details_complete():
    """Test Operation.to_dict with context_details having all fields."""
    error = ErrorObject(
        message="Context error", type="ContextError", data=None, stack_trace=None
    )
    context_details = ContextDetails(
        result="context_result", error=error, replay_children=True
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=context_details,
    )

    result = operation.to_dict()
    context_dict = result["ContextDetails"]
    assert context_dict["Result"] == "context_result"
    # Note: The current implementation only includes Result, not error or replay_children


def test_operation_to_dict_with_execution_details_none():
    """Test Operation.to_dict with execution_details having None input_payload."""
    execution_details = ExecutionDetails(input_payload=None)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=execution_details,
    )

    result = operation.to_dict()
    exec_dict = result["ExecutionDetails"]
    assert exec_dict["InputPayload"] is None


def test_operation_to_dict_with_step_details_error():
    """Test Operation.to_dict with step_details having error."""
    error = ErrorObject(
        message="Step failed", type="StepError", data=None, stack_trace=None
    )
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=None, result=None, error=error
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=step_details,
    )

    result = operation.to_dict()
    step_dict = result["StepDetails"]
    assert step_dict["Error"]["ErrorMessage"] == "Step failed"
    assert step_dict["Error"]["ErrorType"] == "StepError"


def test_operation_to_dict_with_callback_details_error():
    """Test Operation.to_dict with callback_details having error."""
    error = ErrorObject(
        message="Callback failed", type="CallbackError", data=None, stack_trace=None
    )
    callback_details = CallbackDetails(callback_id="cb123", result=None, error=error)

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )

    result = operation.to_dict()
    callback_dict = result["CallbackDetails"]
    assert callback_dict["Error"]["ErrorMessage"] == "Callback failed"
    assert callback_dict["Error"]["ErrorType"] == "CallbackError"


def test_operation_to_dict_with_invoke_details_error():
    """Test Operation.to_dict with invoke_details having error."""
    error = ErrorObject(
        message="Invoke failed", type="InvokeError", data=None, stack_trace=None
    )
    invoke_details = InvokeDetails(
        durable_execution_arn="arn:test", result=None, error=error
    )

    operation = Operation(
        operation_id="test",
        operation_type=OperationType.INVOKE,
        status=OperationStatus.FAILED,
        invoke_details=invoke_details,
    )

    result = operation.to_dict()
    invoke_dict = result["InvokeDetails"]
    assert invoke_dict["Error"]["ErrorMessage"] == "Invoke failed"
    assert invoke_dict["Error"]["ErrorType"] == "InvokeError"


def test_operation_from_dict():
    """Test Operation.from_dict method."""
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ParentId": "parent1",
        "Name": "test_step",
        "StepDetails": {"Result": "step_result"},
    }

    operation = Operation.from_dict(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.parent_id == "parent1"
    assert operation.name == "test_step"
    assert operation.step_details.result == "step_result"


def test_operation_from_dict_with_subtype():
    """Test Operation.from_dict method with SubType field."""
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "SubType": "Step",
    }

    operation = Operation.from_dict(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.sub_type is OperationSubType.STEP


def test_operation_from_dict_complete():
    """Test Operation.from_dict with all fields."""
    start_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.UTC)
    data = {
        "Id": "op1",
        "Type": "STEP",
        "Status": "SUCCEEDED",
        "ParentId": "parent1",
        "Name": "test_step",
        "StartTimestamp": start_time,
        "EndTimestamp": end_time,
        "SubType": "Step",
        "ExecutionDetails": {"InputPayload": "exec_payload"},
        "ContextDetails": {"Result": "context_result"},
        "StepDetails": {"Result": "step_result", "Attempt": 1},
        "WaitDetails": {"ScheduledTimestamp": start_time},
        "CallbackDetails": {"CallbackId": "cb1", "Result": "callback_result"},
        "InvokeDetails": {"DurableExecutionArn": "arn:test", "Result": "invoke_result"},
    }
    operation = Operation.from_dict(data)
    assert operation.operation_id == "op1"
    assert operation.operation_type is OperationType.STEP
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.parent_id == "parent1"
    assert operation.name == "test_step"
    assert operation.start_timestamp == start_time
    assert operation.end_timestamp == end_time
    assert operation.sub_type is OperationSubType.STEP
    assert operation.execution_details.input_payload == "exec_payload"
    assert operation.context_details.result == "context_result"
    assert operation.step_details.result == "step_result"
    assert operation.wait_details.scheduled_timestamp == start_time
    assert operation.callback_details.callback_id == "cb1"
    assert operation.invoke_details.durable_execution_arn == "arn:test"


def test_operation_to_dict_with_subtype():
    """Test Operation.to_dict method includes SubType field."""
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        sub_type=OperationSubType.STEP,
    )
    result = operation.to_dict()
    assert result["SubType"] == "Step"


def test_operation_to_dict_all_optional_fields():
    """Test Operation.to_dict with all optional fields."""

    operation = Operation(
        operation_id="test1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        parent_id="parent1",
        name="test-step",
        start_timestamp=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        end_timestamp=datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC),
        sub_type=OperationSubType.STEP,
    )

    result = operation.to_dict()

    assert result["ParentId"] == "parent1"
    assert result["Name"] == "test-step"
    assert result["StartTimestamp"] == datetime.datetime(
        2023, 1, 1, tzinfo=datetime.UTC
    )
    assert result["EndTimestamp"] == datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC)
    assert result["SubType"] == "Step"


# =============================================================================
# Tests for Checkpoint Classes
# =============================================================================


def test_checkpoint_output_from_dict():
    """Test CheckpointOutput.from_dict method."""
    data = {
        "CheckpointToken": "token123",
        "NewExecutionState": {
            "Operations": [{"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"}],
            "NextMarker": "marker123",
        },
    }
    output = CheckpointOutput.from_dict(data)
    assert output.checkpoint_token == "token123"  # noqa: S105
    assert len(output.new_execution_state.operations) == 1
    assert output.new_execution_state.next_marker == "marker123"


def test_checkpoint_output_from_dict_empty():
    """Test CheckpointOutput.from_dict with empty data."""
    data = {}
    output = CheckpointOutput.from_dict(data)
    assert output.checkpoint_token == ""
    assert len(output.new_execution_state.operations) == 0
    assert output.new_execution_state.next_marker is None


def test_checkpoint_updated_execution_state_from_dict():
    """Test CheckpointUpdatedExecutionState.from_dict method."""
    data = {
        "Operations": [
            {"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"},
            {"Id": "op2", "Type": "WAIT", "Status": "PENDING"},
        ],
        "NextMarker": "marker456",
    }
    state = CheckpointUpdatedExecutionState.from_dict(data)
    assert len(state.operations) == 2
    assert state.next_marker == "marker456"
    assert state.operations[0].operation_id == "op1"
    assert state.operations[1].operation_id == "op2"


def test_checkpoint_updated_execution_state_from_dict_empty():
    """Test CheckpointUpdatedExecutionState.from_dict with empty data."""
    data = {}
    state = CheckpointUpdatedExecutionState.from_dict(data)
    assert len(state.operations) == 0
    assert state.next_marker is None


def test_state_output_from_dict():
    """Test StateOutput.from_dict method."""
    data = {
        "Operations": [
            {"Id": "op1", "Type": "EXECUTION", "Status": "SUCCEEDED"},
        ],
        "NextMarker": "state_marker",
    }
    output = StateOutput.from_dict(data)
    assert len(output.operations) == 1
    assert output.next_marker == "state_marker"
    assert output.operations[0].operation_type is OperationType.EXECUTION


def test_state_output_from_dict_empty():
    """Test StateOutput.from_dict with empty data."""
    data = {}
    output = StateOutput.from_dict(data)
    assert len(output.operations) == 0
    assert output.next_marker is None


def test_state_output_from_dict_empty_operations():
    """Test StateOutput.from_dict with no operations."""
    data = {"NextMarker": "marker123"}  # No Operations key

    output = StateOutput.from_dict(data)
    assert len(output.operations) == 0
    assert output.next_marker == "marker123"


def test_checkpoint_output_from_dict_with_empty_operations():
    """Test CheckpointOutput.from_dict with empty operations list."""
    data = {
        "CheckpointToken": "token123",
        "NewExecutionState": {"Operations": [], "NextMarker": "marker123"},
    }

    output = CheckpointOutput.from_dict(data)
    assert output.checkpoint_token == "token123"  # noqa: S105
    assert len(output.new_execution_state.operations) == 0
    assert output.new_execution_state.next_marker == "marker123"


def test_state_output_from_dict_with_next_marker_only():
    """Test StateOutput.from_dict with NextMarker but no operations."""
    data = {"NextMarker": "marker456"}

    output = StateOutput.from_dict(data)
    assert len(output.operations) == 0
    assert output.next_marker == "marker456"


def test_checkpoint_updated_execution_state_from_dict_with_operations():
    """Test CheckpointUpdatedExecutionState.from_dict with operations."""
    data = {
        "Operations": [
            {"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"},
            {"Id": "op2", "Type": "WAIT", "Status": "PENDING"},
        ],
        "NextMarker": "marker123",
    }

    state = CheckpointUpdatedExecutionState.from_dict(data)
    assert len(state.operations) == 2
    assert state.operations[0].operation_id == "op1"
    assert state.operations[1].operation_id == "op2"
    assert state.next_marker == "marker123"


@patch.dict(
    "os.environ",
    {
        "DURABLE_LOCAL_RUNNER_ENDPOINT": "http://test:5000",
        "DURABLE_LOCAL_RUNNER_REGION": "us-west-1",
    },
)
@patch("aws_durable_execution_sdk_python.lambda_service.boto3")
def test_lambda_client_checkpoint(mock_boto3):
    """Test LambdaClient.checkpoint method."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = lambda_client.checkpoint("arn123", "token123", [update], None)

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


def test_lambda_client_checkpoint_with_client_token():
    """Test LambdaClient.checkpoint method with client_token."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = lambda_client.checkpoint(
        "arn123", "token123", [update], "client-token-123"
    )

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="client-token-123",
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


def test_lambda_client_checkpoint_with_explicit_none_client_token():
    """Test LambdaClient.checkpoint method with explicit None client_token - should not pass ClientToken."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = lambda_client.checkpoint("arn123", "token123", [update], None)

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


def test_lambda_client_checkpoint_with_empty_string_client_token():
    """Test LambdaClient.checkpoint method with empty string client_token - should pass empty string."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = lambda_client.checkpoint("arn123", "token123", [update], "")

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="",
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


def test_lambda_client_checkpoint_with_string_value_client_token():
    """Test LambdaClient.checkpoint method with string value client_token - should pass the value."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = lambda_client.checkpoint("arn123", "token123", [update], "my-client-token")

    mock_client.checkpoint_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Updates=[update.to_dict()],
        ClientToken="my-client-token",
    )
    assert isinstance(result, CheckpointOutput)
    assert result.checkpoint_token == "new_token"  # noqa: S105


def test_lambda_client_checkpoint_with_exception():
    """Test LambdaClient.checkpoint method with exception."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.side_effect = Exception("API Error")

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="op1",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(CheckpointError):
        lambda_client.checkpoint("arn123", "token123", [update], None)


def test_durable_service_client_protocol_checkpoint():
    """Test DurableServiceClient protocol checkpoint method signature."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = CheckpointOutput(
        checkpoint_token="new_token",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(),
    )
    mock_client.checkpoint.return_value = mock_output

    updates = [
        OperationUpdate(
            operation_id="test", operation_type=OperationType.STEP, action="START"
        )
    ]

    result = mock_client.checkpoint("arn123", "token", updates, "client_token")

    mock_client.checkpoint.assert_called_once_with(
        "arn123", "token", updates, "client_token"
    )
    assert result == mock_output


# =============================================================================
# Tests for Client Classes (DurableServiceClient, LambdaClient)
# =============================================================================


def test_lambda_client_constructor():
    """Test LambdaClient constructor to cover lines 931-945."""
    mock_client = Mock()
    client = LambdaClient(mock_client)
    assert isinstance(client, LambdaClient)


@patch.dict("os.environ", {}, clear=True)
@patch("boto3.client")
def test_lambda_client_initialize_from_env_default(mock_boto_client):
    """Test LambdaClient.initialize_from_env with default endpoint."""
    mock_client = Mock()
    mock_boto_client.return_value = mock_client

    with patch.object(LambdaClient, "load_preview_botocore_models"):
        client = LambdaClient.initialize_from_env()

    mock_boto_client.assert_called_with("lambdainternal")
    assert isinstance(client, LambdaClient)


@patch.dict("os.environ", {"AWS_ENDPOINT_URL_LAMBDA": "http://localhost:3000"})
@patch("boto3.client")
def test_lambda_client_initialize_from_env_with_endpoint(mock_boto_client):
    """Test LambdaClient.initialize_from_env with custom endpoint."""
    mock_client = Mock()
    mock_boto_client.return_value = mock_client

    with patch.object(LambdaClient, "load_preview_botocore_models"):
        client = LambdaClient.initialize_from_env()

    mock_boto_client.assert_called_with(
        "lambdainternal", endpoint_url="http://localhost:3000"
    )
    assert isinstance(client, LambdaClient)


@patch("aws_durable_execution_sdk_python.lambda_service.boto3")
def test_lambda_client_initialize_local_runner_client(mock_boto3):
    """Test LambdaClient.initialize_local_runner_client method."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    lambda_client = LambdaClient.initialize_local_runner_client()

    mock_boto3.client.assert_called_once_with(
        "lambdainternal-local",
        endpoint_url="http://host.docker.internal:5000",
        region_name="us-west-2",
    )
    assert lambda_client.client == mock_client


def test_lambda_client_get_execution_state():
    """Test LambdaClient.get_execution_state method."""
    mock_client = Mock()
    mock_client.get_durable_execution_state.return_value = {
        "Operations": [{"Id": "op1", "Type": "STEP", "Status": "SUCCEEDED"}]
    }

    lambda_client = LambdaClient(mock_client)
    result = lambda_client.get_execution_state("arn123", "token123", "marker", 500)

    mock_client.get_durable_execution_state.assert_called_once_with(
        DurableExecutionArn="arn123",
        CheckpointToken="token123",
        Marker="marker",
        MaxItems=500,
    )
    assert len(result.operations) == 1


def test_durable_service_client_protocol_get_execution_state():
    """Test DurableServiceClient protocol get_execution_state method signature."""
    mock_client = Mock(spec=DurableServiceClient)
    mock_output = StateOutput(operations=[], next_marker="marker")
    mock_client.get_execution_state.return_value = mock_output

    result = mock_client.get_execution_state("arn123", "token", "marker", 1000)

    mock_client.get_execution_state.assert_called_once_with(
        "arn123", "token", "marker", 1000
    )
    assert result == mock_output


@patch("aws_durable_execution_sdk_python.lambda_service.boto3")
def test_lambda_client_initialize_local_runner_client_defaults(mock_boto3):
    """Test LambdaClient.initialize_local_runner_client with default environment values."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    lambda_client = LambdaClient.initialize_local_runner_client()

    mock_boto3.client.assert_called_once_with(
        "lambdainternal-local",
        endpoint_url="http://host.docker.internal:5000",
        region_name="us-west-2",
    )
    assert lambda_client.client == mock_client


@patch.dict("os.environ", {}, clear=True)
@patch(
    "aws_durable_execution_sdk_python.lambda_service.LambdaClient.initialize_from_env"
)
def test_lambda_client_initialize_from_env_defaults(mock_init):
    """Test LambdaClient.initialize_from_env with default environment values."""
    LambdaClient.initialize_from_env()
    mock_init.assert_called_once_with()


@patch("os.environ")
def test_lambda_client_load_preview_botocore_models(mock_environ):
    """Test LambdaClient.load_preview_botocore_models method."""
    LambdaClient.load_preview_botocore_models()
    # Verify that AWS_DATA_PATH is set
    assert "AWS_DATA_PATH" in mock_environ.__setitem__.call_args[0]


def test_checkpoint_error_handling():
    """Test CheckpointError exception handling in LambdaClient.checkpoint."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.side_effect = Exception("API Error")

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="test",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    with pytest.raises(CheckpointError):
        lambda_client.checkpoint("arn:test", "token", [update], None)


@patch.dict("os.environ", {}, clear=True)
@patch("boto3.client")
def test_lambda_client_initialize_from_env_no_endpoint(mock_boto_client):
    """Test LambdaClient.initialize_from_env without AWS_ENDPOINT_URL_LAMBDA."""
    mock_client = Mock()
    mock_boto_client.return_value = mock_client

    with patch.object(LambdaClient, "load_preview_botocore_models"):
        client = LambdaClient.initialize_from_env()

    mock_boto_client.assert_called_with("lambdainternal")
    assert isinstance(client, LambdaClient)


def test_lambda_client_checkpoint_with_non_none_client_token():
    """Test LambdaClient.checkpoint with non-None client_token."""
    mock_client = Mock()
    mock_client.checkpoint_durable_execution.return_value = {
        "CheckpointToken": "new_token",
        "NewExecutionState": {"Operations": []},
    }

    lambda_client = LambdaClient(mock_client)
    update = OperationUpdate(
        operation_id="test",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = lambda_client.checkpoint("arn:test", "token", [update], "client_token_123")

    # Verify ClientToken was passed
    mock_client.checkpoint_durable_execution.assert_called_once()
    call_args = mock_client.checkpoint_durable_execution.call_args[1]
    assert call_args["ClientToken"] == "client_token_123"
    assert result.checkpoint_token == "new_token"  # noqa: S105
