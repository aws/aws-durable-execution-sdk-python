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


def test_step_options_to_dict():
    """Test StepOptions.to_dict method."""
    options = StepOptions(next_attempt_delay_seconds=30)
    result = options.to_dict()
    assert result == {"NextAttemptDelaySeconds": 30}


def test_wait_options_to_dict():
    """Test WaitOptions.to_dict method."""
    options = WaitOptions(seconds=60)
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
    wait_options = WaitOptions(seconds=60)
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
    wait_options = WaitOptions(seconds=30)
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
    wait_options = WaitOptions(seconds=30)
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


@patch("aws_durable_execution_sdk_python.lambda_service.boto3")
def test_lambda_client_initialize_from_endpoint_and_region(mock_boto3):
    """Test LambdaClient.initialize_from_endpoint_and_region method."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    lambda_client = LambdaClient.initialize_from_endpoint_and_region(
        "https://test.com", "us-east-1"
    )

    mock_boto3.client.assert_called_once_with(
        "lambdainternal", endpoint_url="https://test.com", region_name="us-east-1"
    )
    assert lambda_client.client == mock_client


@patch.dict(
    "os.environ",
    {"LOCAL_RUNNER_ENDPOINT": "http://test:5000", "LOCAL_RUNNER_REGION": "us-west-1"},
)
@patch("aws_durable_execution_sdk_python.lambda_service.boto3")
def test_lambda_client_initialize_local_runner_client(mock_boto3):
    """Test LambdaClient.initialize_local_runner_client method."""
    mock_client = Mock()
    mock_boto3.client.return_value = mock_client

    lambda_client = LambdaClient.initialize_local_runner_client()

    mock_boto3.client.assert_called_once_with(
        "lambdainternal-local", endpoint_url="http://test:5000", region_name="us-west-1"
    )
    assert lambda_client.client == mock_client


@patch.dict(
    "os.environ", {"DEX_ENDPOINT": "https://lambda.test.com", "DEX_REGION": "eu-west-1"}
)
@patch(
    "aws_durable_execution_sdk_python.lambda_service.LambdaClient.initialize_from_endpoint_and_region"
)
def test_lambda_client_initialize_from_env(mock_init):
    """Test LambdaClient.initialize_from_env method."""
    LambdaClient.initialize_from_env()
    mock_init.assert_called_once_with(
        endpoint="https://lambda.test.com", region="eu-west-1"
    )


def test_lambda_client_checkpoint():
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


def test_lambda_client_stop():
    """Test LambdaClient.stop method."""
    mock_client = Mock()
    mock_client.stop_durable_execution.return_value = {
        "StopDate": "2023-01-01T00:00:00Z"
    }

    lambda_client = LambdaClient(mock_client)
    result = lambda_client.stop("arn:test", b"payload")

    mock_client.stop_durable_execution.assert_called_once_with(
        ExecutionArn="arn:test", Payload=b"payload"
    )
    assert result == "2023-01-01T00:00:00Z"


@pytest.mark.skip(reason="little informal integration test for interactive running.")
def test_lambda_client_with_env_defaults():
    client = LambdaClient.initialize_from_endpoint_and_region(
        "http://127.0.0.1:5000", "us-east-1"
    )
    client.get_execution_state("9692ca80-399d-4f52-8d0a-41acc9cd0492", next_marker="")


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


def test_durable_service_client_protocol_stop():
    """Test DurableServiceClient protocol stop method signature."""
    mock_client = Mock(spec=DurableServiceClient)
    stop_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    mock_client.stop.return_value = stop_time

    result = mock_client.stop("arn:test", b"payload")

    mock_client.stop.assert_called_once_with("arn:test", b"payload")
    assert result == stop_time


def test_operation_update_create_wait():
    """Test OperationUpdate factory method for WAIT operations."""
    wait_options = WaitOptions(seconds=30)
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


def test_operation_to_dict_with_execution_details():
    """Test Operation.to_dict with execution_details field."""
    execution_details = ExecutionDetails(input_payload="test_payload")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=execution_details,
    )
    result = operation.to_dict()
    assert result["ExecutionDetails"] == {"InputPayload": "test_payload"}


def test_operation_to_dict_with_context_details():
    """Test Operation.to_dict with context_details field."""
    context_details = ContextDetails(result="context_result")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=context_details,
    )
    result = operation.to_dict()
    assert result["ContextDetails"] == {"Result": "context_result"}


def test_operation_to_dict_with_step_details_minimal():
    """Test Operation.to_dict with minimal step_details."""
    step_details = StepDetails(attempt=1)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=step_details,
    )
    result = operation.to_dict()
    assert result["StepDetails"] == {"Attempt": 1}


def test_operation_to_dict_with_step_details_complete():
    """Test Operation.to_dict with complete step_details."""
    error = ErrorObject(
        message="Step error", type="StepError", data=None, stack_trace=None
    )
    step_details = StepDetails(
        attempt=2,
        next_attempt_timestamp="2023-01-01T00:00:00Z",
        result="step_result",
        error=error,
    )
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=step_details,
    )
    result = operation.to_dict()
    expected_step_details = {
        "Attempt": 2,
        "NextAttemptTimestamp": "2023-01-01T00:00:00Z",
        "Result": "step_result",
        "Error": {"ErrorMessage": "Step error", "ErrorType": "StepError"},
    }
    assert result["StepDetails"] == expected_step_details


def test_operation_to_dict_with_wait_details():
    """Test Operation.to_dict with wait_details field."""
    timestamp = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    wait_details = WaitDetails(scheduled_timestamp=timestamp)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.WAIT,
        status=OperationStatus.PENDING,
        wait_details=wait_details,
    )
    result = operation.to_dict()
    assert result["WaitDetails"] == {"ScheduledTimestamp": timestamp}


def test_operation_to_dict_with_callback_details_minimal():
    """Test Operation.to_dict with minimal callback_details."""
    callback_details = CallbackDetails(callback_id="cb123")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.PENDING,
        callback_details=callback_details,
    )
    result = operation.to_dict()
    assert result["CallbackDetails"] == {"CallbackId": "cb123"}


def test_operation_to_dict_with_callback_details_complete():
    """Test Operation.to_dict with complete callback_details."""
    error = ErrorObject(
        message="Callback error", type="CallbackError", data=None, stack_trace=None
    )
    callback_details = CallbackDetails(
        callback_id="cb123",
        result="callback_result",
        error=error,
    )
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=callback_details,
    )
    result = operation.to_dict()
    expected_callback_details = {
        "CallbackId": "cb123",
        "Result": "callback_result",
        "Error": {"ErrorMessage": "Callback error", "ErrorType": "CallbackError"},
    }
    assert result["CallbackDetails"] == expected_callback_details


def test_operation_to_dict_with_invoke_details_minimal():
    """Test Operation.to_dict with minimal invoke_details."""
    invoke_details = InvokeDetails(durable_execution_arn="arn:test")
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.INVOKE,
        status=OperationStatus.PENDING,
        invoke_details=invoke_details,
    )
    result = operation.to_dict()
    assert result["InvokeDetails"] == {"DurableExecutionArn": "arn:test"}


def test_operation_to_dict_with_invoke_details_complete():
    """Test Operation.to_dict with complete invoke_details."""
    error = ErrorObject(
        message="Invoke error", type="InvokeError", data=None, stack_trace=None
    )
    invoke_details = InvokeDetails(
        durable_execution_arn="arn:test",
        result="invoke_result",
        error=error,
    )
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.INVOKE,
        status=OperationStatus.FAILED,
        invoke_details=invoke_details,
    )
    result = operation.to_dict()
    expected_invoke_details = {
        "DurableExecutionArn": "arn:test",
        "Result": "invoke_result",
        "Error": {"ErrorMessage": "Invoke error", "ErrorType": "InvokeError"},
    }
    assert result["InvokeDetails"] == expected_invoke_details


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
    wait_options = WaitOptions(seconds=60)
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


# Tests for new wait-for-condition factory methods
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
def test_context_options_to_dict_false():
    """Test ContextOptions.to_dict with replay_children=False."""
    options = ContextOptions(replay_children=False)
    result = options.to_dict()
    assert result == {"ReplayChildren": False}


# Tests for sub_type field in OperationUpdate.to_dict
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


# Additional tests for LambdaClient factory methods with environment variables
@patch.dict("os.environ", {}, clear=True)
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
    "aws_durable_execution_sdk_python.lambda_service.LambdaClient.initialize_from_endpoint_and_region"
)
def test_lambda_client_initialize_from_env_defaults(mock_init):
    """Test LambdaClient.initialize_from_env with default environment values."""
    LambdaClient.initialize_from_env()
    mock_init.assert_called_once_with(
        endpoint="http://host.docker.internal:5000", region="us-east-1"
    )
