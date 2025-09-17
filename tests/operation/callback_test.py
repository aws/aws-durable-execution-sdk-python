"""Unit tests for callback handler."""

from unittest.mock import ANY, Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.exceptions import FatalError
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    CallbackOptions,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.operation.callback import (
    create_callback_handler,
    wait_for_callback_handler,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState
from aws_durable_execution_sdk_python.types import DurableContext, StepContext


# region create_callback_handler
def test_create_callback_handler_new_operation_with_config():
    """Test create_callback_handler creates new checkpoint when operation doesn't exist."""
    mock_state = Mock(spec=ExecutionState)

    # First call returns not found, second call returns the created operation
    callback_details = CallbackDetails(callback_id="cb123")
    operation = Operation(
        operation_id="callback1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    config = CallbackConfig(timeout_seconds=300, heartbeat_timeout_seconds=60)

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback1", None, "test_callback"),
        config=config,
    )

    assert result == "cb123"
    expected_operation = OperationUpdate(
        operation_id="callback1",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name="test_callback",
        callback_options=CallbackOptions(
            timeout_seconds=300, heartbeat_timeout_seconds=60
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )
    assert mock_state.get_checkpoint_result.call_count == 2


def test_create_callback_handler_new_operation_without_config():
    """Test create_callback_handler creates new checkpoint without config."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="cb456")
    operation = Operation(
        operation_id="callback2",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback2", None),
        config=None,
    )

    assert result == "cb456"
    expected_operation = OperationUpdate(
        operation_id="callback2",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name=None,
        callback_options=CallbackOptions(),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


def test_create_callback_handler_existing_started_operation():
    """Test create_callback_handler returns existing callback_id for started operation."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="existing_cb123")
    operation = Operation(
        operation_id="callback3",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback3", None),
        config=None,
    )

    assert result == "existing_cb123"
    # Should not create new checkpoint for existing operation
    mock_state.create_checkpoint.assert_not_called()
    mock_state.get_checkpoint_result.assert_called_once_with("callback3")


def test_create_callback_handler_existing_failed_operation():
    """Test create_callback_handler raises error for failed operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_failed.return_value = True
    mock_result.is_started.return_value = False
    msg = "Checkpointed error"
    mock_result.raise_callable_error.side_effect = Exception(msg)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(Exception, match="Checkpointed error"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("callback4", None),
            config=None,
        )

    mock_result.raise_callable_error.assert_called_once()
    mock_state.create_checkpoint.assert_not_called()


def test_create_callback_handler_existing_started_missing_callback_details():
    """Test create_callback_handler raises error when existing started operation has no callback details."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback5",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(FatalError, match="Missing callback details"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("callback5", None),
            config=None,
        )


def test_create_callback_handler_new_operation_missing_callback_details_after_checkpoint():
    """Test create_callback_handler raises error when new operation has no callback details after checkpoint."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback6",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    with pytest.raises(FatalError, match="Missing callback details"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("callback6", None),
            config=None,
        )


def test_create_callback_handler_existing_timed_out_operation():
    """Test create_callback_handler returns existing callback_id for timed out operation."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="timed_out_cb123")
    operation = Operation(
        operation_id="callback_timed_out",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=callback_details,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback_timed_out", None),
        config=None,
    )

    assert result == "timed_out_cb123"
    mock_state.create_checkpoint.assert_not_called()


def test_create_callback_handler_existing_timed_out_missing_callback_details():
    """Test create_callback_handler raises error when timed out operation has no callback details."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback_timed_out_no_details",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(FatalError, match="Missing callback details"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback_timed_out_no_details", None
            ),
            config=None,
        )


# endregion create_callback_handler


# region wait_for_callback_handler
def test_wait_for_callback_handler_basic():
    """Test wait_for_callback_handler with basic parameters."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback789"
    mock_callback.result.return_value = "callback_result"
    mock_context.create_callback.return_value = mock_callback
    mock_context.step = Mock()
    mock_submitter = Mock()

    result = wait_for_callback_handler(mock_context, mock_submitter)

    assert result == "callback_result"
    mock_context.step.assert_called_once()
    mock_callback.result.assert_called_once()


def test_wait_for_callback_handler_with_name_and_config():
    """Test wait_for_callback_handler with name and config."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback999"
    mock_callback.result.return_value = "named_callback_result"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()
    config = CallbackConfig()

    result = wait_for_callback_handler(
        mock_context, mock_submitter, "test_callback", config
    )

    assert result == "named_callback_result"
    mock_context.create_callback.assert_called_once_with(
        name="test_callback create callback id", config=config
    )
    mock_context.step.assert_called_once()


def test_wait_for_callback_handler_submitter_called_with_callback_id():
    """Test wait_for_callback_handler calls submitter with callback_id."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_test_id"
    mock_callback.result.return_value = "test_result"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    def capture_step_call(func, name):
        # Execute the step callable to verify submitter is called correctly
        step_context = Mock(spec=StepContext)
        func(step_context)

    mock_context.step.side_effect = capture_step_call

    wait_for_callback_handler(mock_context, mock_submitter, "test")

    mock_submitter.assert_called_once_with("callback_test_id")


def test_create_callback_handler_with_none_operation_in_result():
    """Test create_callback_handler when CheckpointedResult has None operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = True
    mock_result.is_succeeded.return_value = False
    mock_result.operation = None
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(FatalError, match="Missing callback details"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("none_operation", None),
            config=None,
        )


def test_create_callback_handler_with_negative_timeouts():
    """Test create_callback_handler with negative timeout values in config."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="negative_timeout_cb")
    operation = Operation(
        operation_id="negative_timeout",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    config = CallbackConfig(timeout_seconds=-100, heartbeat_timeout_seconds=-50)

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("negative_timeout", None),
        config=config,
    )

    assert result == "negative_timeout_cb"
    mock_state.create_checkpoint.assert_called_once()


def test_wait_for_callback_handler_with_none_callback_id():
    """Test wait_for_callback_handler when callback has None callback_id."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = None
    mock_callback.result.return_value = "result_with_none_id"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    def execute_step(func, name):
        step_context = Mock(spec=StepContext)
        return func(step_context)

    mock_context.step.side_effect = execute_step

    result = wait_for_callback_handler(mock_context, mock_submitter, "test")

    assert result == "result_with_none_id"
    mock_submitter.assert_called_once_with(None)


def test_wait_for_callback_handler_with_empty_string_callback_id():
    """Test wait_for_callback_handler when callback has empty string callback_id."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = ""
    mock_callback.result.return_value = "result_with_empty_id"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    def execute_step(func, name):
        step_context = Mock(spec=StepContext)
        return func(step_context)

    mock_context.step.side_effect = execute_step

    result = wait_for_callback_handler(mock_context, mock_submitter, "test")

    assert result == "result_with_empty_id"
    mock_submitter.assert_called_once_with("")


def test_wait_for_callback_handler_with_large_data():
    """Test wait_for_callback_handler with large result data."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "large_data_cb"

    large_result = {
        "data": ["item_" + str(i) for i in range(1000)],
        "metadata": {"size": 1000, "type": "large_dataset"},
    }
    mock_callback.result.return_value = large_result
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    result = wait_for_callback_handler(mock_context, mock_submitter, "large_data_test")

    assert result == large_result
    assert len(result["data"]) == 1000


def test_wait_for_callback_handler_with_unicode_names():
    """Test wait_for_callback_handler with unicode characters in names."""
    unicode_names = ["测试回调", "コールバック", "🔄 callback test 🚀"]

    for name in unicode_names:
        mock_context = Mock(spec=DurableContext)
        mock_callback = Mock()
        mock_callback.callback_id = f"unicode_cb_{hash(name) % 1000}"
        mock_callback.result.return_value = f"result_for_{name}"
        mock_context.create_callback.return_value = mock_callback
        mock_submitter = Mock()

        result = wait_for_callback_handler(mock_context, mock_submitter, name)

        assert result == f"result_for_{name}"
        expected_name = f"{name} submitter"
        mock_context.step.assert_called_once_with(func=ANY, name=expected_name)
        mock_context.reset_mock()


def test_create_callback_handler_existing_succeeded_operation():
    """Test create_callback_handler returns existing callback_id for succeeded operation."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="succeeded_cb123")
    operation = Operation(
        operation_id="callback_succeeded",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback_succeeded", None),
        config=None,
    )

    assert result == "succeeded_cb123"
    mock_state.create_checkpoint.assert_not_called()


def test_create_callback_handler_existing_succeeded_missing_callback_details():
    """Test create_callback_handler raises error when succeeded operation has no callback details."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="callback_succeeded_no_details",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(FatalError, match="Missing callback details"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                "callback_succeeded_no_details", None
            ),
            config=None,
        )


def test_create_callback_handler_config_with_zero_timeouts():
    """Test create_callback_handler with config having zero timeout values."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="cb_zero_timeout")
    operation = Operation(
        operation_id="callback_zero",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    config = CallbackConfig(timeout_seconds=0, heartbeat_timeout_seconds=0)

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback_zero", None),
        config=config,
    )

    assert result == "cb_zero_timeout"
    expected_operation = OperationUpdate(
        operation_id="callback_zero",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name=None,
        callback_options=CallbackOptions(
            timeout_seconds=0, heartbeat_timeout_seconds=0
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


def test_create_callback_handler_config_with_large_timeouts():
    """Test create_callback_handler with config having large timeout values."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="cb_large_timeout")
    operation = Operation(
        operation_id="callback_large",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    config = CallbackConfig(timeout_seconds=86400, heartbeat_timeout_seconds=3600)

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("callback_large", None),
        config=config,
    )

    assert result == "cb_large_timeout"
    expected_operation = OperationUpdate(
        operation_id="callback_large",
        parent_id=None,
        operation_type=OperationType.CALLBACK,
        sub_type=OperationSubType.CALLBACK,
        action=OperationAction.START,
        name=None,
        callback_options=CallbackOptions(
            timeout_seconds=86400, heartbeat_timeout_seconds=3600
        ),
    )
    mock_state.create_checkpoint.assert_called_once_with(
        operation_update=expected_operation
    )


def test_create_callback_handler_empty_operation_id():
    """Test create_callback_handler with empty operation_id."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="cb_empty_id")
    operation = Operation(
        operation_id="",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    result = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("", None),
        config=None,
    )

    assert result == "cb_empty_id"


def test_wait_for_callback_handler_submitter_exception_handling():
    """Test wait_for_callback_handler when submitter raises exception."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_exception"
    mock_callback.result.return_value = "exception_result"
    mock_context.create_callback.return_value = mock_callback

    def failing_submitter(callback_id):
        msg = "Submitter failed"
        raise ValueError(msg)

    def step_side_effect(func, name):
        step_context = Mock(spec=StepContext)
        func(step_context)

    mock_context.step.side_effect = step_side_effect

    with pytest.raises(ValueError, match="Submitter failed"):
        wait_for_callback_handler(mock_context, failing_submitter, "test")


def test_wait_for_callback_handler_callback_result_exception():
    """Test wait_for_callback_handler when callback.result() raises exception."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_result_exception"
    mock_callback.result.side_effect = RuntimeError("Callback result failed")
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    with pytest.raises(RuntimeError, match="Callback result failed"):
        wait_for_callback_handler(mock_context, mock_submitter, "test")


def test_wait_for_callback_handler_empty_name_handling():
    """Test wait_for_callback_handler with empty string name."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_empty_name"
    mock_callback.result.return_value = "empty_name_result"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    result = wait_for_callback_handler(mock_context, mock_submitter, "", None)

    assert result == "empty_name_result"
    mock_context.step.assert_called_once()


def test_wait_for_callback_handler_complex_callback_result():
    """Test wait_for_callback_handler with complex callback result."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_complex"
    complex_result = {
        "status": "success",
        "data": [1, 2, 3],
        "metadata": {"timestamp": 123456},
    }
    mock_callback.result.return_value = complex_result
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    result = wait_for_callback_handler(mock_context, mock_submitter, "complex_test")

    assert result == complex_result
    mock_callback.result.assert_called_once()


def test_wait_for_callback_handler_step_name_formatting():
    """Test wait_for_callback_handler step name formatting with various inputs."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_name_format"
    mock_callback.result.return_value = "formatted_result"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    wait_for_callback_handler(mock_context, mock_submitter, "test with spaces")

    step_calls = mock_context.step.call_args_list
    assert len(step_calls) == 1
    _, kwargs = step_calls[0]
    assert kwargs["name"] == "test with spaces submitter"


def test_wait_for_callback_handler_config_propagation():
    """Test wait_for_callback_handler properly passes config to create_callback."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "callback_config_prop"
    mock_callback.result.return_value = "config_result"
    mock_context.create_callback.return_value = mock_callback
    mock_submitter = Mock()

    config = CallbackConfig(timeout_seconds=120, heartbeat_timeout_seconds=30)

    result = wait_for_callback_handler(
        mock_context, mock_submitter, "config_test", config
    )

    assert result == "config_result"
    mock_context.create_callback.assert_called_once_with(
        name="config_test create callback id", config=config
    )


def test_wait_for_callback_handler_with_various_result_types():
    """Test wait_for_callback_handler with various result types."""
    result_types = [None, True, False, 0, 3.14, "", "string", [], {"key": "value"}]

    for i, expected_result in enumerate(result_types):
        mock_context = Mock(spec=DurableContext)
        mock_callback = Mock()
        mock_callback.callback_id = f"type_test_cb_{i}"
        mock_callback.result.return_value = expected_result
        mock_context.create_callback.return_value = mock_callback
        mock_submitter = Mock()

        result = wait_for_callback_handler(
            mock_context, mock_submitter, f"type_test_{i}"
        )

        assert result == expected_result
        assert type(result) is type(expected_result)
        mock_context.reset_mock()


def test_callback_lifecycle_complete_flow():
    """Test complete callback lifecycle from creation to completion."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="lifecycle_cb123")
    operation = Operation(
        operation_id="lifecycle_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "lifecycle_cb123"
    mock_callback.result.return_value = {"status": "completed", "data": "test_data"}
    mock_context.create_callback.return_value = mock_callback

    config = CallbackConfig(timeout_seconds=300, heartbeat_timeout_seconds=60)
    callback_id = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("lifecycle_callback", None),
        config=config,
    )

    assert callback_id == "lifecycle_cb123"

    def mock_submitter(cb_id):
        assert cb_id == "lifecycle_cb123"
        return "submitted"

    def execute_step(func, name):
        step_context = Mock(spec=StepContext)
        return func(step_context)

    mock_context.step.side_effect = execute_step

    result = wait_for_callback_handler(
        mock_context, mock_submitter, "lifecycle_test", config
    )

    assert result == {"status": "completed", "data": "test_data"}


def test_callback_retry_scenario():
    """Test callback behavior during retry scenarios."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="retry_cb456")
    operation = Operation(
        operation_id="retry_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    callback_id_1 = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("retry_callback", None),
        config=None,
    )
    callback_id_2 = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("retry_callback", None),
        config=None,
    )

    assert callback_id_1 == callback_id_2 == "retry_cb456"
    mock_state.create_checkpoint.assert_not_called()


def test_callback_timeout_configuration():
    """Test callback with various timeout configurations."""
    test_cases = [(0, 0), (30, 10), (3600, 300), (86400, 3600)]

    for timeout_seconds, heartbeat_timeout_seconds in test_cases:
        mock_state = Mock(spec=ExecutionState)
        callback_details = CallbackDetails(callback_id=f"timeout_cb_{timeout_seconds}")
        operation = Operation(
            operation_id=f"timeout_callback_{timeout_seconds}",
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.STARTED,
            callback_details=callback_details,
        )
        mock_state.get_checkpoint_result.side_effect = [
            CheckpointedResult.create_not_found(),
            CheckpointedResult.create_from_operation(operation),
        ]

        config = CallbackConfig(
            timeout_seconds=timeout_seconds,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )

        callback_id = create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier(
                f"timeout_callback_{timeout_seconds}", None
            ),
            config=config,
        )

        assert callback_id == f"timeout_cb_{timeout_seconds}"


def test_callback_error_propagation():
    """Test error propagation through callback operations."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = Mock(spec=CheckpointedResult)
    mock_result.is_failed.return_value = True
    msg = "Callback creation failed"
    mock_result.raise_callable_error.side_effect = RuntimeError(msg)
    mock_state.get_checkpoint_result.return_value = mock_result

    with pytest.raises(RuntimeError, match="Callback creation failed"):
        create_callback_handler(
            state=mock_state,
            operation_identifier=OperationIdentifier("error_callback", None),
            config=None,
        )

    mock_context = Mock(spec=DurableContext)
    mock_context.create_callback.side_effect = ValueError("Context creation failed")

    with pytest.raises(ValueError, match="Context creation failed"):
        wait_for_callback_handler(mock_context, Mock(), "error_test")


def test_callback_with_complex_submitter():
    """Test callback with complex submitter logic."""
    mock_context = Mock(spec=DurableContext)
    mock_callback = Mock()
    mock_callback.callback_id = "complex_cb789"
    mock_callback.result.return_value = "complex_result"
    mock_context.create_callback.return_value = mock_callback

    submission_log = []

    def complex_submitter(callback_id):
        submission_log.append(f"received_id: {callback_id}")
        if callback_id == "complex_cb789":
            submission_log.append("api_call_success")
            return {"submitted": True, "callback_id": callback_id}

        submission_log.append("api_call_failed")
        msg = "Invalid callback ID"
        raise ValueError(msg)

    def execute_step(func, name):
        step_context = Mock(spec=StepContext)
        return func(step_context)

    mock_context.step.side_effect = execute_step

    result = wait_for_callback_handler(mock_context, complex_submitter, "complex_test")

    assert result == "complex_result"
    assert submission_log == ["received_id: complex_cb789", "api_call_success"]


def test_callback_state_consistency():
    """Test callback state consistency across multiple operations."""
    mock_state = Mock(spec=ExecutionState)

    callback_details = CallbackDetails(callback_id="consistent_cb")
    started_operation = Operation(
        operation_id="consistent_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    succeeded_operation = Operation(
        operation_id="consistent_callback",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=callback_details,
    )

    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(started_operation),
    ]

    callback_id_1 = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("consistent_callback", None),
        config=None,
    )

    mock_state.get_checkpoint_result.side_effect = None
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(succeeded_operation)
    )

    callback_id_2 = create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("consistent_callback", None),
        config=None,
    )

    assert callback_id_1 == callback_id_2 == "consistent_cb"


def test_callback_name_variations():
    """Test callback operations with various name formats."""
    name_test_cases = [
        None,
        "",
        "simple",
        "name with spaces",
        "name-with-dashes",
        "name_with_underscores",
        "name.with.dots",
        "name with special chars: !@#$%^&*()",
    ]

    for name in name_test_cases:
        mock_context = Mock(spec=DurableContext)
        mock_callback = Mock()
        mock_callback.callback_id = f"name_test_{hash(str(name)) % 1000}"
        mock_callback.result.return_value = f"result_for_{name}"
        mock_context.create_callback.return_value = mock_callback
        mock_submitter = Mock()

        result = wait_for_callback_handler(mock_context, mock_submitter, name)

        assert result == f"result_for_{name}"
        expected_name = f"{name} submitter" if name else "submitter"
        mock_context.step.assert_called_once_with(func=ANY, name=expected_name)
        mock_context.reset_mock()


@patch("aws_durable_execution_sdk_python.operation.callback.OperationUpdate")
def test_callback_operation_update_creation(mock_operation_update):
    """Test that OperationUpdate.create_callback is called with correct parameters."""
    mock_state = Mock(spec=ExecutionState)
    callback_details = CallbackDetails(callback_id="update_test_cb")
    operation = Operation(
        operation_id="update_test",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    mock_state.get_checkpoint_result.side_effect = [
        CheckpointedResult.create_not_found(),
        CheckpointedResult.create_from_operation(operation),
    ]

    config = CallbackConfig(timeout_seconds=600, heartbeat_timeout_seconds=120)

    create_callback_handler(
        state=mock_state,
        operation_identifier=OperationIdentifier("update_test", None),
        config=config,
    )

    mock_operation_update.create_callback.assert_called_once_with(
        identifier=OperationIdentifier("update_test", None),
        callback_options=CallbackOptions(
            timeout_seconds=600, heartbeat_timeout_seconds=120
        ),
    )


# endregion wait_for_callback_handler
