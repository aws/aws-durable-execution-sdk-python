"""Unit tests for execution module."""

from datetime import datetime, timezone
from unittest.mock import patch, Mock

import pytest
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
    CallbackDetails,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    IllegalStateException,
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import (
    CheckpointIdempotencyRecord,
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


def test_execution_init():
    """Test Execution initialization."""
    arn = "test-arn"
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = []

    execution = Execution(arn, start_input, operations)

    assert execution.durable_execution_arn == arn
    assert execution.start_input == start_input
    assert execution.operations == operations
    assert execution.updates == []
    assert execution.token_sequence == 0
    assert execution.is_complete is False
    assert execution.consecutive_failed_invocation_attempts == 0


@patch("aws_durable_execution_sdk_python_testing.execution.uuid4")
def test_execution_new(mock_uuid4):
    """Test Execution.new static method."""
    mock_uuid = "test-uuid-123"
    mock_uuid4.return_value = mock_uuid

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id-1234",
    )

    execution = Execution.new(start_input)

    assert (
        execution.durable_execution_arn == str(mock_uuid) + "/test-invocation-id-1234"
    )
    assert execution.start_input == start_input
    assert execution.operations == []


def test_execution_start():
    """Test Execution.start method."""
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
        input='{"key": "value"}',
    )
    execution = Execution("test-arn", start_input, [])

    execution.start(now=mock_now)

    assert len(execution.operations) == 1
    operation = execution.operations[0]
    assert operation.operation_id == "test-invocation-id"
    assert operation.parent_id is None
    assert operation.name == "test-execution"
    assert operation.start_timestamp == mock_now
    assert operation.operation_type == OperationType.EXECUTION
    assert operation.status == OperationStatus.STARTED
    assert operation.execution_details.input_payload == '{"key": "value"}'


def test_get_operation_execution_started():
    """Test get_operation_execution_started method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])
    execution.start()

    result = execution.get_operation_execution_started()

    assert result == execution.operations[0]
    assert result.operation_type == OperationType.EXECUTION


def test_get_operation_execution_started_not_started():
    """Test get_operation_execution_started raises error when not started."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(IllegalStateException, match="execution not started"):
        execution.get_operation_execution_started()


def test_get_new_checkpoint_token():
    """Test get_new_checkpoint_token method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    token1 = execution.get_new_checkpoint_token()
    token2 = execution.get_new_checkpoint_token()

    # cleanup: get_new_checkpoint_token no longer bumps
    # token_sequence, and the used_tokens set was removed.
    # Only Executor.checkpoint_execution advances token_sequence (via
    # advance_token_sequence). Both tokens encode seq=0.
    assert execution.token_sequence == 0
    assert token1 == token2


def test_get_navigable_operations():
    """Test get_navigable_operations method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.get_navigable_operations()

    assert result == operations


def test_get_assertable_operations():
    """Test get_assertable_operations method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution_op = Operation(
        operation_id="exec-op",
        parent_id=None,
        name="execution",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
    )
    step_op = Operation(
        operation_id="step-op",
        parent_id=None,
        name="step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    operations = [execution_op, step_op]
    execution = Execution("test-arn", start_input, operations)

    result = execution.get_assertable_operations()

    assert len(result) == 1
    assert result[0] == step_op


def test_has_pending_operations_with_pending_step():
    """Test has_pending_operations returns True for pending STEP operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.STEP,
            status=OperationStatus.PENDING,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_with_started_wait():
    """Test has_pending_operations returns True for started WAIT operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_with_started_callback():
    """Test has_pending_operations returns True for started CALLBACK operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_with_started_invoke():
    """Test has_pending_operations returns True for started INVOKE operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.STARTED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is True


def test_has_pending_operations_no_pending():
    """Test has_pending_operations returns False when no pending operations."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operations = [
        Operation(
            operation_id="op1",
            parent_id=None,
            name="test",
            start_timestamp=datetime.now(timezone.utc),
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
        )
    ]
    execution = Execution("test-arn", start_input, operations)

    result = execution.has_pending_operations(execution)

    assert result is False


def test_complete_success_with_string_result():
    """Test complete_success method with string result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])

    execution.complete_success("success result")

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.SUCCEEDED
    assert execution.result.result == "success result"


def test_complete_success_with_none_result():
    """Test complete_success method with None result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])

    execution.complete_success(None)

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.SUCCEEDED
    assert execution.result.result is None


def test_complete_fail():
    """Test complete_fail method."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject.from_message("Test error message")

    execution.complete_fail(error)

    assert execution.is_complete is True
    assert execution.result.status == InvocationStatus.FAILED
    assert execution.result.error == error


def test_find_operation_exists():
    """Test find_operation method when operation exists."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="test-op-id",
        parent_id=None,
        name="test",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    index, found_operation = execution.find_operation("test-op-id")

    assert index == 0
    assert found_operation == operation


def test_find_operation_not_exists():
    """Test find_operation method when operation doesn't exist."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(
        IllegalStateException, match="Attempting to update state of an Operation"
    ):
        execution.find_operation("non-existent-id")


def test_complete_wait_success():
    """Test complete_wait method successful completion."""
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_wait("wait-op-id", now=mock_now)

    assert result.status == OperationStatus.SUCCEEDED
    assert result.end_timestamp == mock_now
    # Async completion bumps seq_counter , not
    # token_sequence. token_sequence only advances on
    # accepted checkpoint calls.
    assert execution.token_sequence == 0
    assert execution.seq_counter == 1
    assert execution.operation_last_touched_seq["wait-op-id"] == 1
    assert execution.operations[0] == result


def test_complete_wait_wrong_status():
    """Test complete_wait method with wrong operation status."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.SUCCEEDED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(
        IllegalStateException, match="Attempting to transition a Wait Operation"
    ):
        execution.complete_wait("wait-op-id")


def test_complete_wait_wrong_type():
    """Test complete_wait method with wrong operation type."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(IllegalStateException, match="Expected WAIT operation"):
        execution.complete_wait("step-op-id")


def test_complete_retry_success():
    """Test complete_retry method successful completion."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    step_details = StepDetails(
        next_attempt_timestamp=str(datetime.now(timezone.utc)),
        attempt=1,
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_retry("step-op-id")

    assert result.status == OperationStatus.READY
    assert result.step_details.next_attempt_timestamp is None
    # Async completion bumps seq_counter, not token_sequence.
    assert execution.token_sequence == 0
    assert execution.seq_counter == 1
    assert execution.operations[0] == result


def test_complete_retry_no_step_details():
    """Test complete_retry method with no step_details."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
    )
    execution = Execution("test-arn", start_input, [operation])

    result = execution.complete_retry("step-op-id")

    assert result.status == OperationStatus.READY
    assert result.step_details is None
    # Async completion bumps seq_counter, not token_sequence.
    assert execution.token_sequence == 0
    assert execution.seq_counter == 1


def test_complete_retry_wrong_status():
    """Test complete_retry method with wrong operation status."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="step-op-id",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(
        IllegalStateException, match="Attempting to transition a Step Operation"
    ):
        execution.complete_retry("step-op-id")


def test_complete_retry_wrong_type():
    """Test complete_retry method with wrong operation type."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    operation = Operation(
        operation_id="wait-op-id",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.PENDING,
    )
    execution = Execution("test-arn", start_input, [operation])

    with pytest.raises(IllegalStateException, match="Expected STEP operation"):
        execution.complete_retry("wait-op-id")


def test_status_running():
    """Test status property returns RUNNING for incomplete execution."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [])

    assert execution.current_status().value == "RUNNING"


def test_status_succeeded():
    """Test status property returns SUCCEEDED for successful execution."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    execution.complete_success("success result")

    assert execution.current_status().value == "SUCCEEDED"


def test_status_failed():
    """Test status property returns FAILED for failed execution."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject.from_message("Test error")
    execution.complete_fail(error)

    assert execution.current_status().value == "FAILED"


def test_status_timed_out():
    """Test status property returns TIMED_OUT for timeout errors."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject(
        message="Execution timed out", type="TimeoutError", data=None, stack_trace=None
    )
    execution.complete_timeout(error)

    assert execution.current_status().value == "TIMED_OUT"


def test_status_stopped():
    """Test status property returns STOPPED for stop errors."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [Mock()])
    error = ErrorObject(
        message="Execution stopped", type="StopError", data=None, stack_trace=None
    )
    execution.complete_stopped(error)

    assert execution.current_status().value == "STOPPED"


def test_status_no_result():
    """Test status property returns FAILED for completed execution with no result."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [])
    execution.is_complete = True
    execution.result = None
    with pytest.raises(
        IllegalStateException,
        match="close_status must be set when execution is complete",
    ):
        execution.current_status()


def test_complete_retry_with_step_details():
    """Test complete_retry with operation that has step_details."""
    step_details = StepDetails(
        attempt=1, next_attempt_timestamp=datetime.now(timezone.utc)
    )
    step_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=step_details,
    )

    execution = Execution("test-arn", Mock(), [step_op])

    result = execution.complete_retry("op-1")
    assert result.status == OperationStatus.READY
    assert result.step_details.next_attempt_timestamp is None


def test_complete_retry_without_step_details():
    """Test complete_retry with operation that has no step_details."""
    step_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=None,  # No step details
    )

    execution = Execution("test-arn", Mock(), [step_op])

    result = execution.complete_retry("op-1")
    assert result.status == OperationStatus.READY
    assert result.step_details is None


# endregion retry


def test_from_dict_with_none_result():
    """Test from_dict with None result."""
    data = {
        "DurableExecutionArn": "test-arn",
        "StartInput": {"function_name": "test"},
        "Operations": [],
        "Updates": [],
        "UsedTokens": [],
        "TokenSequence": 0,
        "IsComplete": False,
        "Result": None,  # None result
        "ConsecutiveFailedInvocationAttempts": 0,
        "CloseStatus": None,
    }

    with patch(
        "aws_durable_execution_sdk_python_testing.model.StartDurableExecutionInput.from_dict"
    ) as mock_from_dict:
        mock_from_dict.return_value = Mock()
        execution = Execution.from_json_dict(data)
        assert execution.result is None


# region callback
def test_find_callback_operation_not_found():
    """Test find_callback_operation raises exception when callback not found."""
    execution = Execution("test-arn", Mock(), [])

    with pytest.raises(
        IllegalStateException,
        match="Callback operation with callback_id \\[nonexistent\\] not found",
    ):
        execution.find_callback_operation("nonexistent")


def test_complete_callback_success_not_started():
    """Test complete_callback_success raises exception when callback not in STARTED state."""
    # Create callback operation in wrong state
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,  # Wrong state
        callback_details=CallbackDetails(callback_id="test-id"),
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    with pytest.raises(
        IllegalStateException,
        match="Callback operation \\[test-id\\] is not in STARTED state",
    ):
        execution.complete_callback_success("test-id")


def test_complete_callback_failure_not_started():
    """Test complete_callback_failure raises exception when callback not in STARTED state."""
    # Create callback operation in wrong state
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,  # Wrong state
        callback_details=CallbackDetails(callback_id="test-id"),
    )

    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("test error")

    with pytest.raises(
        IllegalStateException,
        match="Callback operation \\[test-id\\] is not in STARTED state",
    ):
        execution.complete_callback_failure("test-id", error)


def test_complete_callback_success_no_callback_details():
    """Test complete_callback_success with operation that has no callback_details."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    # Test with None result
    result = execution.complete_callback_success("test-id", None)
    assert result.status == OperationStatus.SUCCEEDED


def test_complete_callback_failure_no_callback_details():
    """Test complete_callback_failure with operation that has no callback_details."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("test error")

    # Test with actual callback details
    result = execution.complete_callback_failure("test-id", error)
    assert result.status == OperationStatus.FAILED


# region callback - details


def test_complete_callback_success_with_none_callback_details():
    """Test complete_callback_success when operation has None callback_details."""
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,  # None callback details
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    # Mock find_callback_operation to return this operation
    execution.find_callback_operation = Mock(return_value=(0, callback_op))

    result = execution.complete_callback_success("test-id", b"result")
    assert result.status == OperationStatus.SUCCEEDED
    assert result.callback_details is None


def test_complete_callback_failure_with_none_callback_details():
    """Test complete_callback_failure when operation has None callback_details."""
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,  # None callback details
    )

    execution = Execution("test-arn", Mock(), [callback_op])
    error = ErrorObject.from_message("test error")

    # Mock find_callback_operation to return this operation
    execution.find_callback_operation = Mock(return_value=(0, callback_op))

    result = execution.complete_callback_failure("test-id", error)
    assert result.status == OperationStatus.FAILED
    assert result.callback_details is None


def test_complete_callback_success_with_bytes_result():
    """Test complete_callback_success with bytes result that gets decoded."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    result = execution.complete_callback_success("test-id", b"test result")
    assert result.status == OperationStatus.SUCCEEDED
    assert result.callback_details.result == "test result"


def test_complete_callback_success_with_none_result():
    """Test complete_callback_success with None result."""
    callback_details = CallbackDetails(callback_id="test-id")
    callback_op = Operation(
        operation_id="op-1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )

    execution = Execution("test-arn", Mock(), [callback_op])

    result = execution.complete_callback_success("test-id", None)
    assert result.status == OperationStatus.SUCCEEDED
    assert result.callback_details.result is None


def test_complete_callback_timeout_success():
    """Covers complete_callback_timeout on a STARTED callback op.
    The op transitions to TIMED_OUT with the error recorded in
    callback_details."""
    callback_details = CallbackDetails(callback_id="cb-id")
    callback_op = Operation(
        operation_id="op-timeout",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=callback_details,
    )
    execution = Execution("test-arn", Mock(), [callback_op])
    err = ErrorObject.from_message("timed out")

    result = execution.complete_callback_timeout("cb-id", err)

    assert result.status == OperationStatus.TIMED_OUT
    assert result.callback_details.error == err
    # Async completion bumps seq_counter, not token_sequence.
    assert execution.seq_counter == 1
    assert execution.token_sequence == 0


def test_complete_callback_timeout_wrong_status_raises():
    """complete_callback_timeout on a non-STARTED op raises
    IllegalStateException."""
    callback_details = CallbackDetails(callback_id="cb-id")
    callback_op = Operation(
        operation_id="op-bad",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,  # wrong status
        callback_details=callback_details,
    )
    execution = Execution("test-arn", Mock(), [callback_op])
    err = ErrorObject.from_message("timed out")

    with pytest.raises(
        IllegalStateException, match="Callback operation \\[cb-id\\] is not in STARTED"
    ):
        execution.complete_callback_timeout("cb-id", err)


def test_complete_callback_timeout_without_callback_details():
    """Covers the callback_details is None branch."""
    callback_op = Operation(
        operation_id="op-nodetails",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=None,
    )
    execution = Execution("test-arn", Mock(), [callback_op])
    # find_callback_operation requires callback_details.callback_id to
    # match; an op without details won't be found. Confirm that
    # IllegalStateException flows cleanly.
    with pytest.raises(IllegalStateException):
        execution.complete_callback_timeout(
            "any-id", ErrorObject.from_message("timeout")
        )


# endregion callback -details

# endregion callback

# region new fields, CheckpointIdempotencyRecord, helpers


def _make_start_input() -> StartDurableExecutionInput:
    """Build a StartDurableExecutionInput suitable for round-trip tests."""
    return StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )


def test_execution_new_fields_default_to_safe_values():
    """New persisted fields default so an execution loaded from the
    previous format storage behaves identically to a freshly
    constructed one.
    """
    execution = Execution("test-arn", _make_start_input(), [])

    assert execution.seq_counter == 0
    assert execution.token_sequence == 0
    assert execution.handler_seen_seq == 0
    assert execution.operation_last_touched_seq == {}
    assert execution.operation_size_bytes == {}
    assert execution.needs_reinvoke is False
    assert execution.last_checkpoint is None


def test_execution_round_trip_preserves_new_fields():
    """Round-trip through to_json_dict / from_json_dict keeps all new
    persisted fields intact."""
    execution = Execution("test-arn", _make_start_input(), [])
    execution.seq_counter = 7
    execution.token_sequence = 3
    execution.handler_seen_seq = 5
    execution.operation_last_touched_seq = {"op-A": 2, "op-B": 7}
    execution.operation_size_bytes = {"op-A": 42, "op-B": 100}
    execution.needs_reinvoke = True
    execution.last_checkpoint = CheckpointIdempotencyRecord(
        client_token="c1",
        inbound_checkpoint_token="tok-in",
        outbound_checkpoint_token="tok-out",
        operations=[],
        next_marker=None,
    )

    rehydrated = Execution.from_json_dict(execution.to_json_dict())

    assert rehydrated.seq_counter == 7
    assert rehydrated.token_sequence == 3
    assert rehydrated.handler_seen_seq == 5
    assert rehydrated.operation_last_touched_seq == {"op-A": 2, "op-B": 7}
    assert rehydrated.operation_size_bytes == {"op-A": 42, "op-B": 100}
    assert rehydrated.needs_reinvoke is True
    assert rehydrated.last_checkpoint is not None
    assert rehydrated.last_checkpoint.client_token == "c1"
    assert rehydrated.last_checkpoint.inbound_checkpoint_token == "tok-in"
    assert rehydrated.last_checkpoint.outbound_checkpoint_token == "tok-out"
    assert rehydrated.last_checkpoint.operations == []
    assert rehydrated.last_checkpoint.next_marker is None


def test_execution_from_old_format_dict_uses_safe_defaults():
    """Loading a dict produced by a previous format version of the
    library must not raise and must default the new fields
    (backward compatibility)."""
    old_format = {
        "DurableExecutionArn": "test-arn",
        "StartInput": _make_start_input().to_dict(),
        "Operations": [],
        "Updates": [],
        "InvocationCompletions": [],
        "UsedTokens": [],
        "TokenSequence": 0,
        "IsComplete": False,
        "Result": None,
        "ConsecutiveFailedInvocationAttempts": 0,
        "CloseStatus": None,
    }

    execution = Execution.from_json_dict(old_format)

    assert execution.seq_counter == 0
    assert execution.handler_seen_seq == 0
    assert execution.operation_last_touched_seq == {}
    assert execution.operation_size_bytes == {}
    assert execution.needs_reinvoke is False
    assert execution.last_checkpoint is None


def test_checkpoint_idempotency_record_equality():
    """CheckpointIdempotencyRecord is a frozen dataclass with value
    semantics; equal fields imply equal records (used to verify
    byte-identical replay)."""
    op_list: list = []
    a = CheckpointIdempotencyRecord(
        client_token="c",
        inbound_checkpoint_token="i",
        outbound_checkpoint_token="o",
        operations=op_list,
        next_marker=None,
    )
    b = CheckpointIdempotencyRecord(
        client_token="c",
        inbound_checkpoint_token="i",
        outbound_checkpoint_token="o",
        operations=op_list,
        next_marker=None,
    )

    assert a == b


def test_touch_operation_increments_seq_counter_and_records_per_op_seq():
    """touch_operation bumps seq_counter by 1 and records the new value
    against the operation id.
    + ."""
    execution = Execution("test-arn", _make_start_input(), [])

    execution.touch_operation("op-A")
    execution.touch_operation("op-B")
    execution.touch_operation("op-A")  # re-touch updates the recorded seq

    assert execution.seq_counter == 3
    assert execution.operation_last_touched_seq == {"op-A": 3, "op-B": 2}


def test_advance_token_sequence_returns_new_value_and_leaves_seq_counter():
    """advance_token_sequence bumps token_sequence (returns new value)
    without touching seq_counter."""
    execution = Execution("test-arn", _make_start_input(), [])
    execution.seq_counter = 5  # independent counter, must stay put

    first = execution.advance_token_sequence()
    second = execution.advance_token_sequence()

    assert first == 1
    assert second == 2
    assert execution.token_sequence == 2
    assert execution.seq_counter == 5


# endregion # region OperationPaginatorState


def _make_execution_with_ops(
    op_ids: list[str],
    sizes: dict[str, int] | None = None,
    touched: dict[str, int] | None = None,
    handler_seen_seq: int = 0,
    token_sequence: int = 1,
) -> Execution:
    """Build an Execution with a list of simple STEP operations.

    ``sizes`` populates ``operation_size_bytes`` for byte-bounded paging.
    ``touched`` populates ``operation_last_touched_seq`` for delta tests.
    """
    ops = [
        Operation(
            operation_id=op_id,
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        )
        for op_id in op_ids
    ]
    execution = Execution("test-arn", _make_start_input(), ops)
    execution.token_sequence = token_sequence
    execution.handler_seen_seq = handler_seen_seq
    if sizes:
        execution.operation_size_bytes = dict(sizes)
    if touched:
        execution.operation_last_touched_seq = dict(touched)
    return execution


def test_paginator_page_returns_all_ops_when_max_bytes_is_huge():
    """With an unbounded byte cap, page() returns every op in creation
    order and a null next_marker."""
    execution = _make_execution_with_ops(
        ["A", "B", "C"], sizes={"A": 10, "B": 10, "C": 10}
    )
    paginator = OperationPaginatorState.pin(execution)

    ops, next_marker = paginator.page(None, max_size_bytes=1_000_000)

    assert [op.operation_id for op in ops] == ["A", "B", "C"]
    assert next_marker is None


def test_paginator_page_returns_prefix_and_marker_when_bytes_exceeded():
    """When the byte budget runs out mid-walk, page() returns what fits
    plus a non-null next_marker that resumes the walk."""
    execution = _make_execution_with_ops(
        ["A", "B", "C", "D"], sizes={"A": 100, "B": 100, "C": 100, "D": 100}
    )
    paginator = OperationPaginatorState.pin(execution)

    first_ops, first_marker = paginator.page(None, max_size_bytes=250)

    # 100 + 100 = 200 fits; adding a third (300) would exceed 250.
    assert [op.operation_id for op in first_ops] == ["A", "B"]
    assert first_marker is not None


def test_paginator_page_bounded_by_max_items():
    """With a generous byte cap, max_items bounds the page by count and
    yields a resumable marker; a larger count from the same resume point
    returns the remainder."""
    execution = _make_execution_with_ops(
        ["A", "B", "C", "D", "E"],
        sizes={"A": 1, "B": 1, "C": 1, "D": 1, "E": 1},
    )
    paginator = OperationPaginatorState.pin(execution)

    first_ops, first_marker = paginator.page(
        None, max_size_bytes=1_000_000, max_items=2
    )
    assert [op.operation_id for op in first_ops] == ["A", "B"]
    assert first_marker is not None

    second_ops, second_marker = paginator.page(
        first_marker, max_size_bytes=1_000_000, max_items=100
    )
    assert [op.operation_id for op in second_ops] == ["C", "D", "E"]
    assert second_marker is None


def test_paginator_page_max_items_none_is_unbounded_by_count():
    """max_items=None keeps the byte-only behavior (no count bound)."""
    execution = _make_execution_with_ops(
        ["A", "B", "C"], sizes={"A": 1, "B": 1, "C": 1}
    )
    paginator = OperationPaginatorState.pin(execution)

    ops, next_marker = paginator.page(None, max_size_bytes=1_000_000, max_items=None)

    assert [op.operation_id for op in ops] == ["A", "B", "C"]
    assert next_marker is None


def test_paginator_page_resumes_from_marker():
    """Feeding a next_marker from a previous page() back in resumes the
    walk from the right index. Combined pages equal the full op list."""
    execution = _make_execution_with_ops(
        ["A", "B", "C", "D"], sizes={"A": 100, "B": 100, "C": 100, "D": 100}
    )
    paginator = OperationPaginatorState.pin(execution)

    first_ops, first_marker = paginator.page(None, max_size_bytes=250)
    second_ops, second_marker = paginator.page(first_marker, max_size_bytes=250)

    combined = [op.operation_id for op in first_ops] + [
        op.operation_id for op in second_ops
    ]
    assert combined == ["A", "B", "C", "D"]
    assert second_marker is None


def test_paginator_page_rejects_marker_from_stale_sequence():
    """A marker bound to a different token_sequence than the current
    pin must be rejected with InvalidParameterValueException."""
    execution = _make_execution_with_ops(["A", "B"], sizes={"A": 10, "B": 10})
    paginator_old = OperationPaginatorState.pin(execution)
    _, marker_at_old_seq = paginator_old.page(None, max_size_bytes=15)
    assert marker_at_old_seq is not None

    # Advance the execution's token_sequence and pin a new paginator.
    execution.token_sequence += 1
    paginator_new = OperationPaginatorState.pin(execution)

    with pytest.raises(InvalidParameterValueException):
        paginator_new.page(marker_at_old_seq, max_size_bytes=100)


def test_paginator_page_rejects_out_of_range_marker_index():
    """A marker whose index exceeds the snapshot length must be
    rejected."""
    execution = _make_execution_with_ops(["A"], sizes={"A": 10}, token_sequence=5)
    paginator = OperationPaginatorState.pin(execution)

    # Encode a marker for the pinned sequence but with an out-of-range idx.
    # We do this by paging, mutating, then re-using the round-trip result
    # only if public; otherwise validate through a direct crafted string
    # using the same format the paginator emits. To avoid leaking private
    # encoding details, we construct an explicitly-invalid marker
    # matching the format contract.
    with pytest.raises(InvalidParameterValueException):
        paginator.page("5:99", max_size_bytes=100)


def test_paginator_page_rejects_malformed_marker():
    """Unparseable markers raise InvalidParameterValueException."""
    execution = _make_execution_with_ops(["A"], sizes={"A": 10})
    paginator = OperationPaginatorState.pin(execution)

    with pytest.raises(InvalidParameterValueException):
        paginator.page("totally-not-a-marker", max_size_bytes=100)


def test_paginator_page_handles_zero_size_ops_without_infinite_loop():
    """A zero-size op must not prevent pagination from advancing.
    _size_for uses max(..., 1) to guarantee forward progress."""
    execution = _make_execution_with_ops(["A", "B"], sizes={"A": 0, "B": 0})
    paginator = OperationPaginatorState.pin(execution)

    # Generous byte cap: everything should come back in one page even
    # though per-op recorded size is 0.
    ops, next_marker = paginator.page(None, max_size_bytes=1_000)
    assert [op.operation_id for op in ops] == ["A", "B"]
    assert next_marker is None


def test_unseen_operations_strictly_greater_than_watermark():
    """unseen_operations filters ops whose operation_last_touched_seq >
    handler_seen_seq (strict >)."""
    execution = _make_execution_with_ops(
        ["A", "B", "C"],
        touched={"A": 1, "B": 5, "C": 7},
        handler_seen_seq=5,
    )
    paginator = OperationPaginatorState.pin(execution)

    unseen = paginator.unseen_operations()

    # B is at the watermark (not greater than); only C is strictly above.
    assert [op.operation_id for op in unseen] == ["C"]


def test_unseen_operations_returns_empty_when_everything_at_or_below_watermark():
    """Fully-caught-up watermark yields no unseen ops."""
    execution = _make_execution_with_ops(
        ["A", "B"],
        touched={"A": 3, "B": 3},
        handler_seen_seq=3,
    )
    paginator = OperationPaginatorState.pin(execution)

    assert paginator.unseen_operations() == []


def test_unseen_operations_treats_untouched_ops_as_seq_zero():
    """Ops absent from operation_last_touched_seq (e.g. the initial
    EXECUTION op) default to touch-seq 0 and never appear in a delta
    once the watermark has advanced."""
    execution = _make_execution_with_ops(
        ["EXEC", "A"],
        touched={"A": 2},  # EXEC missing, A touched at 2
        handler_seen_seq=1,
    )
    paginator = OperationPaginatorState.pin(execution)

    unseen_ids = [op.operation_id for op in paginator.unseen_operations()]
    assert unseen_ids == ["A"]


def test_advance_handler_seen_is_monotonic():
    """advance_handler_seen only advances forward; smaller seqs are
    ignored."""
    execution = _make_execution_with_ops(["A"])
    execution.handler_seen_seq = 10
    paginator = OperationPaginatorState.pin(execution)

    paginator.advance_handler_seen(5)
    assert execution.handler_seen_seq == 10  # unchanged

    paginator.advance_handler_seen(10)
    assert execution.handler_seen_seq == 10  # equal, no advance

    paginator.advance_handler_seen(20)
    assert execution.handler_seen_seq == 20


# endregion OperationPaginatorState


def test_start_without_invocation_id_raises():
    """Covers the InvalidParameterValueException branch of start()
    when invocation_id is None (start_input built without an id)."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id=None,  # missing
    )
    execution = Execution("test-arn", start_input, [])

    with pytest.raises(
        InvalidParameterValueException, match="invocation_id is required"
    ):
        execution.start()


def test_complete_wait_records_updated_operation_id():
    """Wait completion happens outside the invocation and is reported on the next input."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution(
        "test-arn",
        start_input,
        [
            Operation(
                operation_id="wait-1",
                operation_type=OperationType.WAIT,
                status=OperationStatus.STARTED,
            )
        ],
    )

    execution.complete_wait("wait-1")

    assert execution.updated_operation_ids == ["wait-1"]


def test_record_invocation_completion_clears_updated_operation_ids():
    """Updated IDs are scoped to the next completed invocation."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-id",
    )
    execution = Execution("test-arn", start_input, [])
    execution.updated_operation_ids = ["wait-1"]

    now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    execution.record_invocation_completion(now, now, "request-1")

    assert execution.updated_operation_ids == []
