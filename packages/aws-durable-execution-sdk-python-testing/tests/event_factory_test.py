"""Tests for Event factory methods.

This module tests all the event creation factory methods in the Event class.
"""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationType,
    StepDetails,
    OperationUpdate,
    OperationSubType,
    OperationAction,
    StepOptions,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.model import (
    CheckpointDurableExecutionRequest,
    ErrorResponse,
    Event,
    EventCreationContext,
    EventError,
    EventInput,
    EventResult,
    Execution,
    ExecutionStartedDetails,
    LambdaContext,
    StartDurableExecutionInput,
)


# Helper function to create mock operations
def create_mock_operation(
    operation_id: str = "op-1",
    name: str = "test_op",
    parent_id=None,
    status: OperationStatus = OperationStatus.STARTED,
):
    from unittest.mock import Mock

    op = Mock()
    op.operation_id = operation_id
    op.name = name
    op.parent_id = parent_id
    op.status = status
    return op


# region execution-tests
def test_create_execution_started():
    from unittest.mock import Mock
    from aws_durable_execution_sdk_python.lambda_service import ExecutionDetails

    operation = Mock()
    operation.operation_id = "op-1"
    operation.name = "test_execution"
    operation.parent_id = None
    operation.status = OperationStatus.STARTED
    operation.start_timestamp = datetime.now(UTC)
    operation.operation_type = OperationType.EXECUTION
    operation.sub_type = None
    operation.execution_details = ExecutionDetails(input_payload='{"test": "data"}')

    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        include_execution_data=True,
    )
    event = Event.create_execution_event(context)

    assert event.event_type == "ExecutionStarted"
    assert event.operation_id == "op-1"
    assert event.name == "test_execution"
    assert event.execution_started_details.input.payload == '{"test": "data"}'
    assert event.execution_started_details.execution_timeout == 300


def test_create_execution_succeeded():
    from aws_durable_execution_sdk_python.execution import (
        DurableExecutionInvocationOutput,
        InvocationStatus,
    )

    operation = create_mock_operation("op-1", status=OperationStatus.SUCCEEDED)
    operation.end_timestamp = datetime.now(UTC)

    result = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result='{"result": "success"}'
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=2,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        result=result,
        include_execution_data=True,
    )
    event = Event.create_execution_event(context)

    assert event.event_type == "ExecutionSucceeded"
    assert event.execution_succeeded_details.result.payload == '{"result": "success"}'


def test_create_execution_failed():
    from aws_durable_execution_sdk_python.execution import (
        DurableExecutionInvocationOutput,
        InvocationStatus,
    )

    operation = create_mock_operation("op-1", status=OperationStatus.FAILED)
    operation.end_timestamp = datetime.now(UTC)

    error_result = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED,
        error=ErrorObject.from_message("Execution failed"),
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        result=error_result,
        include_execution_data=True,
    )
    event = Event.create_execution_event(context)

    assert event.event_type == "ExecutionFailed"
    assert event.execution_failed_details.error.payload.message == "Execution failed"


def test_create_execution_timed_out():
    from aws_durable_execution_sdk_python.execution import (
        DurableExecutionInvocationOutput,
        InvocationStatus,
    )

    operation = create_mock_operation("op-1", status=OperationStatus.TIMED_OUT)
    operation.end_timestamp = datetime.now(UTC)

    error_result = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED,
        error=ErrorObject.from_message("Execution timed out"),
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=4,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        result=error_result,
        include_execution_data=True,
    )
    event = Event.create_execution_event(context)

    assert event.event_type == "ExecutionTimedOut"
    assert (
        event.execution_timed_out_details.error.payload.message == "Execution timed out"
    )


def test_create_execution_stopped():
    from aws_durable_execution_sdk_python.execution import (
        DurableExecutionInvocationOutput,
        InvocationStatus,
    )

    operation = create_mock_operation("op-1", status=OperationStatus.STOPPED)
    operation.end_timestamp = datetime.now(UTC)

    error_result = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED,
        error=ErrorObject.from_message("Execution stopped"),
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=5,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        result=error_result,
        include_execution_data=True,
    )
    event = Event.create_execution_event(context)

    assert event.event_type == "ExecutionStopped"
    assert event.execution_stopped_details.error.payload.message == "Execution stopped"


def test_create_execution_invalid_status():
    operation = create_mock_operation("op-1", status=OperationStatus.CANCELLED)
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    with pytest.raises(
        InvalidParameterValueException,
        match="Operation status .* is not valid for execution operations",
    ):
        Event.create_execution_event(context)


# endregion execution-tests


# region context-tests
def test_create_context_started():
    operation = create_mock_operation(
        "ctx-1", "test_context", status=OperationStatus.STARTED
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_context_event(context)

    assert event.event_type == "ContextStarted"
    assert event.operation_id == "ctx-1"
    assert event.name == "test_context"
    assert event.context_started_details is not None


def test_create_context_succeeded():
    operation = create_mock_operation("ctx-1", status=OperationStatus.SUCCEEDED)
    operation.context_details = type(
        "MockDetails", (), {"result": '{"context": "result"}', "error": None}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=2,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        include_execution_data=True,
    )
    event = Event.create_context_event(context)

    assert event.event_type == "ContextSucceeded"
    assert event.context_succeeded_details.result.payload == '{"context": "result"}'


def test_create_context_failed():
    operation = create_mock_operation("ctx-1", status=OperationStatus.FAILED)
    error_obj = ErrorObject.from_message("Context failed")
    operation.context_details = type(
        "MockDetails", (), {"result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_context_event(context)

    assert event.event_type == "ContextFailed"
    assert event.context_failed_details.error.payload.message == "Context failed"


def test_create_context_invalid_status():
    operation = create_mock_operation("ctx-1", status=OperationStatus.TIMED_OUT)
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    with pytest.raises(
        InvalidParameterValueException,
        match="Operation status .* is not valid for context operations",
    ):
        Event.create_context_event(context)


# endregion context-tests


# region wait-tests
def test_create_wait_started():
    operation = create_mock_operation("wait-1", status=OperationStatus.STARTED)
    operation.start_timestamp = datetime.fromisoformat("2024-01-01T12:00:00Z")
    operation.wait_details = type(
        "MockDetails",
        (),
        {"scheduled_end_timestamp": datetime.fromisoformat("2024-01-01T12:05:00Z")},
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_wait_event(context)

    assert event.event_type == "WaitStarted"
    assert event.wait_started_details.duration == 300
    assert event.wait_started_details.scheduled_end_timestamp == datetime.fromisoformat(
        "2024-01-01T12:05:00Z"
    )


def test_create_wait_succeeded():
    operation = create_mock_operation("wait-1", status=OperationStatus.SUCCEEDED)
    operation.start_timestamp = datetime.fromisoformat("2024-01-01T12:00:00Z")
    operation.wait_details = type(
        "MockDetails",
        (),
        {"scheduled_end_timestamp": datetime.fromisoformat("2024-01-01T12:05:00Z")},
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=2,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_wait_event(context)

    assert event.event_type == "WaitSucceeded"
    assert event.wait_succeeded_details.duration == 300


def test_create_wait_cancelled():
    operation = create_mock_operation("wait-1", status=OperationStatus.CANCELLED)
    operation.wait_details = None
    mock_operation_update = Mock()
    mock_operation_update.operation_type = OperationType.WAIT
    mock_operation_update.operation_update.action = OperationAction.CANCEL
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        operation_update=mock_operation_update,
    )
    event = Event.create_wait_event(context)

    assert event.event_type == "WaitCancelled"
    assert event.wait_cancelled_details is not None


def test_create_wait_invalid_status():
    operation = create_mock_operation("wait-1", status=OperationStatus.FAILED)
    operation.wait_details.scheduled_end_timestamp = operation.start_timestamp = (
        datetime.fromisoformat("2024-01-01T12:00:00Z")
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    with pytest.raises(
        InvalidParameterValueException,
        match="Operation status .* is not valid for wait operations",
    ):
        Event.create_wait_event(context)


# endregion wait-tests


# region step-tests
def test_create_step_started():
    operation = create_mock_operation(
        "step-1", "test_step", status=OperationStatus.STARTED
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_step_event(context)

    assert event.event_type == "StepStarted"
    assert event.operation_id == "step-1"
    assert event.name == "test_step"
    assert event.step_started_details is not None


def test_create_step_succeeded():
    operation = create_mock_operation("step-1", status=OperationStatus.SUCCEEDED)
    operation.step_details = type(
        "MockDetails", (), {"result": '{"step": "result"}', "error": None}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=2,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        include_execution_data=True,
    )
    event = Event.create_step_event(context)

    assert event.event_type == "StepSucceeded"
    assert event.step_succeeded_details.result.payload == '{"step": "result"}'


def test_create_step_failed():
    operation = create_mock_operation("step-1", status=OperationStatus.FAILED)
    error_obj = ErrorObject.from_message("Step failed")
    operation.step_details = type(
        "MockDetails", (), {"result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_step_event(context)

    assert event.event_type == "StepFailed"
    assert event.step_failed_details.error.payload.message == "Step failed"


def test_create_step_invalid_status():
    operation = create_mock_operation("step-1", status=OperationStatus.TIMED_OUT)
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    with pytest.raises(
        InvalidParameterValueException,
        match="Operation status .* is not valid for step operations",
    ):
        Event.create_step_event(context)


# endregion step-tests


# region chained_invoke
def test_create_chained_invoke_started():
    operation = create_mock_operation(
        "invoke-1", "test_invoke", status=OperationStatus.STARTED
    )
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_chained_invoke_event(context)

    assert event.event_type == "ChainedInvokeStarted"
    assert event.operation_id == "invoke-1"
    assert event.name == "test_invoke"
    assert event.chained_invoke_started_details is not None


# endregion callback


# endregion helpers-test


def test_create_chained_invoke_succeeded():
    operation = create_mock_operation("invoke-1", status=OperationStatus.SUCCEEDED)
    operation.chained_invoke_details = type(
        "MockDetails", (), {"result": '{"invoke": "result"}', "error": None}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=2,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        include_execution_data=True,
    )
    event = Event.create_chained_invoke_event(context)

    assert event.event_type == "ChainedInvokeSucceeded"
    assert (
        event.chained_invoke_succeeded_details.result.payload == '{"invoke": "result"}'
    )


def test_create_chained_invoke_failed():
    operation = create_mock_operation("invoke-1", status=OperationStatus.FAILED)
    error_obj = ErrorObject.from_message("Invoke failed")
    operation.chained_invoke_details = type(
        "MockDetails", (), {"result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_chained_invoke_event(context)

    assert event.event_type == "ChainedInvokeFailed"
    assert event.chained_invoke_failed_details.error.payload.message == "Invoke failed"


def test_create_chained_invoke_timed_out():
    operation = create_mock_operation("invoke-1", status=OperationStatus.TIMED_OUT)
    error_obj = ErrorObject.from_message("Invoke timed out")
    operation.chained_invoke_details = type(
        "MockDetails", (), {"result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=4,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_chained_invoke_event(context)

    assert event.event_type == "ChainedInvokeTimedOut"
    assert (
        event.chained_invoke_timed_out_details.error.payload.message
        == "Invoke timed out"
    )


def test_create_chained_invoke_stopped():
    operation = create_mock_operation("invoke-1", status=OperationStatus.STOPPED)
    error_obj = ErrorObject.from_message("Invoke stopped")
    operation.chained_invoke_details = type(
        "MockDetails", (), {"result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=5,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_chained_invoke_event(context)

    assert event.event_type == "ChainedInvokeStopped"
    assert (
        event.chained_invoke_stopped_details.error.payload.message == "Invoke stopped"
    )


def test_create_chained_invoke_invalid_status():
    operation = create_mock_operation("invoke-1", status=OperationStatus.CANCELLED)
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    with pytest.raises(
        InvalidParameterValueException,
        match="Operation status .* is not valid for chained invoke operations",
    ):
        Event.create_chained_invoke_event(context)


# endregion chained_invoke


# region callback-tests
def test_create_callback_started():
    operation = create_mock_operation(
        "callback-1", "test_callback", status=OperationStatus.STARTED
    )
    operation.callback_details = type(
        "MockDetails", (), {"callback_id": "cb-123", "result": None, "error": None}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_callback_event(context)

    assert event.event_type == "CallbackStarted"
    assert event.operation_id == "callback-1"
    assert event.name == "test_callback"
    assert event.callback_started_details.callback_id == "cb-123"


def test_create_callback_succeeded():
    operation = create_mock_operation("callback-1", status=OperationStatus.SUCCEEDED)
    operation.callback_details = type(
        "MockDetails",
        (),
        {"callback_id": None, "result": '{"callback": "result"}', "error": None},
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=2,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        include_execution_data=True,
    )
    event = Event.create_callback_event(context)

    assert event.event_type == "CallbackSucceeded"
    assert event.callback_succeeded_details.result.payload == '{"callback": "result"}'


def test_create_callback_failed():
    operation = create_mock_operation("callback-1", status=OperationStatus.FAILED)
    error_obj = ErrorObject.from_message("Callback failed")
    operation.callback_details = type(
        "MockDetails", (), {"callback_id": None, "result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=3,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_callback_event(context)

    assert event.event_type == "CallbackFailed"
    assert event.callback_failed_details.error.payload.message == "Callback failed"


def test_create_callback_timed_out():
    operation = create_mock_operation("callback-1", status=OperationStatus.TIMED_OUT)
    error_obj = ErrorObject.from_message("Callback timed out")
    operation.callback_details = type(
        "MockDetails", (), {"callback_id": None, "result": None, "error": error_obj}
    )()
    context = EventCreationContext.create(
        operation=operation,
        event_id=4,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    event = Event.create_callback_event(context)

    assert event.event_type == "CallbackTimedOut"
    assert (
        event.callback_timed_out_details.error.payload.message == "Callback timed out"
    )


def test_create_callback_invalid_status():
    operation = create_mock_operation("callback-1", status=OperationStatus.STOPPED)
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )
    with pytest.raises(
        InvalidParameterValueException,
        match="Operation status .* is not valid for callback operations",
    ):
        Event.create_callback_event(context)


# endregion callback-tests


# region model-tests
def test_lambda_context():
    context = LambdaContext(aws_request_id="test-123")
    assert context.aws_request_id == "test-123"
    assert context.get_remaining_time_in_millis() == 900000
    context.log("test message")  # Should not raise


def test_start_durable_execution_input_missing_field():
    with pytest.raises(
        InvalidParameterValueException, match="Missing required field: AccountId"
    ):
        StartDurableExecutionInput.from_dict({})


def test_start_durable_execution_input_to_dict_with_optionals():
    input_obj = StartDurableExecutionInput(
        account_id="123456789",
        function_name="test-func",
        function_qualifier="$LATEST",
        execution_name="test-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="inv-123",
        trace_fields={"key": "value"},
        tenant_id="tenant-123",
        input='{"test": "data"}',
    )
    result = input_obj.to_dict()
    assert result["InvocationId"] == "inv-123"
    assert result["TraceFields"] == {"key": "value"}
    assert result["TenantId"] == "tenant-123"
    assert result["Input"] == '{"test": "data"}'


def test_execution_from_dict_empty_function_arn():
    data = {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789:function:test",
        "DurableExecutionName": "test-exec",
        "Status": "SUCCEEDED",
        "StartTimestamp": 1640995200.0,
    }
    execution = Execution.from_dict(data)
    assert execution.function_arn == ""


def test_execution_to_dict_with_function_arn():
    execution = Execution(
        durable_execution_arn="arn:aws:lambda:us-east-1:123456789:function:test",
        durable_execution_name="test-exec",
        function_arn="arn:aws:lambda:us-east-1:123456789:function:test",
        status="SUCCEEDED",
        start_timestamp=1640995200.0,
    )
    result = execution.to_dict()
    assert "FunctionArn" in result


def test_event_input_from_details():
    from aws_durable_execution_sdk_python.lambda_service import ExecutionDetails

    details = ExecutionDetails(input_payload='{"test": "data"}')
    event_input = EventInput.from_details(details, include=True)
    assert event_input.payload == '{"test": "data"}'
    assert not event_input.truncated

    event_input_truncated = EventInput.from_details(details, include=False)
    assert event_input_truncated.payload is None
    assert event_input_truncated.truncated


def test_event_result_from_details():
    from aws_durable_execution_sdk_python.lambda_service import StepDetails

    details = StepDetails(result='{"result": "success"}')
    event_result = EventResult.from_details(details, include=True)
    assert event_result.payload == '{"result": "success"}'
    assert not event_result.truncated


def test_event_error_from_details():
    from aws_durable_execution_sdk_python.lambda_service import StepDetails

    error_obj = ErrorObject.from_message("Test error")
    details = StepDetails(error=error_obj)
    event_error = EventError.from_details(details)
    assert event_error.payload.message == "Test error"


def test_event_from_dict_with_all_details():
    data = {
        "EventType": "ExecutionStarted",
        "EventTimestamp": datetime.fromisoformat("2024-01-01T12:00:00Z"),
        "EventId": 1,
        "Id": "op-1",
        "Name": "test",
        "ParentId": "parent-1",
        "SubType": "test-subtype",
        "ExecutionStartedDetails": {
            "Input": {"Payload": '{"test": "data"}', "Truncated": False},
            "ExecutionTimeout": 300,
        },
    }
    event = Event.from_dict(data)
    assert event.sub_type == "test-subtype"
    assert event.parent_id == "parent-1"


def test_event_to_dict_with_all_details():
    event = Event(
        event_type="ExecutionStarted",
        event_timestamp=datetime.fromisoformat("2024-01-01T12:00:00Z"),
        event_id=1,
        operation_id="op-1",
        name="test",
        parent_id="parent-1",
        sub_type="test-subtype",
        execution_started_details=ExecutionStartedDetails(
            input=EventInput(payload='{"test": "data"}', truncated=False),
            execution_timeout=300,
        ),
    )
    result = event.to_dict()
    assert result["SubType"] == "test-subtype"
    assert result["ParentId"] == "parent-1"
    assert result["ExecutionStartedDetails"]["ExecutionTimeout"] == 300


def test_error_response_from_dict_nested():
    data = {
        "error": {
            "type": "ValidationError",
            "message": "Invalid input",
            "code": "400",
            "requestId": "req-123",
        }
    }
    error_response = ErrorResponse.from_dict(data)
    assert error_response.error_type == "ValidationError"
    assert error_response.error_message == "Invalid input"
    assert error_response.error_code == "400"
    assert error_response.request_id == "req-123"


def test_error_response_from_dict_flat():
    data = {"type": "ValidationError", "message": "Invalid input"}
    error_response = ErrorResponse.from_dict(data)
    assert error_response.error_type == "ValidationError"
    assert error_response.error_message == "Invalid input"


def test_checkpoint_durable_execution_request_from_dict():
    token: str = "token-123"
    data = {
        "CheckpointToken": token,
        "Updates": [
            {"Id": "op-1", "Type": "STEP", "Action": "START", "SubType": "Step"}
        ],
    }
    request = CheckpointDurableExecutionRequest.from_dict(data, "arn:test")
    assert request.checkpoint_token == token
    assert len(request.updates) == 1
    assert request.updates[0].operation_id == "op-1"


# endregion model-tests


# region from_operation_started_tests
class TestFromOperationStarted:
    """Tests for Event.from_operation_started method."""

    def test_from_operation_started_execution(self):
        """Test converting execution operation to started event."""
        operation = Mock()
        operation.operation_id = "exec-123"
        operation.name = "test_execution"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.EXECUTION
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        execution_details = Mock()
        execution_details.input_payload = '{"test": "data"}'
        operation.execution_details = execution_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
            include_execution_data=True,
        )
        event = Event.create_event_started(context)

        assert event.event_type == "ExecutionStarted"
        assert event.operation_id == "exec-123"
        assert event.name == "test_execution"
        assert event.parent_id == "parent-123"
        assert event.execution_started_details.input.payload == '{"test": "data"}'
        assert not event.execution_started_details.input.truncated

    def test_from_operation_started_execution_no_data(self):
        """Test execution operation with include_execution_data=False."""
        operation = Mock()
        operation.operation_id = "exec-123"
        operation.name = "test_execution"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.EXECUTION
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        execution_details = Mock()
        execution_details.input_payload = '{"test": "data"}'
        operation.execution_details = execution_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
            include_execution_data=False,
        )
        event = Event.create_event_started(context)

        assert event.event_type == "ExecutionStarted"
        assert event.execution_started_details.input.payload is None
        assert event.execution_started_details.input.truncated

    def test_from_operation_started_step(self):
        """Test converting step operation to started event."""
        operation = Mock()
        operation.operation_id = "step-123"
        operation.name = "test_step"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.STEP
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=2,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_started(context)

        assert event.event_type == "StepStarted"
        assert event.operation_id == "step-123"
        assert event.name == "test_step"
        assert event.parent_id == "parent-123"
        assert event.step_started_details is not None

    def test_from_operation_started_wait(self):
        """Test converting wait operation to started event."""
        operation = Mock()
        operation.operation_id = "wait-123"
        operation.name = "test_wait"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.WAIT
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        wait_details = Mock()
        wait_details.scheduled_end_timestamp = datetime(
            2024, 1, 1, 12, 5, 0, tzinfo=UTC
        )
        operation.wait_details = wait_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=3,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_started(context)

        assert event.event_type == "WaitStarted"
        assert event.operation_id == "wait-123"
        assert event.name == "test_wait"
        assert event.parent_id == "parent-123"
        assert event.wait_started_details.duration == 300
        assert (
            event.wait_started_details.scheduled_end_timestamp
            == datetime.fromisoformat("2024-01-01T12:05:00+00:00")
        )

    def test_from_operation_started_callback(self):
        """Test converting callback operation to started event."""
        operation = Mock()
        operation.operation_id = "callback-123"
        operation.name = "test_callback"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CALLBACK
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        callback_details = Mock()
        callback_details.callback_id = "cb-456"
        operation.callback_details = callback_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=4,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_started(context)

        assert event.event_type == "CallbackStarted"
        assert event.operation_id == "callback-123"
        assert event.name == "test_callback"
        assert event.parent_id == "parent-123"
        assert event.callback_started_details.callback_id == "cb-456"

    def test_from_operation_started_chained_invoke(self):
        """Test converting chained invoke operation to started event."""
        operation = Mock()
        operation.operation_id = "invoke-123"
        operation.name = "test_invoke"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CHAINED_INVOKE
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=5,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_started(context)

        assert event.event_type == "ChainedInvokeStarted"
        assert event.operation_id == "invoke-123"
        assert event.name == "test_invoke"
        assert event.parent_id == "parent-123"
        assert event.chained_invoke_started_details is not None

    def test_from_operation_started_context(self):
        """Test converting context operation to started event."""
        operation = Mock()
        operation.operation_id = "context-123"
        operation.name = "test_context"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CONTEXT
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=6,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_started(context)

        assert event.event_type == "ContextStarted"
        assert event.operation_id == "context-123"
        assert event.name == "test_context"
        assert event.parent_id == "parent-123"
        assert event.context_started_details is not None

    def test_from_operation_started_no_timestamp(self):
        """Test error when operation has no start timestamp."""
        operation = Mock()
        operation.start_timestamp = None

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        with pytest.raises(
            InvalidParameterValueException,
            match="Operation start timestamp cannot be None",
        ):
            Event.create_event_started(context)

    def test_from_operation_started_unknown_type(self):
        """Test error with unknown operation type."""
        operation = Mock()
        operation.operation_type = "UNKNOWN_TYPE"
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        with pytest.raises(
            InvalidParameterValueException, match="Unknown operation type: UNKNOWN_TYPE"
        ):
            Event.create_event_started(context)


# endregion from_operation_started_tests


# region from_operation_finished_tests
class TestFromOperationFinished:
    """Tests for Event.from_operation_finished method."""

    def test_from_operation_finished_execution_succeeded(self):
        """Test converting succeeded execution operation to finished event."""
        operation = Mock()
        operation.operation_id = "exec-123"
        operation.name = "test_execution"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.EXECUTION
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "ExecutionSucceeded"
        assert event.operation_id == "exec-123"
        assert event.name == "test_execution"
        assert event.parent_id == "parent-123"

    def test_from_operation_finished_execution_failed(self):
        """Test converting failed execution operation to finished event."""
        operation = Mock()
        operation.operation_id = "exec-123"
        operation.name = "test_execution"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.EXECUTION
        operation.status = OperationStatus.FAILED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "ExecutionFailed"
        assert event.operation_id == "exec-123"

    def test_from_operation_finished_step_with_result(self):
        """Test converting succeeded step operation with result."""
        operation = Mock()
        operation.operation_id = "step-123"
        operation.name = "test_step"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.STEP
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        step_details = Mock()
        step_details.result = '{"result": "success"}'
        step_details.error = None
        operation.step_details = step_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=2,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
            include_execution_data=True,
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "StepSucceeded"
        assert event.operation_id == "step-123"
        assert event.step_succeeded_details.result.payload == '{"result": "success"}'

    def test_from_operation_finished_step_with_error(self):
        """Test converting failed step operation with error."""
        operation = Mock()
        operation.operation_id = "step-123"
        operation.name = "test_step"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.STEP
        operation.status = OperationStatus.FAILED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        step_details = Mock()
        step_details.result = None
        step_details.error = ErrorObject.from_message("Step failed")
        operation.step_details = step_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=2,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "StepFailed"
        assert event.step_failed_details.error.payload.message == "Step failed"

    def test_from_operation_finished_wait_succeeded(self):
        """Test converting succeeded wait operation."""
        operation = Mock()
        operation.operation_id = "wait-123"
        operation.name = "test_wait"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.WAIT
        operation.status = OperationStatus.SUCCEEDED
        operation.start_timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        wait_details = Mock()
        wait_details.scheduled_end_timestamp = datetime(
            2024, 1, 1, 12, 5, 0, tzinfo=UTC
        )
        operation.wait_details = wait_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=3,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "WaitSucceeded"
        assert event.wait_succeeded_details.duration == 300

    def test_from_operation_finished_wait_cancelled(self):
        """Test converting cancelled wait operation."""
        operation = Mock()
        operation.operation_id = "wait-123"
        operation.name = "test_wait"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.WAIT
        operation.status = OperationStatus.CANCELLED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 3, 0, tzinfo=UTC)
        operation.wait_details = None

        context = EventCreationContext.create(
            operation=operation,
            event_id=3,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "WaitCancelled"
        assert event.wait_cancelled_details is not None

    def test_from_operation_finished_callback_succeeded(self):
        """Test converting succeeded callback operation."""
        operation = Mock()
        operation.operation_id = "callback-123"
        operation.name = "test_callback"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CALLBACK
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        callback_details = Mock()
        callback_details.result = '{"callback": "result"}'
        callback_details.error = None
        operation.callback_details = callback_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=4,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
            include_execution_data=True,
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "CallbackSucceeded"
        assert (
            event.callback_succeeded_details.result.payload == '{"callback": "result"}'
        )

    def test_from_operation_finished_callback_timed_out(self):
        """Test converting timed out callback operation."""
        operation = Mock()
        operation.operation_id = "callback-123"
        operation.name = "test_callback"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CALLBACK
        operation.status = OperationStatus.TIMED_OUT
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        callback_details = Mock()
        callback_details.result = None
        callback_details.error = ErrorObject.from_message("Callback timed out")
        operation.callback_details = callback_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=4,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "CallbackTimedOut"
        assert (
            event.callback_timed_out_details.error.payload.message
            == "Callback timed out"
        )

    def test_from_operation_finished_chained_invoke_succeeded(self):
        """Test converting succeeded chained invoke operation."""
        operation = Mock()
        operation.operation_id = "invoke-123"
        operation.name = "test_invoke"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CHAINED_INVOKE
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        chained_invoke_details = Mock()
        chained_invoke_details.result = '{"invoke": "result"}'
        chained_invoke_details.error = None
        operation.chained_invoke_details = chained_invoke_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=5,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
            include_execution_data=True,
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "ChainedInvokeSucceeded"
        assert (
            event.chained_invoke_succeeded_details.result.payload
            == '{"invoke": "result"}'
        )

    def test_from_operation_finished_chained_invoke_stopped(self):
        """Test converting stopped chained invoke operation."""
        operation = Mock()
        operation.operation_id = "invoke-123"
        operation.name = "test_invoke"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CHAINED_INVOKE
        operation.status = OperationStatus.STOPPED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        chained_invoke_details = Mock()
        chained_invoke_details.result = None
        chained_invoke_details.error = ErrorObject.from_message("Invoke stopped")
        operation.chained_invoke_details = chained_invoke_details

        context = EventCreationContext.create(
            operation=operation,
            event_id=5,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "ChainedInvokeStopped"
        assert (
            event.chained_invoke_stopped_details.error.payload.message
            == "Invoke stopped"
        )

    def test_from_operation_finished_context_succeeded(self):
        """Test converting succeeded context operation."""
        operation = Mock()
        operation.operation_id = "context-123"
        operation.name = "test_context"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CONTEXT
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        context_details = Mock()
        context_details.result = '{"context": "result"}'
        context_details.error = None
        operation.context_details = context_details
        operation.result = None
        operation.error = None

        context = EventCreationContext.create(
            operation=operation,
            event_id=6,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
            include_execution_data=True,
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "ContextSucceeded"
        assert event.context_succeeded_details.result.payload == '{"context": "result"}'

    def test_from_operation_finished_context_failed(self):
        """Test converting failed context operation."""
        operation = Mock()
        operation.operation_id = "context-123"
        operation.name = "test_context"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.CONTEXT
        operation.status = OperationStatus.FAILED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        context_details = Mock()
        context_details.result = None
        context_details.error = ErrorObject.from_message("Context failed")
        operation.context_details = context_details
        operation.result = None
        operation.error = None

        context = EventCreationContext.create(
            operation=operation,
            event_id=6,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "ContextFailed"
        assert event.context_failed_details.error.payload.message == "Context failed"

    def test_from_operation_finished_no_end_timestamp(self):
        """Test error when operation has no end timestamp."""
        operation = Mock()
        operation.end_timestamp = None

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        with pytest.raises(
            InvalidParameterValueException,
            match="Operation end timestamp cannot be None",
        ):
            Event.create_event_terminated(context)

    def test_from_operation_finished_invalid_status(self):
        """Test error with invalid operation status."""
        operation = Mock()
        operation.status = OperationStatus.STARTED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        with pytest.raises(
            InvalidParameterValueException,
            match="Operation status must be one of SUCCEEDED, FAILED, TIMED_OUT, STOPPED, or CANCELLED",
        ):
            Event.create_event_terminated(context)

    def test_from_operation_finished_unknown_type(self):
        """Test error with unknown operation type."""
        operation = Mock()
        operation.operation_type = "UNKNOWN_TYPE"
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

        context = EventCreationContext.create(
            operation=operation,
            event_id=1,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        with pytest.raises(
            InvalidParameterValueException, match="Unknown operation type: UNKNOWN_TYPE"
        ):
            Event.create_event_terminated(context)

    def test_from_operation_finished_no_details(self):
        """Test operations with no detail objects."""
        operation = Mock()
        operation.operation_id = "step-123"
        operation.name = "test_step"
        operation.parent_id = "parent-123"
        operation.operation_type = OperationType.STEP
        operation.status = OperationStatus.SUCCEEDED
        operation.end_timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
        operation.step_details = None

        context = EventCreationContext.create(
            operation=operation,
            event_id=2,
            durable_execution_arn="arn:test",
            start_input=StartDurableExecutionInput(
                account_id="123",
                function_name="test",
                function_qualifier="$LATEST",
                execution_name="test",
                execution_timeout_seconds=300,
                execution_retention_period_days=7,
            ),
        )
        event = Event.create_event_terminated(context)

        assert event.event_type == "StepSucceeded"
        assert event.step_succeeded_details.result is None


# endregion from_operation_finished_tests


def test_chained_invoke_pending_details_from_dict():
    """Test ChainedInvokePendingDetails parsing in Event.from_dict."""
    data = {
        "EventType": "ChainedInvokeStarted",
        "EventTimestamp": datetime.now(UTC),
        "ChainedInvokePendingDetails": {
            "Input": {"Payload": "test-input", "Truncated": False},
            "FunctionName": "test-function",
        },
    }

    event = Event.from_dict(data)
    assert event.chained_invoke_pending_details is not None
    assert event.chained_invoke_pending_details.input.payload == "test-input"
    assert event.chained_invoke_pending_details.function_name == "test-function"


def test_event_creation_context_sub_type_property():
    """Test EventCreationContext.sub_type property with and without sub_type."""
    # Test with sub_type
    operation = Mock()
    operation.sub_type = OperationSubType.STEP

    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )

    assert context.sub_type == "Step"

    # Test without sub_type
    operation.sub_type = None
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )

    assert context.sub_type is None


def test_event_creation_context_get_retry_details():
    """Test EventCreationContext.get_retry_details method."""
    operation = Mock()
    operation.step_details = StepDetails(attempt=2)

    operation_update = OperationUpdate(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        step_options=StepOptions(next_attempt_delay_seconds=30),
    )

    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        operation_update=operation_update,
    )

    retry_details = context.get_retry_details()
    assert retry_details is not None
    assert retry_details.current_attempt == 2
    assert retry_details.next_attempt_delay_seconds == 30

    # Test with no step_details
    operation.step_details = None
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        operation_update=operation_update,
    )

    retry_details = context.get_retry_details()
    assert retry_details is None

    # Test with no operation_update
    operation.step_details = StepDetails(attempt=2)
    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
    )

    retry_details = context.get_retry_details()
    assert retry_details is None


def test_create_chained_invoke_event_pending():
    """Test Event.create_chained_invoke_event_pending method."""
    operation = Mock()
    operation.operation_id = "invoke-1"
    operation.name = "test_invoke"
    operation.parent_id = None
    operation.status = OperationStatus.PENDING
    operation.start_timestamp = datetime.now(UTC)
    operation.sub_type = None

    context = EventCreationContext.create(
        operation=operation,
        event_id=1,
        durable_execution_arn="arn:test",
        start_input=StartDurableExecutionInput(
            account_id="123",
            function_name="test",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
        ),
        include_execution_data=True,
    )

    event = Event.create_chained_invoke_event_pending(context)

    assert event.event_type == "ChainedInvokeStarted"
    assert event.operation_id == "invoke-1"
    assert event.name == "test_invoke"
    assert event.chained_invoke_pending_details is not None
    assert event.chained_invoke_pending_details.function_name == "test"
