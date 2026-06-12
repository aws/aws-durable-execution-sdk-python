"""Tests for base operation processor."""

import datetime
from datetime import timedelta
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ChainedInvokeDetails,
    ChainedInvokeOptions,
    ContextDetails,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepDetails,
    WaitDetails,
    WaitOptions,
    ContextOptions,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)


def test_process_not_implemented():
    processor = OperationProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    try:
        processor.process(update, None, Mock(), "test-arn")
        pytest.fail("Expected NotImplementedError")
    except NotImplementedError:
        pass


class MockProcessor(OperationProcessor):
    """Mock processor for testing base functionality."""

    def process(self, update, current_op, notifier, execution_arn):
        return self._translate_update_to_operation(
            update, current_op, OperationStatus.STARTED
        )

    def translate_update(self, update, current_op, status):
        """Public method to access _translate_update_to_operation for testing."""
        return self._translate_update_to_operation(update, current_op, status)

    def get_end_time(self, current_op, status):
        """Public method to access _get_end_time for testing."""
        return self._get_end_time(current_op, status)

    def create_execution_details(self, update):
        """Public method to access _create_execution_details for testing."""
        return self._create_execution_details(update)

    def create_context_details(self, update):
        """Public method to access _create_context_details for testing."""
        return self._create_context_details(update)

    def create_step_details(self, update, current_operation):
        """Public method to access _create_step_details for testing."""
        return self._create_step_details(update, current_operation)

    def create_callback_details(self, update):
        """Public method to access _create_callback_details for testing."""
        return self._create_callback_details(update)

    def create_invoke_details(self, update):
        """Public method to access _create_invoke_details for testing."""
        return self._create_invoke_details(update)

    def create_wait_details(self, update, current_op):
        """Public method to access _create_wait_details for testing."""
        return self._create_wait_details(update, current_op)


def test_get_end_time_with_existing_end_timestamp():
    processor = MockProcessor()
    end_time = datetime.datetime.now(tz=datetime.UTC)
    current_op = Mock()
    current_op.end_timestamp = end_time

    result = processor.get_end_time(current_op, OperationStatus.STARTED)

    assert result == end_time


def test_get_end_time_with_terminal_status():
    processor = MockProcessor()
    current_op = Mock()
    current_op.end_timestamp = None

    result = processor.get_end_time(current_op, OperationStatus.SUCCEEDED)

    assert result is not None
    assert isinstance(result, datetime.datetime)


def test_get_end_time_with_non_terminal_status():
    processor = MockProcessor()
    current_op = Mock()
    current_op.end_timestamp = None

    result = processor.get_end_time(current_op, OperationStatus.STARTED)

    assert result is None


def test_create_execution_details():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.START,
        payload="test-payload",
    )

    result = processor.create_execution_details(update)

    assert isinstance(result, ExecutionDetails)
    assert result.input_payload == "test-payload"


def test_create_execution_details_non_execution_type():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="test-payload",
    )

    result = processor.create_execution_details(update)

    assert result is None


def test_create_context_details():
    processor = MockProcessor()
    error = ErrorObject.from_message("test error")
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        payload="test-payload",
        error=error,
    )

    result = processor.create_context_details(update)

    assert isinstance(result, ContextDetails)
    assert result.result == "test-payload"
    assert result.error == error


def test_create_context_details_non_context_type():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="test-payload",
    )

    result = processor.create_context_details(update)

    assert result is None


def test_create_step_details():
    processor = MockProcessor()
    error = ErrorObject.from_message("test error")
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="test-payload",
        error=error,
    )

    current_op = Mock()
    current_op.step_details = Mock()
    current_op.step_details.attempt = Mock()

    result = processor.create_step_details(update, current_op)

    assert isinstance(result, StepDetails)
    assert result.result == "test-payload"
    assert result.error == error


def test_create_context_details_with_replay_children():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        payload="test-payload",
        context_options=ContextOptions(replay_children=True),
    )

    result = processor.create_context_details(update)

    assert isinstance(result, ContextDetails)
    assert result.result == "test-payload"
    assert result.replay_children == True


def test_create_step_details_non_step_type():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        payload="test-payload",
    )

    current_op = Mock()
    current_op.step_details = Mock()
    current_op.step_details.attempt = Mock()

    result = processor.create_step_details(update, current_op)

    assert result is None


def test_create_step_details_without_current_operation():
    processor = MockProcessor()
    error = ErrorObject.from_message("test error")
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="test-payload",
        error=error,
    )

    result = processor.create_step_details(update, None)

    assert isinstance(result, StepDetails)
    assert result.result == "test-payload"
    assert result.error == error
    assert result.attempt == 0


def test_create_callback_details():
    processor = MockProcessor()
    error = ErrorObject.from_message("test error")
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        payload="test-payload",
        error=error,
    )

    result = processor.create_callback_details(update)

    assert isinstance(result, CallbackDetails)
    assert result.callback_id == "placeholder"
    assert result.result == "test-payload"
    assert result.error == error


def test_create_callback_details_non_callback_type():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="test-payload",
    )

    result = processor.create_callback_details(update)

    assert result is None


def test_create_invoke_details():
    processor = MockProcessor()
    error = ErrorObject.from_message("test error")
    invoke_options = ChainedInvokeOptions(function_name="test-function")
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        payload="test-payload",
        error=error,
        chained_invoke_options=invoke_options,
    )

    result = processor.create_invoke_details(update)

    assert isinstance(result, ChainedInvokeDetails)
    assert result.result == "test-payload"
    assert result.error == error


def test_create_invoke_details_non_invoke_type():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        payload="test-payload",
    )

    result = processor.create_invoke_details(update)

    assert result is None


def test_create_invoke_details_no_options():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.CHAINED_INVOKE,
        action=OperationAction.START,
        payload="test-payload",
    )

    result = processor.create_invoke_details(update)

    assert result is None


def test_create_wait_details_with_current_operation():
    processor = MockProcessor()
    scheduled_end_timestamp = datetime.datetime.now(tz=datetime.UTC)
    current_op = Mock()
    current_op.wait_details = WaitDetails(
        scheduled_end_timestamp=scheduled_end_timestamp
    )

    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        wait_options=wait_options,
    )

    result = processor.create_wait_details(update, current_op)

    assert isinstance(result, WaitDetails)
    assert result.scheduled_end_timestamp == scheduled_end_timestamp


def test_create_wait_details_without_current_operation():
    processor = MockProcessor()
    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        wait_options=wait_options,
    )

    result = processor.create_wait_details(update, None)

    assert isinstance(result, WaitDetails)
    assert result.scheduled_end_timestamp > datetime.datetime.now(tz=datetime.UTC)


def test_create_wait_details_non_wait_type():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = processor.create_wait_details(update, None)

    assert result is None


def test_translate_update_to_operation_with_current_operation():
    processor = MockProcessor()
    start_time = datetime.datetime.now(tz=datetime.UTC) - timedelta(minutes=5)
    current_op = Mock()
    current_op.start_timestamp = start_time

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent-id",
        name="test-operation",
        sub_type="test-subtype",
    )

    result = processor.translate_update(update, current_op, OperationStatus.STARTED)

    assert isinstance(result, Operation)
    assert result.operation_id == "test-id"
    assert result.parent_id == "parent-id"
    assert result.name == "test-operation"
    assert result.start_timestamp == start_time
    assert result.operation_type == OperationType.STEP
    assert result.status == OperationStatus.STARTED
    assert result.sub_type == "test-subtype"


def test_translate_update_to_operation_without_current_operation():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        parent_id="parent-id",
        name="test-operation",
    )

    result = processor.translate_update(update, None, OperationStatus.STARTED)

    assert isinstance(result, Operation)
    assert result.operation_id == "test-id"
    assert result.parent_id == "parent-id"
    assert result.name == "test-operation"
    assert result.start_timestamp is not None
    assert result.operation_type == OperationType.STEP
    assert result.status == OperationStatus.STARTED


def test_translate_update_to_operation_with_terminal_status():
    processor = MockProcessor()
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )

    result = processor.translate_update(update, None, OperationStatus.SUCCEEDED)

    assert result.end_timestamp is not None
    assert result.status == OperationStatus.SUCCEEDED
