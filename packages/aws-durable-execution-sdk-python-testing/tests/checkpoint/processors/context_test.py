"""Tests for context operation processor."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.context import (
    ContextProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class MockNotifier(ExecutionNotifier):
    """Mock notifier for testing."""

    def __init__(self):
        super().__init__()
        self.completed_calls = []
        self.failed_calls = []
        self.wait_timer_calls = []
        self.step_retry_calls = []

    def notify_completed(self, execution_arn, result=None):
        self.completed_calls.append((execution_arn, result))

    def notify_failed(self, execution_arn, error):
        self.failed_calls.append((execution_arn, error))

    def notify_wait_timer_scheduled(self, execution_arn, operation_id, delay):
        self.wait_timer_calls.append((execution_arn, operation_id, delay))

    def notify_step_retry_scheduled(self, execution_arn, operation_id, delay):
        self.step_retry_calls.append((execution_arn, operation_id, delay))


def test_process_start_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "context-123"
    assert result.operation_type == OperationType.CONTEXT
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-context"
    assert result.context_details is not None


def test_process_start_action_with_current_operation():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.STARTED


def test_process_succeed_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
        payload="success-result",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "context-123"
    assert result.status == OperationStatus.SUCCEEDED
    assert result.context_details.result == "success-result"
    assert result.context_details.error is None


def test_process_succeed_action_with_current_operation():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
        payload="success-result",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.SUCCEEDED


def test_process_fail_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("context failed")
    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "context-123"
    assert result.status == OperationStatus.FAILED
    assert result.context_details.error == error
    assert result.context_details.result is None


def test_process_fail_action_with_current_operation():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)

    error = ErrorObject.from_message("context failed")
    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
        error=error,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.FAILED


def test_process_fail_action_with_payload_and_error():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("context failed")
    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
        payload="partial-result",
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result == "partial-result"
    assert result.context_details.error == error


def test_process_invalid_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.RETRY,
        name="test-context",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CONTEXT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_cancel_action():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.CANCEL,
        name="test-context",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CONTEXT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_with_parent_id():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
        parent_id="parent-456",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.parent_id == "parent-456"


def test_process_with_sub_type():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
        sub_type="parallel",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.sub_type == "parallel"


def test_process_start_without_payload():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result is None
    assert result.context_details.error is None


def test_process_succeed_without_payload():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result is None
    assert result.context_details.error is None


def test_process_fail_without_error():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.FAIL,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.context_details.result is None
    assert result.context_details.error is None


def test_no_notifier_calls():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.wait_timer_calls) == 0
    assert len(notifier.step_retry_calls) == 0


def test_end_timestamp_set_for_terminal_states():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.SUCCEED,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.end_timestamp is not None


def test_end_timestamp_not_set_for_non_terminal_states():
    processor = ContextProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="context-123",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        name="test-context",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.end_timestamp is None
