"""Tests for wait operation processor."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    WaitOptions,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.wait import (
    WaitProcessor,
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
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
        wait_options=wait_options,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "wait-123"
    assert result.operation_type == OperationType.WAIT
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-wait"
    assert result.wait_details is not None
    assert result.wait_details.scheduled_end_timestamp > datetime.now(UTC)

    assert len(notifier.wait_timer_calls) == 1
    assert notifier.wait_timer_calls[0] == (execution_arn, "wait-123", 30)


def test_process_start_action_without_wait_options():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.wait_details is not None

    assert len(notifier.wait_timer_calls) == 1
    assert notifier.wait_timer_calls[0] == (execution_arn, "wait-123", 0)


def test_process_start_action_with_zero_seconds():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    wait_options = WaitOptions(wait_seconds=0)
    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
        wait_options=wait_options,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.wait_details is not None

    assert len(notifier.wait_timer_calls) == 1
    assert notifier.wait_timer_calls[0] == (execution_arn, "wait-123", 0)


def test_process_start_action_with_parent_id():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    wait_options = WaitOptions(wait_seconds=15)
    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
        parent_id="parent-456",
        wait_options=wait_options,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.parent_id == "parent-456"


def test_process_start_action_with_sub_type():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    wait_options = WaitOptions(wait_seconds=15)
    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
        sub_type="timer",
        wait_options=wait_options,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.sub_type == "timer"


def test_process_cancel_action():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.CANCEL,
        name="test-wait",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "wait-123"
    assert result.status == OperationStatus.CANCELLED
    assert result.start_timestamp == current_op.start_timestamp


def test_process_cancel_action_without_current_operation():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.CANCEL,
        name="test-wait",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.status == OperationStatus.CANCELLED


def test_process_invalid_action():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.SUCCEED,
        name="test-wait",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for WAIT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_fail_action():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.FAIL,
        name="test-wait",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for WAIT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_retry_action():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.RETRY,
        name="test-wait",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for WAIT operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_wait_details_created_correctly():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    wait_options = WaitOptions(wait_seconds=60)
    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
        wait_options=wait_options,
    )

    before_time = datetime.now(UTC)
    result = processor.process(update, None, notifier, execution_arn)

    assert result.wait_details.scheduled_end_timestamp > before_time


def test_no_completed_or_failed_calls():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    wait_options = WaitOptions(wait_seconds=30)
    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
        name="test-wait",
        wait_options=wait_options,
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.step_retry_calls) == 0


def test_cancel_no_timer_scheduled():
    processor = WaitProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)

    update = OperationUpdate(
        operation_id="wait-123",
        operation_type=OperationType.WAIT,
        action=OperationAction.CANCEL,
        name="test-wait",
    )

    processor.process(update, current_op, notifier, execution_arn)

    assert len(notifier.wait_timer_calls) == 0
