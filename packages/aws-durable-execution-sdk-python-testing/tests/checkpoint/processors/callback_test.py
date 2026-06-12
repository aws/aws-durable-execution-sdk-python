"""Tests for callback operation processor."""

from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.callback import (
    CallbackProcessor,
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
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert isinstance(result, Operation)
    assert result.operation_id == "callback-123"
    assert result.operation_type == OperationType.CALLBACK
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-callback"
    assert result.callback_details is not None


def test_process_start_action_with_current_operation():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    current_op = Mock()
    current_op.start_timestamp = Mock()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
    )

    result = processor.process(
        update,
        current_op,
        notifier,
        "arn:aws:states:us-east-1:123456789012:execution:test",
    )

    assert isinstance(result, Operation)
    assert result.operation_id == "callback-123"
    assert result.status == OperationStatus.STARTED
    assert result.start_timestamp == current_op.start_timestamp


def test_process_invalid_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.SUCCEED,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_fail_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.FAIL,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_cancel_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.CANCEL,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_retry_action():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.RETRY,
        name="test-callback",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for CALLBACK operation"
    ):
        processor.process(
            update,
            None,
            notifier,
            "arn:aws:states:us-east-1:123456789012:execution:test",
        )


def test_process_with_payload():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
        payload="test-payload",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert result.callback_details.result == "test-payload"


def test_process_with_parent_id():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
        parent_id="parent-456",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert result.parent_id == "parent-456"


def test_process_with_sub_type():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
        sub_type="activity",
    )

    result = processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert result.sub_type == "activity"


def test_notifier_not_called_for_start():
    processor = CallbackProcessor()
    notifier = MockNotifier()

    update = OperationUpdate(
        operation_id="callback-123",
        operation_type=OperationType.CALLBACK,
        action=OperationAction.START,
        name="test-callback",
    )

    processor.process(
        update, None, notifier, "arn:aws:states:us-east-1:123456789012:execution:test"
    )

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.wait_timer_calls) == 0
    assert len(notifier.step_retry_calls) == 0
