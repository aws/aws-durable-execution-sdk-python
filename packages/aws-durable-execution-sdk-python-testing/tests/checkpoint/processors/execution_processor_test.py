"""Tests for execution operation processor."""

from unittest.mock import Mock

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.execution import (
    ExecutionProcessor,
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


def test_process_succeed_action():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="success-result",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.completed_calls) == 1
    assert notifier.completed_calls[0] == (execution_arn, "success-result")
    assert len(notifier.failed_calls) == 0


def test_process_succeed_action_with_current_operation():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="success-result",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result is None
    assert len(notifier.completed_calls) == 1
    assert notifier.completed_calls[0] == (execution_arn, "success-result")


def test_process_succeed_action_without_payload():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.completed_calls) == 1
    assert notifier.completed_calls[0] == (execution_arn, None)


def test_process_fail_action_with_error():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("execution failed")
    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    assert notifier.failed_calls[0] == (execution_arn, error)
    assert len(notifier.completed_calls) == 0


def test_process_fail_action_without_error():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)
    assert (
        "There is no error details but EXECUTION checkpoint action is not SUCCEED"
        in str(error_arg)
    )


def test_process_start_action():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.START,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)


def test_process_retry_action():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.RETRY,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)


def test_process_cancel_action():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.CANCEL,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    execution_arn_arg, error_arg = notifier.failed_calls[0]
    assert execution_arn_arg == execution_arn
    assert isinstance(error_arg, ErrorObject)


def test_process_with_current_operation_and_error():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    error = ErrorObject.from_message("custom error")

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.FAIL,
        error=error,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result is None
    assert len(notifier.failed_calls) == 1
    assert notifier.failed_calls[0] == (execution_arn, error)


def test_no_wait_timer_or_step_retry_calls():
    processor = ExecutionProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="execution-123",
        operation_type=OperationType.EXECUTION,
        action=OperationAction.SUCCEED,
        payload="result",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.wait_timer_calls) == 0
    assert len(notifier.step_retry_calls) == 0
