"""Tests for step operation processor."""

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
    StepDetails,
    StepOptions,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.step import (
    StepProcessor,
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
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.operation_type == OperationType.STEP
    assert result.status == OperationStatus.STARTED
    assert result.name == "test-step"
    assert result.step_details is not None


def test_process_start_action_with_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp


def test_process_retry_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)
    current_op.step_details = StepDetails(attempt=1, result="previous-result")
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    step_options = StepOptions(next_attempt_delay_seconds=30)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.status == OperationStatus.PENDING
    assert result.step_details.attempt == 2
    assert result.step_details.result == "previous-result"
    assert result.step_details.next_attempt_timestamp is not None

    assert len(notifier.step_retry_calls) == 1
    assert notifier.step_retry_calls[0] == (execution_arn, "step-123", 30)


def test_process_retry_action_without_step_options():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)
    current_op.step_details = StepDetails(attempt=0)
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.attempt == 1
    assert len(notifier.step_retry_calls) == 1
    assert notifier.step_retry_calls[0] == (execution_arn, "step-123", 0)


def test_process_retry_action_without_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    step_options = StepOptions(next_attempt_delay_seconds=15)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.step_details.attempt == 1
    assert result.step_details.result is None
    assert result.step_details.error is None


def test_process_retry_action_without_current_step_details():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)
    current_op.step_details = None
    current_op.execution_details = None
    current_op.context_details = None
    current_op.wait_details = None
    current_op.callback_details = None
    current_op.chained_invoke_details = None

    step_options = StepOptions(next_attempt_delay_seconds=45)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.attempt == 1


def test_process_succeed_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        name="test-step",
        payload="success-result",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.status == OperationStatus.SUCCEEDED
    assert result.step_details.result == "success-result"


def test_process_succeed_action_with_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)
    current_op.step_details = StepDetails()

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        name="test-step",
        payload="success-result",
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.SUCCEEDED
    assert result.step_details.attempt == 1


def test_process_fail_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    error = ErrorObject.from_message("step failed")
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        name="test-step",
        error=error,
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert isinstance(result, Operation)
    assert result.operation_id == "step-123"
    assert result.status == OperationStatus.FAILED
    assert result.step_details.error == error


def test_process_fail_action_with_current_operation():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)
    current_op.step_details = StepDetails()

    error = ErrorObject.from_message("step failed")
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.FAIL,
        name="test-step",
        error=error,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.start_timestamp == current_op.start_timestamp
    assert result.status == OperationStatus.FAILED
    assert result.step_details.attempt == 1


def test_process_invalid_action():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.CANCEL,
        name="test-step",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid action for STEP operation"
    ):
        processor.process(update, None, notifier, execution_arn)


def test_process_with_parent_id():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
        parent_id="parent-456",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.parent_id == "parent-456"


def test_process_with_sub_type():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
        sub_type="lambda",
    )

    result = processor.process(update, None, notifier, execution_arn)

    assert result.sub_type == "lambda"


def test_retry_preserves_current_operation_details():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    current_op = Mock()
    current_op.start_timestamp = datetime.now(UTC)
    current_op.step_details = StepDetails(
        attempt=2, result="old-result", error=ErrorObject.from_message("old-error")
    )
    current_op.execution_details = Mock()
    current_op.context_details = Mock()
    current_op.wait_details = Mock()
    current_op.callback_details = Mock()
    current_op.chained_invoke_details = Mock()

    step_options = StepOptions(next_attempt_delay_seconds=60)
    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.RETRY,
        name="test-step",
        step_options=step_options,
    )

    result = processor.process(update, current_op, notifier, execution_arn)

    assert result.step_details.attempt == 3
    assert result.step_details.result == "old-result"
    assert result.step_details.error == current_op.step_details.error
    assert result.execution_details == current_op.execution_details
    assert result.context_details == current_op.context_details
    assert result.wait_details == current_op.wait_details
    assert result.callback_details == current_op.callback_details
    assert result.chained_invoke_details == current_op.chained_invoke_details


def test_no_completed_or_failed_calls_for_non_execution_actions():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="test-step",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.completed_calls) == 0
    assert len(notifier.failed_calls) == 0
    assert len(notifier.wait_timer_calls) == 0


def test_no_step_retry_calls_for_non_retry_actions():
    processor = StepProcessor()
    notifier = MockNotifier()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    update = OperationUpdate(
        operation_id="step-123",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
        name="test-step",
    )

    processor.process(update, None, notifier, execution_arn)

    assert len(notifier.step_retry_calls) == 0
