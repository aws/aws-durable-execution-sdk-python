"""Unit tests for exceptions module."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    CallableRuntimeErrorSerializableDetails,
    CheckpointError,
    DurableExecutionsError,
    ExecutionError,
    InvocationError,
    OrderedLockError,
    StepInterruptedError,
    SuspendExecution,
    TerminationReason,
    TimedSuspendExecution,
    UnrecoverableError,
    UserlandError,
    ValidationError,
)


def test_durable_executions_error():
    """Test DurableExecutionsError base exception."""
    error = DurableExecutionsError("test message")
    assert str(error) == "test message"
    assert isinstance(error, Exception)


def test_invocation_error():
    """Test InvocationError exception."""
    error = InvocationError("invocation error")
    assert str(error) == "invocation error"
    assert isinstance(error, UnrecoverableError)
    assert isinstance(error, DurableExecutionsError)
    assert error.termination_reason == TerminationReason.INVOCATION_ERROR


def test_checkpoint_error():
    """Test CheckpointError exception."""
    error = CheckpointError("checkpoint failed")
    assert str(error) == "checkpoint failed"
    assert isinstance(error, InvocationError)
    assert isinstance(error, UnrecoverableError)
    assert error.termination_reason == TerminationReason.CHECKPOINT_FAILED


def test_validation_error():
    """Test ValidationError exception."""
    error = ValidationError("validation failed")
    assert str(error) == "validation failed"
    assert isinstance(error, DurableExecutionsError)


def test_userland_error():
    """Test UserlandError exception."""
    error = UserlandError("userland error")
    assert str(error) == "userland error"
    assert isinstance(error, DurableExecutionsError)


def test_callable_runtime_error():
    """Test CallableRuntimeError exception."""
    error = CallableRuntimeError(
        "runtime error", "ValueError", "error data", ["line1", "line2"]
    )
    assert str(error) == "runtime error"
    assert error.message == "runtime error"
    assert error.error_type == "ValueError"
    assert error.data == "error data"
    assert isinstance(error, UserlandError)


def test_callable_runtime_error_with_none_values():
    """Test CallableRuntimeError with None values."""
    error = CallableRuntimeError(None, None, None, None)
    assert error.message is None
    assert error.error_type is None
    assert error.data is None


def test_step_interrupted_error():
    """Test StepInterruptedError exception."""
    error = StepInterruptedError("step interrupted", "step_123")
    assert str(error) == "step interrupted"
    assert isinstance(error, InvocationError)
    assert isinstance(error, UnrecoverableError)
    assert error.termination_reason == TerminationReason.STEP_INTERRUPTED
    assert error.step_id == "step_123"


def test_suspend_execution():
    """Test SuspendExecution exception."""
    error = SuspendExecution("suspend execution")
    assert str(error) == "suspend execution"
    assert isinstance(error, BaseException)


def test_ordered_lock_error_without_source():
    """Test OrderedLockError without source exception."""
    error = OrderedLockError("lock error")
    assert str(error) == "lock error"
    assert error.source_exception is None
    assert isinstance(error, DurableExecutionsError)


def test_ordered_lock_error_with_source():
    """Test OrderedLockError with source exception."""
    source = ValueError("source error")
    error = OrderedLockError("lock error", source)
    assert str(error) == "lock error ValueError: source error"
    assert error.source_exception is source


def test_callable_runtime_error_serializable_details_from_exception():
    """Test CallableRuntimeErrorSerializableDetails.from_exception."""
    exception = ValueError("test error")
    details = CallableRuntimeErrorSerializableDetails.from_exception(exception)
    assert details.type == "ValueError"
    assert details.message == "test error"


def test_callable_runtime_error_serializable_details_str():
    """Test CallableRuntimeErrorSerializableDetails.__str__."""
    details = CallableRuntimeErrorSerializableDetails("TypeError", "type error message")
    assert str(details) == "TypeError: type error message"


def test_callable_runtime_error_serializable_details_frozen():
    """Test CallableRuntimeErrorSerializableDetails is frozen."""
    details = CallableRuntimeErrorSerializableDetails("Error", "message")
    with pytest.raises(AttributeError):
        details.type = "NewError"


def test_timed_suspend_execution():
    """Test TimedSuspendExecution exception."""
    scheduled_time = datetime.now(UTC)
    error = TimedSuspendExecution("timed suspend", scheduled_time)
    assert str(error) == "timed suspend"
    assert error.scheduled_timestamp == scheduled_time
    assert isinstance(error, SuspendExecution)
    assert isinstance(error, BaseException)


def test_timed_suspend_execution_from_delay():
    """Test TimedSuspendExecution.from_delay factory method."""
    message = "Waiting for callback"
    delay_seconds = 30

    # Mock datetime.now() to get predictable results
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    with patch("aws_durable_execution_sdk_python.exceptions.datetime") as mock_datetime:
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = datetime
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    expected_time = mock_now + timedelta(seconds=30)
    assert error.scheduled_timestamp == expected_time
    assert isinstance(error, TimedSuspendExecution)
    assert isinstance(error, SuspendExecution)


def test_timed_suspend_execution_from_delay_zero_delay():
    """Test TimedSuspendExecution.from_delay with zero delay."""
    message = "Immediate suspension"
    delay_seconds = 0

    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    with patch("aws_durable_execution_sdk_python.exceptions.datetime") as mock_datetime:
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = datetime
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    assert error.scheduled_timestamp == mock_now  # no delay added
    assert isinstance(error, TimedSuspendExecution)


def test_timed_suspend_execution_from_delay_negative_delay():
    """Test TimedSuspendExecution.from_delay with negative delay."""
    message = "Past suspension"
    delay_seconds = -10

    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    with patch("aws_durable_execution_sdk_python.exceptions.datetime") as mock_datetime:
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = datetime
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    expected_time = mock_now + timedelta(seconds=-10)
    assert error.scheduled_timestamp == expected_time
    assert isinstance(error, TimedSuspendExecution)


def test_timed_suspend_execution_from_delay_large_delay():
    """Test TimedSuspendExecution.from_delay with large delay."""
    message = "Long suspension"
    delay_seconds = 3600  # 1 hour

    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    with patch("aws_durable_execution_sdk_python.exceptions.datetime") as mock_datetime:
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = datetime
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    expected_time = mock_now + timedelta(seconds=3600)
    assert error.scheduled_timestamp == expected_time
    assert isinstance(error, TimedSuspendExecution)


def test_timed_suspend_execution_from_delay_calculation_accuracy():
    """Test that TimedSuspendExecution.from_delay calculates time accurately."""
    message = "Accurate timing test"
    delay_seconds = 42

    # Test with actual datetime.now() to ensure the calculation works in real scenarios
    before_time = datetime.now(UTC)
    error = TimedSuspendExecution.from_delay(message, delay_seconds)
    after_time = datetime.now(UTC)

    # The scheduled timestamp should be within a reasonable range
    # (accounting for the small time difference between calls)
    expected_min = before_time + timedelta(seconds=delay_seconds)
    expected_max = after_time + timedelta(seconds=delay_seconds)

    assert expected_min <= error.scheduled_timestamp <= expected_max
    assert str(error) == message
    assert isinstance(error, TimedSuspendExecution)


def test_unrecoverable_error():
    """Test UnrecoverableError base class."""
    error = UnrecoverableError("unrecoverable error", TerminationReason.EXECUTION_ERROR)
    assert str(error) == "unrecoverable error"
    assert error.termination_reason == TerminationReason.EXECUTION_ERROR
    assert isinstance(error, DurableExecutionsError)


def test_execution_error():
    """Test ExecutionError exception."""
    error = ExecutionError("execution error")
    assert str(error) == "execution error"
    assert isinstance(error, UnrecoverableError)
    assert isinstance(error, DurableExecutionsError)
    assert error.termination_reason == TerminationReason.EXECUTION_ERROR


def test_execution_error_with_custom_termination_reason():
    """Test ExecutionError with custom termination reason."""
    error = ExecutionError("custom error", TerminationReason.SERIALIZATION_ERROR)
    assert str(error) == "custom error"
    assert error.termination_reason == TerminationReason.SERIALIZATION_ERROR
