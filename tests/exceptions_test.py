"""Unit tests for exceptions module."""

import pytest

from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    CallableRuntimeErrorSerializableDetails,
    CheckpointError,
    DurableExecutionsError,
    FatalError,
    OrderedLockError,
    StepInterruptedError,
    SuspendExecution,
    UserlandError,
    ValidationError,
)


def test_durable_executions_error():
    """Test DurableExecutionsError base exception."""
    error = DurableExecutionsError("test message")
    assert str(error) == "test message"
    assert isinstance(error, Exception)


def test_fatal_error():
    """Test FatalError exception."""
    error = FatalError("fatal error")
    assert str(error) == "fatal error"
    assert isinstance(error, DurableExecutionsError)


def test_checkpoint_error():
    """Test CheckpointError exception."""
    error = CheckpointError("checkpoint failed")
    assert str(error) == "checkpoint failed"
    assert isinstance(error, FatalError)


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
    assert error.stack_trace == ["line1", "line2"]
    assert isinstance(error, UserlandError)


def test_callable_runtime_error_with_none_values():
    """Test CallableRuntimeError with None values."""
    error = CallableRuntimeError(None, None, None, None)
    assert error.message is None
    assert error.error_type is None
    assert error.data is None
    assert error.stack_trace is None


def test_step_interrupted_error():
    """Test StepInterruptedError exception."""
    error = StepInterruptedError("step interrupted")
    assert str(error) == "step interrupted"
    assert isinstance(error, UserlandError)


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
