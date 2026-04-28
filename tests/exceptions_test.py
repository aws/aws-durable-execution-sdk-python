"""Unit tests for exceptions module."""

import time
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from aws_durable_execution_sdk_python.exceptions import (
    BotoClientError,
    CallableRuntimeError,
    CallableRuntimeErrorSerializableDetails,
    CheckpointError,
    CheckpointErrorCategory,
    DurableApiErrorCategory,
    DurableExecutionsError,
    ExecutionError,
    GetExecutionStateError,
    InvocationError,
    OrderedLockError,
    OrphanedChildException,
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
    error = CheckpointError(
        "checkpoint failed", error_category=CheckpointErrorCategory.EXECUTION
    )
    assert str(error) == "checkpoint failed"
    assert isinstance(error, InvocationError)
    assert isinstance(error, UnrecoverableError)
    assert error.termination_reason == TerminationReason.CHECKPOINT_FAILED


def test_checkpoint_error_classification_invalid_token_invocation():
    """Test 4xx InvalidParameterValueException with Invalid Checkpoint Token is invocation error."""
    error_response = {
        "Error": {
            "Code": "InvalidParameterValueException",
            "Message": "Invalid Checkpoint Token: token expired",
        },
        "ResponseMetadata": {"HTTPStatusCode": 400},
    }
    client_error = ClientError(error_response, "Checkpoint")

    result = CheckpointError.from_exception(client_error)

    assert result.error_category == CheckpointErrorCategory.INVOCATION
    assert result.is_retriable()


def test_checkpoint_error_classification_payload_size_exceeded_execution():
    """Test 4xx InvalidParameterValueException with STEP output payload size limit exceeded is execution error."""
    error_response = {
        "Error": {
            "Code": "InvalidParameterValueException",
            "Message": "STEP output payload size must be less than or equal to 262144 bytes.",
        },
        "ResponseMetadata": {"HTTPStatusCode": 400},
    }
    client_error = ClientError(error_response, "Checkpoint")

    result = CheckpointError.from_exception(client_error)

    assert result.error_category == CheckpointErrorCategory.EXECUTION
    assert not result.is_retriable()


def test_checkpoint_error_classification_invalid_param_without_token_execution():
    """Test 4xx InvalidParameterValueException without Invalid Checkpoint Token is execution error."""
    error_response = {
        "Error": {
            "Code": "InvalidParameterValueException",
            "Message": "Some other invalid parameter",
        },
        "ResponseMetadata": {"HTTPStatusCode": 400},
    }
    client_error = ClientError(error_response, "Checkpoint")

    result = CheckpointError.from_exception(client_error)

    assert result.error_category == CheckpointErrorCategory.EXECUTION
    assert not result.is_retriable()


# =============================================================================
# Shared Durable API error classification tests (BotoClientError._classify_error_category)
# These test the shared classification logic through each BotoClientError subclass.
# =============================================================================


@pytest.mark.parametrize("error_cls", [CheckpointError, GetExecutionStateError])
@pytest.mark.parametrize(
    "error_code",
    [
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSInvalidStateException",
        "KMSNotFoundException",
    ],
)
def test_durable_api_error_non_retriable_customer_error_codes(error_cls, error_code: str):
    """Test that non-retriable customer error codes (HTTP 502) are classified as EXECUTION."""
    error_response = {
        "Error": {"Code": error_code, "Message": f"{error_code} error"},
        "ResponseMetadata": {"HTTPStatusCode": 502},
    }
    client_error = ClientError(error_response, "Invoke")
    result = error_cls.from_exception(client_error)
    assert result.error_category == DurableApiErrorCategory.EXECUTION
    assert not result.is_retriable()


@pytest.mark.parametrize("error_cls", [CheckpointError, GetExecutionStateError])
def test_durable_api_error_4xx_non_retriable(error_cls):
    """Test 4xx errors are classified as EXECUTION (non-retriable)."""
    error_response = {
        "Error": {"Code": "ValidationException", "Message": "Invalid parameter"},
        "ResponseMetadata": {"HTTPStatusCode": 400},
    }
    client_error = ClientError(error_response, "Invoke")
    result = error_cls.from_exception(client_error)
    assert result.error_category == DurableApiErrorCategory.EXECUTION
    assert not result.is_retriable()


@pytest.mark.parametrize("error_cls", [CheckpointError, GetExecutionStateError])
def test_durable_api_error_429_retriable(error_cls):
    """Test 429 errors are classified as INVOCATION (retriable)."""
    error_response = {
        "Error": {"Code": "TooManyRequestsException", "Message": "Rate limit exceeded"},
        "ResponseMetadata": {"HTTPStatusCode": 429},
    }
    client_error = ClientError(error_response, "Invoke")
    result = error_cls.from_exception(client_error)
    assert result.error_category == DurableApiErrorCategory.INVOCATION
    assert result.is_retriable()


@pytest.mark.parametrize("error_cls", [CheckpointError, GetExecutionStateError])
def test_durable_api_error_5xx_retriable(error_cls):
    """Test 5xx errors are classified as INVOCATION (retriable)."""
    error_response = {
        "Error": {"Code": "InternalServerError", "Message": "Service unavailable"},
        "ResponseMetadata": {"HTTPStatusCode": 500},
    }
    client_error = ClientError(error_response, "Invoke")
    result = error_cls.from_exception(client_error)
    assert result.error_category == DurableApiErrorCategory.INVOCATION
    assert result.is_retriable()


@pytest.mark.parametrize("error_cls", [CheckpointError, GetExecutionStateError])
def test_durable_api_error_retriable_502(error_cls):
    """Test that 502 errors with unrecognized error codes are retriable."""
    error_response = {
        "Error": {
            "Code": "ServiceException",
            "Message": "Service encountered an internal error.",
        },
        "ResponseMetadata": {"HTTPStatusCode": 502},
    }
    client_error = ClientError(error_response, "Invoke")
    result = error_cls.from_exception(client_error)
    assert result.error_category == DurableApiErrorCategory.INVOCATION
    assert result.is_retriable()


@pytest.mark.parametrize("error_cls", [CheckpointError, GetExecutionStateError])
def test_durable_api_error_unknown_retriable(error_cls):
    """Test unknown errors (no HTTP response) are classified as INVOCATION (retriable)."""
    result = error_cls.from_exception(Exception("Network timeout"))
    assert result.error_category == DurableApiErrorCategory.INVOCATION
    assert result.is_retriable()


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
    scheduled_time = 1234567890.0
    error = TimedSuspendExecution("timed suspend", scheduled_time)
    assert str(error) == "timed suspend"
    assert error.scheduled_timestamp == scheduled_time
    assert isinstance(error, SuspendExecution)
    assert isinstance(error, BaseException)


def test_timed_suspend_execution_from_delay():
    """Test TimedSuspendExecution.from_delay factory method."""
    message = "Waiting for callback"
    delay_seconds = 30

    # Mock time.time() to get predictable results
    with patch("time.time", return_value=1000.0):
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    assert error.scheduled_timestamp == 1030.0  # 1000.0 + 30
    assert isinstance(error, TimedSuspendExecution)
    assert isinstance(error, SuspendExecution)


def test_timed_suspend_execution_from_delay_zero_delay():
    """Test TimedSuspendExecution.from_delay with zero delay."""
    message = "Immediate suspension"
    delay_seconds = 0

    with patch("time.time", return_value=500.0):
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    assert error.scheduled_timestamp == 500.0  # 500.0 + 0
    assert isinstance(error, TimedSuspendExecution)


def test_timed_suspend_execution_from_delay_negative_delay():
    """Test TimedSuspendExecution.from_delay with negative delay."""
    message = "Past suspension"
    delay_seconds = -10

    with patch("time.time", return_value=100.0):
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    assert error.scheduled_timestamp == 90.0  # 100.0 + (-10)
    assert isinstance(error, TimedSuspendExecution)


def test_timed_suspend_execution_from_delay_large_delay():
    """Test TimedSuspendExecution.from_delay with large delay."""
    message = "Long suspension"
    delay_seconds = 3600  # 1 hour

    with patch("time.time", return_value=0.0):
        error = TimedSuspendExecution.from_delay(message, delay_seconds)

    assert str(error) == message
    assert error.scheduled_timestamp == 3600.0  # 0.0 + 3600
    assert isinstance(error, TimedSuspendExecution)


def test_timed_suspend_execution_from_delay_calculation_accuracy():
    """Test that TimedSuspendExecution.from_delay calculates time accurately."""
    message = "Accurate timing test"
    delay_seconds = 42

    # Test with actual time.time() to ensure the calculation works in real scenarios
    before_time = time.time()
    error = TimedSuspendExecution.from_delay(message, delay_seconds)
    after_time = time.time()

    # The scheduled timestamp should be within a reasonable range
    # (accounting for the small time difference between calls)
    expected_min = before_time + delay_seconds
    expected_max = after_time + delay_seconds

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


def test_orphaned_child_exception_is_base_exception():
    """Test that OrphanedChildException is a BaseException, not Exception."""
    assert issubclass(OrphanedChildException, BaseException)
    assert not issubclass(OrphanedChildException, Exception)


def test_orphaned_child_exception_bypasses_user_exception_handler():
    """Test that OrphanedChildException cannot be caught by user's except Exception handler."""
    caught_by_exception = False
    caught_by_base_exception = False
    exception_instance = None

    try:
        msg = "test message"
        raise OrphanedChildException(msg, operation_id="test_op_123")
    except Exception:  # noqa: BLE001
        caught_by_exception = True
    except BaseException as e:  # noqa: BLE001
        caught_by_base_exception = True
        exception_instance = e

    expected_msg = "OrphanedChildException should not be caught by except Exception"
    assert not caught_by_exception, expected_msg
    expected_base_msg = (
        "OrphanedChildException should be caught by except BaseException"
    )
    assert caught_by_base_exception, expected_base_msg

    # Verify operation_id is preserved
    assert isinstance(exception_instance, OrphanedChildException)
    assert exception_instance.operation_id == "test_op_123"
    assert str(exception_instance) == "test message"


def test_orphaned_child_exception_with_operation_id():
    """Test OrphanedChildException stores operation_id correctly."""
    exception = OrphanedChildException("parent completed", operation_id="child_op_456")
    assert exception.operation_id == "child_op_456"
    assert str(exception) == "parent completed"


@pytest.mark.parametrize(
    ("error_code", "status_code", "expected_retriable"),
    [
        ("KMSAccessDeniedException", 502, False),
        ("ServiceException", 500, True),
        ("ServiceException", 502, True),
    ],
)
def test_boto_client_error_is_retriable(error_code: str, status_code: int, expected_retriable: bool):
    """Test BotoClientError.is_retriable() classification."""
    error_response = {
        "Error": {"Code": error_code, "Message": "test error"},
        "ResponseMetadata": {"HTTPStatusCode": status_code},
    }
    client_error = ClientError(error_response, "Invoke")
    result = BotoClientError.from_exception(client_error)
    assert result.is_retriable() == expected_retriable


def test_boto_client_error_is_retriable_no_error():
    """Test BotoClientError.is_retriable() returns True with no error info."""
    result = BotoClientError.from_exception(Exception("network error"))
    assert result.is_retriable()


# =============================================================================
# DurableApiErrorCategory backward compatibility
# =============================================================================


def test_durable_api_error_category_backward_compatible_alias():
    """Test CheckpointErrorCategory is a backward-compatible alias for DurableApiErrorCategory."""
    assert CheckpointErrorCategory is DurableApiErrorCategory
    assert CheckpointErrorCategory.INVOCATION is DurableApiErrorCategory.INVOCATION
    assert CheckpointErrorCategory.EXECUTION is DurableApiErrorCategory.EXECUTION


# =============================================================================
# is_retriable() tests
# =============================================================================


def test_invocation_error_is_retriable_default():
    """Test InvocationError.is_retriable() returns True by default."""
    error = InvocationError("some error")
    assert error.is_retriable()