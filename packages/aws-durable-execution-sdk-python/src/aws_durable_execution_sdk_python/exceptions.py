"""Exceptions for the Durable Executions SDK.

Avoid any non-stdlib references in this module, it is at the bottom of the dependency chain.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import TYPE_CHECKING, Self, TypedDict

BAD_REQUEST_ERROR: int = 400
TOO_MANY_REQUESTS_ERROR: int = 429
SERVICE_ERROR: int = 500
INVALID_PARAMETER_VALUE_EXCEPTION: str = "InvalidParameterValueException"
INVALID_CHECKPOINT_TOKEN_PREFIX: str = "Invalid Checkpoint Token"

# Non-retryable customer error codes that arrive as non-4xx (e.g. HTTP 502) from Lambda.
# Unlike typical 5xx errors, these require customer intervention (e.g., fixing
# a KMS key configuration) and will never succeed on retry.
# Add new non-retryable error codes here — they are automatically classified
# as EXECUTION (non-retryable) by _classify_error_category().
_NON_RETRYABLE_CUSTOMER_ERROR_CODES: frozenset[str] = frozenset(
    {
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSInvalidStateException",
        "KMSNotFoundException",
    }
)

if TYPE_CHECKING:
    import datetime


class AwsErrorObj(TypedDict):
    Code: str | None
    Message: str | None


class AwsErrorMetadata(TypedDict):
    RequestId: str | None
    HostId: str | None
    HTTPStatusCode: int | None
    HTTPHeaders: str | None
    RetryAttempts: str | None


class TerminationReason(Enum):
    """Reasons why a durable execution terminated."""

    UNHANDLED_ERROR = "UNHANDLED_ERROR"
    INVOCATION_ERROR = "INVOCATION_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    CHECKPOINT_FAILED = "CHECKPOINT_FAILED"
    NON_DETERMINISTIC_EXECUTION = "NON_DETERMINISTIC_EXECUTION"
    STEP_INTERRUPTED = "STEP_INTERRUPTED"
    SERIALIZATION_ERROR = "SERIALIZATION_ERROR"


class DurableExecutionsError(Exception):
    """Base class for Durable Executions exceptions"""


class UnrecoverableError(DurableExecutionsError):
    """Base class for errors that terminate execution."""

    def __init__(self, message: str, termination_reason: TerminationReason):
        super().__init__(message)
        self.termination_reason = termination_reason


class ExecutionError(UnrecoverableError):
    """Error that returns FAILED status without retry."""

    def __init__(
        self,
        message: str,
        termination_reason: TerminationReason = TerminationReason.EXECUTION_ERROR,
    ):
        super().__init__(message, termination_reason)


class InvocationError(UnrecoverableError):
    """Error that should cause Lambda retry by throwing from handler."""

    def __init__(
        self,
        message: str,
        termination_reason: TerminationReason = TerminationReason.INVOCATION_ERROR,
    ):
        super().__init__(message, termination_reason)

    def is_retryable(self) -> bool:
        """Whether this error is retryable. Returns True by default.

        Subclasses override to implement classification logic based on
        error codes and HTTP status codes.
        """
        return True


class DurableApiErrorCategory(Enum):
    INVOCATION = "INVOCATION"
    EXECUTION = "EXECUTION"


# Backward-compatible alias
CheckpointErrorCategory = DurableApiErrorCategory


class BotoClientError(InvocationError):
    """Error from a Lambda API call (e.g., CheckpointDurableExecution, GetDurableExecutionState).

    Extends InvocationError because the default behavior for API failures is to retry
    the Lambda invocation. However, some errors are non-retryable (e.g., 4xx client errors,
    KMS key misconfiguration) and should fail the execution instead. The error_category field
    and is_retryable() method distinguish these cases at runtime.
    """

    def __init__(
        self,
        message: str,
        error_category: DurableApiErrorCategory = DurableApiErrorCategory.INVOCATION,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
        termination_reason=TerminationReason.INVOCATION_ERROR,
    ):
        super().__init__(message=message, termination_reason=termination_reason)
        self.error: AwsErrorObj | None = error
        self.response_metadata: AwsErrorMetadata | None = response_metadata
        self.error_category: DurableApiErrorCategory = error_category

    @classmethod
    def from_exception(cls, exception: Exception) -> Self:
        response = getattr(exception, "response", {})
        response_metadata = response.get("ResponseMetadata")
        error = response.get("Error")
        error_category = BotoClientError._classify_error_category(
            error, response_metadata
        )
        return cls(
            message=str(exception),
            error_category=error_category,
            error=error,
            response_metadata=response_metadata,
        )

    @staticmethod
    def _classify_error_category(
        error: AwsErrorObj | None,
        response_metadata: AwsErrorMetadata | None,
    ) -> DurableApiErrorCategory:
        """Classify a Durable API error as retryable (INVOCATION) or non-retryable (EXECUTION).

        Classification rules:
        - Non-retryable customer error codes (e.g., KMS key issues) → EXECUTION
          These arrive as HTTP 502 but require customer intervention to fix.
        - 4xx errors → EXECUTION, except:
          - 429 (TooManyRequests) → INVOCATION (throttling is transient)
          - InvalidParameterValueException with "Invalid Checkpoint Token" → INVOCATION
            (stale token from a concurrent checkpoint; next invocation gets a fresh token)
        - 5xx, network errors → INVOCATION
        """
        error_code: str | None = (error and error.get("Code")) or None
        if error_code and error_code in _NON_RETRYABLE_CUSTOMER_ERROR_CODES:
            return DurableApiErrorCategory.EXECUTION

        status_code: int | None = (
            response_metadata and response_metadata.get("HTTPStatusCode")
        ) or None
        if (
            status_code
            and BAD_REQUEST_ERROR <= status_code < SERVICE_ERROR
            and status_code != TOO_MANY_REQUESTS_ERROR
            and error
            and not (
                (error.get("Code") or "") == INVALID_PARAMETER_VALUE_EXCEPTION
                and (error.get("Message") or "").startswith(
                    INVALID_CHECKPOINT_TOKEN_PREFIX
                )
            )
        ):
            return DurableApiErrorCategory.EXECUTION

        return DurableApiErrorCategory.INVOCATION

    def is_retryable(self) -> bool:
        """Whether this error is retryable based on error_category."""
        return self.error_category == DurableApiErrorCategory.INVOCATION

    # Backward-compatible alias
    is_retriable = is_retryable

    def build_logger_extras(self) -> dict:
        extras: dict = {}
        # preserve PascalCase to be consistent with other langauges
        if error := self.error:
            extras["Error"] = error
        if response_metadata := self.response_metadata:
            extras["ResponseMetadata"] = response_metadata
        return extras


class NonDeterministicExecutionError(ExecutionError):
    """Error when execution is non-deterministic."""

    def __init__(self, message: str, step_id: str | None = None):
        super().__init__(message, TerminationReason.NON_DETERMINISTIC_EXECUTION)
        self.step_id = step_id


class CheckpointError(BotoClientError):
    """Failure to checkpoint. Will terminate the lambda."""

    def __init__(
        self,
        message: str,
        error_category: DurableApiErrorCategory = DurableApiErrorCategory.INVOCATION,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
    ):
        super().__init__(
            message,
            error_category,
            error,
            response_metadata,
            termination_reason=TerminationReason.CHECKPOINT_FAILED,
        )


class ValidationError(DurableExecutionsError):
    """Incorrect arguments to a Durable Function operation."""


class GetExecutionStateError(BotoClientError):
    """Raised when failing to retrieve execution state"""

    def __init__(
        self,
        message: str,
        error_category: DurableApiErrorCategory = DurableApiErrorCategory.INVOCATION,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
    ):
        super().__init__(
            message,
            error_category,
            error,
            response_metadata,
            termination_reason=TerminationReason.INVOCATION_ERROR,
        )


class InvalidStateError(DurableExecutionsError):
    """Raised when an operation is attempted on an object in an invalid state."""


class DurableOperationError(DurableExecutionsError):
    """Base class for typed, per-operation failures.

    Wraps a failure that escaped a Durable Function operation (step, invoke,
    child context, wait_for_condition). The concrete class identifies the
    operation kind (so callers can ``except StepError``); the escaping error is
    preserved as ``__cause__`` on the first run and reconstructed on replay.

    Attributes:
        message: Human-readable failure message.
        error_type: Type name of the error that escaped the operation (the
            original/inner error, e.g. ``"ValueError"``), not the operation
            class. Defaults to the class name only when no escaping error is
            known.
        data: Optional serialized error payload, preserved across operation
            boundaries.
        stack_trace: Optional stack trace lines captured from the origin.
    """

    def __init__(
        self,
        message: str | None = None,
        error_type: str | None = None,
        data: str | None = None,
        stack_trace: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.message: str | None = message
        self.error_type: str = error_type or type(self).__name__
        self.data: str | None = data
        self.stack_trace: list[str] | None = stack_trace

    @classmethod
    def from_error_fields(
        cls,
        error_type: str | None,
        message: str | None,
        data: str | None,
        stack_trace: list[str] | None,
    ) -> DurableOperationError:
        """Rebuild the correct subclass from serialized checkpoint error fields.

        Looks the discriminator up in the reconstruction registry, falling back
        to the base :class:`DurableOperationError` when ``error_type`` is unknown
        (e.g. a downstream error surfaced by an async invoke/callback checkpoint).
        """
        target_cls: type[DurableOperationError] = _DURABLE_OPERATION_ERROR_REGISTRY.get(
            error_type or "", DurableOperationError
        )
        return target_cls(
            message=message,
            error_type=error_type,
            data=data,
            stack_trace=stack_trace,
        )


class StepError(DurableOperationError):
    """Raised when a step operation fails."""


class InvokeError(DurableOperationError):
    """Raised when a durable invoke operation fails."""


class ChildContextError(DurableOperationError):
    """Raised when a child context (run_in_child_context, map, parallel) fails."""


class WaitForConditionError(DurableOperationError):
    """Raised when a wait_for_condition operation fails."""


class CallbackError(DurableOperationError):
    """Base class for callback operation failures; catches all of them.

    Graded subclasses distinguish the cause: :class:`CallbackExternalError`
    (external system reported failure), :class:`CallbackTimeoutError` (timeout
    or heartbeat expiry), and :class:`CallbackSubmitterError` (submitter step
    failed). The base is raised directly for internal callback failures.
    """


class CallbackExternalError(CallbackError):
    """Raised when the external system reports a callback failure.

    Corresponds to the callback operation reaching FAILED because an external
    entity called ``SendDurableExecutionCallbackFailure``.
    """


class CallbackTimeoutError(CallbackError):
    """Raised when a callback times out (timeout or heartbeat expiry)."""


class CallbackSubmitterError(CallbackError):
    """Raised when the submitter step of a wait_for_callback fails."""


# Reconstruction registry for replay: only the SDK's own operation-error types.
# Any other discriminator falls back to DurableOperationError in
# from_error_fields, so we never call a constructor the SDK doesn't control.
_DURABLE_OPERATION_ERROR_REGISTRY: dict[str, type[DurableOperationError]] = {
    "StepError": StepError,
    "InvokeError": InvokeError,
    "ChildContextError": ChildContextError,
    "WaitForConditionError": WaitForConditionError,
    "CallbackError": CallbackError,
    "CallbackExternalError": CallbackExternalError,
    "CallbackTimeoutError": CallbackTimeoutError,
    "CallbackSubmitterError": CallbackSubmitterError,
}


class StepInterruptedError(InvocationError):
    """Raised when a step is interrupted before it checkpointed at the end."""

    def __init__(self, message: str, step_id: str | None = None):
        super().__init__(message, TerminationReason.STEP_INTERRUPTED)
        self.step_id = step_id


class BackgroundThreadError(BaseException):
    """Critical error from background checkpoint thread.

    Derives from BaseException to bypass normal exception handlers.
    Similar to KeyboardInterrupt or SystemExit - this is a system-level
    error that should terminate execution immediately without attempting
    to checkpoint or process the error.

    This exception is raised in the user thread when the background
    checkpoint processing thread encounters a fatal error. It propagates
    through CompletionEvent.wait() to interrupt blocked user code.

    Attributes:
        source_exception: The original exception from the background thread
    """

    def __init__(self, message: str, source_exception: Exception):
        super().__init__(message)
        self.source_exception = source_exception


class SuspendExecution(BaseException):
    """Raise this exception to suspend the current execution by returning PENDING to DAR.

    Note this derives from BaseException - in keeping with system-exiting exceptions like
    KeyboardInterrupt or SystemExit.
    """

    def __init__(self, message: str):
        super().__init__(message)


class TimedSuspendExecution(SuspendExecution):
    """Suspend execution until a specific timestamp.

    This is a specialized form of SuspendExecution that includes a scheduled resume time.

    Attributes:
        scheduled_timestamp (float): Unix timestamp in seconds at which to resume.
    """

    def __init__(self, message: str, scheduled_timestamp: float):
        super().__init__(message)
        self.scheduled_timestamp = scheduled_timestamp

    @classmethod
    def from_delay(cls, message: str, delay_seconds: int) -> TimedSuspendExecution:
        """Create a timed suspension with the delay calculated from now.

        Args:
            message: Descriptive message for the suspension
            delay_seconds: Duration to suspend in seconds from current time

        Returns:
            TimedSuspendExecution: Instance with calculated resume time

        Example:
            >>> exception = TimedSuspendExecution.from_delay("Waiting for callback", 30)
            >>> # Will suspend for 30 seconds from now
        """
        resume_time = time.time() + delay_seconds
        return cls(message, scheduled_timestamp=resume_time)

    @classmethod
    def from_datetime(
        cls, message: str, datetime_timestamp: datetime.datetime
    ) -> TimedSuspendExecution:
        """Create a timed suspension with the delay calculated from now.

        Args:
            message: Descriptive message for the suspension
            datetime_timestamp: Unix datetime timestamp in seconds at which to resume

        Returns:
            TimedSuspendExecution: Instance with calculated resume time
        """
        return cls(message, scheduled_timestamp=datetime_timestamp.timestamp())


class OrderedLockError(DurableExecutionsError):
    """An error from OrderedLock.

    Typically raised when a previous lock in the sequentially ordered chain of lock acquire requests failed.

    Because of the order guarantee of OrderedLock, subsequent queued up lock acquire requests cannot proceed,
    and will get this error instead.

    Attributes:
        source_exception (Exception): The exception that caused the lock to break.
    """

    def __init__(self, message: str, source_exception: Exception | None = None) -> None:
        """Initialize with the message and the exception source"""
        msg = (
            f"{message} {type(source_exception).__name__}: {source_exception}"
            if source_exception
            else message
        )
        super().__init__(msg)
        self.source_exception: Exception | None = source_exception


class SerDesError(DurableExecutionsError):
    """Raised when serialization fails."""


class OrphanedChildException(BaseException):
    """Raised when a child operation attempts to checkpoint after its parent context has completed.

    This exception inherits from BaseException (not Exception) so that user-space doesn't
    accidentally catch it with broad exception handlers like 'except Exception'.

    This exception will happen when a parallel branch or map item tries to create a checkpoint
    after its parent context (i.e the parallel/map operation) has already completed due to meeting
    completion criteria (e.g., min_successful reached, failure tolerance exceeded).

    Although you cannot cancel running futures in user-space, this will at least terminate the
    child operation on the next checkpoint attempt, preventing subsequent operations in the
    child scope from executing.

    Attributes:
        operation_id: Operation ID of the orphaned child
    """

    def __init__(self, message: str, operation_id: str):
        """Initialize OrphanedChildException.

        Args:
            message: Human-readable error message
            operation_id: Operation ID of the orphaned child (required)
        """
        super().__init__(message)
        self.operation_id = operation_id
