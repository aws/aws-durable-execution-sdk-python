"""Exceptions for the Durable Executions SDK.

Avoid any non-stdlib references in this module, it is at the bottom of the dependency chain.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime


class DurableExecutionsError(Exception):
    """Base class for Durable Executions exceptions"""


class FatalError(DurableExecutionsError):
    """Unrecoverable error. Will not retry."""


class CheckpointError(FatalError):
    """Failure to checkpoint. Will terminate the lambda."""


class ValidationError(DurableExecutionsError):
    """Incorrect arguments to a Durable Function operation."""


class InvalidStateError(DurableExecutionsError):
    """Raised when an operation is attempted on an object in an invalid state."""


class UserlandError(DurableExecutionsError):
    """Failure in user-land - i.e code passed into durable executions from the caller."""


class CallableRuntimeError(UserlandError):
    """This error wraps any failure from inside the callable code that you pass to a Durable Function operation."""

    def __init__(
        self,
        message: str | None,
        error_type: str | None,
        data: str | None,
        stack_trace: list[str] | None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.data = data
        self.stack_trace = stack_trace


class StepInterruptedError(UserlandError):
    """Raised when a step is interrupted before it checkpointed at the end."""


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


@dataclass(frozen=True)
class CallableRuntimeErrorSerializableDetails:
    """Serializable error details."""

    type: str
    message: str

    @classmethod
    def from_exception(
        cls, exception: Exception
    ) -> CallableRuntimeErrorSerializableDetails:
        """Create an instance from an Exception, using its type and message.

        Args:
            exception: An Exception instance

        Returns:
            A CallableRuntimeErrorDetails instance with the exception's type name and message
        """
        return cls(type=exception.__class__.__name__, message=str(exception))

    def __str__(self) -> str:
        """
        Return a string representation of the object.

        Returns:
            A string in the format "type: message"
        """
        return f"{self.type}: {self.message}"


class SerDesError(DurableExecutionsError):
    """Raised when serialization fails."""
