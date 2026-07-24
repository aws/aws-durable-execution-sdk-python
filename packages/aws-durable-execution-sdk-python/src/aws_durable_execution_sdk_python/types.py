"""Types and Protocols. Don't import anything other than config here - the reason it exists is to avoid circular references."""

from __future__ import annotations

import re
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from aws_durable_execution_sdk_python.config import (
        CallbackConfig,
        ChildConfig,
        Duration,
        MapConfig,
        ParallelBranch,
        ParallelConfig,
        StepConfig,
    )

T = TypeVar("T")
U = TypeVar("U")
C_co = TypeVar("C_co", covariant=True)
C_contra = TypeVar("C_contra", contravariant=True)


class LoggerInterface(Protocol):
    def debug(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover


@dataclass(frozen=True)
class OperationContext:
    logger: LoggerInterface


@dataclass(frozen=True)
class StepContext(OperationContext):
    """Context provided to step functions.

    Attributes:
        attempt: Current attempt number, starting at 1 for the first execution.
    """

    attempt: int


@dataclass(frozen=True)
class WaitForCallbackContext(OperationContext):
    """Context provided to waitForCallback submitter functions."""


@dataclass(frozen=True)
class WaitForConditionCheckContext(OperationContext):
    """Context provided to wait_for_condition check functions.

    Attributes:
        attempt: Current attempt number, starting at 1 for the first check.
    """

    attempt: int


class Callback(Protocol, Generic[C_co]):
    """Protocol for callback futures."""

    callback_id: str

    @abstractmethod
    def result(self) -> C_co | None:
        """Return the result of the future. Will block until result is available."""
        ...  # pragma: no cover


class BatchResult(Protocol, Generic[T]):
    """Protocol for batch operation results."""

    @abstractmethod
    def get_results(self) -> list[T]:
        """Get all successful results."""
        ...  # pragma: no cover


class DurableContext(Protocol):
    """Protocol defining the interface for durable execution contexts."""

    @abstractmethod
    def step(
        self,
        func: Callable[[StepContext], T],
        name: str | None = None,
        config: StepConfig | None = None,
    ) -> T:
        """Execute a step durably."""
        ...  # pragma: no cover

    @abstractmethod
    def run_in_child_context(
        self,
        func: Callable[[DurableContext], T],
        name: str | None = None,
        config: ChildConfig | None = None,
    ) -> T:
        """Run callable in a child context."""
        ...  # pragma: no cover

    @abstractmethod
    def map(
        self,
        inputs: Sequence[U],
        func: Callable[[DurableContext, U, int, Sequence[U]], T],
        name: str | None = None,
        config: MapConfig | None = None,
    ) -> BatchResult[T]:
        """Apply function durably to each item in inputs."""
        ...  # pragma: no cover

    @abstractmethod
    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], T] | ParallelBranch[T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute callables durably in parallel."""
        ...  # pragma: no cover

    @abstractmethod
    def wait(self, duration: Duration, name: str | None = None) -> None:
        """Wait for a specified amount of time."""
        ...  # pragma: no cover

    @abstractmethod
    def create_callback(
        self, name: str | None = None, config: CallbackConfig | None = None
    ) -> Callback:
        """Create a callback."""
        ...  # pragma: no cover


class LambdaContext(Protocol):  # pragma: no cover
    aws_request_id: str
    log_group_name: str | None = None
    log_stream_name: str | None = None
    function_name: str | None = None
    memory_limit_in_mb: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    tenant_id: str | None = None
    client_context: Any | None = None
    identity: Any | None = None

    def get_remaining_time_in_millis(self) -> int: ...
    def log(self, msg) -> None: ...


# region Summary

"""Summary generators for concurrent operations.

Summary generators create compact JSON representations of large BatchResult objects
when the serialized result exceeds the 256KB checkpoint size limit. This prevents
large payloads from being stored in checkpoints while maintaining operation metadata.

When a summary is used, the operation is marked with ReplayChildren=true, causing
the child context to be re-executed during replay to reconstruct the full result.
"""


class SummaryGenerator(Protocol[C_contra]):
    def __call__(self, result: C_contra) -> str: ...  # pragma: no cover


# endregion Summary


# Matches a durable execution ARN of the form:
# arn:<partition>:lambda:<region>:<account>:function:<functionName>:<version>/durable-execution/<executionName>/<invocationId>
_DURABLE_EXECUTION_ARN_PATTERN = re.compile(
    r"^arn:[^:]*:lambda:[^:]*:[^:]*:function:([^:/]+):[^:/]+/durable-execution/([^/]+)/([^/]+)$"
)


@dataclass(frozen=True)
class DurableExecutionArn:
    """Parsed components of a durable execution ARN.

    A durable execution ARN has the form:
        arn:<partition>:lambda:<region>:<account>:function:<functionName>:<version>/durable-execution/<executionName>/<invocationId>

    Attributes:
        function_name: The Lambda function name.
        execution_name: The durable execution name (unique per execution).
        invocation_id: The invocation ID (unique per invocation within an execution).
    """

    function_name: str
    execution_name: str
    invocation_id: str

    @classmethod
    def from_arn(cls, arn: str) -> DurableExecutionArn | None:
        """Parse a durable execution ARN string into its components.

        Args:
            arn: The full ARN string.

        Returns:
            A DurableExecutionArn instance, or None if the ARN doesn't match
            the expected durable execution format.
        """
        match = _DURABLE_EXECUTION_ARN_PATTERN.match(arn)
        if not match:
            return None
        return cls(
            function_name=match.group(1),
            execution_name=match.group(2),
            invocation_id=match.group(3),
        )
