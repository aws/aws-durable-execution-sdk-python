"""Types and Protocols. Don't import anything other than config here - the reason it exists is to avoid circular references."""

from __future__ import annotations

import logging
from abc import abstractmethod
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from aws_durable_execution_sdk_python.lambda_service import ErrorObject

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from aws_durable_execution_sdk_python.config import (
        BatchedInput,
        CallbackConfig,
        ChildConfig,
        CompletionConfig,
        MapConfig,
        ParallelConfig,
        StepConfig,
    )

logger = logging.getLogger(__name__)

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
    pass


@dataclass(frozen=True)
class WaitForConditionCheckContext(OperationContext):
    pass


class Callback(Protocol, Generic[C_co]):
    """Protocol for callback futures."""

    callback_id: str

    @abstractmethod
    def result(self) -> C_co | None:
        """Return the result of the future. Will block until result is available."""
        ...  # pragma: no cover


class BatchResultProtocol(Protocol, Generic[T]):
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
        func: Callable[[DurableContext, U | BatchedInput[Any, U], int, Sequence[U]], T],
        name: str | None = None,
        config: MapConfig | None = None,
    ) -> BatchResult[T]:
        """Apply function durably to each item in inputs."""
        ...  # pragma: no cover

    @abstractmethod
    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute callables durably in parallel."""
        ...  # pragma: no cover

    @abstractmethod
    def wait(self, seconds: int, name: str | None = None) -> None:
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


# region Batch Types


class BatchItemStatus(Enum):
    """Status of a batch item."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class CompletionReason(Enum):
    """Reason for batch operation completion."""

    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"


R = TypeVar("R")


@dataclass(frozen=True)
class BatchItem(Generic[R]):
    """Represents a single item in a batch operation."""

    index: int
    status: BatchItemStatus
    result: R | None = None
    error: ErrorObject | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "index": self.index,
            "status": self.status.value,
            "result": self.result,
            "error": self.error.to_dict() if self.error else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchItem[R]:
        """Create from dictionary representation."""
        return cls(
            index=data["index"],
            status=BatchItemStatus(data["status"]),
            result=data.get("result"),
            error=ErrorObject.from_dict(data["error"]) if data.get("error") else None,
        )


@dataclass(frozen=True)
class BatchResult(Generic[R]):
    """Result of a batch operation containing multiple items."""

    all: list[BatchItem[R]]
    completion_reason: CompletionReason

    @classmethod
    def from_dict(
        cls, data: dict, completion_config: CompletionConfig | None = None
    ) -> BatchResult[R]:
        """Create from dictionary representation."""
        batch_items: list[BatchItem[R]] = [
            BatchItem.from_dict(item) for item in data["all"]
        ]

        completion_reason_value = data.get("completionReason")
        if completion_reason_value is None:
            # Infer completion reason from batch item statuses and completion config
            # This aligns with the TypeScript implementation that uses completion config
            # to accurately reconstruct the completion reason during replay
            result = cls.from_items(batch_items, completion_config)
            logger.warning(
                "Missing completionReason in BatchResult deserialization, "
                "inferred '%s' from batch item statuses. "
                "This may indicate incomplete serialization data.",
                result.completion_reason.value,
            )
            return result

        completion_reason = CompletionReason(completion_reason_value)
        return cls(batch_items, completion_reason)

    @classmethod
    def from_items(
        cls,
        items: list[BatchItem[R]],
        completion_config: CompletionConfig | None = None,
    ):
        """
        Infer completion reason based on batch item statuses and completion config.

        This follows the same logic as the TypeScript implementation:
        - If all items completed: ALL_COMPLETED
        - If minSuccessful threshold met and not all completed: MIN_SUCCESSFUL_REACHED
        - Otherwise: FAILURE_TOLERANCE_EXCEEDED
        """
        statuses = (item.status for item in items)
        counts = Counter(statuses)
        succeeded_count = counts.get(BatchItemStatus.SUCCEEDED, 0)
        failed_count = counts.get(BatchItemStatus.FAILED, 0)
        started_count = counts.get(BatchItemStatus.STARTED, 0)

        completed_count = succeeded_count + failed_count
        total_count = started_count + completed_count

        # If all items completed (no started items), it's ALL_COMPLETED
        if completed_count == total_count:
            completion_reason = CompletionReason.ALL_COMPLETED
        elif (  # If we have completion config and minSuccessful threshold is met
            completion_config
            and (min_successful := completion_config.min_successful) is not None
            and succeeded_count >= min_successful
        ):
            completion_reason = CompletionReason.MIN_SUCCESSFUL_REACHED
        else:
            # Otherwise, assume failure tolerance was exceeded
            completion_reason = CompletionReason.FAILURE_TOLERANCE_EXCEEDED

        return cls(items, completion_reason)

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "all": [item.to_dict() for item in self.all],
            "completionReason": self.completion_reason.value,
        }

    def succeeded(self) -> list[BatchItem[R]]:
        """Get list of succeeded items."""
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def failed(self) -> list[BatchItem[R]]:
        """Get list of failed items."""
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    def started(self) -> list[BatchItem[R]]:
        """Get list of started items."""
        return [item for item in self.all if item.status is BatchItemStatus.STARTED]

    @property
    def status(self) -> BatchItemStatus:
        """Overall status of the batch."""
        return BatchItemStatus.FAILED if self.has_failure else BatchItemStatus.SUCCEEDED

    @property
    def has_failure(self) -> bool:
        """True if any item failed."""
        return any(item.status is BatchItemStatus.FAILED for item in self.all)

    def throw_if_error(self) -> None:
        """Throw first error if any item failed."""
        first_error = next(
            (item.error for item in self.all if item.status is BatchItemStatus.FAILED),
            None,
        )
        if first_error:
            raise first_error.to_callable_runtime_error()

    def get_results(self) -> list[R]:
        """Get results from succeeded items."""
        return [
            item.result
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def get_errors(self) -> list[ErrorObject]:
        """Get errors from failed items."""
        return [
            item.error
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    @property
    def success_count(self) -> int:
        """Count of succeeded items."""
        return sum(1 for item in self.all if item.status is BatchItemStatus.SUCCEEDED)

    @property
    def failure_count(self) -> int:
        """Count of failed items."""
        return sum(1 for item in self.all if item.status is BatchItemStatus.FAILED)

    @property
    def started_count(self) -> int:
        """Count of started items."""
        return sum(1 for item in self.all if item.status is BatchItemStatus.STARTED)

    @property
    def total_count(self) -> int:
        """Total count of items."""
        return len(self.all)


# endregion Batch Types
