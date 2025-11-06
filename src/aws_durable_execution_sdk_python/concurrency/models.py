"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    InvalidStateError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.types import BatchResult as BatchResultProtocol

if TYPE_CHECKING:
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python.config import CompletionConfig


logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

CallableType = TypeVar("CallableType")
ResultType = TypeVar("ResultType")


# region Result models
class BatchItemStatus(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class CompletionReason(Enum):
    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"


@dataclass(frozen=True)
class SuspendResult:
    should_suspend: bool
    exception: SuspendExecution | None = None

    @staticmethod
    def do_not_suspend() -> SuspendResult:
        return SuspendResult(should_suspend=False)

    @staticmethod
    def suspend(exception: SuspendExecution) -> SuspendResult:
        return SuspendResult(should_suspend=True, exception=exception)


@dataclass(frozen=True)
class BatchItem(Generic[R]):
    index: int
    status: BatchItemStatus
    result: R | None = None
    error: ErrorObject | None = None

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "status": self.status.value,
            "result": self.result,
            "error": self.error.to_dict() if self.error else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchItem[R]:
        return cls(
            index=data["index"],
            status=BatchItemStatus(data["status"]),
            result=data.get("result"),
            error=ErrorObject.from_dict(data["error"]) if data.get("error") else None,
        )


@dataclass(frozen=True)
class BatchResult(Generic[R], BatchResultProtocol[R]):  # noqa: PYI059
    all: list[BatchItem[R]]
    completion_reason: CompletionReason

    @classmethod
    def from_dict(
        cls, data: dict, completion_config: CompletionConfig | None = None
    ) -> BatchResult[R]:
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
        return {
            "all": [item.to_dict() for item in self.all],
            "completionReason": self.completion_reason.value,
        }

    def succeeded(self) -> list[BatchItem[R]]:
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def failed(self) -> list[BatchItem[R]]:
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    def started(self) -> list[BatchItem[R]]:
        return [item for item in self.all if item.status is BatchItemStatus.STARTED]

    @property
    def status(self) -> BatchItemStatus:
        return BatchItemStatus.FAILED if self.has_failure else BatchItemStatus.SUCCEEDED

    @property
    def has_failure(self) -> bool:
        return any(item.status is BatchItemStatus.FAILED for item in self.all)

    def throw_if_error(self) -> None:
        first_error = next(
            (item.error for item in self.all if item.status is BatchItemStatus.FAILED),
            None,
        )
        if first_error:
            raise first_error.to_callable_runtime_error()

    def get_results(self) -> list[R]:
        return [
            item.result
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def get_errors(self) -> list[ErrorObject]:
        return [
            item.error
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    @property
    def success_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.SUCCEEDED)

    @property
    def failure_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.FAILED)

    @property
    def started_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.STARTED)

    @property
    def total_count(self) -> int:
        return len(self.all)


# endregion Result models


# region concurrency models
@dataclass(frozen=True)
class Executable(Generic[CallableType]):
    index: int
    func: CallableType


class BranchStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    SUSPENDED_WITH_TIMEOUT = "suspended_with_timeout"
    FAILED = "failed"


class ExecutableWithState(Generic[CallableType, ResultType]):
    """Manages the execution state and lifecycle of an executable."""

    def __init__(self, executable: Executable[CallableType]):
        self.executable = executable
        self._status = BranchStatus.PENDING
        self._future: Future | None = None
        self._suspend_until: float | None = None
        self._result: ResultType = None  # type: ignore[assignment]
        self._is_result_set: bool = False
        self._error: Exception | None = None

    @property
    def future(self) -> Future:
        """Get the future, raising error if not available."""
        if self._future is None:
            msg = f"ExecutableWithState was never started. {self.executable.index}"
            raise InvalidStateError(msg)
        return self._future

    @property
    def status(self) -> BranchStatus:
        """Get current status."""
        return self._status

    @property
    def result(self) -> ResultType:
        """Get result if completed."""
        if not self._is_result_set or self._status != BranchStatus.COMPLETED:
            msg = f"result not available in status {self._status}"
            raise InvalidStateError(msg)
        return self._result

    @property
    def error(self) -> Exception:
        """Get error if failed."""
        if self._error is None or self._status != BranchStatus.FAILED:
            msg = f"error not available in status {self._status}"
            raise InvalidStateError(msg)
        return self._error

    @property
    def suspend_until(self) -> float | None:
        """Get suspend timestamp."""
        return self._suspend_until

    @property
    def is_running(self) -> bool:
        """Check if currently running."""
        return self._status is BranchStatus.RUNNING

    @property
    def can_resume(self) -> bool:
        """Check if can resume from suspension."""
        return self._status is BranchStatus.SUSPENDED or (
            self._status is BranchStatus.SUSPENDED_WITH_TIMEOUT
            and self._suspend_until is not None
            and time.time() >= self._suspend_until
        )

    @property
    def index(self) -> int:
        return self.executable.index

    @property
    def callable(self) -> CallableType:
        return self.executable.func

    # region State transitions
    def run(self, future: Future) -> None:
        """Transition to RUNNING state with a future."""
        if self._status != BranchStatus.PENDING:
            msg = f"Cannot start running from {self._status}"
            raise InvalidStateError(msg)
        self._status = BranchStatus.RUNNING
        self._future = future

    def suspend(self) -> None:
        """Transition to SUSPENDED state (indefinite)."""
        self._status = BranchStatus.SUSPENDED
        self._suspend_until = None

    def suspend_with_timeout(self, timestamp: float) -> None:
        """Transition to SUSPENDED_WITH_TIMEOUT state."""
        self._status = BranchStatus.SUSPENDED_WITH_TIMEOUT
        self._suspend_until = timestamp

    def complete(self, result: ResultType) -> None:
        """Transition to COMPLETED state."""
        self._status = BranchStatus.COMPLETED
        self._result = result
        self._is_result_set = True

    def fail(self, error: Exception) -> None:
        """Transition to FAILED state."""
        self._status = BranchStatus.FAILED
        self._error = error

    def reset_to_pending(self) -> None:
        """Reset to PENDING state for resubmission."""
        self._status = BranchStatus.PENDING
        self._future = None
        self._suspend_until = None

    # endregion State transitions


class ExecutionCounters:
    """Thread-safe counters for tracking execution state."""

    def __init__(
        self,
        total_tasks: int,
        min_successful: int,
        tolerated_failure_count: int | None,
        tolerated_failure_percentage: float | None,
    ):
        self.total_tasks: int = total_tasks
        self.min_successful: int = min_successful
        self.tolerated_failure_count: int | None = tolerated_failure_count
        self.tolerated_failure_percentage: float | None = tolerated_failure_percentage
        self.success_count: int = 0
        self.failure_count: int = 0
        self._lock = threading.Lock()

    def complete_task(self) -> None:
        """Task completed successfully."""
        with self._lock:
            self.success_count += 1

    def fail_task(self) -> None:
        """Task failed."""
        with self._lock:
            self.failure_count += 1

    def should_continue(self) -> bool:
        """
        Check if we should continue starting new tasks (based on failure tolerance).
        Matches TypeScript shouldContinue() logic.
        """
        with self._lock:
            # If no completion config, only continue if no failures
            if (
                self.tolerated_failure_count is None
                and self.tolerated_failure_percentage is None
            ):
                return self.failure_count == 0

            # Check failure count tolerance
            if (
                self.tolerated_failure_count is not None
                and self.failure_count > self.tolerated_failure_count
            ):
                return False

            # Check failure percentage tolerance
            if self.tolerated_failure_percentage is not None and self.total_tasks > 0:
                failure_percentage = (self.failure_count / self.total_tasks) * 100
                if failure_percentage > self.tolerated_failure_percentage:
                    return False

            return True

    def is_complete(self) -> bool:
        """
        Check if execution should complete (based on completion criteria).
        Matches TypeScript isComplete() logic.
        """
        with self._lock:
            completed_count = self.success_count + self.failure_count

            # All tasks completed
            if completed_count == self.total_tasks:
                # Complete if no failure tolerance OR no failures OR min successful reached
                return (
                    (
                        self.tolerated_failure_count is None
                        and self.tolerated_failure_percentage is None
                    )
                    or self.failure_count == 0
                    or self.success_count >= self.min_successful
                )

            # when we breach min successful, we've completed
            return self.success_count >= self.min_successful

    def should_complete(self) -> bool:
        """
        Check if execution should complete.
        Combines TypeScript shouldContinue() and isComplete() logic.
        """
        return self.is_complete() or not self.should_continue()

    def is_all_completed(self) -> bool:
        """True if all tasks completed successfully."""
        with self._lock:
            return self.success_count == self.total_tasks

    def is_min_successful_reached(self) -> bool:
        """True if minimum successful tasks reached."""
        with self._lock:
            return self.success_count >= self.min_successful

    def is_failure_tolerance_exceeded(self) -> bool:
        """True if failure tolerance was exceeded."""
        with self._lock:
            return self._is_failure_condition_reached(
                tolerated_count=self.tolerated_failure_count,
                tolerated_percentage=self.tolerated_failure_percentage,
                failure_count=self.failure_count,
            )

    def _is_failure_condition_reached(
        self,
        tolerated_count: int | None,
        tolerated_percentage: float | None,
        failure_count: int,
    ) -> bool:
        """True if failure conditions are reached (no locking - caller must lock)."""
        # Failure count condition
        if tolerated_count is not None and failure_count > tolerated_count:
            return True

        # Failure percentage condition
        if tolerated_percentage is not None and self.total_tasks > 0:
            failure_percentage = (failure_count / self.total_tasks) * 100
            if failure_percentage > tolerated_percentage:
                return True

        return False


# endegion concurrency models
