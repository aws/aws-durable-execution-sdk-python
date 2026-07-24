"""Models for concurrent map and parallel execution."""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    InvalidStateError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.types import BatchResult as BatchResultProtocol

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.config import CompletionConfig
    from aws_durable_execution_sdk_python.types import SummaryGenerator


logger = logging.getLogger(__name__)

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
class CompletionPolicy:
    """Evaluate completion criteria for a batch of concurrent branches.

    Single home for the completion logic shared by the concurrency
    coordinator (scheduling decisions) and :class:`BatchResult`
    (completion reason inference). A user-supplied completion predicate,
    if introduced later, slots in here.

    Fail-fast semantics: when no criteria are configured at all, a single
    failure exceeds tolerance.
    """

    total: int
    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: int | float | None = None

    @classmethod
    def from_config(
        cls, total: int, config: CompletionConfig | None
    ) -> CompletionPolicy:
        """Build a policy for a batch of ``total`` branches from a CompletionConfig."""
        if config is None:
            return cls(total=total)
        return cls(
            total=total,
            min_successful=config.min_successful,
            tolerated_failure_count=config.tolerated_failure_count,
            tolerated_failure_percentage=config.tolerated_failure_percentage,
        )

    @property
    def has_criteria(self) -> bool:
        """True when any completion criterion is configured."""
        return (
            self.min_successful is not None
            or self.tolerated_failure_count is not None
            or self.tolerated_failure_percentage is not None
        )

    def is_tolerance_exceeded(self, failed: int) -> bool:
        """True when failures exceed the configured tolerance."""
        if not self.has_criteria:
            return failed > 0
        if (
            self.tolerated_failure_count is not None
            and failed > self.tolerated_failure_count
        ):
            return True
        if self.tolerated_failure_percentage is not None and self.total > 0:
            failure_percentage: float = (failed / self.total) * 100
            return failure_percentage > self.tolerated_failure_percentage
        return False

    def should_continue(self, failed: int) -> bool:
        """True while more branches may be scheduled."""
        return not self.is_tolerance_exceeded(failed)

    def is_complete(self, succeeded: int, failed: int) -> bool:
        """True when the batch has met a completion criterion."""
        if succeeded + failed >= self.total:
            return True
        return self.min_successful is not None and succeeded >= self.min_successful

    def reason(self, succeeded: int, failed: int) -> CompletionReason:
        """Infer the completion reason. Tolerance is checked first."""
        if self.is_tolerance_exceeded(failed):
            return CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        if succeeded + failed >= self.total:
            return CompletionReason.ALL_COMPLETED
        if self.min_successful is not None and succeeded >= self.min_successful:
            return CompletionReason.MIN_SUCCESSFUL_REACHED
        return CompletionReason.ALL_COMPLETED


@dataclass(frozen=True)
class CompletionRecord:
    """The completion decision recorded in a batch operation's summary.

    Written by the map/parallel summary generators when a large result is
    summarized, and read back by replay so the reconstructed result matches
    the live result exactly instead of being re-derived.

    ``started_total`` is the number of branches ever started. Branches are
    admitted in index order, so the started set is exactly the index prefix
    ``[0, started_total)``. ``started_indexes`` is the authoritative set of
    branches that were live-reported STARTED (started but not terminal when
    the batch completed).
    """

    completion_reason: CompletionReason
    started_total: int
    started_indexes: frozenset[int]

    @classmethod
    def from_summary_payload(cls, payload: str | None) -> CompletionRecord | None:
        """Parse a recorded completion decision from a summary payload.

        Returns None when the payload does not carry a valid decision
        record: absent or empty payloads, payloads written before the
        record existed, unknown completion reasons, and malformed or
        out-of-range index fields. Never raises on malformed input.
        """
        if not payload:
            return None
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            return None
        if not isinstance(data, dict):
            return None

        reason_value = data.get("completionReason")
        started_total = data.get("totalCount")
        if not isinstance(reason_value, str) or not isinstance(started_total, int):
            return None
        if isinstance(started_total, bool) or started_total < 0:
            return None
        try:
            completion_reason = CompletionReason(reason_value)
        except ValueError:
            return None

        started_raw = data.get("startedIndexes")
        completed_raw = data.get("completedIndexes")
        if isinstance(started_raw, list):
            indexes = cls._validated_indexes(started_raw, started_total)
            if indexes is None:
                return None
            started_indexes = indexes
        elif isinstance(completed_raw, list):
            indexes = cls._validated_indexes(completed_raw, started_total)
            if indexes is None:
                return None
            started_indexes = frozenset(range(started_total)) - indexes
        else:
            return None

        return cls(
            completion_reason=completion_reason,
            started_total=started_total,
            started_indexes=started_indexes,
        )

    @staticmethod
    def _validated_indexes(
        raw: list[object], started_total: int
    ) -> frozenset[int] | None:
        """Validate a raw index list: ints (not bools) within [0, started_total)."""
        validated: set[int] = set()
        for element in raw:
            if isinstance(element, bool) or not isinstance(element, int):
                return None
            if element < 0 or element >= started_total:
                return None
            validated.add(element)
        return frozenset(validated)

    @staticmethod
    def summary_index_fields(items: list[BatchItem]) -> dict[str, list[int]]:
        """Build the index field for a summary from live batch items.

        Records whichever of the started or completed index sets is
        smaller, so the record stays compact at both early-exit extremes.
        """
        started: list[int] = [
            item.index for item in items if item.status is BatchItemStatus.STARTED
        ]
        completed: list[int] = [
            item.index for item in items if item.status is not BatchItemStatus.STARTED
        ]
        if len(started) <= len(completed):
            return {"startedIndexes": started}
        return {"completedIndexes": completed}

    @staticmethod
    def summary_envelope(
        result: BatchResult,
        result_type: str,
        summary: str | None,
    ) -> str:
        """Serialize a batch operation's large-result summary payload.

        The payload is an SDK-owned JSON envelope: the completion decision
        record (``totalCount``, ``completionReason``, and the started or
        completed index set) that replay requires, informational view
        fields, and the optional customer summary verbatim under
        ``summary``. Written and parsed with plain JSON, independent of
        any configured serdes. A payload exceeding the checkpoint size
        limit fails the operation when checkpointed.
        """
        fields: dict[str, object] = {
            "type": result_type,
            "totalCount": result.total_count,
            "completionReason": result.completion_reason.value,
            **CompletionRecord.summary_index_fields(result.all),
            "startedCount": result.started_count,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "status": result.status.value,
        }
        if summary is not None:
            fields["summary"] = summary if isinstance(summary, str) else str(summary)
        return json.dumps(fields)


def envelope_summary_generator(
    result_type: str,
    custom_generator: SummaryGenerator | None,
) -> SummaryGenerator:
    """Build the summary generator for a batch operation's parent context.

    The returned generator wraps the optional customer generator: the SDK
    always writes the envelope with the completion decision record, and
    the customer generator only contributes the ``summary`` field. An
    exception raised by the customer generator propagates and fails the
    operation.
    """

    def generate(result: BatchResult) -> str:
        summary: str | None = None
        if custom_generator is not None:
            summary = custom_generator(result)
        return CompletionRecord.summary_envelope(result, result_type, summary)

    return generate


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
        """Infer completion reason from batch item statuses and completion config.

        The batch total is derived from the items present, so this is only
        exact when every branch of the batch is represented. The concurrency
        executor, which may omit never-started branches, computes the reason
        against the true total instead.
        """
        counts: Counter[BatchItemStatus] = Counter(item.status for item in items)
        succeeded_count: int = counts.get(BatchItemStatus.SUCCEEDED, 0)
        failed_count: int = counts.get(BatchItemStatus.FAILED, 0)

        policy: CompletionPolicy = CompletionPolicy.from_config(
            len(items), completion_config
        )
        return cls(items, policy.reason(succeeded_count, failed_count))

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
        if first_error is not None:
            # Each item runs in a child context, so a failed item surfaces as a
            # ChildContextError wrapping the escaping error (reconstructed as its
            # typed form where possible, with __cause__ set). Remaining errors
            # stay in get_errors().
            first_error.raise_as_operation_error(ChildContextError)

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
    name: str | None = None


class BranchStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    SUSPENDED_WITH_TIMEOUT = "suspended_with_timeout"
    FAILED = "failed"


class Branch(Generic[CallableType, ResultType]):
    """Execution state of a single branch.

    Owned exclusively by the coordinator thread: every transition happens
    there, so no synchronization is needed.
    """

    def __init__(self, executable: Executable[CallableType]) -> None:
        self.executable = executable
        self.status: BranchStatus = BranchStatus.PENDING
        self.result: ResultType | None = None
        self.error: Exception | None = None
        self.resume_at: float | None = None

    @property
    def index(self) -> int:
        return self.executable.index

    def start(self) -> None:
        """Transition to RUNNING for initial submission or timed resume."""
        if self.status not in {
            BranchStatus.PENDING,
            BranchStatus.SUSPENDED_WITH_TIMEOUT,
        }:
            msg = f"Cannot start branch {self.index} from {self.status}"
            raise InvalidStateError(msg)
        self.status = BranchStatus.RUNNING
        self.resume_at = None

    def complete(self, result: ResultType | None) -> None:
        self.status = BranchStatus.COMPLETED
        self.result = result

    def fail(self, error: Exception) -> None:
        self.status = BranchStatus.FAILED
        self.error = error

    def suspend(self) -> None:
        """Suspend indefinitely, pending an external callback."""
        self.status = BranchStatus.SUSPENDED
        self.resume_at = None

    def suspend_until(self, resume_at: float) -> None:
        """Suspend until a timestamp, eligible for in-process resume."""
        self.status = BranchStatus.SUSPENDED_WITH_TIMEOUT
        self.resume_at = resume_at


class BranchEventKind(Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    SUSPENDED = "suspended"
    SUSPENDED_UNTIL = "suspended_until"
    ORPHANED = "orphaned"
    FATAL = "fatal"


@dataclass(frozen=True)
class BranchEvent(Generic[ResultType]):
    """Outcome of one branch execution attempt.

    The only message worker threads send to the coordinator. Workers never
    touch branch state directly.
    """

    index: int
    kind: BranchEventKind
    result: ResultType | None = None
    error: Exception | None = None
    resume_at: float | None = None
    fatal_error: BaseException | None = None

    @classmethod
    def completed(
        cls, index: int, result: ResultType | None
    ) -> BranchEvent[ResultType]:
        return cls(index=index, kind=BranchEventKind.COMPLETED, result=result)

    @classmethod
    def failed(cls, index: int, error: Exception) -> BranchEvent[ResultType]:
        return cls(index=index, kind=BranchEventKind.FAILED, error=error)

    @classmethod
    def suspended(cls, index: int) -> BranchEvent[ResultType]:
        return cls(index=index, kind=BranchEventKind.SUSPENDED)

    @classmethod
    def suspended_until(cls, index: int, resume_at: float) -> BranchEvent[ResultType]:
        return cls(
            index=index, kind=BranchEventKind.SUSPENDED_UNTIL, resume_at=resume_at
        )

    @classmethod
    def orphaned(cls, index: int) -> BranchEvent[ResultType]:
        return cls(index=index, kind=BranchEventKind.ORPHANED)

    @classmethod
    def fatal(cls, index: int, error: BaseException) -> BranchEvent[ResultType]:
        """A system-level error that must propagate to the calling thread.

        Carries the original BaseException (for example a background
        checkpoint failure) so the coordinator re-raises it instead of
        recording a branch failure.
        """
        return cls(index=index, kind=BranchEventKind.FATAL, fatal_error=error)


# endregion concurrency models
