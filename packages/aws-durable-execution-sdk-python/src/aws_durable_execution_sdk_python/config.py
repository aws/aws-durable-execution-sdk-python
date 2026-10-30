"""Configuration types."""

from __future__ import annotations

import math
import random
import string
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

from aws_durable_execution_sdk_python.exceptions import ValidationError


class DistributedMapStatus(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"
    TIMED_OUT = "TIMED_OUT"


class DistributedMapItemStatus(Enum):
    """Terminal status of a single map run item."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN_TO_SDK_VERSION = "UNKNOWN_TO_SDK_VERSION"


class DistributedMapSourceFormat(Enum):
    JSON_LINES = "JSON_LINES"
    JSON_ARRAY = "JSON_ARRAY"
    CSV = "CSV"


class DistributedMapS3SourceType(Enum):
    """What an S3 distributed map source reads."""

    OBJECT = "OBJECT"
    LISTED_OBJECT_METADATA = "LISTED_OBJECT_METADATA"
    LISTED_OBJECTS = "LISTED_OBJECTS"


class ProcessorResponseMode(Enum):
    """How a processor function reports item outcomes."""

    BATCH = "BATCH"
    ITEM_OUTCOMES = "ITEM_OUTCOMES"


class DistributedMapCsvDelimiter(Enum):
    """Column delimiter for a CSV distributed map source."""

    COMMA = "COMMA"
    PIPE = "PIPE"
    SEMICOLON = "SEMICOLON"
    SPACE = "SPACE"
    TAB = "TAB"


P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")
ItemOutput = TypeVar("ItemOutput")  # Map run item output type

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from aws_durable_execution_sdk_python.retries import RetryDecision
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.types import SummaryGenerator


Numeric = int | float  # deliberately leaving off complex


@dataclass(frozen=True)
class Duration:
    """Represents a duration stored as total seconds."""

    seconds: int = 0

    def __post_init__(self) -> None:
        if self.seconds < 0:
            msg = "Duration seconds must be positive"
            raise ValidationError(msg)

    def to_seconds(self) -> int:
        """Convert the duration to total seconds."""
        return self.seconds

    @classmethod
    def from_seconds(cls, value: float) -> Duration:
        """Create a Duration from total seconds."""
        return cls(seconds=int(value))

    @classmethod
    def from_minutes(cls, value: float) -> Duration:
        """Create a Duration from minutes."""
        return cls(seconds=int(value * 60))

    @classmethod
    def from_hours(cls, value: float) -> Duration:
        """Create a Duration from hours."""
        return cls(seconds=int(value * 3600))

    @classmethod
    def from_days(cls, value: float) -> Duration:
        """Create a Duration from days."""
        return cls(seconds=int(value * 86400))


class NestingType(Enum):
    """Control how child contexts are created for batch operations.

    Applies to `map` and `parallel`. Each branch or iteration runs inside a
    child context.

        - NESTED: full checkpointed context
        - FLAT: a virtual context that skips checkpoints for the branch/iteration.

    """

    NESTED = "NESTED"
    """Create CONTEXT operations for each branch/iteration with full checkpointing.

    Operations within each branch/iteration are wrapped in their own context.

    - Observability: high — each branch/iteration appears as a separate
      operation in execution history.
    - Cost: higher — consumes more operations due to CONTEXT creation
      overhead.
    - Scale: lower maximum iterations due to operation limits.
    """

    FLAT = "FLAT"
    """Skip CONTEXT operations for branches/iterations using virtual contexts.

    Operations execute directly without individual context wrapping.

    - Observability: lower — branches/iterations don't appear as separate
      operations in execution history.
    - Cost: ~30% lower — reduces operation consumption by skipping CONTEXT
      overhead.
    - Scale: higher maximum iterations possible within operation limits.
    """


class BatchItemStatus(Enum):
    """The status of a batch item in map/parallel operations."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class CompletionOutcome(Enum):
    """Outcome of a custom completion decision.

    Declares whether completing the batch now represents an overall success
    or failure. This is a batch-level outcome, distinct from BatchItemStatus
    which describes individual items.
    """

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class CompletionDecision:
    """Decision returned by a should_complete predicate.

    Use the factory functions :func:`continue_batch` and
    :func:`complete_batch` to construct instances.

    Attributes:
        complete: Whether to complete the batch now.
        outcome: When complete is True, whether this completion represents
            overall success or failure. None when complete is False.
    """

    complete: bool
    outcome: CompletionOutcome | None = None

    def __post_init__(self) -> None:
        if self.complete and self.outcome is None:
            msg = "outcome is required when complete is True"
            raise ValidationError(msg)
        if not self.complete and self.outcome is not None:
            msg = "outcome must be None when complete is False"
            raise ValidationError(msg)


def continue_batch() -> CompletionDecision:
    """Continue the batch - do not complete early."""
    return CompletionDecision(complete=False)


def complete_batch(
    outcome: CompletionOutcome = CompletionOutcome.SUCCEEDED,
) -> CompletionDecision:
    """Complete the batch now with the given outcome (default SUCCEEDED)."""
    return CompletionDecision(complete=True, outcome=outcome)


@dataclass(frozen=True)
class CompletionItemStatus:
    """Status snapshot of a single branch/item in a batch operation.

    Provided within :class:`CompletionStatus` so custom predicates can
    inspect individual branch outcomes for quorum-style decisions.

    Attributes:
        index: The branch/item index (stable across replay).
        name: Optional custom name of the branch/item, when one was provided.
        status: Current status as a BatchItemStatus enum value, or None if
            the branch has not been scheduled yet (common when max_concurrency
            limits in-flight branches).
    """

    index: int
    name: str | None = None
    status: BatchItemStatus | None = None


@dataclass(frozen=True)
class CompletionStatus:
    """Progress snapshot passed to a custom completion predicate.

    Provided to the ``should_complete`` callable on each branch terminal
    event so the predicate can decide whether the batch should finish early.

    Attributes:
        success_count: Number of branches that have completed successfully.
        failure_count: Number of branches that have failed.
        completed_count: Total terminal branches (success_count + failure_count).
        total_count: Total number of branches in the batch.
        items: Per-branch status snapshot ordered by original index. items[i]
            is always the branch defined at position i, regardless of
            completion order. Enables index-based quorum rules such as
            "complete when branch 0 succeeds OR branches 1 and 2 both
            succeed".
    """

    success_count: int
    failure_count: int
    completed_count: int
    total_count: int
    items: tuple[CompletionItemStatus, ...] = ()


@dataclass(frozen=True)
class CompletionConfig:
    """Configuration for determining when parallel/map operations complete.

    This class defines the success/failure criteria for operations that process
    multiple items or branches concurrently.

    Args:
        min_successful: Minimum number of successful completions required.
            If None, no minimum is enforced. Use this to implement "at least N
            must succeed" semantics.

        tolerated_failure_count: Maximum number of failures allowed before
            the operation is considered failed. If None, no limit on failure count.
            Use this to implement "fail fast after N failures" semantics.

        tolerated_failure_percentage: Maximum percentage of failures allowed
            (0.0 to 100.0). If None, no percentage limit is enforced.
            Use this to implement "fail if more than X% fail" semantics.

        should_complete: Optional predicate for custom completion logic.
            Cannot be combined with threshold fields (min_successful,
            tolerated_failure_count, tolerated_failure_percentage).

            The predicate receives a CompletionStatus snapshot and returns a
            CompletionDecision: either continue_batch() to keep going, or
            complete_batch(outcome) to stop the batch now. The outcome
            determines whether this completion represents overall success
            (CUSTOM_COMPLETION_SUCCEEDED) or failure
            (CUSTOM_COMPLETION_FAILED). A FAILED outcome marks the whole
            batch as failed even when no individual item failed.

            The predicate is evaluated during live execution: once before
            any branch is scheduled (with completed_count == 0) and again
            whenever branch state changes, including terminal and suspension
            events. It must therefore handle the initial zero-progress
            snapshot and never assume any item has completed. When a resumed
            invocation finds the batch already completed, the recorded
            completion outcome is replayed and the predicate is not called
            again; otherwise the batch runs live and the predicate is
            evaluated as described.

            The predicate must be deterministic, side-effect-free, and depend
            only on the CompletionStatus provided, never on external state. It
            must also be monotonic: once a level of progress would complete
            the batch, more progress must not flip the decision back to
            continue (for example success_count >= n). Non-monotonic
            predicates are not supported. On a mid-run resume the batch
            re-runs live and already-completed branches replay in an
            unspecified order, so the predicate may be evaluated on an
            intermediate subset of the final progress; monotonicity is what
            guarantees a subset cannot reach a different decision than the
            full terminal set. The same race-condition caveat as the
            threshold fields applies: when several items finish at once the
            batch may end with slightly more completed items than the
            predicate first observed.

    Note:
        The operation completes when any of the completion criteria are met:
        - Custom predicate returns complete_batch() (when should_complete is provided)
        - Enough successes (min_successful reached)
        - Too many failures (tolerated limits exceeded)
        - All items/branches completed

    Example:
        # Succeed if at least 3 succeed, fail if more than 2 fail
        config = CompletionConfig(
            min_successful=3,
            tolerated_failure_count=2
        )

        # Custom predicate: stop once 2 successes are observed
        config = CompletionConfig(
            should_complete=lambda status: (
                complete_batch() if status.success_count >= 2
                else continue_batch()
            )
        )
    """

    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: int | float | None = None
    should_complete: Callable[[CompletionStatus], CompletionDecision] | None = None

    def __post_init__(self) -> None:
        if self.min_successful is not None and self.min_successful < 1:
            msg = f"min_successful must be at least 1, got: {self.min_successful}"
            raise ValidationError(msg)
        if (
            self.tolerated_failure_count is not None
            and self.tolerated_failure_count < 0
        ):
            msg = (
                "tolerated_failure_count must be non-negative, got: "
                f"{self.tolerated_failure_count}"
            )
            raise ValidationError(msg)
        if self.tolerated_failure_percentage is not None and not (
            0 <= self.tolerated_failure_percentage <= 100  # noqa: PLR2004
        ):
            msg = (
                "tolerated_failure_percentage must be between 0 and 100, got: "
                f"{self.tolerated_failure_percentage}"
            )
            raise ValidationError(msg)
        if self.should_complete is not None and not callable(self.should_complete):
            msg = "should_complete must be callable"
            raise ValidationError(msg)
        if self.should_complete is not None and (
            self.min_successful is not None
            or self.tolerated_failure_count is not None
            or self.tolerated_failure_percentage is not None
        ):
            msg = (
                "should_complete cannot be combined with min_successful, "
                "tolerated_failure_count, or tolerated_failure_percentage"
            )
            raise ValidationError(msg)

    def _validate_for_total(self, total: int) -> None:
        """Validate this config against the number of items it will govern.

        SDK-internal: called by DurableContext.map and DurableContext.parallel
        before the operation's child context starts, so the error surfaces as
        a bare ValidationError (matching wait and wait_for_condition
        validation) instead of a checkpointed operation failure.
        """
        if self.min_successful is not None and self.min_successful > total:
            msg = (
                f"min_successful cannot be greater than total items: "
                f"{self.min_successful} > {total}"
            )
            raise ValidationError(msg)

    # TODO: reevaluate this
    # @staticmethod
    # def first_completed():
    #     return CompletionConfig(
    #         min_successful=None, tolerated_failure_count=None, tolerated_failure_percentage=None
    #     )

    @staticmethod
    def first_successful():
        return CompletionConfig(
            min_successful=1,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
        )

    @staticmethod
    def all_completed():
        # 100% tolerated failures: every item runs regardless of failures.
        # All-None fields would select the fail-fast default instead.
        return CompletionConfig(
            min_successful=None,
            tolerated_failure_count=None,
            tolerated_failure_percentage=100,
        )

    @staticmethod
    def all_successful():
        return CompletionConfig(
            min_successful=None,
            tolerated_failure_count=0,
            tolerated_failure_percentage=0,
        )


@dataclass(frozen=True)
class ParallelConfig:
    """Configuration options for parallel execution operations.

    This class configures how parallel operations are executed, including
    concurrency limits, completion criteria, and serialization behavior.

    Args:
        max_concurrency: Maximum number of parallel branches to execute concurrently.
            If None, no limit is imposed and all branches run concurrently.
            Use this to control resource usage and prevent overwhelming the system.

        completion_config: Defines when the parallel operation should complete.
            Controls success/failure criteria for the overall parallel operation.
            Default is CompletionConfig.all_successful() which requires all branches
            to succeed. Other options include first_successful() and all_completed().

        serdes: Custom serialization/deserialization configuration for BatchResult.
            Applied at the handler level to serialize the entire BatchResult object.
            If None, uses the default JSON serializer for BatchResult.

            Backward Compatibility: If only 'serdes' is provided (no item_serdes),
            it will be used for both individual functions AND BatchResult serialization
            to maintain existing behavior.

        item_serdes: Custom serialization/deserialization configuration for individual functions.
            Applied to each function's result as tasks complete in child contexts.
            If None, uses the default JSON serializer for individual function results.

            When both 'serdes' and 'item_serdes' are provided:
            - item_serdes: Used for individual function results in child contexts
            - serdes: Used for the entire BatchResult at handler level

        summary_generator: Function contributing a customer-facing summary for large
            results (>256KB). When the serialized result exceeds CHECKPOINT_SIZE_LIMIT,
            the SDK checkpoints a compact JSON payload instead of the full result and
            marks the operation ReplayChildren=true so the full result is reconstructed
            during replay. The SDK always writes the fields replay requires; the
            generator's return value is stored verbatim under the payload's "summary"
            key for observability and is never read by the SDK. The summary is
            checkpointed as provided. An exception raised by the generator fails
            the operation. Signature: (result: T) -> str

        nesting_type: How child operations should inherit context from their parent.
            - NESTED: Each branch runs in its own isolated context (default)
            - FLAT: All branches share the same parent context

    Example:
        # Run at most 3 branches concurrently, succeed if any one succeeds
        config = ParallelConfig(
            max_concurrency=3,
            completion_config=CompletionConfig.first_successful()
        )
    """

    max_concurrency: int | None = None
    completion_config: CompletionConfig = field(
        default_factory=CompletionConfig.all_successful
    )
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None
    nesting_type: NestingType = NestingType.NESTED

    def __post_init__(self) -> None:
        if self.max_concurrency is not None and self.max_concurrency < 1:
            msg = f"max_concurrency must be at least 1, got: {self.max_concurrency}"
            raise ValidationError(msg)


@dataclass(frozen=True)
class ParallelBranch(Generic[T]):
    """A named branch for parallel execution.

    Use this to provide custom names for parallel branches, improving
    observability in execution history.

    Type Parameters:
        T: The return type of the branch function.

    Args:
        func: The callable to execute in this branch. Receives a DurableContext.
        name: Optional custom name for this branch. When provided, replaces
            the default "parallel-branch-{index}" naming in execution history.
            This affects observability but not replay determinism.

    Example:
        context.parallel(
            functions=[
                ParallelBranch(func=lambda ctx: fetch_user(ctx), name="fetch-user-data"),
                ParallelBranch(func=lambda ctx: fetch_orders(ctx), name="fetch-order-history"),
            ],
            name="load-data",
            config=ParallelConfig(max_concurrency=2),
        )
    """

    func: Callable
    name: str | None = None

    def __call__(self, *args, **kwargs):
        """Delegate to the wrapped function, making ParallelBranch itself callable."""
        return self.func(*args, **kwargs)


class StepSemantics(Enum):
    AT_MOST_ONCE_PER_RETRY = "AT_MOST_ONCE_PER_RETRY"
    AT_LEAST_ONCE_PER_RETRY = "AT_LEAST_ONCE_PER_RETRY"


_SUB_TYPE_CHARACTERS = frozenset(string.ascii_letters + string.digits + "-_")
_SUB_TYPE_MAX_LENGTH = 32


def _validate_sub_type(sub_type: str | None) -> None:
    """Raise ValidationError when sub_type is set and the service would reject it.

    The service accepts 1 to 32 characters from A-Z, a-z, 0-9, hyphen, and
    underscore. None and "" select the SDK default, so both are accepted.
    Checking here raises before any checkpoint is sent, so a step body never
    runs under a subtype the service will reject.
    """
    if not sub_type:
        return
    if (
        len(sub_type) > _SUB_TYPE_MAX_LENGTH
        or not set(sub_type) <= _SUB_TYPE_CHARACTERS
    ):
        msg = (
            f"sub_type must be 1 to {_SUB_TYPE_MAX_LENGTH} characters from "
            f"A-Z, a-z, 0-9, hyphen, and underscore, got: {sub_type!r}"
        )
        raise ValidationError(msg)


@dataclass(frozen=True)
class StepConfig:
    """Configuration for a step.

    Args:
        retry_strategy: Decides whether and when a failed attempt is retried.
            If None, the SDK uses its default retry strategy.

        step_semantics: How often one attempt may run the function.
            AT_LEAST_ONCE_PER_RETRY (the default) checkpoints START without
            waiting, so an interrupted attempt runs again on the next
            invocation. AT_MOST_ONCE_PER_RETRY waits for the START checkpoint
            first, and an interrupted attempt fails with StepInterruptedError
            and goes to the retry strategy instead of running again.

        serdes: Custom serialization/deserialization for the step result.
            If None, uses the default JSON serializer.

        sub_type: Subtype recorded in the checkpoint, in replay validation, and
            in plugin events. The default is "Step".
            A custom operation built from steps uses this to label its steps,
            so history and plugins can filter them without parsing names.
            The value must be 1 to 32 characters from A-Z, a-z, 0-9, hyphen,
            and underscore; the config raises ValidationError otherwise. None
            or an empty string selects the default. Do not reuse an
            ``OperationSubType`` value: those label the SDK's own operations.
            Replay compares the subtype with the checkpoint. So changing the
            subtype of an existing step is a breaking change for executions
            in flight.
    """

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY
    serdes: SerDes | None = None
    sub_type: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        _validate_sub_type(self.sub_type)


# region map run configuration


@dataclass(frozen=True)
class S3Uri:
    """A parsed ``s3://bucket/path`` URI."""

    bucket: str
    path: str | None = None

    @classmethod
    def parse(cls, uri: str) -> S3Uri:
        """Parse an ``s3://bucket/path`` URI, rejecting any other scheme."""
        if not uri.startswith("s3://"):
            msg = f"S3 URI must start with s3://, got: {uri}"
            raise ValidationError(msg)
        bucket, _, path = uri.removeprefix("s3://").partition("/")
        if not bucket:
            msg = f"Invalid S3 URI: {uri}"
            raise ValidationError(msg)
        return cls(bucket=bucket, path=path or None)


def _validate_bucket_owner(value: str | None) -> None:
    """Validate an expected bucket owner is a 12-digit account id."""
    if value is not None and (
        len(value) != 12 or not (value.isascii() and value.isdigit())  # noqa: PLR2004
    ):
        msg = f"expected_bucket_owner must be a 12-digit account id, got: {value}"
        raise ValidationError(msg)


_MAX_CSV_COLUMNS = 1000
_MAX_CSV_COLUMN_LENGTH = 256
_MAX_ITEMS_TO_READ = 100_000_000
_MAX_DESTINATION_PREFIX_BYTES = 900


def _validate_destination_prefix(value: str) -> None:
    """Validate a destination prefix fits the service's length limit.

    The service caps the prefix at 900 characters and at 900 UTF-8 bytes. A UTF-8
    string never has fewer bytes than characters, so checking the byte length
    enforces both. A longer prefix is rejected by the start checkpoint, so it is
    rejected here instead.
    """
    size = len(value.encode("utf-8"))
    if size > _MAX_DESTINATION_PREFIX_BYTES:
        msg = (
            f"prefix must be at most {_MAX_DESTINATION_PREFIX_BYTES} UTF-8 bytes, "
            f"got: {size}"
        )
        raise ValidationError(msg)


def _validate_columns(name: str, columns: list[str] | None) -> None:
    """Validate a CSV columns/headers list against its count, uniqueness and length limits."""
    if columns is None:
        return
    if len(columns) == 0:
        msg = f"{name} must be non-empty"
        raise ValidationError(msg)
    if len(columns) > _MAX_CSV_COLUMNS:
        msg = f"{name} must hold at most {_MAX_CSV_COLUMNS} names, got: {len(columns)}"
        raise ValidationError(msg)
    if len(set(columns)) != len(columns):
        msg = f"{name} must not contain duplicates"
        raise ValidationError(msg)
    for column in columns:
        if len(column) > _MAX_CSV_COLUMN_LENGTH:
            msg = (
                f"each {name} name must be at most {_MAX_CSV_COLUMN_LENGTH} "
                f"characters, got: {len(column)}"
            )
            raise ValidationError(msg)


# NamespacedFunctionName max length; the service enforces the full grammar.
_MAX_FUNCTION_NAME_LENGTH = 170


def _validate_function_name(name: str) -> None:
    """Validate a Lambda function reference is present and within the length limit."""
    if not name:
        msg = "function name must be non-empty"
        raise ValidationError(msg)
    if len(name) > _MAX_FUNCTION_NAME_LENGTH:
        msg = f"function name must be at most {_MAX_FUNCTION_NAME_LENGTH} characters"
        raise ValidationError(msg)


@dataclass(frozen=True)
class DistributedMapCompletionConfig:
    """Failure-tolerance configuration for a map run."""

    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: int | float | None = None
    minimum_sample_size: int | None = None

    def __post_init__(self) -> None:
        if (
            self.tolerated_failure_count is not None
            and self.tolerated_failure_percentage is not None
        ):
            msg = (
                "tolerated_failure_count and tolerated_failure_percentage "
                "are mutually exclusive"
            )
            raise ValidationError(msg)
        if (
            self.tolerated_failure_count is None
            and self.tolerated_failure_percentage is None
        ):
            # Omitting the config entirely means zero tolerated failures, so an
            # empty one is rejected rather than resolved to the same thing.
            msg = (
                "a completion config must set tolerated_failure_count or "
                "tolerated_failure_percentage"
            )
            raise ValidationError(msg)
        if (
            self.minimum_sample_size is not None
            and self.tolerated_failure_percentage is None
        ):
            msg = "minimum_sample_size is only valid with tolerated_failure_percentage"
            raise ValidationError(msg)
        if (
            self.tolerated_failure_count is not None
            and self.tolerated_failure_count < 0
        ):
            msg = (
                "tolerated_failure_count must be non-negative, got: "
                f"{self.tolerated_failure_count}"
            )
            raise ValidationError(msg)
        if self.tolerated_failure_percentage is not None and not (
            0 <= self.tolerated_failure_percentage <= 100  # noqa: PLR2004
        ):
            msg = (
                "tolerated_failure_percentage must be between 0 and 100, got: "
                f"{self.tolerated_failure_percentage}"
            )
            raise ValidationError(msg)
        if self.minimum_sample_size is not None and self.minimum_sample_size < 1:
            msg = (
                "minimum_sample_size must be at least 1, got: "
                f"{self.minimum_sample_size}"
            )
            raise ValidationError(msg)

    @staticmethod
    def failure_count(count: int) -> DistributedMapCompletionConfig:
        """Abort once this many items have permanently failed."""
        return DistributedMapCompletionConfig(tolerated_failure_count=count)

    @staticmethod
    def failure_percentage(
        percentage: int | float, *, minimum_sample_size: int | None = None
    ) -> DistributedMapCompletionConfig:
        """Abort once the failure rate exceeds this percentage."""
        return DistributedMapCompletionConfig(
            tolerated_failure_percentage=percentage,
            minimum_sample_size=minimum_sample_size,
        )


@dataclass(frozen=True)
class DistributedMapProcessorRetryConfig:
    """Retry policy applied to each failing item of a map run."""

    UNLIMITED: ClassVar[Literal["unlimited"]] = "unlimited"

    max_retry_attempts: int | Literal["unlimited"] | None = None
    max_retry_duration: Duration | None = None

    def __post_init__(self) -> None:
        attempts = self.max_retry_attempts
        # A value that is neither an int nor UNLIMITED carries no retry limit to the
        # wire, so the service would apply its own default instead of this value.
        # Rejecting it here is what makes the omission impossible.
        if (
            attempts is not None
            and attempts != self.UNLIMITED
            and (not isinstance(attempts, int) or attempts < 0)
        ):
            msg = (
                "max_retry_attempts must be a non-negative integer or "
                "DistributedMapProcessorRetryConfig.UNLIMITED, "
                f"got: {attempts!r}"
            )
            raise ValidationError(msg)
        if self.max_retry_duration is not None and not (
            60 <= self.max_retry_duration.to_seconds() <= 21600  # noqa: PLR2004
        ):
            msg = (
                "max_retry_duration must be between 1 minute and 6 hours, got: "
                f"{self.max_retry_duration.to_seconds()}s"
            )
            raise ValidationError(msg)


@dataclass(frozen=True)
class DistributedMapProcessor:
    """Processor configuration for a map run."""

    function_name: str
    response_mode: ProcessorResponseMode = ProcessorResponseMode.BATCH
    batch_size: int | None = None
    retry: DistributedMapProcessorRetryConfig | None = None
    durable_execution_name_prefix: str | None = None

    def __post_init__(self) -> None:
        _validate_function_name(self.function_name)
        if self.batch_size is not None and not (
            1 <= self.batch_size <= 10000  # noqa: PLR2004
        ):
            msg = f"batch_size must be between 1 and 10000, got: {self.batch_size}"
            raise ValidationError(msg)
        if self.durable_execution_name_prefix is not None and not (
            1 <= len(self.durable_execution_name_prefix) <= 36  # noqa: PLR2004
        ):
            msg = (
                "durable_execution_name_prefix must be 1 to 36 characters, got: "
                f"{len(self.durable_execution_name_prefix)}"
            )
            raise ValidationError(msg)

    @classmethod
    def report_batch_outcome(
        cls,
        name: str,
        *,
        batch_size: int | None = None,
        retry: DistributedMapProcessorRetryConfig | None = None,
        durable_execution_name_prefix: str | None = None,
    ) -> DistributedMapProcessor:
        """Processor that reports one outcome for the whole batch. Items have no outputs."""
        return cls(
            function_name=name,
            response_mode=ProcessorResponseMode.BATCH,
            batch_size=batch_size,
            retry=retry,
            durable_execution_name_prefix=durable_execution_name_prefix,
        )

    @classmethod
    def report_item_outcomes(
        cls,
        name: str,
        *,
        batch_size: int | None = None,
        retry: DistributedMapProcessorRetryConfig | None = None,
        durable_execution_name_prefix: str | None = None,
    ) -> DistributedMapProcessor:
        """Processor that may report a failure or an output per item. Unlisted items succeed."""
        return cls(
            function_name=name,
            response_mode=ProcessorResponseMode.ITEM_OUTCOMES,
            batch_size=batch_size,
            retry=retry,
            durable_execution_name_prefix=durable_execution_name_prefix,
        )


def _parse_delimiter(
    value: str | DistributedMapCsvDelimiter,
) -> DistributedMapCsvDelimiter:
    """Normalize a delimiter passed as a string or enum member."""
    if isinstance(value, DistributedMapCsvDelimiter):
        return value
    try:
        return DistributedMapCsvDelimiter(value)
    except ValueError:
        allowed = ", ".join(d.value for d in DistributedMapCsvDelimiter)
        msg = f"delimiter must be one of ({allowed}), got: {value!r}"
        raise ValidationError(msg) from None


@dataclass(frozen=True)
class S3Source:
    """Resolved S3 source."""

    source_type: DistributedMapS3SourceType
    bucket: str
    key: str | None = None
    key_prefix: str | None = None
    fmt: DistributedMapSourceFormat | None = None
    delimiter: DistributedMapCsvDelimiter | None = None
    headers: list[str] | None = None
    expected_bucket_owner: str | None = None

    def __post_init__(self) -> None:
        _validate_bucket_owner(self.expected_bucket_owner)
        _validate_columns("headers", self.headers)
        if self.source_type is DistributedMapS3SourceType.OBJECT:
            if not self.key:
                msg = "an OBJECT source requires a key"
                raise ValidationError(msg)
            if self.fmt is None:
                msg = "an OBJECT source requires a format"
                raise ValidationError(msg)
        else:
            if self.key_prefix is None:
                msg = f"a {self.source_type.value} source requires a key prefix"
                raise ValidationError(msg)
            if (
                self.source_type is DistributedMapS3SourceType.LISTED_OBJECTS
                and self.fmt is None
            ):
                msg = "a LISTED_OBJECTS source requires a format"
                raise ValidationError(msg)
            if (
                self.source_type is DistributedMapS3SourceType.LISTED_OBJECT_METADATA
                and self.fmt is not None
            ):
                msg = "a LISTED_OBJECT_METADATA source reads no contents, so it takes no format"
                raise ValidationError(msg)


@dataclass(frozen=True)
class ReaderSource:
    """Resolved reader-function source."""

    function_name: str
    initial_state: Any = None
    state_serdes: SerDes | None = None  # None = DEFAULT_JSON_SERDES

    def __post_init__(self) -> None:
        _validate_function_name(self.function_name)


@dataclass(frozen=True)
class DistributedMapSource:
    """Source configuration for a map run."""

    max_items: int | None = None
    inline_items: list[Any] | None = None
    inline_serdes: SerDes | None = None  # None = DEFAULT_JSON_SERDES
    s3: S3Source | None = None
    reader: ReaderSource | None = None

    def __post_init__(self) -> None:
        if self.max_items is not None and not (
            1 <= self.max_items <= _MAX_ITEMS_TO_READ
        ):
            msg = (
                f"max_items must be between 1 and {_MAX_ITEMS_TO_READ}, "
                f"got: {self.max_items}"
            )
            raise ValidationError(msg)
        populated = sum(
            1
            for value in (self.inline_items, self.s3, self.reader)
            if value is not None
        )
        if populated != 1:
            msg = "exactly one of inline_items, s3 or reader must be set"
            raise ValidationError(msg)

    @classmethod
    def inline(
        cls,
        items: Sequence[Any],
        *,
        serdes: SerDes | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """An in-memory list of items embedded in the start checkpoint."""
        return cls(
            inline_items=list(items),
            inline_serdes=serdes,
            max_items=max_items,
        )

    @classmethod
    def s3_json_lines(
        cls,
        uri: str,
        *,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read a single object, treating each line as an item."""
        parsed_uri = S3Uri.parse(uri)
        bucket, key = parsed_uri.bucket, parsed_uri.path
        if key is None:
            msg = "s3_json_lines requires an S3 object key"
            raise ValidationError(msg)
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.OBJECT,
                bucket=bucket,
                key=key,
                fmt=DistributedMapSourceFormat.JSON_LINES,
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def s3_json_array(
        cls,
        uri: str,
        *,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read a single object holding a JSON array, treating each element as an item."""
        parsed_uri = S3Uri.parse(uri)
        bucket, key = parsed_uri.bucket, parsed_uri.path
        if key is None:
            msg = "s3_json_array requires an S3 object key"
            raise ValidationError(msg)
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.OBJECT,
                bucket=bucket,
                key=key,
                fmt=DistributedMapSourceFormat.JSON_ARRAY,
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def s3_csv(
        cls,
        uri: str,
        *,
        headers: Sequence[str] | None = None,
        delimiter: str | DistributedMapCsvDelimiter = DistributedMapCsvDelimiter.COMMA,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read a single object, treating each record as an item."""
        parsed_uri = S3Uri.parse(uri)
        bucket, key = parsed_uri.bucket, parsed_uri.path
        if key is None:
            msg = "s3_csv requires an S3 object key"
            raise ValidationError(msg)
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.OBJECT,
                bucket=bucket,
                key=key,
                fmt=DistributedMapSourceFormat.CSV,
                delimiter=_parse_delimiter(delimiter),
                headers=list(headers) if headers is not None else None,
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def s3_objects(
        cls,
        prefix_uri: str,
        *,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read each object under a prefix as one item, without reading its contents."""
        parsed_uri = S3Uri.parse(prefix_uri)
        bucket, prefix = parsed_uri.bucket, parsed_uri.path
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.LISTED_OBJECT_METADATA,
                bucket=bucket,
                key_prefix=prefix or "",
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def s3_flattened_json_lines(
        cls,
        prefix_uri: str,
        *,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read every object under a prefix, treating each line as an item."""
        parsed_uri = S3Uri.parse(prefix_uri)
        bucket, prefix = parsed_uri.bucket, parsed_uri.path
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.LISTED_OBJECTS,
                bucket=bucket,
                key_prefix=prefix or "",
                fmt=DistributedMapSourceFormat.JSON_LINES,
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def s3_flattened_json_array(
        cls,
        prefix_uri: str,
        *,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read every object under a prefix, treating each array element as an item."""
        parsed_uri = S3Uri.parse(prefix_uri)
        bucket, prefix = parsed_uri.bucket, parsed_uri.path
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.LISTED_OBJECTS,
                bucket=bucket,
                key_prefix=prefix or "",
                fmt=DistributedMapSourceFormat.JSON_ARRAY,
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def s3_flattened_csv(
        cls,
        prefix_uri: str,
        *,
        headers: Sequence[str] | None = None,
        delimiter: str | DistributedMapCsvDelimiter = DistributedMapCsvDelimiter.COMMA,
        expected_bucket_owner: str | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Read every object under a prefix, treating each record as an item."""
        parsed_uri = S3Uri.parse(prefix_uri)
        bucket, prefix = parsed_uri.bucket, parsed_uri.path
        return cls(
            max_items=max_items,
            s3=S3Source(
                source_type=DistributedMapS3SourceType.LISTED_OBJECTS,
                bucket=bucket,
                key_prefix=prefix or "",
                fmt=DistributedMapSourceFormat.CSV,
                delimiter=_parse_delimiter(delimiter),
                headers=list(headers) if headers is not None else None,
                expected_bucket_owner=expected_bucket_owner,
            ),
        )

    @classmethod
    def reader_function(
        cls,
        name: str,
        *,
        initial_state: Any = None,
        state_serdes: SerDes | None = None,
        max_items: int | None = None,
    ) -> DistributedMapSource:
        """Page items from a customer-supplied reader Lambda function."""
        return cls(
            max_items=max_items,
            reader=ReaderSource(
                function_name=name,
                initial_state=initial_state,
                state_serdes=state_serdes,
            ),
        )


@dataclass(frozen=True)
class SuccessDestination:
    """S3 destination for succeeded items."""

    bucket: str
    prefix: str
    include_input: bool = False
    include_output: bool = True
    expected_bucket_owner: str | None = None

    def __post_init__(self) -> None:
        _validate_bucket_owner(self.expected_bucket_owner)
        _validate_destination_prefix(self.prefix)
        if not (self.include_input or self.include_output):
            msg = "success destination must include input or output"
            raise ValidationError(msg)


@dataclass(frozen=True)
class FailureDestination:
    """S3 destination for permanently-failed items."""

    bucket: str
    prefix: str
    include_input: bool = True
    include_error: bool = True
    expected_bucket_owner: str | None = None

    def __post_init__(self) -> None:
        _validate_bucket_owner(self.expected_bucket_owner)
        _validate_destination_prefix(self.prefix)
        if not (self.include_input or self.include_error):
            msg = "failure destination must include input or error"
            raise ValidationError(msg)


class DistributedMapDestination:
    """Factories for the destinations a map run writes item records to."""

    @staticmethod
    def s3_successes(
        prefix_uri: str,
        *,
        include_input: bool = False,
        include_output: bool = True,
        expected_bucket_owner: str | None = None,
    ) -> SuccessDestination:
        """Route succeeded item records to an S3 prefix."""
        parsed_uri = S3Uri.parse(prefix_uri)
        return SuccessDestination(
            bucket=parsed_uri.bucket,
            prefix=parsed_uri.path or "",
            include_input=include_input,
            include_output=include_output,
            expected_bucket_owner=expected_bucket_owner,
        )

    @staticmethod
    def s3_failures(
        prefix_uri: str,
        *,
        include_input: bool = True,
        include_error: bool = True,
        expected_bucket_owner: str | None = None,
    ) -> FailureDestination:
        """Route permanently-failed item records to an S3 prefix."""
        parsed_uri = S3Uri.parse(prefix_uri)
        return FailureDestination(
            bucket=parsed_uri.bucket,
            prefix=parsed_uri.path or "",
            include_input=include_input,
            include_error=include_error,
            expected_bucket_owner=expected_bucket_owner,
        )


@dataclass(frozen=True)
class DistributedMapDestinationConfig:
    """Destination routing for map run results."""

    on_success: SuccessDestination | None = None
    on_failure: FailureDestination | None = None


@dataclass(frozen=True)
class DistributedMapConfig:
    """Configuration for map run operations."""

    destination: DistributedMapDestinationConfig | None = None
    completion_config: DistributedMapCompletionConfig | None = None
    timeout: Duration | None = None

    def __post_init__(self) -> None:
        if self.timeout is not None and not (
            0 < self.timeout.to_seconds() <= 7776000  # noqa: PLR2004
        ):
            msg = (
                "timeout must be positive and at most 90 days, got: "
                f"{self.timeout.to_seconds()}s"
            )
            raise ValidationError(msg)


@dataclass(frozen=True)
class DistributedMapResultConfig(DistributedMapConfig, Generic[ItemOutput]):
    """Configuration that collects per-item results inline.

    Passing this instead of ``DistributedMapConfig`` makes ``ctx.distributed_map``
    return a ``DistributedMapResult``, which carries the per-item outcomes.
    """

    result_serdes: SerDes[ItemOutput] | None = None  # None = DEFAULT_JSON_SERDES


# endregion map run configuration


@dataclass(frozen=True)
class ChildConfig(Generic[T]):
    """Configuration options for child context operations.

    This class configures how child contexts are executed and checkpointed,
    matching the TypeScript ChildConfig interface behavior.

    Args:
        serdes: Custom serialization/deserialization configuration for BatchResult.
            Applied at the handler level to serialize the entire BatchResult object.
            If None, uses the default JSON serializer for BatchResult.

        sub_type: Subtype recorded in the checkpoint, in replay validation, and
            in plugin events. The default is "RunInChildContext".
            A custom operation built from several durable operations uses this
            to label its child context, so history and plugins can filter it
            without parsing names. The value must be 1 to 32 characters from
            A-Z, a-z, 0-9, hyphen, and underscore; the config raises
            ValidationError otherwise. None or an empty string selects the
            default. Do not reuse an ``OperationSubType`` value: those label
            the SDK's own operations.
            Replay compares the subtype with the checkpoint. So changing the
            subtype of an existing child context is a breaking change for
            executions in flight.

        summary_generator: Function generating the checkpoint payload for large
            results (>256KB). When the serialized result exceeds CHECKPOINT_SIZE_LIMIT,
            the SDK checkpoints the generator's output instead of the full result and
            marks the operation ReplayChildren=true so the full result is reconstructed
            during replay. The output is checkpointed as provided. For map and
            parallel operations the SDK supplies a generator that writes the
            completion-record envelope; see MapConfig and ParallelConfig. An
            exception raised by the generator fails the operation.
            Signature: (result: T) -> str

        is_virtual: When True, skip all checkpoints (START, SUCCEED,
            FAIL) for this child context and propagate the caller's reporting
            parent id through to operations created inside the child. The
            branch is a logical scope for step-id prefixing but does not
            appear in the execution history. Used internally by
            NestingType.FLAT branches. Use this to group operations without
            adding a CONTEXT entry to the execution history.

    See TypeScript reference: aws-durable-execution-sdk-js/src/types/index.ts
    """

    serdes: SerDes | None = None
    sub_type: str | None = None
    summary_generator: SummaryGenerator | None = None
    is_virtual: bool = False

    def __post_init__(self) -> None:
        _validate_sub_type(self.sub_type)


@dataclass(frozen=True)
class MapConfig(Generic[T]):
    """Configuration options for map operations over collections.

    This class configures how map operations process collections of items,
    including concurrency, completion criteria, and serialization.

    Type Parameters:
        T: The type of items being processed in the map operation.

    Args:
        max_concurrency: Maximum number of items to process concurrently.
            If None, no limit is imposed and all items are processed concurrently.
            Use this to control resource usage when processing large collections.

        completion_config: Defines when the map operation should complete.
            Controls success/failure criteria for the overall map operation.
            The default fails fast: the first failed item fails the batch.
            Use CompletionConfig.all_completed() to process every item
            regardless of failures.

        serdes: Custom serialization/deserialization configuration for BatchResult.
            Applied at the handler level to serialize the entire BatchResult object.
            If None, uses the default JSON serializer for BatchResult.

            Backward Compatibility: If only 'serdes' is provided (no item_serdes),
            it will be used for both individual items AND BatchResult serialization
            to maintain existing behavior.

        item_serdes: Custom serialization/deserialization configuration for individual items.
            Applied to each item's result as tasks complete in child contexts.
            If None, uses the default JSON serializer for individual items.

            When both 'serdes' and 'item_serdes' are provided:
            - item_serdes: Used for individual item results in child contexts
            - serdes: Used for the entire BatchResult at handler level

        summary_generator: Function contributing a customer-facing summary for large
            results (>256KB). When the serialized result exceeds CHECKPOINT_SIZE_LIMIT,
            the SDK checkpoints a compact JSON payload instead of the full result and
            marks the operation ReplayChildren=true so the full result is reconstructed
            during replay. The SDK always writes the fields replay requires; the
            generator's return value is stored verbatim under the payload's "summary"
            key for observability and is never read by the SDK. The summary is
            checkpointed as provided. An exception raised by the generator fails
            the operation. Signature: (result: T) -> str

        nesting_type: How child operations should inherit context from their parent.
            - NESTED: Each item runs in its own isolated context (default)
            - FLAT: All items share the same parent context

        item_namer: Optional callable to generate custom names for each map iteration.
            When provided, replaces the default "map-item-{index}" naming scheme.
            Receives the item and its index, and returns a string name for that
            iteration. Called eagerly for every input when the map starts,
            including items that never run due to early completion, and again
            on every replay, so it must be deterministic and side-effect-free.
            An exception raised by the callable fails the map operation.
            If None, uses the default naming: "map-item-{index}".

    Example:
        # Process 5 items at a time, require all to succeed
        config = MapConfig(
            max_concurrency=5,
            completion_config=CompletionConfig.all_successful()
        )

        # With custom iteration names
        config = MapConfig(
            max_concurrency=5,
            item_namer=lambda item, index: f"process-order-{item.id}"
        )
    """

    max_concurrency: int | None = None
    completion_config: CompletionConfig = field(default_factory=CompletionConfig)
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None
    nesting_type: NestingType = NestingType.NESTED
    item_namer: Callable[[T, int], str] | None = None

    def __post_init__(self) -> None:
        if self.max_concurrency is not None and self.max_concurrency < 1:
            msg = f"max_concurrency must be at least 1, got: {self.max_concurrency}"
            raise ValidationError(msg)


@dataclass(frozen=True)
class InvokeConfig(Generic[P, R]):
    """
    Configuration for invoke operations.

    This class configures how function invocations are executed, including
    serialization and tenant isolation.

    Args:
        serdes_payload: Custom serialization/deserialization for the payload
            sent to the invoked function. Defaults to DEFAULT_JSON_SERDES when
            not set.

        serdes_result: Custom serialization/deserialization for the result
            returned from the invoked function. Defaults to DEFAULT_JSON_SERDES when
            not set.

        tenant_id: Optional tenant identifier for multi-tenant isolation.
            If provided, the invocation will be scoped to this tenant.
    """

    serdes_payload: SerDes[P] | None = None
    serdes_result: SerDes[R] | None = None
    tenant_id: str | None = None


@dataclass(frozen=True)
class CallbackConfig:
    """Configuration for callbacks.

    Args:
        timeout: Maximum time to wait for the callback result. The default,
            a zero Duration, means no timeout.

        heartbeat_timeout: Maximum time between heartbeats before the callback
            fails. The default, a zero Duration, means no heartbeat timeout.

        serdes: Custom deserialization for the callback result.
            If None, the payload is returned as is, without JSON parsing.

        sub_type: Subtype recorded in the checkpoint, in replay validation, and
            in plugin events. The default is "Callback".
            A custom operation built from callbacks uses this to label its
            callbacks, so history and plugins can filter them without parsing
            names. The value must be 1 to 32 characters from A-Z, a-z, 0-9,
            hyphen, and underscore; the config raises ValidationError
            otherwise. None or an empty string selects the default. Do not
            reuse an ``OperationSubType`` value: those label the SDK's own
            operations. Under
            ``wait_for_callback`` the subtype labels the callback the
            operation creates; the surrounding context keeps the subtype
            "WaitForCallback".
            Replay compares the subtype with the checkpoint. So changing the
            subtype of an existing callback is a breaking change for
            executions in flight.
    """

    timeout: Duration = field(default_factory=Duration)
    heartbeat_timeout: Duration = field(default_factory=Duration)
    serdes: SerDes | None = None
    # Keyword-only, so the positional order of every config, including
    # WaitForCallbackConfig(timeout, heartbeat_timeout, serdes, retry_strategy),
    # is unchanged from 2.x.
    sub_type: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        _validate_sub_type(self.sub_type)

    @property
    def timeout_seconds(self) -> int:
        """Get timeout in seconds."""
        return self.timeout.to_seconds()

    @property
    def heartbeat_timeout_seconds(self) -> int:
        """Get heartbeat timeout in seconds."""
        return self.heartbeat_timeout.to_seconds()


@dataclass(frozen=True)
class WaitForCallbackConfig(CallbackConfig):
    """Configuration for wait for callback."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None


# region Jitter


class JitterStrategy(StrEnum):
    """
    Jitter strategies are used to introduce noise when attempting to retry
    an invoke. We introduce noise to prevent a thundering-herd effect where
    a group of accesses (e.g. invokes) happen at once.

    Jitter is meant to be used to spread operations across time.

    Based on AWS Architecture Blog: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    members:
        :NONE: No jitter; use the exact calculated delay
        :FULL: Full jitter; random delay between 0 and calculated delay
        :HALF: Equal jitter; random delay between 0.5x and 1.0x of the calculated delay
    """

    NONE = "NONE"
    FULL = "FULL"
    HALF = "HALF"

    def apply_jitter(self, delay: float) -> float:
        """Apply jitter to a delay value and return the final delay.

        Args:
            delay: The base delay value to apply jitter to

        Returns:
            The final delay after applying jitter strategy
        """
        match self:
            case JitterStrategy.NONE:
                return delay
            case JitterStrategy.HALF:
                # Equal jitter: delay/2 + random(0, delay/2)
                return delay / 2 + random.random() * (delay / 2)  # noqa: S311
            case _:  # default is FULL
                # Full jitter: random(0, delay)
                return random.random() * delay  # noqa: S311

    def finalize_delay(self, base_delay: float) -> int:
        """Apply jitter, round up, and clamp to a minimum of 1 second.

        Args:
            base_delay: The base delay value before jitter is applied

        Returns:
            The final delay in whole seconds, at least 1
        """
        return max(1, math.ceil(self.apply_jitter(base_delay)))


# endregion Jitter
