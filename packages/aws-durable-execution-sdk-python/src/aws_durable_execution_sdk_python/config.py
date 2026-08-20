"""Configuration types."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

from aws_durable_execution_sdk_python.exceptions import ValidationError


P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
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

    Note:
        The operation completes when any of the completion criteria are met:
        - Enough successes (min_successful reached)
        - Too many failures (tolerated limits exceeded)
        - All items/branches completed

    Example:
        # Succeed if at least 3 succeed, fail if more than 2 fail
        config = CompletionConfig(
            min_successful=3,
            tolerated_failure_count=2
        )
    """

    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: int | float | None = None

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


@dataclass(frozen=True)
class StepConfig:
    """Configuration for a step."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY
    serdes: SerDes | None = None


# region map run configuration


def _parse_s3_uri(uri: str) -> tuple[str, str | None]:
    """Split an ``s3://bucket/path`` URI into its bucket and path parts."""
    rest = uri.removeprefix("s3://")
    bucket, _, path = rest.partition("/")
    if not bucket:
        msg = f"Invalid S3 URI: {uri}"
        raise ValidationError(msg)
    return bucket, path or None


def _validate_bucket_owner(value: str | None) -> None:
    """Validate an expected bucket owner is a 12-digit account id."""
    if value is not None and (len(value) != 12 or not value.isdigit()):  # noqa: PLR2004
        msg = f"expected_bucket_owner must be a 12-digit account id, got: {value}"
        raise ValidationError(msg)


def _validate_columns(name: str, columns: tuple[str, ...] | None) -> None:
    """Validate a CSV columns/headers tuple is non-empty with no duplicates."""
    if columns is None:
        return
    if len(columns) == 0:
        msg = f"{name} must be non-empty"
        raise ValidationError(msg)
    if len(set(columns)) != len(columns):
        msg = f"{name} must not contain duplicates"
        raise ValidationError(msg)


def _validate_function_name(name: str) -> None:
    """Validate a Lambda function reference is present and correctly formatted."""
    if not name:
        msg = "function name must be non-empty"
        raise ValidationError(msg)
    from aws_durable_execution_sdk_python.lambda_service import (
        lambda_function_name_matcher,
    )

    pattern, max_len = lambda_function_name_matcher()
    if len(name) > max_len or pattern.fullmatch(name) is None:
        msg = f"invalid Lambda function reference: {name!r}"
        raise ValidationError(msg)


@dataclass(frozen=True)
class DistributedMapCompletionConfig:
    """Failure-tolerance configuration for a map run."""

    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: float | None = None
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
        percentage: float, *, minimum_sample_size: int | None = None
    ) -> DistributedMapCompletionConfig:
        """Abort once the failure rate exceeds this percentage."""
        return DistributedMapCompletionConfig(
            tolerated_failure_percentage=percentage,
            minimum_sample_size=minimum_sample_size,
        )


@dataclass(frozen=True)
class ProcessorRetryConfig:
    """Retry configuration for a map run processor."""

    UNLIMITED: ClassVar[str] = "unlimited"

    max_retry_attempts: int | Literal["unlimited"] | None = None
    max_retry_duration: Duration | None = None

    def __post_init__(self) -> None:
        if isinstance(self.max_retry_attempts, int) and self.max_retry_attempts < 0:
            msg = (
                "max_retry_attempts must be non-negative or "
                "ProcessorRetryConfig.UNLIMITED, "
                f"got: {self.max_retry_attempts}"
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
    response_mode: str | None = None  # None = batch mode
    batch_size: int | None = None
    retry: ProcessorRetryConfig | None = None
    durable_execution_name_prefix: str | None = None

    def __post_init__(self) -> None:
        _validate_function_name(self.function_name)
        if self.batch_size is not None and not (
            1 <= self.batch_size <= 10000  # noqa: PLR2004
        ):
            msg = f"batch_size must be between 1 and 10000, got: {self.batch_size}"
            raise ValidationError(msg)

    @classmethod
    def report_batch_outcome(
        cls,
        name: str,
        *,
        batch_size: int | None = None,
        retry: ProcessorRetryConfig | None = None,
        durable_execution_name_prefix: str | None = None,
    ) -> DistributedMapProcessor:
        """Processor that reports a single pass/fail outcome for the whole batch, with no per-item results."""
        return cls(
            function_name=name,
            response_mode=None,
            batch_size=batch_size,
            retry=retry,
            durable_execution_name_prefix=durable_execution_name_prefix,
        )

    @classmethod
    def report_failed_items(
        cls,
        name: str,
        *,
        batch_size: int | None = None,
        retry: ProcessorRetryConfig | None = None,
        durable_execution_name_prefix: str | None = None,
    ) -> DistributedMapProcessor:
        """Processor that reports the ids of failed items, with all others marked succeeded."""
        return cls(
            function_name=name,
            response_mode="ReportBatchItemFailures",
            batch_size=batch_size,
            retry=retry,
            durable_execution_name_prefix=durable_execution_name_prefix,
        )

    @classmethod
    def report_item_results(
        cls,
        name: str,
        *,
        batch_size: int | None = None,
        retry: ProcessorRetryConfig | None = None,
        durable_execution_name_prefix: str | None = None,
    ) -> DistributedMapProcessor:
        """Processor that reports the results (output or error) for every item."""
        return cls(
            function_name=name,
            response_mode="ReportBatchItemResults",
            batch_size=batch_size,
            retry=retry,
            durable_execution_name_prefix=durable_execution_name_prefix,
        )


@dataclass(frozen=True)
class S3SourceConfig:
    """Resolved S3 source configuration."""

    bucket: str
    key: str | None = None
    prefix: str | None = None
    transform: str | None = None
    fmt: str | None = None
    delimiter: str | None = None
    headers: tuple[str, ...] | None = None
    expected_bucket_owner: str | None = None

    def __post_init__(self) -> None:
        _validate_bucket_owner(self.expected_bucket_owner)
        _validate_columns("headers", self.headers)


@dataclass(frozen=True)
class ReaderSourceConfig:
    """Resolved reader-function source configuration."""

    function_name: str
    initial_state: Any = None
    state_serdes: SerDes | None = None  # None = DEFAULT_JSON_SERDES

    def __post_init__(self) -> None:
        _validate_function_name(self.function_name)


@dataclass(frozen=True)
class DistributedMapSource:
    """Source configuration for a map run."""

    source_type: str
    max_items: int | None = None
    inline_items: tuple[Any, ...] | None = None
    inline_serdes: SerDes | None = None  # None = DEFAULT_JSON_SERDES
    s3: S3SourceConfig | None = None
    reader: ReaderSourceConfig | None = None

    def __post_init__(self) -> None:
        if self.max_items is not None and self.max_items < 1:
            msg = f"max_items must be at least 1, got: {self.max_items}"
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
            source_type="INLINE",
            inline_items=tuple(items),
            inline_serdes=serdes,
            max_items=max_items,
        )

    class S3:
        """S3 source factories."""

        @staticmethod
        def json_lines(
            uri: str,
            *,
            expected_bucket_owner: str | None = None,
            max_items: int | None = None,
        ) -> DistributedMapSource:
            """Read a single object, treating each line as an item."""
            bucket, key = _parse_s3_uri(uri)
            if key is None:
                msg = "json_lines requires an S3 object key"
                raise ValidationError(msg)
            return DistributedMapSource(
                source_type="S3",
                max_items=max_items,
                s3=S3SourceConfig(
                    bucket=bucket,
                    key=key,
                    fmt="JSON_LINES",
                    expected_bucket_owner=expected_bucket_owner,
                ),
            )

        @staticmethod
        def csv(
            uri: str,
            *,
            headers: Sequence[str] | None = None,
            delimiter: str = "COMMA",
            expected_bucket_owner: str | None = None,
            max_items: int | None = None,
        ) -> DistributedMapSource:
            """Read a single object, treating each record as an item."""
            bucket, key = _parse_s3_uri(uri)
            if key is None:
                msg = "csv requires an S3 object key"
                raise ValidationError(msg)
            return DistributedMapSource(
                source_type="S3",
                max_items=max_items,
                s3=S3SourceConfig(
                    bucket=bucket,
                    key=key,
                    fmt="CSV",
                    delimiter=delimiter,
                    headers=tuple(headers) if headers is not None else None,
                    expected_bucket_owner=expected_bucket_owner,
                ),
            )

        @staticmethod
        def objects(
            prefix_uri: str,
            *,
            expected_bucket_owner: str | None = None,
            max_items: int | None = None,
        ) -> DistributedMapSource:
            """Read each object under a prefix as one item."""
            bucket, prefix = _parse_s3_uri(prefix_uri)
            return DistributedMapSource(
                source_type="S3",
                max_items=max_items,
                s3=S3SourceConfig(
                    bucket=bucket,
                    prefix=prefix or "",
                    transform="NONE",
                    expected_bucket_owner=expected_bucket_owner,
                ),
            )

        @staticmethod
        def flattened_json_lines(
            prefix_uri: str,
            *,
            expected_bucket_owner: str | None = None,
            max_items: int | None = None,
        ) -> DistributedMapSource:
            """Read a prefix, flattening each object's lines into items."""
            bucket, prefix = _parse_s3_uri(prefix_uri)
            return DistributedMapSource(
                source_type="S3",
                max_items=max_items,
                s3=S3SourceConfig(
                    bucket=bucket,
                    prefix=prefix or "",
                    transform="LOAD_AND_FLATTEN",
                    fmt="JSON_LINES",
                    expected_bucket_owner=expected_bucket_owner,
                ),
            )

        @staticmethod
        def flattened_csv(
            prefix_uri: str,
            *,
            headers: Sequence[str] | None = None,
            delimiter: str = "COMMA",
            expected_bucket_owner: str | None = None,
            max_items: int | None = None,
        ) -> DistributedMapSource:
            """Read a prefix, flattening each object's records into items."""
            bucket, prefix = _parse_s3_uri(prefix_uri)
            return DistributedMapSource(
                source_type="S3",
                max_items=max_items,
                s3=S3SourceConfig(
                    bucket=bucket,
                    prefix=prefix or "",
                    transform="LOAD_AND_FLATTEN",
                    fmt="CSV",
                    delimiter=delimiter,
                    headers=tuple(headers) if headers is not None else None,
                    expected_bucket_owner=expected_bucket_owner,
                ),
            )

    class Reader:
        """Reader-function source factories."""

        @staticmethod
        def from_function(
            name: str,
            *,
            initial_state: Any = None,
            state_serdes: SerDes | None = None,
            max_items: int | None = None,
        ) -> DistributedMapSource:
            """Page items from a customer-supplied reader Lambda function."""
            return DistributedMapSource(
                source_type="READER_FUNCTION",
                max_items=max_items,
                reader=ReaderSourceConfig(
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
        if not (self.include_input or self.include_error):
            msg = "failure destination must include input or error"
            raise ValidationError(msg)


@dataclass(frozen=True)
class DistributedMapDestinationConfig:
    """Destination routing for map run results."""

    on_success: SuccessDestination | None = None
    on_failure: FailureDestination | None = None


@dataclass(frozen=True)
class DistributedMapDestination:
    """Destination factories for a map run."""

    class S3:
        """S3 destination factories."""

        @staticmethod
        def successes(
            prefix_uri: str,
            *,
            include_input: bool = False,
            include_output: bool = True,
            expected_bucket_owner: str | None = None,
        ) -> SuccessDestination:
            """Route succeeded item records to an S3 prefix."""
            bucket, prefix = _parse_s3_uri(prefix_uri)
            return SuccessDestination(
                bucket=bucket,
                prefix=prefix or "",
                include_input=include_input,
                include_output=include_output,
                expected_bucket_owner=expected_bucket_owner,
            )

        @staticmethod
        def failures(
            prefix_uri: str,
            *,
            include_input: bool = True,
            include_error: bool = True,
            expected_bucket_owner: str | None = None,
        ) -> FailureDestination:
            """Route permanently-failed item records to an S3 prefix."""
            bucket, prefix = _parse_s3_uri(prefix_uri)
            return FailureDestination(
                bucket=bucket,
                prefix=prefix or "",
                include_input=include_input,
                include_error=include_error,
                expected_bucket_owner=expected_bucket_owner,
            )


@dataclass(frozen=True)
class DistributedMapConfig:
    """Configuration for map run operations."""

    destination: DistributedMapDestinationConfig | None = None
    completion_config: DistributedMapCompletionConfig | None = None
    timeout: Duration | None = None
    collect_results: bool = False
    item_serdes: SerDes | None = None  # None = DEFAULT_JSON_SERDES

    def __post_init__(self) -> None:
        if self.timeout is not None and not (
            0 < self.timeout.to_seconds() <= 7776000  # noqa: PLR2004
        ):
            msg = (
                "timeout must be positive and at most 90 days, got: "
                f"{self.timeout.to_seconds()}s"
            )
            raise ValidationError(msg)
        if self.item_serdes is not None and not self.collect_results:
            msg = "item_serdes requires collect_results=True"
            raise ValidationError(msg)


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

        sub_type: Operation subtype identifier used for tracking and debugging.
            Examples: OperationSubType.MAP_ITERATION, OperationSubType.PARALLEL_BRANCH.
            Used internally by the execution engine for operation classification.

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
    sub_type: OperationSubType | None = None
    summary_generator: SummaryGenerator | None = None
    is_virtual: bool = False


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
            Default allows any number of failures. Use CompletionConfig.all_successful()
            to require all items to succeed.

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
    """Configuration for callbacks."""

    timeout: Duration = field(default_factory=Duration)
    heartbeat_timeout: Duration = field(default_factory=Duration)
    serdes: SerDes | None = None

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
