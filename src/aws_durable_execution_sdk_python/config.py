"""Configuration types."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.exceptions import ValidationError

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")
U = TypeVar("U")

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
    from aws_durable_execution_sdk_python.retries import RetryDecision
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.types import SummaryGenerator


Numeric = int | float  # deliberately leaving off complex


@dataclass(frozen=True)
class Duration:
    """Represents a duration stored as total seconds."""

    seconds: int = 0

    def __post_init__(self):
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


@dataclass(frozen=True)
class BatchedInput(Generic[T, U]):
    batch_input: T
    items: list[U]


class TerminationMode(Enum):
    TERMINATE = "TERMINATE"
    CANCEL = "CANCEL"
    WAIT = "WAIT"
    ABANDON = "ABANDON"


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
        return CompletionConfig(
            min_successful=None,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
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

        summary_generator: Function to generate compact summaries for large results (>256KB).
            When the serialized result exceeds CHECKPOINT_SIZE_LIMIT, this generator
            creates a JSON summary instead of checkpointing the full result. The operation
            is marked with ReplayChildren=true to reconstruct the full result during replay.

            Used internally by map/parallel operations to handle large BatchResult payloads.
            Signature: (result: T) -> str

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


class StepSemantics(Enum):
    AT_MOST_ONCE_PER_RETRY = "AT_MOST_ONCE_PER_RETRY"
    AT_LEAST_ONCE_PER_RETRY = "AT_LEAST_ONCE_PER_RETRY"


@dataclass(frozen=True)
class StepConfig:
    """Configuration for a step."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY
    serdes: SerDes | None = None


class CheckpointMode(Enum):
    NO_CHECKPOINT = ("NO_CHECKPOINT",)
    CHECKPOINT_AT_FINISH = ("CHECKPOINT_AT_FINISH",)
    CHECKPOINT_AT_START_AND_FINISH = "CHECKPOINT_AT_START_AND_FINISH"


@dataclass(frozen=True)
class ChildConfig(Generic[T]):
    """Configuration options for child context operations.

    This class configures how child contexts are executed and checkpointed,
    matching the TypeScript ChildConfig interface behavior.

    Args:
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

        sub_type: Operation subtype identifier used for tracking and debugging.
            Examples: OperationSubType.MAP_ITERATION, OperationSubType.PARALLEL_BRANCH.
            Used internally by the execution engine for operation classification.

        summary_generator: Function to generate compact summaries for large results (>256KB).
            When the serialized result exceeds CHECKPOINT_SIZE_LIMIT, this generator
            creates a JSON summary instead of checkpointing the full result. The operation
            is marked with ReplayChildren=true to reconstruct the full result during replay.

            Used internally by map/parallel operations to handle large BatchResult payloads.
            Signature: (result: T) -> str
    Note:
        checkpoint_mode field is commented out as it's not currently implemented.
        When implemented, it will control when checkpoints are created:
        - CHECKPOINT_AT_START_AND_FINISH: Checkpoint at both start and completion (default)
        - CHECKPOINT_AT_FINISH: Only checkpoint when operation completes
        - NO_CHECKPOINT: No automatic checkpointing

    See TypeScript reference: aws-durable-execution-sdk-js/src/types/index.ts
    """

    # checkpoint_mode: CheckpointMode = CheckpointMode.CHECKPOINT_AT_START_AND_FINISH
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    sub_type: OperationSubType | None = None
    summary_generator: SummaryGenerator | None = None


class ItemsPerBatchUnit(Enum):
    COUNT = ("COUNT",)
    BYTES = "BYTES"


@dataclass(frozen=True)
class ItemBatcher(Generic[T]):
    """Configuration for batching items in map operations.

    This class defines how individual items should be grouped together into batches
    for more efficient processing in map operations.

    Args:
        max_items_per_batch: Maximum number of items to include in a single batch.
            If 0 (default), no item count limit is applied. Use this to control
            batch size when processing many small items.

        max_item_bytes_per_batch: Maximum total size in bytes for items in a batch.
            If 0 (default), no size limit is applied. Use this to control memory
            usage when processing large items or when items vary significantly in size.

        batch_input: Additional data to include with each batch.
            This data is passed to the processing function along with the batched items.
            Useful for providing context or configuration that applies to all items
            in the batch.

    Example:
        # Batch up to 100 items or 1MB, whichever comes first
        batcher = ItemBatcher(
            max_items_per_batch=100,
            max_item_bytes_per_batch=1024*1024,
            batch_input={"processing_mode": "fast"}
        )
    """

    max_items_per_batch: int = 0
    max_item_bytes_per_batch: int | float = 0
    batch_input: T | None = None


@dataclass(frozen=True)
class MapConfig:
    """Configuration options for map operations over collections.

    This class configures how map operations process collections of items,
    including concurrency, batching, completion criteria, and serialization.

    Args:
        max_concurrency: Maximum number of items to process concurrently.
            If None, no limit is imposed and all items are processed concurrently.
            Use this to control resource usage when processing large collections.

        item_batcher: Configuration for batching multiple items together for processing.
            Allows grouping items by count or size to optimize processing efficiency.
            Default is no batching (each item processed individually).

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

        summary_generator: Function to generate compact summaries for large results (>256KB).
            When the serialized result exceeds CHECKPOINT_SIZE_LIMIT, this generator
            creates a JSON summary instead of checkpointing the full result. The operation
            is marked with ReplayChildren=true to reconstruct the full result during replay.

            Used internally by map/parallel operations to handle large BatchResult payloads.
            Signature: (result: T) -> str

    Example:
        # Process 5 items at a time, batch by count, require all to succeed
        config = MapConfig(
            max_concurrency=5,
            item_batcher=ItemBatcher(max_items_per_batch=10),
            completion_config=CompletionConfig.all_successful()
        )
    """

    max_concurrency: int | None = None
    item_batcher: ItemBatcher = field(default_factory=ItemBatcher)
    completion_config: CompletionConfig = field(default_factory=CompletionConfig)
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None


@dataclass(frozen=True)
class InvokeConfig(Generic[P, R]):
    """
    Configuration for invoke operations.

    This class configures how function invocations are executed, including
    timeout behavior, serialization, and tenant isolation.

    Args:
        timeout: Maximum duration to wait for the invoked function to complete.
            Default is no timeout. Use this to prevent long-running invocations
            from blocking execution indefinitely.

        serdes_payload: Custom serialization/deserialization for the payload
            sent to the invoked function. Defaults to DEFAULT_JSON_SERDES when
            not set.

        serdes_result: Custom serialization/deserialization for the result
            returned from the invoked function. Defaults to DEFAULT_JSON_SERDES when
            not set.

        tenant_id: Optional tenant identifier for multi-tenant isolation.
            If provided, the invocation will be scoped to this tenant.
    """

    # retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    timeout: Duration = field(default_factory=Duration)
    serdes_payload: SerDes[P] | None = None
    serdes_result: SerDes[R] | None = None
    tenant_id: str | None = None

    @property
    def timeout_seconds(self) -> int:
        """Get timeout in seconds."""
        return self.timeout.to_seconds()


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


class StepFuture(Generic[T]):
    """A future that will block on result() until the step returns."""

    def __init__(self, future: Future[T], name: str | None = None):
        self.name = name
        self.future = future

    def result(self, timeout_seconds: int | None = None) -> T:
        """Return the result of the Future."""
        return self.future.result(timeout=timeout_seconds)


# region Jitter


class JitterStrategy(StrEnum):
    """
    Jitter strategies are used to introduce noise when attempting to retry
    an invoke. We introduce noise to prevent a thundering-herd effect where
    a group of accesses (e.g. invokes) happen at once.

    Jitter is meant to be used to spread operations across time.

    members:
        :NONE: No jitter; use the exact calculated delay
        :FULL: Full jitter; random delay between 0 and calculated delay
        :HALF: Half jitter; random delay between 0.5x and 1.0x of the calculated delay
    """

    NONE = "NONE"
    FULL = "FULL"
    HALF = "HALF"

    def compute_jitter(self, delay) -> float:
        match self:
            case JitterStrategy.NONE:
                return 0
            case JitterStrategy.HALF:
                return delay * (random.random() * 0.5 + 0.5)  # noqa: S311
            case _:  # default is FULL
                return random.random() * delay  # noqa: S311


# endregion Jitter
