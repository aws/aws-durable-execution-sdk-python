"""Configuration types."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.retries import RetryDecision  # noqa: TCH001

R = TypeVar("R")
T = TypeVar("T")
U = TypeVar("U")

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python.lambda_service import OperationSubType
    from aws_durable_execution_sdk_python.serdes import SerDes


Numeric = int | float  # deliberately leaving off complex


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
    max_concurrency: int | None = None
    completion_config: CompletionConfig = field(
        default_factory=CompletionConfig.all_successful
    )
    serdes: SerDes | None = None


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
class ChildConfig:
    """Options when running inside a child context."""

    # checkpoint_mode: CheckpointMode = CheckpointMode.CHECKPOINT_AT_START_AND_FINISH
    serdes: SerDes | None = None
    sub_type: OperationSubType | None = None


class ItemsPerBatchUnit(Enum):
    COUNT = ("COUNT",)
    BYTES = "BYTES"


@dataclass(frozen=True)
class ItemBatcher(Generic[T]):
    max_items_per_batch: int = 0
    max_item_bytes_per_batch: int | float = 0
    batch_input: T | None = None


@dataclass(frozen=True)
class MapConfig:
    max_concurrency: int | None = None
    item_batcher: ItemBatcher = field(default_factory=ItemBatcher)
    completion_config: CompletionConfig = field(default_factory=CompletionConfig)
    serdes: SerDes | None = None


@dataclass(frozen=True)
class CallbackConfig:
    """Configuration for callbacks."""

    timeout_seconds: int = 0
    heartbeat_timeout_seconds: int = 0
    serdes: SerDes | None = None


@dataclass(frozen=True)
class WaitForCallbackConfig(CallbackConfig):
    """Configuration for wait for callback."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None


@dataclass(frozen=True)
class WaitForConditionDecision:
    """Decision about whether to continue waiting."""

    should_continue: bool
    delay_seconds: int

    @classmethod
    def continue_waiting(cls, delay_seconds: int) -> WaitForConditionDecision:
        """Create a decision to continue waiting for delay_seconds."""
        return cls(should_continue=True, delay_seconds=delay_seconds)

    @classmethod
    def stop_polling(cls) -> WaitForConditionDecision:
        """Create a decision to stop polling."""
        return cls(should_continue=False, delay_seconds=-1)


@dataclass(frozen=True)
class WaitForConditionConfig(Generic[T]):
    """Configuration for wait_for_condition."""

    wait_strategy: Callable[[T, int], WaitForConditionDecision]
    initial_state: T
    serdes: SerDes | None = None


class StepFuture(Generic[T]):
    """A future that will block on result() until the step returns."""

    def __init__(self, future: Future[T], name: str | None = None):
        self.name = name
        self.future = future

    def result(self, timeout_seconds: int | None = None) -> T:
        """Return the result of the Future."""
        return self.future.result(timeout=timeout_seconds)
