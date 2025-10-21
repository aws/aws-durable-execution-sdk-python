"""Types and Protocols. Don't import anything other than config here - the reason it exists is to avoid circular references."""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from aws_durable_execution_sdk_python.config import (
        BatchedInput,
        CallbackConfig,
        ChildConfig,
        MapConfig,
        ParallelConfig,
        StepConfig,
    )

T = TypeVar("T")
U = TypeVar("U")
C_co = TypeVar("C_co", covariant=True)


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


class SummaryGenerator(Protocol):
    def __call__(self, result: BatchResult) -> str: ...


# endregion Summary
