"""Public API for the experimental DAG operation (``context.dag()``).

.. warning::
   **Experimental.** This API is experimental and may be changed or removed in
   future releases without a major-version bump. Do not depend on it in
   production until it is promoted to stable.

This module defines the public, user-facing surface of the DAG primitive:
enums (:class:`TriggerRule`, :class:`TaskStatus`, :class:`SkipReason`,
:class:`DagCompletionReason`), the per-task result record
(:class:`TaskExecution`), configuration (:class:`DagConfig`), the registration
handle (:class:`TaskHandle`), the resolved-dependency mapping
(:class:`DepsMap`), the registration protocol (:class:`DagContext`), and the
aggregate result type (:class:`DagResult`).

Implementation lives under ``operation/dag*.py``; this module holds only the
declarative public types so it can sit low in the dependency graph.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from aws_durable_execution_sdk_python.config import (
        ChildConfig,
        CompletionConfig,
        InvokeConfig,
        MapConfig,
        ParallelConfig,
        StepConfig,
        WaitForCallbackConfig,
        WaitForConditionConfig,
    )
    from aws_durable_execution_sdk_python.lambda_service import ErrorObject
    from aws_durable_execution_sdk_python.retries import RetryDecision
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.types import (
        DurableContext,
    )

T = TypeVar("T")
P = TypeVar("P")
R = TypeVar("R")
U = TypeVar("U")


# region enums
class TriggerRule(Enum):
    """Controls when a task runs based on the terminal state of its upstream deps.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    ALL_SUCCESS = "ALL_SUCCESS"  # default: run only if every dep SUCCEEDED
    ALL_FAILED = "ALL_FAILED"  # run only if every dep FAILED (and there is >=1)
    ALL_DONE = "ALL_DONE"  # run once every dep is terminal, regardless of outcome
    ONE_SUCCESS = "ONE_SUCCESS"  # run if at least one dep SUCCEEDED
    ONE_FAILED = "ONE_FAILED"  # run if at least one dep FAILED
    NONE_FAILED = "NONE_FAILED"  # run if no dep FAILED (SUCCEEDED/SKIPPED allowed)


class TaskStatus(Enum):
    """Terminal (or in-flight) status of a single DAG task.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    STARTED = "STARTED"


class SkipReason(Enum):
    """Why a task was SKIPPED.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    TRIGGER_RULE = "TRIGGER_RULE"
    RUN_IF_PREDICATE = "RUN_IF_PREDICATE"


class DagCompletionReason(Enum):
    """Why the DAG stopped scheduling.

    The first three members are value-compatible with
    ``concurrency.CompletionReason``; ``COMPLETED_WITH_FAILURES`` is DAG-only and
    signals the default drain-on-failure path finished with >=1 failed task.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES"


# endregion enums


@dataclass(frozen=True)
class TaskExecution(Generic[T]):
    """Immutable record of a single task's outcome within a DAG.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    name: str
    status: TaskStatus
    skip_reason: SkipReason | None = None
    result: T | None = None
    error: ErrorObject | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class DagConfig:
    """Configuration for a DAG.

    Reuses the existing threshold-only :class:`~...config.CompletionConfig`
    verbatim; result-based custom completion is v2-deferred.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    max_concurrency: int | None = None
    completion_config: CompletionConfig | None = None
    default_retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    default_trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS
    serdes: SerDes | None = None
    summary_generator: Callable[[Any], str] | None = None


# region handle & deps-map
@dataclass(eq=False)
class TaskHandle(Generic[T]):
    """Registration-time reference to a task, plus a small chaining builder.

    Never serialized. Identity is the task ``name`` (unique within a DAG scope).
    ``__hash__`` is keyed on the name so handles can live in ``set``\\ s (e.g. the
    dependency edge set); note this does NOT make ``deps[handle]`` resolve against
    a name-keyed dict — :meth:`DepsMap.__getitem__` dispatches on the handle type
    explicitly.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    _name: str
    _dag: Any  # back-ref to DagContextImpl; typed Any to avoid a circular import

    def __hash__(self) -> int:
        return hash(self._name)

    @property
    def name(self) -> str:
        """The resolved task name."""
        return self._name

    def after(self, *deps: TaskHandle[Any]) -> TaskHandle[T]:
        """Add ordering-only dependencies (wait for them, but do not receive
        their results in the ``DepsMap``). Returns ``self`` for chaining.

        .. warning::
           **Experimental.**
        """
        self._dag._register_after(self, deps)
        return self

    def trigger_rule(self, rule: TriggerRule) -> TaskHandle[T]:
        """Override this task's trigger rule. Returns ``self`` for chaining.

        .. warning::
           **Experimental.**
        """
        self._dag._register_trigger_rule(self, rule)
        return self


class DepsMap(Mapping[str, Any]):
    """Resolved upstream results, keyed by dependency task name.

    Access by string name (``deps["fetch"] -> Any``) or, for static typing, by
    the originating :class:`TaskHandle` (``deps[handle] -> T``). The handle path
    dispatches at runtime on ``isinstance(key, TaskHandle)`` and extracts the
    name; it does not rely on hashing.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    def __init__(self, by_name: dict[str, Any]) -> None:
        self._by_name = by_name

    @overload
    def __getitem__(self, key: TaskHandle[T]) -> T: ...
    @overload
    def __getitem__(self, key: str) -> Any: ...
    def __getitem__(self, key: str | TaskHandle[Any]) -> Any:
        name = key._name if isinstance(key, TaskHandle) else key
        return self._by_name[name]

    def __iter__(self):
        return iter(self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)

    def __contains__(self, key: object) -> bool:
        name = key._name if isinstance(key, TaskHandle) else key
        return name in self._by_name


# endregion handle & deps-map


# Type alias: dependencies are declared as a list of TaskHandles (or None).
DepsArg = "Sequence[TaskHandle[Any]] | None"


# region DagContext protocol
class DagContext(ABC):
    """Declarative task-registration surface passed to a DAG ``register`` callback.

    Each method registers exactly one task and returns a :class:`TaskHandle`.
    Task bodies always receive the resolved :class:`DepsMap` as their first
    positional argument (empty for root tasks).

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    @abstractmethod
    def step(
        self,
        func: Callable[..., T],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: StepConfig | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[T]:
        """Register a step task. Body signature: ``(deps, step_ctx)``.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def invoke(
        self,
        function_name: str,
        payload_fn: Callable[[DepsMap], Any] | Any,
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: InvokeConfig[Any, Any] | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[Any]:
        """Register an invoke task. ``payload_fn`` is a plain value or a
        ``(deps) -> payload`` callable materialized at scheduling time.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def wait_for_callback(
        self,
        submitter: Callable[..., None],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: WaitForCallbackConfig | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[Any]:
        """Register a wait-for-callback task. Body signature: ``(deps, callback_id, ctx)``.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def wait(
        self,
        seconds: int,
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[None]:
        """Register a wait task.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def wait_for_condition(
        self,
        check: Callable[..., T],
        config: WaitForConditionConfig[T],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[T]:
        """Register a wait-for-condition task. Body signature: ``(deps, state, ctx)``.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def run_in_child_context(
        self,
        func: Callable[..., T],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: ChildConfig | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[T]:
        """Register a child-context task. Body signature: ``(deps, child_ctx)``.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def map(
        self,
        inputs: Sequence[U] | Callable[[DepsMap], Sequence[U]],
        func: Callable[..., T],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: MapConfig | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[Any]:
        """Register a map task. ``inputs`` may be a sequence or ``(deps) -> sequence``.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], Any]],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: ParallelConfig | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[Any]:
        """Register a parallel task.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover

    @abstractmethod
    def dag(
        self,
        register: Callable[[DagContext], None],
        deps: Sequence[TaskHandle[Any]] | None = None,
        name: str | None = None,
        config: DagConfig | None = None,
        *,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        run_if: Callable[[DepsMap], bool] | None = None,
    ) -> TaskHandle[Any]:
        """Register a nested DAG task.

        .. warning::
           **Experimental.**
        """
        ...  # pragma: no cover


# endregion DagContext protocol


# region DagResult surface
class DagResult(ABC):
    """Aggregate result of a DAG run. Concrete impl lives in
    ``operation/dag_result.py``.

    .. warning::
       **Experimental.** This API is experimental and may be changed or removed
       in future releases.
    """

    @abstractmethod
    def get_result(self, task: str | TaskHandle[Any]) -> Any: ...  # pragma: no cover

    @abstractmethod
    def get_status(
        self, task: str | TaskHandle[Any]
    ) -> TaskStatus | None: ...  # pragma: no cover

    @abstractmethod
    def succeeded(self) -> list[TaskExecution]: ...  # pragma: no cover

    @abstractmethod
    def failed(self) -> list[TaskExecution]: ...  # pragma: no cover

    @abstractmethod
    def skipped(self) -> list[TaskExecution]: ...  # pragma: no cover

    @abstractmethod
    def throw_if_error(self) -> None: ...  # pragma: no cover


# endregion DagResult surface
