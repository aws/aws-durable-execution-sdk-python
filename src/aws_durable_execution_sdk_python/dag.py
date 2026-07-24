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

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from aws_durable_execution_sdk_python.config import CompletionConfig
    from aws_durable_execution_sdk_python.lambda_service import ErrorObject
    from aws_durable_execution_sdk_python.retries import RetryDecision
    from aws_durable_execution_sdk_python.serdes import SerDes

T = TypeVar("T")


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


# Type alias: dependencies are declared as a list of TaskHandles (or None).
DepsArg = "list[TaskHandle[Any]] | None"
