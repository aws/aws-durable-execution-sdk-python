"""Unit tests for the execution worker, registry, and task contract."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from aws_durable_execution_sdk_python_testing.worker.registry import ExecutionRegistry
from aws_durable_execution_sdk_python_testing.worker.status import InvocationState
from aws_durable_execution_sdk_python_testing.worker.task import (
    ExecutionTask,
    TaskOutcome,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
    from aws_durable_execution_sdk_python_testing.worker.task import WorkerContext

ARN = "arn:aws:lambda:us-west-2:123456789012:function:fn/durable-execution/exec/inv"


class _FakeStore:
    """Minimal store stub: ``load`` returns a sentinel the tasks ignore."""

    def load(self, execution_arn: str) -> object:  # noqa: ARG002
        return object()


def _registry() -> ExecutionRegistry:
    return ExecutionRegistry(
        cast("ExecutionStore", _FakeStore()), cast("Scheduler", object())
    )


class _ReturnTask(ExecutionTask[str]):
    """Trivial task: returns a fixed value and next status."""

    def __init__(self, next_status: InvocationState, value: str) -> None:
        self._next = next_status
        self._value = value

    def execute(
        self,
        status: InvocationState,  # noqa: ARG002
        worker: WorkerContext,  # noqa: ARG002
    ) -> TaskOutcome[str]:
        return TaskOutcome(self._next, self._value)


class _RecordStatusTask(ExecutionTask[InvocationState]):
    """Records the status it observed and returns it as the value."""

    def __init__(self, next_status: InvocationState) -> None:
        self._next = next_status

    def execute(
        self,
        status: InvocationState,
        worker: WorkerContext,  # noqa: ARG002
    ) -> TaskOutcome[InvocationState]:
        return TaskOutcome(self._next, status)


def test_worker_runs_task_and_returns_value() -> None:
    registry = _registry()
    worker = registry.get_or_create(ARN)
    future = worker.submit(_ReturnTask(InvocationState.PRE_INVOKE, "hello"))
    assert future.result(timeout=5) == "hello"


def test_worker_threads_status_between_tasks() -> None:
    registry = _registry()
    worker = registry.get_or_create(ARN)

    # First task sees the initial PRE_INVOKE and advances to INVOKING.
    first = worker.submit(_RecordStatusTask(InvocationState.INVOKING))
    # Second task must observe the INVOKING carried over from the first.
    second = worker.submit(_RecordStatusTask(InvocationState.INVOKING))

    assert first.result(timeout=5) is InvocationState.PRE_INVOKE
    assert second.result(timeout=5) is InvocationState.INVOKING
    assert worker.status is InvocationState.INVOKING


def test_worker_tears_down_on_completed() -> None:
    registry = _registry()
    worker = registry.get_or_create(ARN)

    future = worker.submit(_ReturnTask(InvocationState.COMPLETED, "done"))
    assert future.result(timeout=5) == "done"

    # Completion removes the worker from the registry and stops its lane.
    assert registry.get(ARN) is None
    assert registry.active_count() == 0
    with pytest.raises(RuntimeError, match="stopped"):
        worker.submit(_ReturnTask(InvocationState.PRE_INVOKE, "late"))


def test_registry_get_or_create_is_idempotent_per_arn() -> None:
    registry = _registry()
    first = registry.get_or_create(ARN)
    second = registry.get_or_create(ARN)
    assert first is second
    assert registry.get(ARN) is first
    assert registry.active_count() == 1


def test_registry_distinct_workers_per_arn() -> None:
    registry = _registry()
    worker_a = registry.get_or_create(ARN + "-a")
    worker_b = registry.get_or_create(ARN + "-b")
    assert worker_a is not worker_b
    assert registry.active_count() == 2


def test_registry_remove_drops_worker() -> None:
    registry = _registry()
    registry.get_or_create(ARN)
    registry.remove(ARN)
    assert registry.get(ARN) is None
    assert registry.active_count() == 0
    # Removing an unknown ARN is a no-op.
    registry.remove(ARN)
