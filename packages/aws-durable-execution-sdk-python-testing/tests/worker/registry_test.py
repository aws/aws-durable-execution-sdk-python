"""Registry submit race-safety and shutdown."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from aws_durable_execution_sdk_python_testing.worker.registry import ExecutionRegistry
from aws_durable_execution_sdk_python_testing.worker.task import (
    ExecutionTask,
    TaskOutcome,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
    from aws_durable_execution_sdk_python_testing.worker.status import InvocationState
    from aws_durable_execution_sdk_python_testing.worker.task import WorkerContext


class _FakeExecution:
    def __init__(self, *, is_complete: bool = False) -> None:
        self.is_complete = is_complete


class _FakeStore:
    def __init__(self, execution: _FakeExecution) -> None:
        self._execution = execution

    def load(self, execution_arn: str) -> _FakeExecution:  # noqa: ARG002
        return self._execution


def _registry() -> ExecutionRegistry:
    execution = _FakeExecution()
    return ExecutionRegistry(
        cast("ExecutionStore", _FakeStore(execution)), cast("Scheduler", object())
    )


class _EchoTask(ExecutionTask[str]):
    """Returns a value, carrying the current status forward unchanged."""

    def __init__(self, value: str) -> None:
        self._value = value

    def execute(
        self,
        status: InvocationState,
        worker: WorkerContext,  # noqa: ARG002
    ) -> TaskOutcome[str]:
        return TaskOutcome(status, self._value)


def test_submit_retries_when_handed_out_worker_lane_is_stopped() -> None:
    """The teardown race: a worker is handed out and then stops its own
    lane while still briefly mapped. submit must evict the dead worker
    and retry on a fresh lane rather than surfacing RuntimeError."""
    registry = _registry()
    arn = "arn-race"

    stopped = registry.get_or_create(arn)
    stopped.stop()

    result = registry.submit(arn, _EchoTask("ok")).result(timeout=5)

    assert result == "ok"
    # The dead worker was evicted and replaced with a fresh one.
    assert registry.get(arn) is not stopped


def test_shutdown_stops_all_lanes_and_drops_workers() -> None:
    """shutdown stops every lane and clears the registry so per-execution
    lane threads do not outlive the owning runner."""
    registry = _registry()
    workers = [registry.get_or_create(f"arn-{i}") for i in range(3)]
    assert registry.active_count() == 3

    registry.shutdown()

    assert registry.active_count() == 0
    # A stopped lane rejects further submits, proving it was stopped.
    for worker in workers:
        with pytest.raises(RuntimeError):
            worker.submit(_EchoTask("x"))
