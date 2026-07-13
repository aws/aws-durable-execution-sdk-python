"""Concurrency invariants of the per-execution worker model.

These prove the properties the design relies on: a completing task tears
its own lane down without deadlocking, one execution's tasks never
overlap, and different executions run in parallel.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, cast

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


class _FakeExecution:
    def __init__(self, *, is_complete: bool = False) -> None:
        self.is_complete = is_complete


class _FakeStore:
    def __init__(self, execution: _FakeExecution) -> None:
        self._execution = execution

    def load(self, execution_arn: str) -> _FakeExecution:  # noqa: ARG002
        return self._execution


def _registry(execution: _FakeExecution) -> ExecutionRegistry:
    return ExecutionRegistry(
        cast("ExecutionStore", _FakeStore(execution)), cast("Scheduler", object())
    )


class _CompletingTask(ExecutionTask[str]):
    """Marks the execution complete and returns COMPLETED, which makes
    the worker tear down its own lane from inside the running task."""

    def __init__(self, execution: _FakeExecution) -> None:
        self._execution = execution

    def execute(
        self,
        status: InvocationState,  # noqa: ARG002
        worker: WorkerContext,  # noqa: ARG002
    ) -> TaskOutcome[str]:
        self._execution.is_complete = True
        return TaskOutcome(InvocationState.COMPLETED, "done")


def test_completing_task_tears_down_without_deadlock() -> None:
    execution = _FakeExecution(is_complete=False)
    registry = _registry(execution)
    worker = registry.get_or_create("arn-complete")

    future = worker.submit(_CompletingTask(execution))

    # A self-join during teardown would hang here; the bounded wait is
    # the deadlock guard.
    assert future.result(timeout=5) == "done"
    # The worker removed itself on completion.
    assert registry.get("arn-complete") is None


class _OverlapTask(ExecutionTask[None]):
    """Records the peak number of concurrently-running instances."""

    def __init__(self, state: _Concurrency) -> None:
        self._state = state

    def execute(
        self,
        status: InvocationState,
        worker: WorkerContext,  # noqa: ARG002
    ) -> TaskOutcome[None]:
        self._state.enter()
        time.sleep(0.01)
        self._state.leave()
        return TaskOutcome(status, None)


class _Concurrency:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def enter(self) -> None:
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)

    def leave(self) -> None:
        with self._lock:
            self.active -= 1


def test_same_execution_tasks_never_overlap() -> None:
    execution = _FakeExecution(is_complete=False)
    registry = _registry(execution)
    worker = registry.get_or_create("arn-serial")
    state = _Concurrency()

    futures = [worker.submit(_OverlapTask(state)) for _ in range(10)]
    for f in futures:
        f.result(timeout=5)

    assert state.peak == 1


class _BarrierTask(ExecutionTask[None]):
    """Blocks on a shared barrier; only completes if every lane runs at
    the same time, proving cross-execution parallelism."""

    def __init__(self, barrier: threading.Barrier) -> None:
        self._barrier = barrier

    def execute(
        self,
        status: InvocationState,
        worker: WorkerContext,  # noqa: ARG002
    ) -> TaskOutcome[None]:
        self._barrier.wait(timeout=5)
        return TaskOutcome(status, None)


def test_different_executions_run_in_parallel() -> None:
    execution = _FakeExecution(is_complete=False)
    registry = _registry(execution)
    parties = 4
    barrier = threading.Barrier(parties)

    # Distinct ARNs -> distinct workers -> distinct lanes. If the lanes
    # did not run in parallel the barrier would never trip and result()
    # would raise BrokenBarrierError.
    futures = [
        registry.get_or_create(f"arn-{i}").submit(_BarrierTask(barrier))
        for i in range(parties)
    ]
    for f in futures:
        f.result(timeout=10)
