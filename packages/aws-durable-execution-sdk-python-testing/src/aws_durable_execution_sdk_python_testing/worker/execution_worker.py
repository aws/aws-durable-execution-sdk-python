"""The per-execution serial worker."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python_testing.worker.lane import SerialTaskLane
from aws_durable_execution_sdk_python_testing.worker.status import InvocationState

if TYPE_CHECKING:
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python_testing.execution import Execution
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
    from aws_durable_execution_sdk_python_testing.worker.registry import (
        ExecutionRegistry,
    )
    from aws_durable_execution_sdk_python_testing.worker.task import (
        ExecutionTask,
        TaskOutcome,
    )

T = TypeVar("T")


class ExecutionWorker:
    """Runs every operation for one execution, one at a time.

    Operations are submitted as :class:`ExecutionTask` instances and run
    on a single lane, so they never overlap and the execution's state
    stays consistent. The worker carries the invocation state returned by
    each task into the next, and once an execution completes it removes
    itself from the registry and stops its lane.

    Build instances with :meth:`create`, which starts the worker's lane;
    the constructor only initializes fields.
    """

    def __init__(
        self,
        execution_arn: str,
        store: ExecutionStore,
        scheduler: Scheduler,
        registry: ExecutionRegistry,
        lane: SerialTaskLane,
    ) -> None:
        self._arn: str = execution_arn
        self._store: ExecutionStore = store
        self._scheduler: Scheduler = scheduler
        self._registry: ExecutionRegistry = registry
        self._lane: SerialTaskLane = lane
        self._status: InvocationState = InvocationState.PRE_INVOKE

    @classmethod
    def create(
        cls,
        execution_arn: str,
        store: ExecutionStore,
        scheduler: Scheduler,
        registry: ExecutionRegistry,
    ) -> ExecutionWorker:
        """Build a worker and start its lane."""
        lane: SerialTaskLane = SerialTaskLane.create(
            name=f"execution-worker-{execution_arn}"
        )
        return cls(execution_arn, store, scheduler, registry, lane)

    @property
    def status(self) -> InvocationState:
        """The current invocation state.

        Authoritative only when read on the lane (inside a task); reads
        from other threads may observe a value about to change.
        """
        return self._status

    def set_status(self, status: InvocationState) -> None:
        """Set the invocation state directly.

        Used by the checkpoint gate before the invoke lifecycle runs on
        the lane; the lane otherwise threads status through task outcomes.
        """
        self._status = status

    def submit(self, task: ExecutionTask[T]) -> Future[T]:
        """Enqueue ``task`` on this worker's lane and return its future."""
        return self._lane.submit(lambda: self._run(task))

    def reload(self) -> Execution:
        """Load this execution's current state from the store."""
        return self._store.load(self._arn)

    def stop(self) -> None:
        """Stop this worker's lane without touching the registry.

        Used by :meth:`ExecutionRegistry.shutdown`, which has already
        dropped the worker from its map.
        """
        self._lane.stop(wait=False)

    def _run(self, task: ExecutionTask[T]) -> T:
        outcome: TaskOutcome[T] = task.execute(self._status, self)
        self._status = outcome.next_status
        if self._status is InvocationState.COMPLETED:
            self._teardown()
        return outcome.value

    def _teardown(self) -> None:
        # Runs on the lane thread. Stop the lane first (without joining,
        # which would wait on the current thread) so the stop flag is
        # set before the worker leaves the registry. A caller that was
        # handed this worker then fails fast on submit and the registry
        # hands it a fresh lane, rather than enqueueing onto a lane that
        # is about to exit.
        self._lane.stop(wait=False)
        self._registry.remove(self._arn)
