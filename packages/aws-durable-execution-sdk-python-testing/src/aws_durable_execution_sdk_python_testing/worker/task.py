"""The execution-task contract and its outcome."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution
    from aws_durable_execution_sdk_python_testing.worker.status import InvocationState

T = TypeVar("T")


class WorkerContext(Protocol):
    """The view of its worker that a task is given while it runs.

    Kept minimal (and defined here, below the task it serves) so the
    task module does not depend on the concrete worker, avoiding a
    circular import.
    """

    @property
    def status(self) -> InvocationState: ...  # pragma: no cover

    def reload(self) -> Execution: ...  # pragma: no cover


@dataclass(frozen=True)
class TaskOutcome(Generic[T]):
    """What a task produced: the invocation state to carry forward and
    the value to return to the task's caller.
    """

    next_status: InvocationState
    value: T


class ExecutionTask(ABC, Generic[T]):
    """A single operation against one execution, run on its worker.

    A task runs with exclusive access to its execution's state for the
    duration of :meth:`execute` (the worker runs one at a time), so it
    may load, read, and mutate the execution without further
    synchronization. It receives the current invocation state and returns
    a :class:`TaskOutcome` carrying the next state and the value to
    deliver to the caller.
    """

    @abstractmethod
    def execute(
        self,
        status: InvocationState,
        worker: WorkerContext,
    ) -> TaskOutcome[T]:
        """Run the operation and return its outcome."""
