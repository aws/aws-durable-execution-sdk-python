"""Worker tasks for the checkpoint and get-state operations.

Routing these through a per-execution worker serializes all checkpoint
and get-state calls for one execution, so concurrent callers can never
observe or write half-applied state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.worker.status import InvocationState
from aws_durable_execution_sdk_python_testing.worker.task import (
    ExecutionTask,
    TaskOutcome,
    WorkerContext,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.lambda_service import OperationUpdate

    from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
        CheckpointProcessor,
    )

T = TypeVar("T")


def _status_after(worker: WorkerContext) -> InvocationState:
    """The status to carry forward after a task.

    COMPLETED once the execution is terminal (so the worker tears down);
    otherwise whatever status the task left on the worker. A store error
    during the completion check is treated as not-yet-terminal.
    """
    try:
        is_complete: bool = worker.reload().is_complete
    except Exception:  # noqa: BLE001 — store may be unavailable; keep the worker
        return worker.status
    return InvocationState.COMPLETED if is_complete else worker.status


class CallableTask(ExecutionTask[T]):
    """Runs a checkpoint-style callable on the worker lane.

    Lets a caller serialize an existing operation on the execution's
    lane without restating it as a task; the lane is released once the
    execution completes.
    """

    def __init__(self, fn: Callable[[], T]) -> None:
        self._fn: Callable[[], T] = fn

    def execute(
        self,
        status: InvocationState,  # noqa: ARG002
        worker: WorkerContext,
    ) -> TaskOutcome[T]:
        result: T = self._fn()
        return TaskOutcome(_status_after(worker), result)


class CheckpointTask(ExecutionTask[CheckpointOutput]):
    """Applies a checkpoint for one execution."""

    def __init__(
        self,
        processor: CheckpointProcessor,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> None:
        self._processor: CheckpointProcessor = processor
        self._checkpoint_token: str = checkpoint_token
        self._updates: list[OperationUpdate] = updates
        self._client_token: str | None = client_token

    def execute(
        self,
        status: InvocationState,  # noqa: ARG002
        worker: WorkerContext,
    ) -> TaskOutcome[CheckpointOutput]:
        response: CheckpointOutput = self._processor.process_checkpoint(
            self._checkpoint_token, self._updates, self._client_token
        )
        return TaskOutcome(_status_after(worker), response)


class GetStateTask(ExecutionTask[StateOutput]):
    """Reads the current state for one execution."""

    def __init__(
        self,
        processor: CheckpointProcessor,
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> None:
        self._processor: CheckpointProcessor = processor
        self._checkpoint_token: str = checkpoint_token
        self._next_marker: str = next_marker
        self._max_items: int = max_items

    def execute(
        self,
        status: InvocationState,  # noqa: ARG002
        worker: WorkerContext,
    ) -> TaskOutcome[StateOutput]:
        response: StateOutput = self._processor.get_execution_state(
            self._checkpoint_token, self._next_marker, self._max_items
        )
        return TaskOutcome(_status_after(worker), response)
