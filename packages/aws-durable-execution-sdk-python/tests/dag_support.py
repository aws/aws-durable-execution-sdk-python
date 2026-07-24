"""Shared test support: an in-memory DurableServiceClient that actually stores
operations, so checkpoint fast paths / replay behave realistically for DAG tests.
"""

from __future__ import annotations

import datetime
import threading
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ChainedInvokeDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ContextDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    StepDetails,
    WaitDetails,
)
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.state import ExecutionState

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import OperationUpdate

_ACTION_STATUS = {
    OperationAction.START: OperationStatus.STARTED,
    OperationAction.SUCCEED: OperationStatus.SUCCEEDED,
    OperationAction.FAIL: OperationStatus.FAILED,
    OperationAction.RETRY: OperationStatus.STARTED,
    OperationAction.CANCEL: OperationStatus.CANCELLED,
}


class InMemoryServiceClient:
    """A minimal in-memory backend that persists operations by id.

    Enough fidelity to exercise checkpoint fast paths and replay: converts each
    ``OperationUpdate`` into a stored ``Operation`` and returns the full set on
    every checkpoint so ``ExecutionState`` reflects completion.
    """

    def __init__(self) -> None:
        self.operations: dict[str, Operation] = {}
        self._lock = threading.Lock()
        self.checkpoint_count = 0

    def _to_operation(self, update: OperationUpdate) -> Operation:
        status = _ACTION_STATUS[update.action]
        prev = self.operations.get(update.operation_id)
        step_details = None
        context_details = None
        chained_invoke_details = None
        wait_details = None
        callback_details = None

        if update.operation_type is OperationType.STEP:
            attempt = 0
            if prev and prev.step_details:
                attempt = prev.step_details.attempt
            if update.action is OperationAction.RETRY:
                attempt += 1
            step_details = StepDetails(
                attempt=attempt, result=update.payload, error=update.error
            )
        elif update.operation_type is OperationType.CONTEXT:
            replay = (
                update.context_options.replay_children
                if update.context_options
                else False
            )
            context_details = ContextDetails(
                replay_children=replay, result=update.payload, error=update.error
            )
        elif update.operation_type is OperationType.CHAINED_INVOKE:
            chained_invoke_details = ChainedInvokeDetails(
                result=update.payload,
                error=update.error,
            )
        elif update.operation_type is OperationType.WAIT:
            wait_details = WaitDetails()
        elif update.operation_type is OperationType.CALLBACK:
            callback_details = CallbackDetails(
                callback_id=update.operation_id,
                result=update.payload,
                error=update.error,
            )

        return Operation(
            operation_id=update.operation_id,
            operation_type=update.operation_type,
            status=status,
            parent_id=update.parent_id,
            name=update.name,
            sub_type=update.sub_type,
            step_details=step_details,
            context_details=context_details,
            chained_invoke_details=chained_invoke_details,
            wait_details=wait_details,
            callback_details=callback_details,
        )

    def checkpoint(
        self, durable_execution_arn, checkpoint_token, updates, client_token
    ) -> CheckpointOutput:
        with self._lock:
            self.checkpoint_count += 1
            for update in updates:
                self.operations[update.operation_id] = self._to_operation(update)
            all_ops = list(self.operations.values())
        return CheckpointOutput(
            checkpoint_token="token",
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=all_ops, next_marker=None
            ),
        )

    def get_execution_state(
        self, durable_execution_arn, checkpoint_token, next_marker, max_items=1000
    ):  # pragma: no cover - not used by these tests
        raise NotImplementedError

    def stop(self, execution_arn, payload) -> datetime.datetime:  # pragma: no cover
        return datetime.datetime.now(tz=datetime.UTC)


def make_state(
    client: InMemoryServiceClient | None = None,
) -> tuple[ExecutionState, InMemoryServiceClient]:
    """Build a fresh ExecutionState wired to an in-memory client.

    Starts the background checkpoint-processing thread exactly like the real
    runtime (``execution.py`` submits ``checkpoint_batches_forever`` to its
    executor). Without this thread every synchronous checkpoint blocks forever
    on its completion event — which is the deadlock DAG tasks hit when they run
    their operations on the executor's worker threads. The thread is a daemon so
    it never blocks interpreter exit; it idles on the queue between tests.
    """
    client = client or InMemoryServiceClient()
    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="token",  # noqa: S106
        operations={},
        service_client=client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    checkpoint_thread = threading.Thread(
        target=state.checkpoint_batches_forever,
        name="dag-test-checkpointer",
        daemon=True,
    )
    checkpoint_thread.start()
    return state, client


def make_context(state: ExecutionState, parent_id: str | None = None) -> DurableContext:
    """Build a DurableContext over the given state."""
    return DurableContext(
        state=state,
        execution_context=ExecutionContext(
            durable_execution_arn=state.durable_execution_arn
        ),
        parent_id=parent_id,
    )
