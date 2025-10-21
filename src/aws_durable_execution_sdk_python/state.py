"""Model for execution state."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.exceptions import DurableExecutionsError
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    DurableServiceClient,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StateOutput,
)
from aws_durable_execution_sdk_python.threading import OrderedLock

if TYPE_CHECKING:
    import datetime
    from collections.abc import MutableMapping


@dataclass(frozen=True)
class CheckpointedResult:
    """Result of a checkpointed operation.

    Set by ExecutionState.get_checkpoint_result. This is a convenience wrapper around
    Operation.

    Attributes:
        operation (Operation): The wrapped operation for the checkpoint result.
        status (OperationStatus): The status of the operation.
        result (str): the result of the operation.
        error (ErrorObject): the error of the operation.
    """

    operation: Operation | None = None
    status: OperationStatus | None = None
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def create_from_operation(cls, operation: Operation) -> CheckpointedResult:
        """Create a result from an operation."""
        result: str | None = None
        error: ErrorObject | None = None
        match operation.operation_type:
            case OperationType.STEP:
                step_details = operation.step_details
                result = step_details.result if step_details else None
                error = step_details.error if step_details else None

            case OperationType.CALLBACK:
                callback_details = operation.callback_details
                result = callback_details.result if callback_details else None
                error = callback_details.error if callback_details else None

            case OperationType.INVOKE:
                invoke_details = operation.invoke_details
                result = invoke_details.result if invoke_details else None
                error = invoke_details.error if invoke_details else None

            case OperationType.CONTEXT:
                context_details = operation.context_details
                result = context_details.result if context_details else None
                error = context_details.error if context_details else None

        return cls(
            operation=operation, status=operation.status, result=result, error=error
        )

    @classmethod
    def create_not_found(cls) -> CheckpointedResult:
        """Create a result when the checkpoint was not found."""
        return cls(operation=None)

    def is_existent(self) -> bool:
        """Return true if a checkpoint of any type exists."""
        return self.operation is not None

    def is_succeeded(self) -> bool:
        """Return True if the checkpointed operation is SUCCEEDED."""
        op = self.operation
        if not op:
            return False

        return op.status is OperationStatus.SUCCEEDED

    def is_failed(self) -> bool:
        """Return True if the checkpointed operation is FAILED."""
        op = self.operation
        if not op:
            return False

        return op.status is OperationStatus.FAILED

    def is_started(self) -> bool:
        """Return True if the checkpointed operation is STARTED."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.STARTED

    def is_started_or_ready(self) -> bool:
        """Return True if the checkpointed operation is STARTED or READY."""
        op = self.operation
        if not op:
            return False
        return op.status in {OperationStatus.STARTED, OperationStatus.READY}

    def is_pending(self) -> bool:
        """Return True if the checkpointed operation is PENDING."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.PENDING

    def is_timed_out(self) -> bool:
        """Return True if the checkpointed operation is TIMED_OUT."""
        op = self.operation
        if not op:
            return False
        return op.status is OperationStatus.TIMED_OUT

    def is_replay_children(self) -> bool:
        op = self.operation
        if not op:
            return False
        return op.context_details.replay_children if op.context_details else False

    def raise_callable_error(self) -> None:
        if self.error is None:
            msg: str = "Attempted to throw exception, but no ErrorObject exists on the Checkpoint Operation."
            raise DurableExecutionsError(msg)

        raise self.error.to_callable_runtime_error()

    def get_next_attempt_timestamp(self) -> datetime.datetime | None:
        if self.operation and self.operation.step_details:
            return self.operation.step_details.next_attempt_timestamp
        return None


# shared so don't need to create an instance for each not found check
CHECKPOINT_NOT_FOUND = CheckpointedResult.create_not_found()


class ExecutionState:
    """Get, set and maintain execution state. This is mutable. Create and check checkpoints."""

    def __init__(
        self,
        durable_execution_arn: str,
        initial_checkpoint_token: str,
        operations: MutableMapping[str, Operation],
        service_client: DurableServiceClient,
    ):
        self.durable_execution_arn: str = durable_execution_arn
        self._current_checkpoint_token: str = initial_checkpoint_token
        self.operations: MutableMapping[str, Operation] = operations
        self._service_client: DurableServiceClient = service_client
        self._ordered_checkpoint_lock: OrderedLock = OrderedLock()
        self._operations_lock: Lock = Lock()

    def fetch_paginated_operations(
        self,
        initial_operations: list[Operation],
        checkpoint_token: str,
        next_marker: str | None,
    ) -> None:
        """Add initial operations and fetch all paginated operations from the Durable Functions API. This method is thread_safe.

        The checkpoint_token is passed explicitly as a parameter rather than using the instance variable to ensure thread safety.

        Args:
            initial_operations: initial operations to be added to ExecutionState
            checkpoint_token: checkpoint token used to call Durable Functions API.
            next_marker: a marker indicates that there are paginated operations.
        """
        all_operations: list[Operation] = (
            initial_operations.copy() if initial_operations else []
        )
        while next_marker:
            output: StateOutput = self._service_client.get_execution_state(
                durable_execution_arn=self.durable_execution_arn,
                checkpoint_token=checkpoint_token,
                next_marker=next_marker,
            )
            all_operations.extend(output.operations)
            next_marker = output.next_marker
        with self._operations_lock:
            self.operations.update({op.operation_id: op for op in all_operations})

    def get_checkpoint_result(self, checkpoint_id: str) -> CheckpointedResult:
        """Get checkpoint result.

        Note this does not invoke the Durable Functions API. It only checks
        against the checkpoints currently saved in ExecutionState. The current
        saved checkpoints are from InitialExecutionState as retrieved
        at the start of the current execution/replay (see execution.durable_handler),
        and from each create_checkpoint response.

        Args:
            checkpoint_id: str - id for checkpoint to retrieve.

        Returns:
            CheckpointedResult with is_succeeded True if the checkpoint exists and its
                status is SUCCEEDED. If the checkpoint exists but its status is not
                SUCCEEDED, or if the checkpoint doesn't exist, then return
                CheckpointedResult with is_succeeded=False,result=None.
        """
        # checking status are deliberately under a lighter non-serialized lock
        with self._operations_lock:
            if checkpoint := self.operations.get(checkpoint_id):
                return CheckpointedResult.create_from_operation(checkpoint)

        return CHECKPOINT_NOT_FOUND

    def create_checkpoint(
        self, operation_update: OperationUpdate | None = None
    ) -> None:
        """Create a checkpoint by persisting it to the Durable Functions API.

        This method is thread-safe. It will enqueue checkpoints in the order of
        invocation. The order is guaranteed. This means if a checkpoint fails,
        later checkpoints enqueued behind it will NOT continue and will return
        errors instead.

        This method will block until it has successfully created the checkpoint
        and updated the internal state to include the newly updated operations state.

        If you call create_checkpoint in order, A -> B -> C, C will block until
        A and B successfully creates. If A or B fails, C will never attempt to checkpoint
        and raise an OrderedLockError instead.

        Args:
            operation_update (OperationUpdate | None): the checkpoint to create.
                                                       If None, create empty checkpoint. An
                                                       empty checkpoint gets a fresh checkpoint
                                                       token and updated operations list.

        Raises:
            OrderedLockError: Current checkpoint couldn't complete because a checkpoint
                              before it in the queue failed to complete.
        """
        with self._ordered_checkpoint_lock:
            updates: list[OperationUpdate] = (
                [operation_update] if operation_update is not None else []
            )
            output: CheckpointOutput = self._service_client.checkpoint(
                durable_execution_arn=self.durable_execution_arn,
                checkpoint_token=self._current_checkpoint_token,
                updates=updates,
                client_token=None,
            )

            self._current_checkpoint_token = output.checkpoint_token
            self.fetch_paginated_operations(
                output.new_execution_state.operations,
                output.checkpoint_token,
                output.new_execution_state.next_marker,
            )
