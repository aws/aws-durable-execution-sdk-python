"""Main checkpoint processor that orchestrates operation transformations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationUpdate,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (
    OperationTransformer,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.checkpoint import (
    CheckpointValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier
from aws_durable_execution_sdk_python_testing.token import CheckpointToken


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore


class CheckpointProcessor:
    """Handle OperationUpdate transformations and execution state updates."""

    def __init__(self, store: ExecutionStore, scheduler: Scheduler):
        self._store = store
        self._scheduler = scheduler
        self._notifier = ExecutionNotifier()
        self._transformer = OperationTransformer()

    def add_execution_observer(self, observer) -> None:
        """Add observer for execution events."""
        self._notifier.add_observer(observer)

    def process_checkpoint(
        self,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,  # noqa: ARG002
    ) -> CheckpointOutput:
        """Process checkpoint updates and return result with updated execution state."""
        # 1. Get current execution state
        token: CheckpointToken = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._store.load(token.execution_arn)

        # 2. Validate checkpoint token
        if execution.is_complete or token.token_sequence != execution.token_sequence:
            msg: str = "Invalid checkpoint token"

            raise InvalidParameterValueException(msg)

        # 3. Validate all updates, state transitions are valid, sizes etc.
        CheckpointValidator.validate_input(updates, execution)

        # 4. Transform OperationUpdate -> Operation and schedule future replays
        updated_operations, all_updates = self._transformer.process_updates(
            updates=updates,
            current_operations=execution.operations,
            notifier=self._notifier,
            execution_arn=token.execution_arn,
        )

        # 5. Generate a new checkpoint token and save updated operations
        new_checkpoint_token = execution.get_new_checkpoint_token()
        execution.operations = updated_operations
        execution.updates.extend(all_updates)
        self._store.update(execution)

        # 6. Return checkpoint result
        return CheckpointOutput(
            checkpoint_token=new_checkpoint_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=execution.get_navigable_operations(), next_marker=None
            ),
        )

    def get_execution_state(
        self,
        checkpoint_token: str,
        next_marker: str,  # noqa: ARG002
        max_items: int = 1000,  # noqa: ARG002
    ) -> StateOutput:
        """Get current execution state."""
        token: CheckpointToken = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._store.load(token.execution_arn)

        # TODO: paging when size or max
        return StateOutput(
            operations=execution.get_navigable_operations(), next_marker=None
        )
