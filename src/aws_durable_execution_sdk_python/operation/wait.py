"""Implement the durable wait operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import OperationUpdate, WaitOptions
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.suspend import suspend_with_optional_resume_delay

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)


class WaitOperationExecutor(OperationExecutor[None]):
    """Executor for wait operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.
    """

    def __init__(
        self,
        seconds: int,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ):
        """Initialize the wait operation executor.

        Args:
            seconds: Number of seconds to wait
            state: The execution state
            operation_identifier: The operation identifier
        """
        self.seconds = seconds
        self.state = state
        self.operation_identifier = operation_identifier

    def check_result_status(self) -> CheckResult[None]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            SuspendExecution: When wait timer has not completed
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Terminal success - wait completed
        if checkpointed_result.is_succeeded():
            logger.debug(
                "Wait already completed, skipping wait for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return CheckResult.create_completed(None)

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent():
            operation: OperationUpdate = OperationUpdate.create_wait_start(
                identifier=self.operation_identifier,
                wait_options=WaitOptions(wait_seconds=self.seconds),
            )
            # Checkpoint wait START with blocking (is_sync=True, default).
            # Must ensure the wait operation and scheduled timestamp are persisted before suspending.
            # This guarantees the wait will resume at the correct time on the next invocation.
            self.state.create_checkpoint(operation_update=operation, is_sync=True)

            logger.debug(
                "Wait checkpoint created for id: %s, name: %s, will check for immediate response",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            # Signal to process() that checkpoint was created - which will re-run this check_result_status
            # check from the top
            return CheckResult.create_started()

        # Ready to suspend (checkpoint exists)
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    def execute(self, _checkpointed_result: CheckpointedResult) -> None:
        """Execute wait by suspending.

        Wait operations 'execute' by suspending execution until the timer completes.
        This method never returns normally - it always suspends.

        Args:
            _checkpointed_result: The checkpoint data (unused for wait)

        Raises:
            SuspendExecution: Always suspends to wait for timer completion
        """
        msg: str = f"Wait for {self.seconds} seconds"
        suspend_with_optional_resume_delay(msg, self.seconds)  # throws suspend
