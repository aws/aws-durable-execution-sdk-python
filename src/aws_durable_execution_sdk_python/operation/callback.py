"""Implementation for the Durable create_callback and wait_for_callback operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.exceptions import CallbackError
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackOptions,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.types import WaitForCallbackContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.config import (
        CallbackConfig,
        WaitForCallbackConfig,
    )
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )
    from aws_durable_execution_sdk_python.types import (
        Callback,
        DurableContext,
        StepContext,
    )


class CallbackOperationExecutor(OperationExecutor[str]):
    """Executor for callback operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.

    Unlike other operations, callbacks NEVER execute logic - they only create
    checkpoints and return callback IDs.

    CRITICAL: Errors are deferred to Callback.result() for deterministic replay.
    create_callback() always returns the callback_id, even for FAILED callbacks.
    """

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: CallbackConfig | None,
    ):
        """Initialize the callback operation executor.

        Args:
            state: The execution state
            operation_identifier: The operation identifier
            config: The callback configuration (optional)
        """
        self.state = state
        self.operation_identifier = operation_identifier
        self.config = config

    def check_result_status(self) -> CheckResult[str]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        CRITICAL: This method does NOT raise on FAILED status. Errors are deferred
        to Callback.result() to ensure deterministic replay. Code between
        create_callback() and callback.result() must always execute.

        Returns:
            CheckResult.create_is_ready_to_execute() for any existing status (including FAILED)
            or CheckResult.create_started() after creating checkpoint

        Raises:
            CallbackError: If callback_details are missing from checkpoint
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # CRITICAL: Do NOT raise on FAILED - defer error to Callback.result()
        # If checkpoint exists (any status including FAILED), return ready to execute
        # The execute() method will extract the callback_id
        if checkpointed_result.is_existent():
            if (
                not checkpointed_result.operation
                or not checkpointed_result.operation.callback_details
            ):
                msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
                raise CallbackError(msg)

            return CheckResult.create_is_ready_to_execute(checkpointed_result)

        # Create START checkpoint
        callback_options: CallbackOptions = (
            CallbackOptions(
                timeout_seconds=self.config.timeout_seconds,
                heartbeat_timeout_seconds=self.config.heartbeat_timeout_seconds,
            )
            if self.config
            else CallbackOptions()
        )

        create_callback_operation: OperationUpdate = OperationUpdate.create_callback(
            identifier=self.operation_identifier,
            callback_options=callback_options,
        )

        # Checkpoint callback START with blocking (is_sync=True, default).
        # Must wait for the API to generate and return the callback ID before proceeding.
        # The callback ID is needed immediately by the caller to pass to external systems.
        self.state.create_checkpoint(operation_update=create_callback_operation)

        # Signal to process() to check status again for immediate response
        return CheckResult.create_started()

    def execute(self, checkpointed_result: CheckpointedResult) -> str:
        """Execute callback operation by extracting the callback_id.

        Callbacks don't execute logic - they just extract and return the callback_id
        from the checkpoint data.

        Args:
            checkpointed_result: The checkpoint data containing callback_details

        Returns:
            The callback_id from the checkpoint

        Raises:
            CallbackError: If callback_details are missing (should never happen)
        """
        if (
            not checkpointed_result.operation
            or not checkpointed_result.operation.callback_details
        ):
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)

        return checkpointed_result.operation.callback_details.callback_id


def wait_for_callback_handler(
    context: DurableContext,
    submitter: Callable[[str, WaitForCallbackContext], None],
    name: str | None = None,
    config: WaitForCallbackConfig | None = None,
) -> Any:
    """Wait for a callback to be invoked by an external system.

    This is a helper function that is used to create a callback and wait for it to be invoked by an external system.
    """
    name_with_space: str = f"{name} " if name else ""
    callback: Callback = context.create_callback(
        name=f"{name_with_space}create callback id", config=config
    )

    def submitter_step(step_context: StepContext):
        return submitter(
            callback.callback_id, WaitForCallbackContext(logger=step_context.logger)
        )

    step_config = (
        StepConfig(
            retry_strategy=config.retry_strategy,
            serdes=config.serdes,
        )
        if config
        else None
    )
    context.step(
        func=submitter_step, name=f"{name_with_space}submitter", config=step_config
    )

    return callback.result()
