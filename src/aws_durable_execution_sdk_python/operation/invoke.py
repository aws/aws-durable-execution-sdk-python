"""Implement the Durable invoke operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.exceptions import ExecutionError
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    OperationUpdate,
)

# Import base classes for operation executor pattern
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import (
    DEFAULT_JSON_SERDES,
    deserialize,
    serialize,
)
from aws_durable_execution_sdk_python.suspend import suspend_with_optional_resume_delay

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.config import InvokeConfig
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type

logger = logging.getLogger(__name__)


class InvokeOperationExecutor(OperationExecutor[R]):
    """Executor for invoke operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.

    The invoke operation never actually "executes" in the traditional sense -
    it always suspends to wait for the async invocation to complete.
    """

    def __init__(
        self,
        function_name: str,
        payload: P,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: InvokeConfig[P, R],
    ):
        """Initialize the invoke operation executor.

        Args:
            function_name: Name of the function to invoke
            payload: The payload to pass to the invoked function
            state: The execution state
            operation_identifier: The operation identifier
            config: Configuration for the invoke operation
        """
        self.function_name = function_name
        self.payload = payload
        self.state = state
        self.operation_identifier = operation_identifier
        self.payload = payload
        self.config = config

    def check_result_status(self) -> CheckResult[R]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            CallableRuntimeError: For FAILED, TIMED_OUT, or STOPPED operations
            SuspendExecution: For STARTED operations waiting for completion
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Terminal success - deserialize and return
        if checkpointed_result.is_succeeded():
            if checkpointed_result.result is None:
                return CheckResult.create_completed(None)  # type: ignore

            result: R = deserialize(
                serdes=self.config.serdes_result or DEFAULT_JSON_SERDES,
                data=checkpointed_result.result,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )
            return CheckResult.create_completed(result)

        # Terminal failures
        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            checkpointed_result.raise_callable_error()

        # Still running - ready to suspend
        if checkpointed_result.is_started():
            logger.debug(
                "⏳ Invoke %s still in progress, will suspend",
                self.operation_identifier.name or self.function_name,
            )
            return CheckResult.create_is_ready_to_execute(checkpointed_result)

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent():
            serialized_payload: str = serialize(
                serdes=self.config.serdes_payload or DEFAULT_JSON_SERDES,
                value=self.payload,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )
            start_operation: OperationUpdate = OperationUpdate.create_invoke_start(
                identifier=self.operation_identifier,
                payload=serialized_payload,
                chained_invoke_options=ChainedInvokeOptions(
                    function_name=self.function_name,
                    tenant_id=self.config.tenant_id,
                ),
            )
            # Checkpoint invoke START with blocking (is_sync=True).
            # Must ensure the chained invocation is recorded before suspending execution.
            self.state.create_checkpoint(operation_update=start_operation, is_sync=True)

            logger.debug(
                "🚀 Invoke %s started, will check for immediate response",
                self.operation_identifier.name or self.function_name,
            )

            # Signal to process() that checkpoint was created - to recheck status for permissions errs etc.
            # before proceeding.
            return CheckResult.create_started()

        # Ready to suspend (checkpoint exists but not in a terminal or started state)
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    def execute(self, _checkpointed_result: CheckpointedResult) -> R:
        """Execute invoke operation by suspending to wait for async completion.

        The invoke operation doesn't execute synchronously - it suspends and
        the backend executes the invoked function asynchronously.

        Args:
            checkpointed_result: The checkpoint data (unused, but required by interface)

        Returns:
            Never returns - always suspends

        Raises:
            Always suspends via suspend_with_optional_resume_delay
            ExecutionError: If suspend doesn't raise (should never happen)
        """
        msg: str = f"Invoke {self.operation_identifier.operation_id} started, suspending for completion"
        suspend_with_optional_resume_delay(msg, self.config.timeout_seconds)
        # This line should never be reached since suspend_with_optional_resume_delay always raises
        error_msg: str = "suspend_with_optional_resume_delay should have raised an exception, but did not."
        raise ExecutionError(error_msg) from None
