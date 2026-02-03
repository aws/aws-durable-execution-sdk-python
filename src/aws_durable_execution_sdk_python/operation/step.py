"""Implement the Durable step operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.config import (
    StepConfig,
    StepSemantics,
)
from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
    InvalidStateError,
    StepInterruptedError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.logger import Logger, LogInfo
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.retries import RetryDecision, RetryPresets
from aws_durable_execution_sdk_python.serdes import deserialize, serialize
from aws_durable_execution_sdk_python.suspend import (
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)
from aws_durable_execution_sdk_python.types import StepContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StepOperationExecutor(OperationExecutor[T]):
    """Executor for step operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.
    """

    def __init__(
        self,
        func: Callable[[StepContext], T],
        config: StepConfig,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        context_logger: Logger,
    ):
        """Initialize the step operation executor.

        Args:
            func: The step function to execute
            config: The step configuration
            state: The execution state
            operation_identifier: The operation identifier
            context_logger: The logger for the step context
        """
        self.func = func
        self.config = config
        self.state = state
        self.operation_identifier = operation_identifier
        self.context_logger = context_logger
        self._checkpoint_created = False  # Track if we created the checkpoint

    def check_result_status(self) -> CheckResult[T]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            CallableRuntimeError: For FAILED operations
            StepInterruptedError: For interrupted AT_MOST_ONCE operations
            SuspendExecution: For PENDING operations waiting for retry
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Terminal success - deserialize and return
        if checkpointed_result.is_succeeded():
            logger.debug(
                "Step already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            if checkpointed_result.result is None:
                return CheckResult.create_completed(None)  # type: ignore

            result: T = deserialize(
                serdes=self.config.serdes,
                data=checkpointed_result.result,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )
            return CheckResult.create_completed(result)

        # Terminal failure
        if checkpointed_result.is_failed():
            # Have to throw the exact same error on replay as the checkpointed failure
            checkpointed_result.raise_callable_error()

        # Pending retry
        if checkpointed_result.is_pending():
            scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
            # Normally, we'd ensure that a suspension here would be for > 0 seconds;
            # however, this is coming from a checkpoint, and we can trust that it is a correct target timestamp.
            suspend_with_optional_resume_timestamp(
                msg=f"Retry scheduled for {self.operation_identifier.name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        # Handle interrupted AT_MOST_ONCE (replay scenario only)
        # This check only applies on REPLAY when a new Lambda invocation starts after interruption.
        # A STARTED checkpoint with AT_MOST_ONCE on entry means the previous invocation
        # was interrupted and it should NOT re-execute.
        #
        # This check is skipped on fresh executions because:
        #   - First call (fresh): checkpoint doesn't exist → is_started() returns False → skip this check
        #   - After creating sync checkpoint and refreshing: if status is STARTED, we return
        #     ready_to_execute directly, so process() never calls check_result_status() again
        if (
            checkpointed_result.is_started()
            and self.config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        ):
            # Step was previously interrupted in a prior invocation - handle retry
            msg: str = f"Step operation_id={self.operation_identifier.operation_id} name={self.operation_identifier.name} was previously interrupted"
            self.retry_handler(StepInterruptedError(msg), checkpointed_result)
            checkpointed_result.raise_callable_error()

        # Ready to execute if STARTED + AT_LEAST_ONCE
        if (
            checkpointed_result.is_started()
            and self.config.step_semantics is StepSemantics.AT_LEAST_ONCE_PER_RETRY
        ):
            return CheckResult.create_is_ready_to_execute(checkpointed_result)

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent():
            start_operation: OperationUpdate = OperationUpdate.create_step_start(
                identifier=self.operation_identifier,
            )
            # Checkpoint START operation with appropriate synchronization:
            # - AtMostOncePerRetry: Use blocking checkpoint (is_sync=True) to prevent duplicate execution.
            #   The step must not execute until the START checkpoint is persisted, ensuring exactly-once semantics.
            # - AtLeastOncePerRetry: Use non-blocking checkpoint (is_sync=False) for performance optimization.
            #   The step can execute immediately without waiting for checkpoint persistence, allowing at-least-once semantics.
            is_sync: bool = (
                self.config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
            )
            self.state.create_checkpoint(
                operation_update=start_operation, is_sync=is_sync
            )

            # After creating sync checkpoint, check the status
            if is_sync:
                # Refresh checkpoint result to check for immediate response
                refreshed_result: CheckpointedResult = self.state.get_checkpoint_result(
                    self.operation_identifier.operation_id
                )

                # START checkpoint only returns STARTED status
                # Any errors would be thrown as runtime exceptions during checkpoint creation
                if not refreshed_result.is_started():
                    # This should never happen - defensive check
                    error_msg: str = f"Unexpected status after START checkpoint: {refreshed_result.status}"
                    raise InvalidStateError(error_msg)

                # If we reach here, status must be STARTED - ready to execute
                return CheckResult.create_is_ready_to_execute(refreshed_result)

        # Ready to execute
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute step function with error handling and retry logic.

        Args:
            checkpointed_result: The checkpoint data containing operation state

        Returns:
            The result of executing the step function

        Raises:
            ExecutionError: For fatal errors that should not be retried
            May raise other exceptions that will be handled by retry_handler
        """
        # Get current attempt - checkpointed attempts + 1
        attempt: int = 1
        if checkpointed_result.operation and checkpointed_result.operation.step_details:
            attempt = checkpointed_result.operation.step_details.attempt + 1

        step_context: StepContext = StepContext(
            logger=self.context_logger.with_log_info(
                LogInfo.from_operation_identifier(
                    execution_state=self.state,
                    op_id=self.operation_identifier,
                    attempt=attempt,
                )
            )
        )

        try:
            # This is the actual code provided by the caller to execute durably inside the step
            raw_result: T = self.func(step_context)
            serialized_result: str = serialize(
                serdes=self.config.serdes,
                value=raw_result,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

            success_operation: OperationUpdate = OperationUpdate.create_step_succeed(
                identifier=self.operation_identifier,
                payload=serialized_result,
            )

            # Checkpoint SUCCEED operation with blocking (is_sync=True, default).
            # Must ensure the success state is persisted before returning the result to the caller.
            # This guarantees the step result is durable and won't be lost if Lambda terminates.
            self.state.create_checkpoint(operation_update=success_operation)

            logger.debug(
                "✅ Successfully completed step for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return raw_result  # noqa: TRY300
        except Exception as e:
            if isinstance(e, ExecutionError):
                # No retry on fatal - e.g checkpoint exception
                logger.debug(
                    "💥 Fatal error for id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                # This bubbles up to execution.durable_execution, where it will exit with FAILED
                raise

            logger.exception(
                "❌ failed step for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            self.retry_handler(e, checkpointed_result)
            # If we've failed to raise an exception from the retry_handler, then we are in a
            # weird state, and should crash terminate the execution
            msg = "retry handler should have raised an exception, but did not."
            raise ExecutionError(msg) from None

    def retry_handler(
        self,
        error: Exception,
        checkpointed_result: CheckpointedResult,
    ):
        """Checkpoint and suspend for replay if retry required, otherwise raise error.

        Args:
            error: The exception that occurred during step execution
            checkpointed_result: The checkpoint data containing operation state

        Raises:
            SuspendExecution: If retry is scheduled
            StepInterruptedError: If the error is a StepInterruptedError
            CallableRuntimeError: If retry is exhausted or error is not retryable
        """
        error_object = ErrorObject.from_exception(error)

        retry_strategy = self.config.retry_strategy or RetryPresets.default()

        retry_attempt: int = (
            checkpointed_result.operation.step_details.attempt
            if (
                checkpointed_result.operation
                and checkpointed_result.operation.step_details
            )
            else 0
        )
        retry_decision: RetryDecision = retry_strategy(error, retry_attempt + 1)

        if retry_decision.should_retry:
            logger.debug(
                "Retrying step for id: %s, name: %s, attempt: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
                retry_attempt + 1,
            )

            # because we are issuing a retry and create an OperationUpdate
            # we enforce a minimum delay second of 1, to match model behaviour.
            # we localize enforcement and keep it outside suspension methods as:
            # a) those are used throughout the codebase, e.g. in wait(..) <- enforcement is done in context
            # b) they shouldn't know model specific details <- enforcement is done above
            # and c) this "issue" arises from retry-decision and we shouldn't push it down
            delay_seconds = retry_decision.delay_seconds
            if delay_seconds < 1:
                logger.warning(
                    (
                        "Retry delay_seconds step for id: %s, name: %s,"
                        "attempt: %s is %d < 1. Setting to minimum of 1 seconds."
                    ),
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                    retry_attempt + 1,
                    delay_seconds,
                )
                delay_seconds = 1

            retry_operation: OperationUpdate = OperationUpdate.create_step_retry(
                identifier=self.operation_identifier,
                error=error_object,
                next_attempt_delay_seconds=delay_seconds,
            )

            # Checkpoint RETRY operation with blocking (is_sync=True, default).
            # Must ensure retry state is persisted before suspending execution.
            # This guarantees the retry attempt count and next attempt timestamp are durable.
            self.state.create_checkpoint(operation_update=retry_operation)

            suspend_with_optional_resume_delay(
                msg=(
                    f"Retry scheduled for {self.operation_identifier.operation_id}"
                    f"in {retry_decision.delay_seconds} seconds"
                ),
                delay_seconds=delay_seconds,
            )

        # no retry
        fail_operation: OperationUpdate = OperationUpdate.create_step_fail(
            identifier=self.operation_identifier, error=error_object
        )

        # Checkpoint FAIL operation with blocking (is_sync=True, default).
        # Must ensure the failure state is persisted before raising the exception.
        # This guarantees the error is durable and the step won't be retried on replay.
        self.state.create_checkpoint(operation_update=fail_operation)

        if isinstance(error, StepInterruptedError):
            raise error

        raise error_object.to_callable_runtime_error()
