"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.logger import LogInfo
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import deserialize, serialize
from aws_durable_execution_sdk_python.suspend import (
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.logger import Logger
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )
    from aws_durable_execution_sdk_python.waits import (
        WaitForConditionConfig,
        WaitForConditionDecision,
    )


T = TypeVar("T")

logger = logging.getLogger(__name__)


class WaitForConditionOperationExecutor(OperationExecutor[T]):
    """Executor for wait_for_condition operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.
    """

    def __init__(
        self,
        check: Callable[[T, WaitForConditionCheckContext], T],
        config: WaitForConditionConfig[T],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        context_logger: Logger,
    ):
        """Initialize the wait_for_condition executor.

        Args:
            check: The check function to evaluate the condition
            config: Configuration for the wait_for_condition operation
            state: The execution state
            operation_identifier: The operation identifier
            context_logger: Logger for the operation context
        """
        self.check = check
        self.config = config
        self.state = state
        self.operation_identifier = operation_identifier
        self.context_logger = context_logger

    def check_result_status(self) -> CheckResult[T]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            CallableRuntimeError: For FAILED operations
            SuspendExecution: For PENDING operations waiting for retry
        """
        checkpointed_result = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Check if already completed
        if checkpointed_result.is_succeeded():
            logger.debug(
                "wait_for_condition already completed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            if checkpointed_result.result is None:
                return CheckResult.create_completed(None)  # type: ignore
            result = deserialize(
                serdes=self.config.serdes,
                data=checkpointed_result.result,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )
            return CheckResult.create_completed(result)

        # Terminal failure
        if checkpointed_result.is_failed():
            checkpointed_result.raise_callable_error()

        # Pending retry
        if checkpointed_result.is_pending():
            scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
            suspend_with_optional_resume_timestamp(
                msg=f"wait_for_condition {self.operation_identifier.name or self.operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
                datetime_timestamp=scheduled_timestamp,
            )

        # Create START checkpoint if not started
        if not checkpointed_result.is_started():
            start_operation = OperationUpdate.create_wait_for_condition_start(
                identifier=self.operation_identifier,
            )
            # Checkpoint wait_for_condition START with non-blocking (is_sync=False).
            # This is purely for observability - we don't need to wait for persistence before
            # executing the check function. The START checkpoint just records that polling began.
            self.state.create_checkpoint(
                operation_update=start_operation, is_sync=False
            )
            # For async checkpoint, no immediate response possible
            # Proceed directly to execute with current checkpoint data

        # Ready to execute check function
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute check function and handle decision.

        Args:
            checkpointed_result: The checkpoint data

        Returns:
            The final state when condition is met

        Raises:
            Suspends if condition not met
            Raises error if check function fails
        """
        # Determine current state from checkpoint
        if checkpointed_result.is_started_or_ready() and checkpointed_result.result:
            try:
                current_state = deserialize(
                    serdes=self.config.serdes,
                    data=checkpointed_result.result,
                    operation_id=self.operation_identifier.operation_id,
                    durable_execution_arn=self.state.durable_execution_arn,
                )
            except Exception:
                # Default to initial state if there's an error getting checkpointed state
                logger.exception(
                    "⚠️ wait_for_condition failed to deserialize state for id: %s, name: %s. Using initial state.",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                current_state = self.config.initial_state
        else:
            current_state = self.config.initial_state

        # Get attempt number - current attempt is checkpointed attempts + 1
        # The checkpoint stores completed attempts, so the current attempt being executed is one more
        attempt: int = 1
        if checkpointed_result.operation and checkpointed_result.operation.step_details:
            attempt = checkpointed_result.operation.step_details.attempt + 1

        try:
            # Execute the check function with the injected logger
            check_context = WaitForConditionCheckContext(
                logger=self.context_logger.with_log_info(
                    LogInfo.from_operation_identifier(
                        execution_state=self.state,
                        op_id=self.operation_identifier,
                        attempt=attempt,
                    )
                )
            )

            new_state = self.check(current_state, check_context)

            # Check if condition is met with the wait strategy
            decision: WaitForConditionDecision = self.config.wait_strategy(
                new_state, attempt
            )

            serialized_state = serialize(
                serdes=self.config.serdes,
                value=new_state,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

            logger.debug(
                "wait_for_condition check completed: %s, name: %s, attempt: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
                attempt,
            )

            if not decision.should_continue:
                # Condition is met - complete successfully
                success_operation = OperationUpdate.create_wait_for_condition_succeed(
                    identifier=self.operation_identifier,
                    payload=serialized_state,
                )
                # Checkpoint SUCCEED operation with blocking (is_sync=True, default).
                # Must ensure the final state is persisted before returning to the caller.
                # This guarantees the condition result is durable and won't be re-evaluated on replay.
                self.state.create_checkpoint(operation_update=success_operation)

                logger.debug(
                    "✅ wait_for_condition completed for id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                return new_state

            # Condition not met - schedule retry
            # We enforce a minimum delay second of 1, to match model behaviour.
            delay_seconds = decision.delay_seconds
            if delay_seconds is not None and delay_seconds < 1:
                logger.warning(
                    (
                        "WaitDecision delay_seconds step for id: %s, name: %s,"
                        "is %d < 1. Setting to minimum of 1 seconds."
                    ),
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                    delay_seconds,
                )
                delay_seconds = 1

            retry_operation = OperationUpdate.create_wait_for_condition_retry(
                identifier=self.operation_identifier,
                payload=serialized_state,
                next_attempt_delay_seconds=delay_seconds,
            )

            # Checkpoint RETRY operation with blocking (is_sync=True, default).
            # Must ensure the current state and next attempt timestamp are persisted before suspending.
            # This guarantees the polling state is durable and will resume correctly on the next invocation.
            self.state.create_checkpoint(operation_update=retry_operation)

            suspend_with_optional_resume_delay(
                msg=f"wait_for_condition {self.operation_identifier.name or self.operation_identifier.operation_id} will retry in {decision.delay_seconds} seconds",
                delay_seconds=decision.delay_seconds,
            )

        except Exception as e:
            # Mark as failed - waitForCondition doesn't have its own retry logic for errors
            # If the check function throws, it's considered a failure
            logger.exception(
                "❌ wait_for_condition failed for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )

            fail_operation = OperationUpdate.create_wait_for_condition_fail(
                identifier=self.operation_identifier,
                error=ErrorObject.from_exception(e),
            )
            # Checkpoint FAIL operation with blocking (is_sync=True, default).
            # Must ensure the failure state is persisted before raising the exception.
            # This guarantees the error is durable and the condition won't be re-evaluated on replay.
            self.state.create_checkpoint(operation_update=fail_operation)
            raise

        msg: str = (
            "wait_for_condition should never reach this point"  # pragma: no cover
        )
        raise ExecutionError(msg)  # pragma: no cover
