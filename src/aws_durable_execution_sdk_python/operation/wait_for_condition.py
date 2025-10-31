"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

import aws_durable_execution_sdk_python.logger as logging
from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.logger import LogInfo
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


def wait_for_condition_handler(
    check: Callable[[T, WaitForConditionCheckContext], T],
    config: WaitForConditionConfig[T],
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    context_logger: Logger,
) -> T:
    """Handle wait_for_condition operation.

    wait_for_condition creates a STEP checkpoint.
    """
    logger.debug(
        "▶️ Executing wait_for_condition for id: %s, name: %s",
        operation_identifier.operation_id,
        operation_identifier.name,
    )

    checkpointed_result: CheckpointedResult = state.get_checkpoint_result(
        operation_identifier.operation_id
    )

    # Check if already completed
    if checkpointed_result.is_succeeded():
        logger.debug(
            "wait_for_condition already completed for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        if checkpointed_result.result is None:
            return None  # type: ignore
        return deserialize(
            serdes=config.serdes,
            data=checkpointed_result.result,
            operation_id=operation_identifier.operation_id,
            durable_execution_arn=state.durable_execution_arn,
        )

    if checkpointed_result.is_failed():
        checkpointed_result.raise_callable_error()

    if checkpointed_result.is_pending():
        scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
        suspend_with_optional_resume_timestamp(
            msg=f"wait_for_condition {operation_identifier.name or operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
            datetime_timestamp=scheduled_timestamp,
        )

    attempt: int = 1
    if checkpointed_result.is_started_or_ready():
        # This is a retry - get state from previous checkpoint
        if checkpointed_result.result:
            try:
                current_state = deserialize(
                    serdes=config.serdes,
                    data=checkpointed_result.result,
                    operation_id=operation_identifier.operation_id,
                    durable_execution_arn=state.durable_execution_arn,
                )
            except Exception:
                # default to initial state if there's an error getting checkpointed state
                logger.exception(
                    "⚠️ wait_for_condition failed to deserialize state for id: %s, name: %s. Using initial state.",
                    operation_identifier.operation_id,
                    operation_identifier.name,
                )
                current_state = config.initial_state
        else:
            current_state = config.initial_state

        # at this point operation has to exist. Nonetheless, just in case somehow it's not there.
        if checkpointed_result.operation and checkpointed_result.operation.step_details:
            attempt = checkpointed_result.operation.step_details.attempt
    else:
        # First execution
        current_state = config.initial_state

    # Checkpoint START for observability.
    if not checkpointed_result.is_started():
        start_operation: OperationUpdate = (
            OperationUpdate.create_wait_for_condition_start(
                identifier=operation_identifier,
            )
        )
        # Checkpoint wait_for_condition START with non-blocking (is_sync=False).
        # This is purely for observability - we don't need to wait for persistence before
        # executing the check function. The START checkpoint just records that polling began.
        state.create_checkpoint(operation_update=start_operation, is_sync=False)

    try:
        # Execute the check function with the injected logger
        check_context = WaitForConditionCheckContext(
            logger=context_logger.with_log_info(
                LogInfo.from_operation_identifier(
                    execution_arn=state.durable_execution_arn,
                    op_id=operation_identifier,
                    attempt=attempt,
                )
            )
        )

        new_state = check(current_state, check_context)

        # Check if condition is met with the wait strategy
        decision: WaitForConditionDecision = config.wait_strategy(new_state, attempt)

        serialized_state = serialize(
            serdes=config.serdes,
            value=new_state,
            operation_id=operation_identifier.operation_id,
            durable_execution_arn=state.durable_execution_arn,
        )

        logger.debug(
            "wait_for_condition check completed: %s, name: %s, attempt: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
            attempt,
        )

        if not decision.should_continue:
            # Condition is met - complete successfully
            success_operation = OperationUpdate.create_wait_for_condition_succeed(
                identifier=operation_identifier,
                payload=serialized_state,
            )
            # Checkpoint SUCCEED operation with blocking (is_sync=True, default).
            # Must ensure the final state is persisted before returning to the caller.
            # This guarantees the condition result is durable and won't be re-evaluated on replay.
            state.create_checkpoint(operation_update=success_operation)

            logger.debug(
                "✅ wait_for_condition completed for id: %s, name: %s",
                operation_identifier.operation_id,
                operation_identifier.name,
            )
            return new_state

        # Condition not met - schedule retry
        # we enforce a minimum delay second of 1, to match model behaviour.
        # we localize enforcement and keep it outside suspension methods as:
        # a) those are used throughout the codebase, e.g. in wait(..) <- enforcement is done in context
        # b) they shouldn't know model specific details <- enforcement is done above
        # and c) this "issue" arises from retry-decision and shouldn't be chased deeper.
        delay_seconds = decision.delay_seconds
        if delay_seconds is not None and delay_seconds < 1:
            logger.warning(
                (
                    "WaitDecision delay_seconds step for id: %s, name: %s,"
                    "is %d < 1. Setting to minimum of 1 seconds."
                ),
                operation_identifier.operation_id,
                operation_identifier.name,
                delay_seconds,
            )
            delay_seconds = 1

        retry_operation = OperationUpdate.create_wait_for_condition_retry(
            identifier=operation_identifier,
            payload=serialized_state,
            next_attempt_delay_seconds=delay_seconds,
        )

        # Checkpoint RETRY operation with blocking (is_sync=True, default).
        # Must ensure the current state and next attempt timestamp are persisted before suspending.
        # This guarantees the polling state is durable and will resume correctly on the next invocation.
        state.create_checkpoint(operation_update=retry_operation)

        suspend_with_optional_resume_delay(
            msg=f"wait_for_condition {operation_identifier.name or operation_identifier.operation_id} will retry in {decision.delay_seconds} seconds",
            delay_seconds=decision.delay_seconds,
        )

    except Exception as e:
        # Mark as failed - waitForCondition doesn't have its own retry logic for errors
        # If the check function throws, it's considered a failure
        logger.exception(
            "❌ wait_for_condition failed for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )

        fail_operation = OperationUpdate.create_wait_for_condition_fail(
            identifier=operation_identifier,
            error=ErrorObject.from_exception(e),
        )
        # Checkpoint FAIL operation with blocking (is_sync=True, default).
        # Must ensure the failure state is persisted before raising the exception.
        # This guarantees the error is durable and the condition won't be re-evaluated on replay.
        state.create_checkpoint(operation_update=fail_operation)
        raise

    msg: str = "wait_for_condition should never reach this point"  # pragma: no cover
    raise ExecutionError(msg)  # pragma: no cover
