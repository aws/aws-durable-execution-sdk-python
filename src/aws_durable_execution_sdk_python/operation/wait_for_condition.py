"""Implement the durable wait_for_condition operation."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    FatalError,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject, OperationUpdate
from aws_durable_execution_sdk_python.logger import LogInfo
from aws_durable_execution_sdk_python.serdes import deserialize, serialize
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.config import (
        WaitForConditionConfig,
        WaitForConditionDecision,
    )
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.logger import Logger
    from aws_durable_execution_sdk_python.state import ExecutionState


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

    checkpointed_result = state.get_checkpoint_result(operation_identifier.operation_id)

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
    if not checkpointed_result.is_existent():
        start_operation: OperationUpdate = (
            OperationUpdate.create_wait_for_condition_start(
                identifier=operation_identifier,
            )
        )

        state.create_checkpoint(operation_update=start_operation)

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
            state.create_checkpoint(operation_update=success_operation)

            logger.debug(
                "✅ wait_for_condition completed for id: %s, name: %s",
                operation_identifier.operation_id,
                operation_identifier.name,
            )
            return new_state

        # Condition not met - schedule retry
        retry_operation = OperationUpdate.create_wait_for_condition_retry(
            identifier=operation_identifier,
            payload=serialized_state,
            next_attempt_delay_seconds=decision.delay_seconds or 0,
        )

        state.create_checkpoint(operation_update=retry_operation)

        _suspend_execution(operation_identifier, decision)

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
        state.create_checkpoint(operation_update=fail_operation)
        raise

    msg: str = "wait_for_condition should never reach this point"
    raise FatalError(msg)


def _suspend_execution(
    operation_identifier: OperationIdentifier, decision: WaitForConditionDecision
) -> None:
    scheduled_timestamp = time.time() + (decision.delay_seconds or 0)
    msg = f"wait_for_condition {operation_identifier.name or operation_identifier.operation_id} will retry in {decision.delay_seconds} seconds"
    raise TimedSuspendExecution(
        msg,
        scheduled_timestamp=scheduled_timestamp,
    )
