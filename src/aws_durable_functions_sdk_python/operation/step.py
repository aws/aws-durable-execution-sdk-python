"""Implement the Durable step operation."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, TypeVar

from aws_durable_functions_sdk_python.config import (
    RetryDecision,
    StepConfig,
    StepSemantics,
)
from aws_durable_functions_sdk_python.exceptions import (
    FatalError,
    StepInterruptedError,
    TimedSuspendExecution,
)
from aws_durable_functions_sdk_python.lambda_service import ErrorObject, OperationUpdate
from aws_durable_functions_sdk_python.logger import Logger, LogInfo
from aws_durable_functions_sdk_python.retries import RetryPresets
from aws_durable_functions_sdk_python.serdes import deserialize, serialize
from aws_durable_functions_sdk_python.types import StepContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_functions_sdk_python.identifier import OperationIdentifier
    from aws_durable_functions_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")


def step_handler(
    func: Callable[[StepContext], T],
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: StepConfig | None,
    context_logger: Logger,
) -> T:
    logger.debug(
        "▶️ Executing step for id: %s, name: %s",
        operation_identifier.operation_id,
        operation_identifier.name,
    )

    if not config:
        config = StepConfig()

    checkpointed_result = state.get_checkpoint_result(operation_identifier.operation_id)
    if checkpointed_result.is_succeeded():
        logger.debug(
            "Step already completed, skipping execution for id: %s, name: %s",
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
        # have to throw the exact same error on replay as the checkpointed failure
        checkpointed_result.raise_callable_error()

    if checkpointed_result.is_started():
        # step was previously interrupted
        if config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY:
            msg = f"Step operation_id={operation_identifier.operation_id} name={operation_identifier.name} was previously interrupted"
            retry_handler(
                StepInterruptedError(msg),
                state,
                operation_identifier,
                config,
                checkpointed_result,
            )

        checkpointed_result.raise_callable_error()

    if config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY:
        # At least once needs checkpoint at the start
        start_operation: OperationUpdate = OperationUpdate.create_step_start(
            identifier=operation_identifier,
        )

        state.create_checkpoint(operation_update=start_operation)

    attempt: int = 0
    if checkpointed_result.operation and checkpointed_result.operation.step_details:
        attempt = checkpointed_result.operation.step_details.attempt

    step_context = StepContext(
        logger=context_logger.with_log_info(
            LogInfo.from_operation_identifier(
                execution_arn=state.durable_execution_arn,
                op_id=operation_identifier,
                attempt=attempt,
            )
        )
    )
    try:
        # this is the actual code provided by the caller to execute durably inside the step
        raw_result: T = func(step_context)
        serialized_result: str = serialize(
            serdes=config.serdes,
            value=raw_result,
            operation_id=operation_identifier.operation_id,
            durable_execution_arn=state.durable_execution_arn,
        )

        success_operation: OperationUpdate = OperationUpdate.create_step_succeed(
            identifier=operation_identifier,
            payload=serialized_result,
        )

        state.create_checkpoint(operation_update=success_operation)

        logger.debug(
            "✅ Successfully completed step for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        return raw_result  # noqa: TRY300
    except Exception as e:
        if isinstance(e, FatalError):
            # no retry on fatal - e.g checkpoint exception
            logger.debug(
                "💥 Fatal error for id: %s, name: %s",
                operation_identifier.operation_id,
                operation_identifier.name,
            )
            # this bubbles up to execution.durable_handler, where it will exit with PENDING. TODO: confirm if still correct
            raise

        logger.exception(
            "❌ failed step for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )

        retry_handler(e, state, operation_identifier, config, checkpointed_result)
        msg = "retry handler should have raised an exception, but did not."
        raise FatalError(msg) from None


# TODO: I don't much like this func, needs refactor. Messy grab-bag of args, refine.
def retry_handler(
    error: Exception,
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: StepConfig,
    checkpointed_result: CheckpointedResult,
):
    """Checkpoint and suspend for replay if retry required, otherwise raise error."""
    error_object = ErrorObject.from_exception(error)

    retry_strategy = (
        config.retry_strategy if config.retry_strategy else RetryPresets.default()
    )

    retry_attempt: int = (
        checkpointed_result.operation.step_details.attempt
        if (
            checkpointed_result.operation and checkpointed_result.operation.step_details
        )
        else 0
    )
    retry_decision: RetryDecision = retry_strategy(error, retry_attempt + 1)

    if retry_decision.should_retry:
        logger.debug(
            "Retrying step for id: %s, name: %s, attempt: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
            retry_attempt + 1,
        )

        retry_operation: OperationUpdate = OperationUpdate.create_step_retry(
            identifier=operation_identifier,
            error=error_object,
            next_attempt_delay_seconds=retry_decision.delay_seconds,
        )

        state.create_checkpoint(operation_update=retry_operation)

        _suspend(operation_identifier, retry_decision)

    # no retry
    fail_operation: OperationUpdate = OperationUpdate.create_step_fail(
        identifier=operation_identifier, error=error_object
    )

    state.create_checkpoint(operation_update=fail_operation)

    if isinstance(error, StepInterruptedError):
        raise error

    raise error_object.to_callable_runtime_error()


def _suspend(operation_identifier: OperationIdentifier, retry_decision: RetryDecision):
    scheduled_timestamp = time.time() + retry_decision.delay_seconds
    msg = f"Retry scheduled for {operation_identifier.operation_id} in {retry_decision.delay_seconds} seconds"
    raise TimedSuspendExecution(
        msg,
        scheduled_timestamp=scheduled_timestamp,
    )
