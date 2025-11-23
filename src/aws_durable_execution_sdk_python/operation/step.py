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
    StepInterruptedError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.logger import Logger, LogInfo
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

    checkpointed_result: CheckpointedResult = state.get_checkpoint_result(
        operation_identifier.operation_id
    )
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

    if checkpointed_result.is_pending():
        scheduled_timestamp = checkpointed_result.get_next_attempt_timestamp()
        # normally, we'd ensure that a suspension here would be for > 0 seconds;
        # however, this is coming from a checkpoint, and we can trust that it is a correct target timestamp.
        suspend_with_optional_resume_timestamp(
            msg=f"Retry scheduled for {operation_identifier.name or operation_identifier.operation_id} will retry at timestamp {scheduled_timestamp}",
            datetime_timestamp=scheduled_timestamp,
        )

    if (
        checkpointed_result.is_started()
        and config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
    ):
        # step was previously interrupted
        msg = f"Step operation_id={operation_identifier.operation_id} name={operation_identifier.name} was previously interrupted"
        retry_handler(
            StepInterruptedError(msg),
            state,
            operation_identifier,
            config,
            checkpointed_result,
        )

        checkpointed_result.raise_callable_error()

    if not (
        checkpointed_result.is_started()
        and config.step_semantics is StepSemantics.AT_LEAST_ONCE_PER_RETRY
    ):
        # Do not checkpoint start for started & AT_LEAST_ONCE execution
        # Checkpoint start for the other
        start_operation: OperationUpdate = OperationUpdate.create_step_start(
            identifier=operation_identifier,
        )
        # Checkpoint START operation with appropriate synchronization:
        # - AtMostOncePerRetry: Use blocking checkpoint (is_sync=True) to prevent duplicate execution.
        #   The step must not execute until the START checkpoint is persisted, ensuring exactly-once semantics.
        # - AtLeastOncePerRetry: Use non-blocking checkpoint (is_sync=False) for performance optimization.
        #   The step can execute immediately without waiting for checkpoint persistence, allowing at-least-once semantics.
        is_sync: bool = config.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        state.create_checkpoint(operation_update=start_operation, is_sync=is_sync)

    attempt: int = 0
    if checkpointed_result.operation and checkpointed_result.operation.step_details:
        attempt = checkpointed_result.operation.step_details.attempt

    step_context = StepContext(
        logger=context_logger.with_log_info(
            LogInfo.from_operation_identifier(
                execution_state=state,
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

        # Checkpoint SUCCEED operation with blocking (is_sync=True, default).
        # Must ensure the success state is persisted before returning the result to the caller.
        # This guarantees the step result is durable and won't be lost if Lambda terminates.
        state.create_checkpoint(operation_update=success_operation)

        logger.debug(
            "✅ Successfully completed step for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        return raw_result  # noqa: TRY300
    except Exception as e:
        if isinstance(e, ExecutionError):
            # no retry on fatal - e.g checkpoint exception
            logger.debug(
                "💥 Fatal error for id: %s, name: %s",
                operation_identifier.operation_id,
                operation_identifier.name,
            )
            # this bubbles up to execution.durable_execution, where it will exit with FAILED
            raise

        logger.exception(
            "❌ failed step for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )

        retry_handler(e, state, operation_identifier, config, checkpointed_result)
        # if we've failed to raise an exception from the retry_handler, then we are in a
        # weird state, and should crash terminate the execution
        msg = "retry handler should have raised an exception, but did not."
        raise ExecutionError(msg) from None


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

    retry_strategy = config.retry_strategy or RetryPresets.default()

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
                operation_identifier.operation_id,
                operation_identifier.name,
                retry_attempt + 1,
                delay_seconds,
            )
            delay_seconds = 1

        retry_operation: OperationUpdate = OperationUpdate.create_step_retry(
            identifier=operation_identifier,
            error=error_object,
            next_attempt_delay_seconds=delay_seconds,
        )

        # Checkpoint RETRY operation with blocking (is_sync=True, default).
        # Must ensure retry state is persisted before suspending execution.
        # This guarantees the retry attempt count and next attempt timestamp are durable.
        state.create_checkpoint(operation_update=retry_operation)

        suspend_with_optional_resume_delay(
            msg=(
                f"Retry scheduled for {operation_identifier.operation_id}"
                f"in {retry_decision.delay_seconds} seconds"
            ),
            delay_seconds=delay_seconds,
        )

    # no retry
    fail_operation: OperationUpdate = OperationUpdate.create_step_fail(
        identifier=operation_identifier, error=error_object
    )

    # Checkpoint FAIL operation with blocking (is_sync=True, default).
    # Must ensure the failure state is persisted before raising the exception.
    # This guarantees the error is durable and the step won't be retried on replay.
    state.create_checkpoint(operation_update=fail_operation)

    if isinstance(error, StepInterruptedError):
        raise error

    raise error_object.to_callable_runtime_error()
