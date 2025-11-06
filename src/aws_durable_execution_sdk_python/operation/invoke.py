"""Implement the Durable invoke operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.exceptions import ExecutionError
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.serdes import deserialize, serialize
from aws_durable_execution_sdk_python.suspend import suspend_with_optional_resume_delay

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import ExecutionState

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type

logger = logging.getLogger(__name__)


def invoke_handler(
    function_name: str,
    payload: P,
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: InvokeConfig[P, R] | None,
) -> R:
    """Invoke another Durable Function."""
    logger.debug(
        "🔗 Invoke %s (%s)",
        operation_identifier.name or function_name,
        operation_identifier.operation_id,
    )

    if not config:
        config = InvokeConfig[P, R]()

    # Check if we have existing step data
    checkpointed_result = state.get_checkpoint_result(operation_identifier.operation_id)

    if checkpointed_result.is_succeeded():
        # Return persisted result - no need to check for errors in successful operations
        if (
            checkpointed_result.operation
            and checkpointed_result.operation.chained_invoke_details
            and checkpointed_result.operation.chained_invoke_details.result
        ):
            return deserialize(
                serdes=config.serdes_result,
                data=checkpointed_result.operation.chained_invoke_details.result,
                operation_id=operation_identifier.operation_id,
                durable_execution_arn=state.durable_execution_arn,
            )
        return None  # type: ignore

    if (
        checkpointed_result.is_failed()
        or checkpointed_result.is_timed_out()
        or checkpointed_result.is_stopped()
    ):
        # Operation failed, throw the exact same error on replay as the checkpointed failure
        checkpointed_result.raise_callable_error()

    if checkpointed_result.is_started():
        # Operation is still running, suspend until completion
        logger.debug(
            "⏳ Invoke %s still in progress, suspending",
            operation_identifier.name or function_name,
        )
        msg = f"Invoke {operation_identifier.operation_id} still in progress"
        suspend_with_optional_resume_delay(msg, config.timeout_seconds)

    serialized_payload: str = serialize(
        serdes=config.serdes_payload,
        value=payload,
        operation_id=operation_identifier.operation_id,
        durable_execution_arn=state.durable_execution_arn,
    )

    # the backend will do the invoke once it gets this checkpoint
    start_operation: OperationUpdate = OperationUpdate.create_invoke_start(
        identifier=operation_identifier,
        payload=serialized_payload,
        chained_invoke_options=ChainedInvokeOptions(function_name=function_name),
    )

    # Checkpoint invoke START with blocking (is_sync=True, default).
    # Must ensure the chained invocation is recorded before suspending execution.
    # This guarantees the invoke operation is durable and will be tracked by the backend.
    state.create_checkpoint(operation_update=start_operation)

    logger.debug(
        "🚀 Invoke %s started, suspending for async execution",
        operation_identifier.name or function_name,
    )

    # Suspend so invoke executes asynchronously without consuming cpu here
    msg = (
        f"Invoke {operation_identifier.operation_id} started, suspending for completion"
    )
    suspend_with_optional_resume_delay(msg, config.timeout_seconds)
    # This line should never be reached since suspend_with_optional_resume_delay always raises
    # if it is ever reached, we will crash in a non-retryable manner via ExecutionError
    msg = "suspend_with_optional_resume_delay should have raised an exception, but did not."
    raise ExecutionError(msg) from None
