"""Implementation for the Durable create_callback and wait_for_callback operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.exceptions import FatalError
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackOptions,
    OperationUpdate,
)

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
    from aws_durable_execution_sdk_python.types import Callback, DurableContext


def create_callback_handler(
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: CallbackConfig | None = None,
) -> str:
    """Create the callback checkpoint and return the callback id."""
    callback_options: CallbackOptions = (
        CallbackOptions(
            timeout_seconds=config.timeout_seconds,
            heartbeat_timeout_seconds=config.heartbeat_timeout_seconds,
        )
        if config
        else CallbackOptions()
    )

    checkpointed_result: CheckpointedResult = state.get_checkpoint_result(
        operation_identifier.operation_id
    )
    if checkpointed_result.is_failed():
        # have to throw the exact same error on replay as the checkpointed failure
        checkpointed_result.raise_callable_error()

    if (
        checkpointed_result.is_started()
        or checkpointed_result.is_succeeded()
        or checkpointed_result.is_timed_out()
    ):
        # callback id should already exist
        if (
            not checkpointed_result.operation
            or not checkpointed_result.operation.callback_details
        ):
            msg = "Missing callback details"
            raise FatalError(msg)

        return checkpointed_result.operation.callback_details.callback_id

    create_callback_operation = OperationUpdate.create_callback(
        identifier=operation_identifier,
        callback_options=callback_options,
    )
    state.create_checkpoint(operation_update=create_callback_operation)

    result: CheckpointedResult = state.get_checkpoint_result(
        operation_identifier.operation_id
    )

    if not result.operation or not result.operation.callback_details:
        msg = "Missing callback details"
        raise FatalError(msg)

    return result.operation.callback_details.callback_id


def wait_for_callback_handler(
    context: DurableContext,
    submitter: Callable[[str], None],
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

    def submitter_step(step_context):  # noqa: ARG001
        return submitter(callback.callback_id)

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
