"""Implement the durable wait operation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import OperationUpdate, WaitOptions
from aws_durable_execution_sdk_python.suspend import suspend_with_optional_resume_delay

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)


def wait_handler(
    seconds: int, state: ExecutionState, operation_identifier: OperationIdentifier
) -> None:
    logger.debug(
        "Wait requested for id: %s, name: %s",
        operation_identifier.operation_id,
        operation_identifier.name,
    )

    checkpointed_result: CheckpointedResult = state.get_checkpoint_result(
        operation_identifier.operation_id
    )

    if checkpointed_result.is_succeeded():
        logger.debug(
            "Wait already completed, skipping wait for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        return

    if not checkpointed_result.is_existent():
        operation = OperationUpdate.create_wait_start(
            identifier=operation_identifier,
            wait_options=WaitOptions(wait_seconds=seconds),
        )
        state.create_checkpoint(operation_update=operation)

    msg = f"Wait for {seconds} seconds"
    suspend_with_optional_resume_delay(msg, seconds)  # throws suspend
