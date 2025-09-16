"""Implement the durable wait operation."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from aws_durable_functions_sdk_python.exceptions import TimedSuspendExecution
from aws_durable_functions_sdk_python.lambda_service import OperationUpdate, WaitOptions

if TYPE_CHECKING:
    from aws_durable_functions_sdk_python.identifier import OperationIdentifier
    from aws_durable_functions_sdk_python.state import ExecutionState

logger = logging.getLogger(__name__)


def wait_handler(
    seconds: int, state: ExecutionState, operation_identifier: OperationIdentifier
) -> None:
    logger.debug(
        "Wait requested for id: %s, name: %s",
        operation_identifier.operation_id,
        operation_identifier.name,
    )

    if state.get_checkpoint_result(operation_identifier.operation_id).is_succeeded():
        logger.debug(
            "Wait already completed, skipping wait for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        return

    operation = OperationUpdate.create_wait_start(
        identifier=operation_identifier,
        wait_options=WaitOptions(seconds=seconds),
    )

    state.create_checkpoint(operation_update=operation)

    # Calculate when to resume
    resume_time = time.time() + seconds
    msg = f"Wait for {seconds} seconds"
    raise TimedSuspendExecution(msg, scheduled_timestamp=resume_time)
