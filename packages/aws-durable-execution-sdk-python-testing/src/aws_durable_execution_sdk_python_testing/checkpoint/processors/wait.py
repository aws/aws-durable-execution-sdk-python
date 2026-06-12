"""Wait operation processor for handling WAIT operation updates."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
    WaitDetails,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class WaitProcessor(OperationProcessor):
    """Processes WAIT operation updates with timer scheduling."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,
        execution_arn: str,
    ) -> Operation:
        """Process WAIT operation update with scheduler integration for timers."""
        match update.action:
            case OperationAction.START:
                wait_seconds = (
                    update.wait_options.wait_seconds if update.wait_options else 0
                )
                time_scale = float(os.getenv("DURABLE_EXECUTION_TIME_SCALE", "1.0"))
                logging.info("Using DURABLE_EXECUTION_TIME_SCALE: %f", time_scale)
                scaled_wait_seconds = wait_seconds * time_scale

                scheduled_end_timestamp = datetime.now(UTC) + timedelta(
                    seconds=scaled_wait_seconds
                )

                # Create WaitDetails with scheduled timestamp
                wait_details = WaitDetails(
                    scheduled_end_timestamp=scheduled_end_timestamp
                )

                # Create new operation with wait details
                wait_operation = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=datetime.now(UTC),
                    end_timestamp=None,
                    sub_type=update.sub_type,
                    execution_details=None,
                    context_details=None,
                    step_details=None,
                    wait_details=wait_details,
                    callback_details=None,
                    chained_invoke_details=None,
                )

                # Schedule wait timer to complete after delay
                notifier.notify_wait_timer_scheduled(
                    execution_arn=execution_arn,
                    operation_id=update.operation_id,
                    delay=scaled_wait_seconds,
                )
                return wait_operation
            case OperationAction.CANCEL:
                # TODO: need to cancel the WAIT in the executor
                # TODO: increase sequence id
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.CANCELLED,
                )
            case _:
                msg: str = "Invalid action for WAIT operation."

                raise InvalidParameterValueException(msg)
