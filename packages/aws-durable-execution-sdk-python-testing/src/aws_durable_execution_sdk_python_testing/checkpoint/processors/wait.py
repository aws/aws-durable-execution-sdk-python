"""Wait operation processor for handling WAIT operation updates."""

from __future__ import annotations

from datetime import datetime, timedelta
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
        now: datetime,
    ) -> Operation:
        """Process WAIT operation update with scheduler integration for timers."""
        match update.action:
            case OperationAction.START:
                wait_seconds = (
                    update.wait_options.wait_seconds if update.wait_options else 0
                )

                scheduled_end_timestamp = now + timedelta(seconds=wait_seconds)

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
                    start_timestamp=now,
                    end_timestamp=None,
                    sub_type=update.sub_type,
                    execution_details=None,
                    context_details=None,
                    step_details=None,
                    wait_details=wait_details,
                    callback_details=None,
                    chained_invoke_details=None,
                )

                # Timer scheduling is handled centrally by
                # Executor._schedule_earliest_pending. The processor
                # only needs to set wait_details.scheduled_end_timestamp;
                # the executor finds the earliest pending wake across
                # all ops and arms a single timer.
                return wait_operation
            case OperationAction.CANCEL:
                # TODO: need to cancel the WAIT in the executor
                # TODO: increase sequence id
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.CANCELLED,
                    now=now,
                )
            case _:
                msg: str = "Invalid action for WAIT operation."

                raise InvalidParameterValueException(msg)
