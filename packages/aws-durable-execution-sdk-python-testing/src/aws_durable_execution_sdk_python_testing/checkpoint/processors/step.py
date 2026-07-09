"""Step operation processor for handling STEP operation updates."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
    StepDetails,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class StepProcessor(OperationProcessor):
    """Processes STEP operation updates with retry scheduling."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,
        execution_arn: str,
        now: datetime,
    ) -> Operation:
        """Process STEP operation update with scheduler integration for retries."""
        match update.action:
            case OperationAction.START:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.STARTED,
                    now=now,
                )
            case OperationAction.RETRY:
                # set Status=PENDING, next attempt time, attempt count + 1
                delay = (
                    update.step_options.next_attempt_delay_seconds
                    if update.step_options
                    else 0
                )
                next_attempt_time = now + timedelta(seconds=delay)

                # Build new step_details with incremented attempt
                current_attempt = (
                    current_op.step_details.attempt
                    if current_op and current_op.step_details
                    else 0
                )
                new_step_details = StepDetails(
                    attempt=current_attempt + 1,
                    next_attempt_timestamp=next_attempt_time,
                    result=update.payload,
                    error=update.error,
                )

                # Create new operation with updated step_details
                retry_operation = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.PENDING,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=(current_op.start_timestamp if current_op else now),
                    end_timestamp=None,
                    sub_type=update.sub_type,
                    execution_details=current_op.execution_details
                    if current_op
                    else None,
                    context_details=current_op.context_details if current_op else None,
                    step_details=new_step_details,
                    wait_details=current_op.wait_details if current_op else None,
                    callback_details=current_op.callback_details
                    if current_op
                    else None,
                    chained_invoke_details=current_op.chained_invoke_details
                    if current_op
                    else None,
                )

                # Timer scheduling is handled centrally by
                # Executor._schedule_earliest_pending. The
                # processor only needs to set
                # step_details.next_attempt_timestamp; the executor
                # finds the earliest pending wake across all ops and
                # arms a single timer.
                return retry_operation
            case OperationAction.SUCCEED:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.SUCCEEDED,
                    now=now,
                )
            case OperationAction.FAIL:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.FAILED,
                    now=now,
                )
            case _:
                msg: str = "Invalid action for STEP operation."

                raise InvalidParameterValueException(msg)
