"""Step operation processor for handling STEP operation updates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
    ) -> Operation:
        """Process STEP operation update with scheduler integration for retries."""
        match update.action:
            case OperationAction.START:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.STARTED,
                )
            case OperationAction.RETRY:
                # set Status=PENDING, next attempt time, attempt count + 1
                delay = (
                    update.step_options.next_attempt_delay_seconds
                    if update.step_options
                    else 0
                )
                next_attempt_time = datetime.now(UTC) + timedelta(seconds=delay)

                # Build new step_details with incremented attempt
                current_attempt = (
                    current_op.step_details.attempt
                    if current_op and current_op.step_details
                    else 0
                )
                new_step_details = StepDetails(
                    attempt=current_attempt + 1,
                    next_attempt_timestamp=next_attempt_time,
                    result=(
                        current_op.step_details.result
                        if current_op and current_op.step_details
                        else None
                    ),
                    error=(
                        current_op.step_details.error
                        if current_op and current_op.step_details
                        else None
                    ),
                )

                # Create new operation with updated step_details
                retry_operation = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.PENDING,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=(
                        current_op.start_timestamp if current_op else datetime.now(UTC)
                    ),
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

                # Schedule step retry timer to fire after delay
                notifier.notify_step_retry_scheduled(
                    execution_arn=execution_arn,
                    operation_id=update.operation_id,
                    delay=delay,
                )
                return retry_operation
            case OperationAction.SUCCEED:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.SUCCEEDED,
                )
            case OperationAction.FAIL:
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.FAILED,
                )
            case _:
                msg: str = "Invalid action for STEP operation."

                raise InvalidParameterValueException(msg)
