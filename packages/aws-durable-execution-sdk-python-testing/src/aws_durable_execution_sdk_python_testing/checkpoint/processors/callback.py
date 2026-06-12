"""Callback operation processor for handling CALLBACK operation updates."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
    CallbackDetails,
    OperationType,
    CallbackOptions,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.token import CallbackToken

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class CallbackProcessor(OperationProcessor):
    """Processes CALLBACK operation updates with activity scheduling."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,  # noqa: ARG002
        execution_arn: str,  # noqa: ARG002
    ) -> Operation:
        """Process CALLBACK operation update with scheduler integration for activities."""
        match update.action:
            case OperationAction.START:
                callback_token: CallbackToken = CallbackToken(
                    execution_arn=execution_arn,
                    operation_id=update.operation_id,
                )

                callback_id: str = callback_token.to_str()

                callback_details: CallbackDetails | None = (
                    CallbackDetails(
                        callback_id=callback_id,
                        result=update.payload,
                        error=update.error,
                    )
                    if update.operation_type == OperationType.CALLBACK
                    else None
                )

                status: OperationStatus = OperationStatus.STARTED

                start_time: datetime.datetime | None = self._get_start_time(current_op)

                end_time: datetime.datetime | None = self._get_end_time(
                    current_op, status
                )

                operation: Operation = Operation(
                    operation_id=update.operation_id,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=start_time,
                    end_timestamp=end_time,
                    operation_type=update.operation_type,
                    status=status,
                    sub_type=update.sub_type,
                    callback_details=callback_details,
                )
                callback_options: CallbackOptions | None = update.callback_options

                notifier.notify_callback_created(
                    execution_arn=execution_arn,
                    operation_id=update.operation_id,
                    callback_options=callback_options,
                    callback_token=callback_token,
                )
                return operation
            case _:
                msg: str = "Invalid action for CALLBACK operation."
                raise InvalidParameterValueException(msg)
