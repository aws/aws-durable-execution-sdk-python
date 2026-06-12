"""Base processor class for operation transformations."""

from __future__ import annotations

import datetime
from datetime import timedelta
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ChainedInvokeDetails,
    ContextDetails,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepDetails,
    WaitDetails,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class OperationProcessor:
    """Base class for processing OperationUpdate to Operation transformations."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,
        execution_arn: str,
    ) -> Operation | None:
        """Process an operation update and return the transformed operation."""
        raise NotImplementedError

    def _get_start_time(
        self, current_operation: Operation | None
    ) -> datetime.datetime | None:
        start_time: datetime.datetime | None = (
            current_operation.start_timestamp
            if current_operation
            else datetime.datetime.now(tz=datetime.UTC)
        )
        return start_time

    def _get_end_time(
        self, current_operation: Operation | None, status: OperationStatus
    ) -> datetime.datetime | None:
        """Get end timestamp for operation based on current state and status."""
        if current_operation and current_operation.end_timestamp:
            return current_operation.end_timestamp
        if status in {
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
            OperationStatus.TIMED_OUT,
            OperationStatus.STOPPED,
        }:
            return datetime.datetime.now(tz=datetime.UTC)
        return None

    def _create_execution_details(
        self, update: OperationUpdate
    ) -> ExecutionDetails | None:
        """Create ExecutionDetails from OperationUpdate."""
        return (
            ExecutionDetails(input_payload=update.payload)
            if update.operation_type == OperationType.EXECUTION
            else None
        )

    def _create_context_details(self, update: OperationUpdate) -> ContextDetails | None:
        """Create ContextDetails from OperationUpdate."""
        return (
            ContextDetails(
                result=update.payload,
                error=update.error,
                replay_children=update.context_options.replay_children
                if update.context_options
                else False,
            )
            if update.operation_type == OperationType.CONTEXT
            else None
        )

    def _create_step_details(
        self,
        update: OperationUpdate,
        current_operation: Operation | None = None,
    ) -> StepDetails | None:
        """Create StepDetails from OperationUpdate.

        Automatically increments attempt count for RETRY, SUCCEED, and FAIL actions.
        """

        attempt: int = 0
        next_attempt_timestamp: datetime.datetime | None = None

        if update.operation_type is OperationType.STEP:
            if current_operation and current_operation.step_details:
                attempt = current_operation.step_details.attempt
                next_attempt_timestamp = (
                    current_operation.step_details.next_attempt_timestamp
                )
            # Increment attempt for RETRY, SUCCEED, and FAIL actions
            if update.action in {
                OperationAction.RETRY,
                OperationAction.SUCCEED,
                OperationAction.FAIL,
            }:
                attempt += 1
            return StepDetails(
                attempt=attempt,
                next_attempt_timestamp=next_attempt_timestamp,
                result=update.payload,
                error=update.error,
            )

        return None

    def _create_callback_details(
        self, update: OperationUpdate
    ) -> CallbackDetails | None:
        """Create CallbackDetails from OperationUpdate."""
        return (
            CallbackDetails(
                callback_id="placeholder", result=update.payload, error=update.error
            )
            if update.operation_type == OperationType.CALLBACK
            else None
        )

    def _create_invoke_details(
        self, update: OperationUpdate
    ) -> ChainedInvokeDetails | None:
        """Create ChainedInvokeDetails from OperationUpdate."""
        if (
            update.operation_type == OperationType.CHAINED_INVOKE
            and update.chained_invoke_options
        ):
            return ChainedInvokeDetails(result=update.payload, error=update.error)
        return None

    def _translate_update_to_operation(
        self,
        update: OperationUpdate,
        current_operation: Operation | None,
        status: OperationStatus,
    ) -> Operation:
        """Transform OperationUpdate to Operation, always creating new Operation."""
        start_time: datetime.datetime | None = self._get_start_time(current_operation)
        end_time: datetime.datetime | None = self._get_end_time(
            current_operation, status
        )

        execution_details = self._create_execution_details(update)
        context_details = self._create_context_details(update)
        step_details = self._create_step_details(update, current_operation)
        callback_details = self._create_callback_details(update)
        invoke_details = self._create_invoke_details(update)
        wait_details = self._create_wait_details(update, current_operation)

        return Operation(
            operation_id=update.operation_id,
            parent_id=update.parent_id,
            name=update.name,
            start_timestamp=start_time,
            end_timestamp=end_time,
            operation_type=update.operation_type,
            status=status,
            sub_type=update.sub_type,
            execution_details=execution_details,
            context_details=context_details,
            step_details=step_details,
            callback_details=callback_details,
            chained_invoke_details=invoke_details,
            wait_details=wait_details,
        )

    def _create_wait_details(
        self, update: OperationUpdate, current_operation: Operation | None
    ) -> WaitDetails | None:
        """Create WaitDetails from OperationUpdate."""
        if update.operation_type == OperationType.WAIT and update.wait_options:
            if current_operation and current_operation.wait_details:
                scheduled_end_timestamp = (
                    current_operation.wait_details.scheduled_end_timestamp
                )
            else:
                scheduled_end_timestamp = datetime.datetime.now(
                    tz=datetime.UTC
                ) + timedelta(seconds=update.wait_options.wait_seconds)
            return WaitDetails(scheduled_end_timestamp=scheduled_end_timestamp)
        return None
