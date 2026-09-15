"""Chained invoke operation processor for handling CHAINED_INVOKE operation updates."""

from __future__ import annotations

import datetime
from dataclasses import replace
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class ChainedInvokeProcessor(OperationProcessor):
    """Processes CHAINED_INVOKE operation updates.

    The service resolves the target before it schedules anything. Its
    checkpoint response then carries the operation STARTED, because the
    scheduling event is written as part of completing the checkpoint,
    or FAILED when the target could not be resolved, so the handler
    sees such a failure in the response to its own checkpoint and
    raises without suspending. This processor does the same with
    ``preflight``: a START update whose target fails the preflight
    comes back FAILED with that error; otherwise it comes back STARTED
    and raises a :class:`ChainedInvokeStarted` effect for the caller to
    dispatch the target once the checkpoint write completes. The
    operation holds STARTED until its terminal transition, so a handler
    observing it sees exactly one status change; the target's terminal
    state completes it. Completion is never a handler checkpoint, so
    START is the only accepted action.
    """

    def __init__(self, preflight: Callable[[str], ErrorObject | None] | None = None):
        """``preflight`` resolves a target at checkpoint time; without it
        every target is dispatched and any failure arrives later."""
        self._preflight = preflight

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,
        execution_arn: str,
        now: datetime.datetime,
    ) -> Operation:
        """Process CHAINED_INVOKE operation update."""
        match update.action:
            case OperationAction.START:
                options = update.chained_invoke_options
                if options is None:
                    msg_options_required: str = (
                        "Update for CHAINED_INVOKE operation requires "
                        "ChainedInvokeOptions."
                    )
                    raise InvalidParameterValueException(msg_options_required)

                start_timestamp: datetime.datetime | None = self._get_start_time(
                    current_op, now
                )
                error: ErrorObject | None = (
                    self._preflight(options.function_name)
                    if self._preflight is not None
                    else None
                )
                if error is not None:
                    return Operation(
                        operation_id=update.operation_id,
                        parent_id=update.parent_id,
                        name=update.name,
                        start_timestamp=start_timestamp,
                        end_timestamp=now,
                        operation_type=update.operation_type,
                        status=OperationStatus.FAILED,
                        sub_type=update.sub_type,
                        chained_invoke_details=ChainedInvokeDetails(
                            result=None, error=error
                        ),
                    )

                operation: Operation = Operation(
                    operation_id=update.operation_id,
                    parent_id=update.parent_id,
                    name=update.name,
                    start_timestamp=start_timestamp,
                    end_timestamp=None,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    sub_type=update.sub_type,
                    chained_invoke_details=ChainedInvokeDetails(
                        result=None, error=None
                    ),
                )

                notifier.notify_chained_invoke_started(
                    execution_arn=execution_arn,
                    operation_id=update.operation_id,
                    function_name=options.function_name,
                    tenant_id=options.tenant_id,
                    payload=update.payload,
                )
                return operation
            case _:
                msg_invalid_action: str = "Invalid action for CHAINED_INVOKE operation."
                raise InvalidParameterValueException(msg_invalid_action)

    def stored_update(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        updated_op: Operation,
    ) -> OperationUpdate:
        """Keep no input for a chained invoke that failed before starting.

        The service stores no input payload when the target cannot be
        resolved: it records a payload size of zero and an empty payload
        location, and its history shows the function name and the error
        without the input. So the runner stores the update without its
        payload, which also keeps it out of the operation's size.
        """
        if current_op is None and updated_op.status is OperationStatus.FAILED:
            return replace(update, payload=None)
        return update
