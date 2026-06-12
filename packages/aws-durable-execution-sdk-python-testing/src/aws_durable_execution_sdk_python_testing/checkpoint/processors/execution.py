"""Execution operation processor for handling EXECUTION operation updates."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class ExecutionProcessor(OperationProcessor):
    """Processes EXECUTION operation updates for workflow completion."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,  # noqa: ARG002
        notifier: ExecutionNotifier,
        execution_arn: str,
    ) -> Operation | None:
        """Process EXECUTION operation update for workflow completion/failure."""
        match update.action:
            case OperationAction.SUCCEED:
                notifier.notify_completed(
                    execution_arn=execution_arn, result=update.payload
                )
            case _:
                # intentional. actual service will fail any EXECUTION update that is not SUCCEED.
                error = (
                    update.error
                    if update.error
                    else ErrorObject.from_message(
                        "There is no error details but EXECUTION checkpoint action is not SUCCEED."
                    )
                )
                # All EXECUTION failures go through normal fail path
                # Timeout/Stop status is set by executor based on the operation that caused it
                notifier.notify_failed(execution_arn=execution_arn, error=error)
        # TODO: Svc doesn't actually create checkpoint for EXECUTION. might have to for localrunner though.
        return None
