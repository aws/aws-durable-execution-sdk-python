"""Context operation processor for handling CONTEXT operation updates."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
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
    from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


class ContextProcessor(OperationProcessor):
    """Processes CONTEXT operation updates for execution context management."""

    def process(
        self,
        update: OperationUpdate,
        current_op: Operation | None,
        notifier: ExecutionNotifier,  # noqa: ARG002
        execution_arn: str,  # noqa: ARG002
    ) -> Operation:
        """Process CONTEXT operation update for context state transitions."""
        match update.action:
            case OperationAction.START:
                # TODO: check for "Cannot start a CONTEXT operation that already exists."
                return self._translate_update_to_operation(
                    update=update,
                    current_operation=current_op,
                    status=OperationStatus.STARTED,
                )
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
                msg: str = "Invalid action for CONTEXT operation."
                raise InvalidParameterValueException(msg)
