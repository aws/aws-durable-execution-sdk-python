"""Invoke operation validator."""

from __future__ import annotations

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationStatus,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


VALID_ACTIONS_FOR_INVOKE = frozenset(
    [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
)


class ChainedInvokeOperationValidator:
    """Validates INVOKE operation transitions."""

    _ALLOWED_STATUS_TO_CANCEL = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @staticmethod
    def validate(current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate INVOKE operation update."""
        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_invoke_exists: str = (
                        "Cannot start an INVOKE that already exist."
                    )

                    raise InvalidParameterValueException(msg_invoke_exists)
            case OperationAction.CANCEL:
                if (
                    current_state is None
                    or current_state.status
                    not in ChainedInvokeOperationValidator._ALLOWED_STATUS_TO_CANCEL
                ):
                    msg_invoke_cancel: str = "Cannot cancel an INVOKE that does not exist or has already completed."
                    raise InvalidParameterValueException(msg_invoke_cancel)
            case _:
                msg_invoke_invalid: str = "Invalid INVOKE action."

                raise InvalidParameterValueException(msg_invoke_invalid)
