"""Callback operation validator."""

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


VALID_ACTIONS_FOR_CALLBACK = frozenset(
    [
        OperationAction.START,
    ]
)


class CallbackOperationValidator:
    """Validates CALLBACK operation transitions."""

    _ALLOWED_STATUS_TO_CANCEL = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @staticmethod
    def validate(current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate CALLBACK operation update."""
        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_callback_exists: str = (
                        "Cannot start a CALLBACK that already exist."
                    )
                    raise InvalidParameterValueException(msg_callback_exists)
            case _:
                msg: str = "Invalid action for CALLBACK operation."
                raise InvalidParameterValueException(msg)
