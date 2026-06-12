"""Wait operation validator."""

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


VALID_ACTIONS_FOR_WAIT = frozenset(
    [
        OperationAction.START,
        OperationAction.CANCEL,
    ]
)


class WaitOperationValidator:
    """Validates WAIT operation transitions."""

    _ALLOWED_STATUS_TO_CANCEL = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @staticmethod
    def validate(current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate WAIT operation update."""
        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_wait_exists: str = "Cannot start a WAIT that already exist."

                    raise InvalidParameterValueException(msg_wait_exists)
            case OperationAction.CANCEL:
                if (
                    current_state is None
                    or current_state.status
                    not in WaitOperationValidator._ALLOWED_STATUS_TO_CANCEL
                ):
                    msg_wait_cancel: str = "Cannot cancel a WAIT that does not exist or has already completed."
                    raise InvalidParameterValueException(msg_wait_cancel)
            case _:
                msg_wait_invalid: str = "Invalid WAIT action."

                raise InvalidParameterValueException(msg_wait_invalid)
