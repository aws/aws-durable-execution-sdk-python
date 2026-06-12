"""Context operation validator."""

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


VALID_ACTIONS_FOR_CONTEXT = frozenset(
    [
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.SUCCEED,
    ]
)


class ContextOperationValidator:
    """Validates CONTEXT operation transitions."""

    _ALLOWED_STATUS_TO_CLOSE = frozenset(
        [
            OperationStatus.STARTED,
        ]
    )

    @staticmethod
    def validate(current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate CONTEXT operation update."""
        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_context_exists: str = (
                        "Cannot start a CONTEXT that already exist."
                    )

                    raise InvalidParameterValueException(msg_context_exists)
            case OperationAction.FAIL | OperationAction.SUCCEED:
                if (
                    current_state is not None
                    and current_state.status
                    not in ContextOperationValidator._ALLOWED_STATUS_TO_CLOSE
                ):
                    msg_context_close: str = "Invalid current CONTEXT state to close."

                    raise InvalidParameterValueException(msg_context_close)
                if update.action == OperationAction.FAIL and update.payload is not None:
                    msg_context_fail_payload: str = (
                        "Cannot provide a Payload for FAIL action."
                    )

                    raise InvalidParameterValueException(msg_context_fail_payload)
                if (
                    update.action == OperationAction.SUCCEED
                    and update.error is not None
                ):
                    msg_context_succeed_error: str = (
                        "Cannot provide an Error for SUCCEED action."
                    )

                    raise InvalidParameterValueException(msg_context_succeed_error)
            case _:
                msg_context_invalid: str = "Invalid CONTEXT action."

                raise InvalidParameterValueException(msg_context_invalid)
