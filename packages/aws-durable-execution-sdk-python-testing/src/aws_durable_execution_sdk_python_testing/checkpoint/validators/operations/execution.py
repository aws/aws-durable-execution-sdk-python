"""Execution operation validator."""

from __future__ import annotations

from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


VALID_ACTIONS_FOR_EXECUTION = frozenset(
    [
        OperationAction.SUCCEED,
        OperationAction.FAIL,
    ]
)


class ExecutionOperationValidator:
    """Validates EXECUTION operation transitions."""

    @staticmethod
    def validate(update: OperationUpdate) -> None:
        """Validate EXECUTION operation update."""
        match update.action:
            case OperationAction.SUCCEED:
                if update.error is not None:
                    msg_exec_succeed_error: str = (
                        "Cannot provide an Error for SUCCEED action."
                    )

                    raise InvalidParameterValueException(msg_exec_succeed_error)
            case OperationAction.FAIL:
                if update.payload is not None:
                    msg_exec_fail_payload: str = (
                        "Cannot provide a Payload for FAIL action."
                    )

                    raise InvalidParameterValueException(msg_exec_fail_payload)
            case _:
                msg_exec_invalid: str = "Invalid EXECUTION action."

                raise InvalidParameterValueException(msg_exec_invalid)
