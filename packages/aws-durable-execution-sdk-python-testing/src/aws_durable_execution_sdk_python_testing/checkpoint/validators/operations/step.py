"""Step operation validator."""

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


VALID_ACTIONS_FOR_STEP = frozenset(
    [
        OperationAction.START,
        OperationAction.FAIL,
        OperationAction.RETRY,
        OperationAction.SUCCEED,
    ]
)


class StepOperationValidator:
    """Validates STEP operation transitions."""

    _ALLOWED_STATUS_TO_CLOSE = frozenset(
        [
            OperationStatus.STARTED,
            OperationStatus.READY,
        ]
    )

    _ALLOWED_STATUS_TO_START = frozenset(
        [
            OperationStatus.READY,
        ]
    )

    _ALLOWED_STATUS_TO_REATTEMPT = frozenset(
        [
            OperationStatus.STARTED,
            OperationStatus.READY,
        ]
    )

    @staticmethod
    def validate(current_state: Operation | None, update: OperationUpdate) -> None:
        """Validate STEP operation update."""
        if current_state is None:
            return

        match update.action:
            case OperationAction.START:
                if (
                    current_state.status
                    not in StepOperationValidator._ALLOWED_STATUS_TO_START
                ):
                    msg_step_start: str = "Invalid current STEP state to start."

                    raise InvalidParameterValueException(msg_step_start)
            case OperationAction.FAIL | OperationAction.SUCCEED:
                if (
                    current_state.status
                    not in StepOperationValidator._ALLOWED_STATUS_TO_CLOSE
                ):
                    msg_step_close: str = "Invalid current STEP state to close."

                    raise InvalidParameterValueException(msg_step_close)
                if update.action == OperationAction.FAIL and update.payload is not None:
                    msg_fail_payload: str = "Cannot provide a Payload for FAIL action."

                    raise InvalidParameterValueException(msg_fail_payload)
                if (
                    update.action == OperationAction.SUCCEED
                    and update.error is not None
                ):
                    msg_succeed_error: str = (
                        "Cannot provide an Error for SUCCEED action."
                    )

                    raise InvalidParameterValueException(msg_succeed_error)
            case OperationAction.RETRY:
                if (
                    current_state.status
                    not in StepOperationValidator._ALLOWED_STATUS_TO_REATTEMPT
                ):
                    msg_step_retry: str = "Invalid current STEP state to re-attempt."

                    raise InvalidParameterValueException(msg_step_retry)
                if update.step_options is None:
                    msg_step_options: str = "Invalid StepOptions for the given action."

                    raise InvalidParameterValueException(msg_step_options)
                if update.error is not None and update.payload is not None:
                    msg_retry_both: str = (
                        "Cannot provide both error and payload to RETRY a STEP."
                    )
                    raise InvalidParameterValueException(msg_retry_both)
            case _:
                msg_step_invalid: str = "Invalid STEP action."

                raise InvalidParameterValueException(msg_step_invalid)
