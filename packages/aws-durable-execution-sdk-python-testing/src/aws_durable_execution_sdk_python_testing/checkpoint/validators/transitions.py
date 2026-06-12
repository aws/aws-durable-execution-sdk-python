"""Validator for valid actions by operation type."""

from __future__ import annotations

from typing import ClassVar

from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.callback import (
    VALID_ACTIONS_FOR_CALLBACK,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.context import (
    VALID_ACTIONS_FOR_CONTEXT,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.execution import (
    VALID_ACTIONS_FOR_EXECUTION,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.invoke import (
    VALID_ACTIONS_FOR_INVOKE,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.step import (
    VALID_ACTIONS_FOR_STEP,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.wait import (
    VALID_ACTIONS_FOR_WAIT,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


class ValidActionsByOperationTypeValidator:
    """Validates that the given action is valid for the given operation type."""

    _VALID_ACTIONS_BY_OPERATION_TYPE: ClassVar[
        dict[OperationType, frozenset[OperationAction]]
    ] = {
        OperationType.STEP: VALID_ACTIONS_FOR_STEP,
        OperationType.CONTEXT: VALID_ACTIONS_FOR_CONTEXT,
        OperationType.WAIT: VALID_ACTIONS_FOR_WAIT,
        OperationType.CALLBACK: VALID_ACTIONS_FOR_CALLBACK,
        OperationType.CHAINED_INVOKE: VALID_ACTIONS_FOR_INVOKE,
        OperationType.EXECUTION: VALID_ACTIONS_FOR_EXECUTION,
    }

    @staticmethod
    def validate(operation_type: OperationType, action: OperationAction) -> None:
        """Validate that the action is valid for the operation type."""
        valid_actions = (
            ValidActionsByOperationTypeValidator._VALID_ACTIONS_BY_OPERATION_TYPE.get(
                operation_type
            )
        )

        if valid_actions is None:
            msg_unknown_op: str = "Unknown operation type."

            raise InvalidParameterValueException(msg_unknown_op)

        if action not in valid_actions:
            msg_invalid_action: str = "Invalid action for the given operation type."

            raise InvalidParameterValueException(msg_invalid_action)
