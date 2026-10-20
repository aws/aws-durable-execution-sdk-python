"""Invoke operation validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationAction,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.child_dispatcher import (
    CHAINED_INVOKE_DIFFERENT_ACCOUNT_MESSAGE,
    CHAINED_INVOKE_DIFFERENT_REGION_MESSAGE,
    INVALID_FUNCTION_ARN_MESSAGE_FORMAT,
    INVALID_TENANT_ID_MESSAGE,
    FunctionTarget,
    is_valid_tenant_id,
    parse_function_target,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution


VALID_ACTIONS_FOR_INVOKE = frozenset(
    [
        OperationAction.START,
    ]
)


class ChainedInvokeOperationValidator:
    """Validates INVOKE operation transitions.

    START is the only accepted action: a chained invoke completes through
    the invoked function's terminal state, not through a handler
    checkpoint. START also validates the options, as the service does:
    the target must be a well-formed Lambda function name or ARN in the
    parent's account and region, and a TenantId must satisfy the API
    constraint.
    """

    @staticmethod
    def validate(
        current_state: Operation | None,
        update: OperationUpdate,
        execution: Execution,
    ) -> None:
        """Validate INVOKE operation update."""
        match update.action:
            case OperationAction.START:
                if current_state is not None:
                    msg_invoke_exists: str = (
                        "Cannot start a CHAINED_INVOKE operation that already exists."
                    )

                    raise InvalidParameterValueException(msg_invoke_exists)
                if update.chained_invoke_options is None:
                    msg_options_required: str = (
                        "Update for CHAINED_INVOKE operation requires "
                        "ChainedInvokeOptions."
                    )
                    raise InvalidParameterValueException(msg_options_required)
                ChainedInvokeOperationValidator._validate_target(
                    update.chained_invoke_options.function_name, execution
                )
                # The API model types TenantId as a string. A value of
                # another type is a validation error, not a server error.
                tenant_id: str | None = update.chained_invoke_options.tenant_id
                if tenant_id is not None and (
                    not isinstance(tenant_id, str) or not is_valid_tenant_id(tenant_id)
                ):
                    raise InvalidParameterValueException(INVALID_TENANT_ID_MESSAGE)
            case _:
                msg_invoke_invalid: str = "Invalid action for CHAINED_INVOKE operation."

                raise InvalidParameterValueException(msg_invoke_invalid)

    @staticmethod
    def _validate_target(function_name: str, execution: Execution) -> None:
        """Reject a malformed target or one in another account or region.

        The region check applies only when the execution recorded a
        region; an execution built without one skips it.
        """
        # The API model requires FunctionName as a string. A missing or
        # non-string value is a validation error, not a server error.
        if not isinstance(function_name, str):
            raise InvalidParameterValueException(
                INVALID_FUNCTION_ARN_MESSAGE_FORMAT.format(function_name)
            )
        try:
            target: FunctionTarget = parse_function_target(function_name)
        except ValueError as err:
            raise InvalidParameterValueException(
                INVALID_FUNCTION_ARN_MESSAGE_FORMAT.format(function_name)
            ) from err
        if (
            target.account_id is not None
            and target.account_id != execution.start_input.account_id
        ):
            raise InvalidParameterValueException(
                CHAINED_INVOKE_DIFFERENT_ACCOUNT_MESSAGE
            )
        if (
            target.region is not None
            and execution.region is not None
            and target.region != execution.region
        ):
            raise InvalidParameterValueException(
                CHAINED_INVOKE_DIFFERENT_REGION_MESSAGE
            )
