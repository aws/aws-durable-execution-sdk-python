"""Main checkpoint input validator."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.callback import (
    CallbackOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.context import (
    ContextOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.execution import (
    ExecutionOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.invoke import (
    ChainedInvokeOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.step import (
    StepOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.operations.wait import (
    WaitOperationValidator,
)
from aws_durable_execution_sdk_python_testing.checkpoint.validators.transitions import (
    ValidActionsByOperationTypeValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from aws_durable_execution_sdk_python_testing.execution import Execution

MAX_ERROR_PAYLOAD_SIZE_BYTES = 32768


class CheckpointValidator:
    """Validates checkpoint input based on current state."""

    @staticmethod
    def validate_input(updates: list[OperationUpdate], execution: Execution) -> None:
        """Perform validation on the given input based on the current state."""
        if not updates:
            return

        CheckpointValidator._validate_conflicting_execution_update(updates)
        CheckpointValidator._validate_parent_id_and_duplicate_id(updates, execution)

        for update in updates:
            CheckpointValidator._validate_operation_update(update, execution)

    @staticmethod
    def _validate_conflicting_execution_update(updates: list[OperationUpdate]) -> None:
        """Validate that there are no conflicting execution updates."""
        execution_updates = [
            update
            for update in updates
            if update.operation_type == OperationType.EXECUTION
        ]

        if len(execution_updates) > 1:
            msg_multiple_exec: str = "Cannot checkpoint multiple EXECUTION updates."

            raise InvalidParameterValueException(msg_multiple_exec)

        if execution_updates and updates[-1].operation_type != OperationType.EXECUTION:
            msg_exec_last: str = "EXECUTION checkpoint must be the last update."

            raise InvalidParameterValueException(msg_exec_last)

    @staticmethod
    def _validate_operation_update(
        update: OperationUpdate, execution: Execution
    ) -> None:
        """Validate a single operation update."""
        CheckpointValidator._validate_inconsistent_operation_metadata(update, execution)
        CheckpointValidator._validate_payload_sizes(update)
        ValidActionsByOperationTypeValidator.validate(
            update.operation_type, update.action
        )
        CheckpointValidator._validate_operation_status_transition(update, execution)

    @staticmethod
    def _validate_payload_sizes(update: OperationUpdate) -> None:
        """Validate that operation payload sizes are not too large."""
        if update.error is not None:
            payload = json.dumps(update.error.to_dict())
            if len(payload) > MAX_ERROR_PAYLOAD_SIZE_BYTES:
                msg: str = f"Error object size must be less than {MAX_ERROR_PAYLOAD_SIZE_BYTES} bytes."
                raise InvalidParameterValueException(msg)

    @staticmethod
    def _validate_operation_status_transition(
        update: OperationUpdate, execution: Execution
    ) -> None:
        """Validate that the operation status transition is valid."""
        current_state = None
        for operation in execution.operations:
            if operation.operation_id == update.operation_id:
                current_state = operation
                break

        match update.operation_type:
            case OperationType.STEP:
                StepOperationValidator.validate(current_state, update)
            case OperationType.CONTEXT:
                ContextOperationValidator.validate(current_state, update)
            case OperationType.WAIT:
                WaitOperationValidator.validate(current_state, update)
            case OperationType.CALLBACK:
                CallbackOperationValidator.validate(current_state, update)
            case OperationType.CHAINED_INVOKE:
                ChainedInvokeOperationValidator.validate(current_state, update)
            case OperationType.EXECUTION:
                ExecutionOperationValidator.validate(update)
            case _:  # pragma: no cover
                msg: str = "Invalid operation type."

                raise InvalidParameterValueException(msg)

    @staticmethod
    def _validate_inconsistent_operation_metadata(
        update: OperationUpdate, execution: Execution
    ) -> None:
        """Validate that operation metadata is consistent with existing operation."""
        current_state = None
        for operation in execution.operations:
            if operation.operation_id == update.operation_id:
                current_state = operation
                break

        if current_state is not None:
            if (
                update.operation_type is not None
                and update.operation_type != current_state.operation_type
            ):
                msg: str = "Inconsistent operation type."
                raise InvalidParameterValueException(msg)

            if (
                update.sub_type is not None
                and update.sub_type != current_state.sub_type
            ):
                msg_subtype: str = "Inconsistent operation subtype."
                raise InvalidParameterValueException(msg_subtype)

            if update.name is not None and update.name != current_state.name:
                msg_name: str = "Inconsistent operation name."
                raise InvalidParameterValueException(msg_name)

            if (
                update.parent_id is not None
                and update.parent_id != current_state.parent_id
            ):
                msg_parent: str = "Inconsistent parent operation id."
                raise InvalidParameterValueException(msg_parent)

    @staticmethod
    def _validate_parent_id_and_duplicate_id(
        updates: list[OperationUpdate], execution: Execution
    ) -> None:
        """Validate parent IDs and check for duplicate operation IDs.

        Validate that any provided parentId is valid, and also validate no duplicate operation is being
        updated at the same time (unless it is a STEP/CONTEXT starting + performing one more non-START action).
        """
        operations_started: MutableMapping[str, OperationUpdate] = {}
        last_updates_seen: MutableMapping[str, OperationUpdate] = {}

        for update in updates:
            if CheckpointValidator._is_invalid_duplicate_update(
                update, last_updates_seen
            ):
                msg_duplicate: str = (
                    "Cannot checkpoint multiple operations with the same ID."
                )
                raise InvalidParameterValueException(msg_duplicate)

            if not CheckpointValidator._is_valid_parent_for_update(
                execution, update, operations_started
            ):
                msg_parent: str = "Invalid parent operation id."
                raise InvalidParameterValueException(msg_parent)

            if update.action == OperationAction.START:
                operations_started[update.operation_id] = update

            last_updates_seen[update.operation_id] = update

    @staticmethod
    def _is_invalid_duplicate_update(
        update: OperationUpdate, last_updates_seen: MutableMapping[str, OperationUpdate]
    ) -> bool:
        """Check if this is an invalid duplicate update."""
        last_update = last_updates_seen.get(update.operation_id)
        if last_update is None:
            return False

        if last_update.operation_type in (OperationType.STEP, OperationType.CONTEXT):
            # Allow duplicate for STEP/CONTEXT if last was START and current is not START
            allow_duplicate = (
                last_update.action == OperationAction.START
                and update.action != OperationAction.START
            )
            return not allow_duplicate

        return True

    @staticmethod
    def _is_valid_parent_for_update(
        execution: Execution,
        update: OperationUpdate,
        operations_started: MutableMapping[str, OperationUpdate],
    ) -> bool:
        """Check if the parent ID is valid for the update."""
        parent_id = update.parent_id

        if parent_id is None:
            return True

        # Check if parent is in operations started in this batch
        if parent_id in operations_started:
            parent_update = operations_started[parent_id]
            return parent_update.operation_type == OperationType.CONTEXT

        # Check if parent exists in current execution state
        for operation in execution.operations:
            if operation.operation_id == parent_id:
                return operation.operation_type == OperationType.CONTEXT

        return False
