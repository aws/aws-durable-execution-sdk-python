"""Implementation for run_in_child_context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from aws_durable_functions_sdk_python.config import ChildConfig
from aws_durable_functions_sdk_python.exceptions import FatalError, SuspendExecution
from aws_durable_functions_sdk_python.lambda_service import (
    ErrorObject,
    OperationSubType,
    OperationUpdate,
)
from aws_durable_functions_sdk_python.serdes import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_functions_sdk_python.identifier import OperationIdentifier
    from aws_durable_functions_sdk_python.state import ExecutionState

logger = logging.getLogger(__name__)

T = TypeVar("T")


def child_handler(
    func: Callable[[], T],
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: ChildConfig | None,
) -> T:
    logger.debug(
        "▶️ Executing child context for id: %s, name: %s",
        operation_identifier.operation_id,
        operation_identifier.name,
    )

    if not config:
        config = ChildConfig()

    # TODO: ReplayChildren
    checkpointed_result = state.get_checkpoint_result(operation_identifier.operation_id)
    if checkpointed_result.is_succeeded():
        logger.debug(
            "Child context already completed, skipping execution for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        if checkpointed_result.result is None:
            return None  # type: ignore
        return deserialize(
            serdes=config.serdes,
            data=checkpointed_result.result,
            operation_id=operation_identifier.operation_id,
            durable_execution_arn=state.durable_execution_arn,
        )
    if checkpointed_result.is_failed():
        checkpointed_result.raise_callable_error()
    sub_type = (
        config.sub_type if config.sub_type else OperationSubType.RUN_IN_CHILD_CONTEXT
    )

    if not checkpointed_result.is_started():
        start_operation = OperationUpdate.create_context_start(
            identifier=operation_identifier,
            sub_type=sub_type,
        )
        state.create_checkpoint(operation_update=start_operation)

    try:
        raw_result: T = func()
        serialized_result: str = serialize(
            serdes=config.serdes,
            value=raw_result,
            operation_id=operation_identifier.operation_id,
            durable_execution_arn=state.durable_execution_arn,
        )

        success_operation = OperationUpdate.create_context_succeed(
            identifier=operation_identifier,
            payload=serialized_result,
            sub_type=sub_type,
        )
        state.create_checkpoint(operation_update=success_operation)

        logger.debug(
            "✅ Successfully completed child context for id: %s, name: %s",
            operation_identifier.operation_id,
            operation_identifier.name,
        )
        return raw_result  # noqa: TRY300
    except SuspendExecution:
        # Don't checkpoint SuspendExecution - let it bubble up
        raise
    except Exception as e:
        error_object = ErrorObject.from_exception(e)
        fail_operation = OperationUpdate.create_context_fail(
            identifier=operation_identifier, error=error_object, sub_type=sub_type
        )
        state.create_checkpoint(operation_update=fail_operation)

        # TODO: rethink FatalError
        if isinstance(e, FatalError):
            raise
        raise error_object.to_callable_runtime_error() from e
