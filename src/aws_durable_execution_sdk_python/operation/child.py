"""Implementation for run_in_child_context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.exceptions import (
    InvocationError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ContextOptions,
    ErrorObject,
    OperationSubType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.serdes import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import ExecutionState

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Checkpoint size limit in bytes (256KB)
CHECKPOINT_SIZE_LIMIT = 256 * 1024


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

    checkpointed_result = state.get_checkpoint_result(operation_identifier.operation_id)
    if (
        checkpointed_result.is_succeeded()
        and not checkpointed_result.is_replay_children()
    ):
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
    sub_type = config.sub_type or OperationSubType.RUN_IN_CHILD_CONTEXT

    if not checkpointed_result.is_existent():
        start_operation = OperationUpdate.create_context_start(
            identifier=operation_identifier,
            sub_type=sub_type,
        )
        # Checkpoint child context START with non-blocking (is_sync=False).
        # This is a fire-and-forget operation for performance - we don't need to wait for
        # persistence before executing the child context. The START checkpoint is purely
        # for observability and tracking the operation hierarchy.
        state.create_checkpoint(operation_update=start_operation, is_sync=False)

    try:
        raw_result: T = func()
        if checkpointed_result.is_replay_children():
            logger.debug(
                "ReplayChildren mode: Executed child context again on replay due to large payload. Exiting child context without creating another checkpoint. id: %s, name: %s",
                operation_identifier.operation_id,
                operation_identifier.name,
            )
            return raw_result
        serialized_result: str = serialize(
            serdes=config.serdes,
            value=raw_result,
            operation_id=operation_identifier.operation_id,
            durable_execution_arn=state.durable_execution_arn,
        )
        # Summary Generator Logic:
        # When the serialized result exceeds 256KB, we use ReplayChildren mode to avoid
        # checkpointing large payloads. Instead, we checkpoint a compact summary and mark
        # the operation for replay. This matches the TypeScript implementation behavior.
        #
        # See TypeScript reference:
        # - aws-durable-execution-sdk-js/src/handlers/run-in-child-context-handler/run-in-child-context-handler.ts (lines ~200-220)
        #
        # The summary generator creates a JSON summary with metadata (type, counts, status)
        # instead of the full BatchResult. During replay, the child context is re-executed
        # to reconstruct the full result rather than deserializing from the checkpoint.
        replay_children: bool = False
        if len(serialized_result) > CHECKPOINT_SIZE_LIMIT:
            logger.debug(
                "Large payload detected, using ReplayChildren mode: id: %s, name: %s, payload_size: %d, limit: %d",
                operation_identifier.operation_id,
                operation_identifier.name,
                len(serialized_result),
                CHECKPOINT_SIZE_LIMIT,
            )
            replay_children = True
            # Use summary generator if provided, otherwise use empty string (matches TypeScript)
            serialized_result = (
                config.summary_generator(raw_result) if config.summary_generator else ""
            )

        success_operation = OperationUpdate.create_context_succeed(
            identifier=operation_identifier,
            payload=serialized_result,
            sub_type=sub_type,
            context_options=ContextOptions(replay_children=replay_children),
        )
        # Checkpoint child context SUCCEED with blocking (is_sync=True, default).
        # Must ensure the child context result is persisted before returning to the parent.
        # This guarantees the result is durable and child operations won't be re-executed on replay
        # (unless replay_children=True for large payloads).
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
        # Checkpoint child context FAIL with blocking (is_sync=True, default).
        # Must ensure the failure state is persisted before raising the exception.
        # This guarantees the error is durable and child operations won't be re-executed on replay.
        state.create_checkpoint(operation_update=fail_operation)

        # InvocationError and its derivatives can be retried
        # When we encounter an invocation error (in all of its forms), we bubble that
        # error upwards (with the checkpoint in place) such that we reach the
        # execution handler at the very top, which will then induce a retry from the
        # dataplane.
        if isinstance(e, InvocationError):
            raise
        raise error_object.to_callable_runtime_error() from e
