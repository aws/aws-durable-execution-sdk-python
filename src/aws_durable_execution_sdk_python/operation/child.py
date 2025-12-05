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
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Checkpoint size limit in bytes (256KB)
CHECKPOINT_SIZE_LIMIT = 256 * 1024


class ChildOperationExecutor(OperationExecutor[T]):
    """Executor for child context operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.

    Handles large payload scenarios with ReplayChildren mode.
    """

    def __init__(
        self,
        func: Callable[[], T],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: ChildConfig,
    ):
        """Initialize the child operation executor.

        Args:
            func: The child context function to execute
            state: The execution state
            operation_identifier: The operation identifier
            config: The child configuration
        """
        self.func = func
        self.state = state
        self.operation_identifier = operation_identifier
        self.config = config
        self.sub_type = config.sub_type or OperationSubType.RUN_IN_CHILD_CONTEXT

    def check_result_status(self) -> CheckResult[T]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            CallableRuntimeError: For FAILED operations
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Terminal success without replay_children - deserialize and return
        if (
            checkpointed_result.is_succeeded()
            and not checkpointed_result.is_replay_children()
        ):
            logger.debug(
                "Child context already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            if checkpointed_result.result is None:
                return CheckResult.create_completed(None)  # type: ignore

            result: T = deserialize(
                serdes=self.config.serdes,
                data=checkpointed_result.result,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )
            return CheckResult.create_completed(result)

        # Terminal success with replay_children - re-execute
        if (
            checkpointed_result.is_succeeded()
            and checkpointed_result.is_replay_children()
        ):
            return CheckResult.create_is_ready_to_execute(checkpointed_result)

        # Terminal failure
        if checkpointed_result.is_failed():
            checkpointed_result.raise_callable_error()

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent():
            start_operation: OperationUpdate = OperationUpdate.create_context_start(
                identifier=self.operation_identifier,
                sub_type=self.sub_type,
            )
            # Checkpoint child context START with non-blocking (is_sync=False).
            # This is a fire-and-forget operation for performance - we don't need to wait for
            # persistence before executing the child context. The START checkpoint is purely
            # for observability and tracking the operation hierarchy.
            self.state.create_checkpoint(
                operation_update=start_operation, is_sync=False
            )

        # Ready to execute (checkpoint exists or was just created)
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute child context function with error handling and large payload support.

        Args:
            checkpointed_result: The checkpoint data containing operation state

        Returns:
            The result of executing the child context function

        Raises:
            SuspendExecution: Re-raised without checkpointing
            InvocationError: Re-raised after checkpointing FAIL
            CallableRuntimeError: Raised for other exceptions after checkpointing FAIL
        """
        logger.debug(
            "▶️ Executing child context for id: %s, name: %s",
            self.operation_identifier.operation_id,
            self.operation_identifier.name,
        )

        try:
            raw_result: T = self.func()

            # If in replay_children mode, return without checkpointing
            if checkpointed_result.is_replay_children():
                logger.debug(
                    "ReplayChildren mode: Executed child context again on replay due to large payload. Exiting child context without creating another checkpoint. id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                return raw_result

            # Serialize result
            serialized_result: str = serialize(
                serdes=self.config.serdes,
                value=raw_result,
                operation_id=self.operation_identifier.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

            # Check payload size and use ReplayChildren mode if needed
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
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                    len(serialized_result),
                    CHECKPOINT_SIZE_LIMIT,
                )
                replay_children = True
                # Use summary generator if provided, otherwise use empty string (matches TypeScript)
                serialized_result = (
                    self.config.summary_generator(raw_result)
                    if self.config.summary_generator
                    else ""
                )

            # Checkpoint SUCCEED
            success_operation: OperationUpdate = OperationUpdate.create_context_succeed(
                identifier=self.operation_identifier,
                payload=serialized_result,
                sub_type=self.sub_type,
                context_options=ContextOptions(replay_children=replay_children),
            )
            # Checkpoint child context SUCCEED with blocking (is_sync=True, default).
            # Must ensure the child context result is persisted before returning to the parent.
            # This guarantees the result is durable and child operations won't be re-executed on replay
            # (unless replay_children=True for large payloads).
            self.state.create_checkpoint(operation_update=success_operation)

            logger.debug(
                "✅ Successfully completed child context for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return raw_result  # noqa: TRY300
        except SuspendExecution:
            # Don't checkpoint SuspendExecution - let it bubble up
            raise
        except Exception as e:
            error_object = ErrorObject.from_exception(e)
            fail_operation: OperationUpdate = OperationUpdate.create_context_fail(
                identifier=self.operation_identifier,
                error=error_object,
                sub_type=self.sub_type,
            )
            # Checkpoint child context FAIL with blocking (is_sync=True, default).
            # Must ensure the failure state is persisted before raising the exception.
            # This guarantees the error is durable and child operations won't be re-executed on replay.
            self.state.create_checkpoint(operation_update=fail_operation)

            # InvocationError and its derivatives can be retried
            # When we encounter an invocation error (in all of its forms), we bubble that
            # error upwards (with the checkpoint in place) such that we reach the
            # execution handler at the very top, which will then induce a retry from the
            # dataplane.
            if isinstance(e, InvocationError):
                raise
            raise error_object.to_callable_runtime_error() from e


def child_handler(
    func: Callable[[], T],
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: ChildConfig | None,
) -> T:
    """Public API for child context operations - maintains existing signature.

    This function creates a ChildOperationExecutor and delegates to its process() method,
    maintaining backward compatibility with existing code that calls child_handler.

    Args:
        func: The child context function to execute
        state: The execution state
        operation_identifier: The operation identifier
        config: The child configuration (optional)

    Returns:
        The result of executing the child context

    Raises:
        May raise operation-specific errors during execution
    """
    if not config:
        config = ChildConfig()

    executor = ChildOperationExecutor(func, state, operation_identifier, config)
    return executor.process()
