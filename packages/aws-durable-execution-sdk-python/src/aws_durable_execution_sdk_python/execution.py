from __future__ import annotations

import contextlib
import functools
import json
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    BotoClientError,
    CheckpointError,
    DurableExecutionsError,
    ExecutionError,
    InvocationError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    DurableExecutionInvocationOutput,
    DurableServiceClient,
    ErrorObject,
    InvocationStatus,
    LambdaClient,
    Operation,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    PluginExecutor,
)
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus


if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

    from mypy_boto3_lambda import LambdaClient as Boto3LambdaClient

    from aws_durable_execution_sdk_python.types import LambdaContext


logger = logging.getLogger(__name__)

# 6MB in bytes, minus 50 bytes for envelope
LAMBDA_RESPONSE_SIZE_LIMIT = 6 * 1024 * 1024 - 50


# region Invocation models
@dataclass(frozen=True)
class InitialExecutionState:
    operations: list[Operation]
    next_marker: str

    @staticmethod
    def from_dict(input_dict: MutableMapping[str, Any]) -> InitialExecutionState:
        operations = []
        if input_operations := input_dict.get("Operations"):
            operations = [Operation.from_dict(op) for op in input_operations]
        return InitialExecutionState(
            operations=operations,
            next_marker=input_dict.get("NextMarker", ""),
        )

    @staticmethod
    def from_json_dict(input_dict: MutableMapping[str, Any]) -> InitialExecutionState:
        operations = []
        if input_operations := input_dict.get("Operations"):
            operations = [Operation.from_json_dict(op) for op in input_operations]
        return InitialExecutionState(
            operations=operations,
            next_marker=input_dict.get("NextMarker", ""),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "Operations": [op.to_dict() for op in self.operations],
            "NextMarker": self.next_marker,
        }

    def to_json_dict(self) -> MutableMapping[str, Any]:
        return {
            "Operations": [op.to_json_dict() for op in self.operations],
            "NextMarker": self.next_marker,
        }


@dataclass(frozen=True)
class DurableExecutionInvocationInput:
    durable_execution_arn: str
    checkpoint_token: str
    initial_execution_state: InitialExecutionState
    updated_operation_ids: list[str] = field(default_factory=list, kw_only=True)

    @staticmethod
    def from_dict(
        input_dict: MutableMapping[str, Any],
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=input_dict["DurableExecutionArn"],
            checkpoint_token=input_dict["CheckpointToken"],
            initial_execution_state=InitialExecutionState.from_dict(
                input_dict.get("InitialExecutionState", {})
            ),
            updated_operation_ids=list(input_dict.get("UpdatedOperationIds", [])),
        )

    @staticmethod
    def from_json_dict(
        input_dict: MutableMapping[str, Any],
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=input_dict["DurableExecutionArn"],
            checkpoint_token=input_dict["CheckpointToken"],
            initial_execution_state=InitialExecutionState.from_json_dict(
                input_dict.get("InitialExecutionState", {})
            ),
            updated_operation_ids=list(input_dict.get("UpdatedOperationIds", [])),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_dict(),
            "UpdatedOperationIds": self.updated_operation_ids,
        }

    def to_json_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_json_dict(),
            "UpdatedOperationIds": self.updated_operation_ids,
        }


@dataclass(frozen=True)
class DurableExecutionInvocationInputWithClient(DurableExecutionInvocationInput):
    """Invocation input with Lambda boto client injected.

    This is useful for testing scenarios where you want to inject a mock client.
    """

    service_client: DurableServiceClient

    @staticmethod
    def from_durable_execution_invocation_input(
        invocation_input: DurableExecutionInvocationInput,
        service_client: DurableServiceClient,
    ):
        return DurableExecutionInvocationInputWithClient(
            durable_execution_arn=invocation_input.durable_execution_arn,
            checkpoint_token=invocation_input.checkpoint_token,
            initial_execution_state=invocation_input.initial_execution_state,
            updated_operation_ids=invocation_input.updated_operation_ids,
            service_client=service_client,
        )


# endregion Invocation models


def durable_execution(
    func: Callable[[Any, DurableContext], Any] | None = None,
    *,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPlugin] | None = None,
) -> Callable[[Any, LambdaContext], Any]:
    """
    Decorator to create a durable execution handler.

    Args:
        func: The user function to decorate
        boto3_client: Optional boto3 Lambda client to use
        plugins: Optional list of plugins to use (EXPERIMENTAL: This
            feature has known issues and this parameter may change or be removed.)
    """
    # Decorator called with parameters
    if func is None:
        logger.debug("Decorator called with parameters")
        return functools.partial(
            durable_execution, boto3_client=boto3_client, plugins=plugins
        )

    logger.debug("Starting durable execution handler...")

    if plugins:
        warnings.warn(
            "The 'plugins' parameter is provisional and may be altered or removed.",
            category=FutureWarning,
            stacklevel=2,  # point the warning to the caller of durable_execution
        )

    plugin_executor = PluginExecutor(plugins)

    @plugin_executor.handle_durable_output
    def wrapper(event: Any, context: LambdaContext) -> MutableMapping[str, Any]:
        invocation_input: DurableExecutionInvocationInput
        service_client: DurableServiceClient

        # event likely only to be DurableExecutionInvocationInputWithClient when directly injected by test framework
        if isinstance(event, DurableExecutionInvocationInputWithClient):
            logger.debug("durableExecutionArn: %s", event.durable_execution_arn)
            invocation_input = event
            service_client = invocation_input.service_client
        else:
            try:
                logger.debug(
                    "durableExecutionArn: %s", event.get("DurableExecutionArn")
                )
                invocation_input = DurableExecutionInvocationInput.from_json_dict(event)
            except (KeyError, TypeError, AttributeError) as e:
                msg = (
                    "Unexpected payload provided to start the durable execution. "
                    "Check your resource configurations to confirm the durability is set."
                )
                raise ExecutionError(msg) from e

            # Use custom client if provided, otherwise initialize from environment
            service_client = (
                LambdaClient(client=boto3_client)
                if boto3_client is not None
                else LambdaClient.initialize_client()
            )

        execution_state: ExecutionState = ExecutionState(
            durable_execution_arn=invocation_input.durable_execution_arn,
            initial_checkpoint_token=invocation_input.checkpoint_token,
            operations={},
            service_client=service_client,
            plugin_executor=plugin_executor,
            updated_operation_ids=invocation_input.updated_operation_ids,
        )

        try:
            execution_state.fetch_paginated_operations(
                invocation_input.initial_execution_state.operations,
                invocation_input.checkpoint_token,
                invocation_input.initial_execution_state.next_marker,
            )
        except BotoClientError as e:
            # Non-retryable Durable API errors (e.g., customer configuration issues,
            # 4xx client errors) will never succeed on retry — fail the execution immediately.
            if not e.is_retryable():
                logger.exception(
                    "Non-retryable Durable API error during initial state fetch. Must fail execution "
                    "without retry.",
                    extra=e.build_logger_extras(),
                )
                return DurableExecutionInvocationOutput(
                    status=InvocationStatus.FAILED,
                    error=ErrorObject.from_exception(e),
                ).to_dict()
            raise

        # Determine whether this is a replay (prior operations exist) at the
        # execution level. This seeds the root context's replay status and the
        # plugin's is_first_invocation flag. Replay status itself is then
        # tracked per-context as execution proceeds.
        has_prior_operations: bool = execution_state.has_prior_operations()

        raw_input_payload: str | None = execution_state.get_input_payload()

        # Python RIC LambdaMarshaller just uses standard json deserialization for event
        # https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/main/awslambdaric/lambda_runtime_marshaller.py#L46
        input_event: MutableMapping[str, Any] = {}
        if raw_input_payload and raw_input_payload.strip():
            try:
                input_event = json.loads(raw_input_payload)
            except json.JSONDecodeError:
                logger.exception(
                    "Failed to parse input payload as JSON: payload: %r",
                    raw_input_payload,
                )
                raise

        durable_context: DurableContext = DurableContext.from_lambda_context(
            state=execution_state,
            lambda_context=context,
            replay_status=(
                ReplayStatus.REPLAY if has_prior_operations else ReplayStatus.NEW
            ),
        )

        # Use ThreadPoolExecutor for concurrent execution of user code and background checkpoint processing
        with (
            ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="dex-handler"
            ) as executor,
            contextlib.closing(execution_state) as execution_state,
        ):
            execution_operation = execution_state.get_execution_operation()

            # execute the plugins
            plugin_executor.on_invocation_start(
                execution_arn=invocation_input.durable_execution_arn,
                lambda_context=context,
                execution_start_time=(
                    execution_operation.start_timestamp
                    if execution_operation is not None
                    else None
                ),
                is_first_invocation=not has_prior_operations,
            )
            # Thread 1: Run background checkpoint processing
            executor.submit(execution_state.checkpoint_batches_forever)

            # Thread 2: Execute user function
            logger.debug(
                "%s entering user-space...", invocation_input.durable_execution_arn
            )
            user_future = executor.submit(func, input_event, durable_context)

            logger.debug(
                "%s waiting for user code completion...",
                invocation_input.durable_execution_arn,
            )

            try:
                # Background checkpointing errors will propagate through CompletionEvent.wait() as BackgroundThreadError
                result = user_future.result()

                # done with userland
                logger.debug(
                    "%s exiting user-space...",
                    invocation_input.durable_execution_arn,
                )
                serialized_result = json.dumps(result)
                # large response handling here. Remember if checkpointing to complete, NOT to include
                # payload in response
                if (
                    serialized_result
                    and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT
                ):
                    logger.debug(
                        "Response size (%s bytes) exceeds Lambda limit (%s) bytes). Checkpointing result.",
                        len(serialized_result),
                        LAMBDA_RESPONSE_SIZE_LIMIT,
                    )
                    success_operation = OperationUpdate.create_execution_succeed(
                        payload=serialized_result
                    )
                    # Checkpoint large result with blocking (is_sync=True, default).
                    # Must ensure the result is persisted before returning to Lambda.
                    # Large results exceed Lambda response limits and must be stored durably
                    # before the execution completes.
                    try:
                        execution_state.create_checkpoint(
                            success_operation, is_sync=True
                        )
                    except CheckpointError as e:
                        return handle_checkpoint_error(e).to_dict()
                    return DurableExecutionInvocationOutput.create_succeeded(
                        result=""
                    ).to_dict()

                return DurableExecutionInvocationOutput.create_succeeded(
                    result=serialized_result
                ).to_dict()

            except BackgroundThreadError as bg_error:
                # Background checkpoint system failed - propagated through CompletionEvent
                # Do not attempt to checkpoint anything, just terminate immediately
                if isinstance(bg_error.source_exception, BotoClientError):
                    logger.exception(
                        "Checkpoint processing failed",
                        extra=bg_error.source_exception.build_logger_extras(),
                    )
                    # Non-retryable Durable API errors (e.g., customer configuration issues,
                    # 4xx client errors) will never succeed on retry — fail the execution immediately.
                    if not bg_error.source_exception.is_retryable():
                        logger.exception(
                            "Non-retryable Durable API error from background thread. Must fail execution "
                            "without retry.",
                            extra=bg_error.source_exception.build_logger_extras(),
                        )
                        return DurableExecutionInvocationOutput(
                            status=InvocationStatus.FAILED,
                            error=ErrorObject.from_exception(bg_error.source_exception),
                        ).to_dict()
                else:
                    logger.exception("Checkpoint processing failed")
                raise bg_error.source_exception from bg_error

            except SuspendExecution:
                # User code suspended - stop background checkpointing thread
                logger.debug("Suspending execution...")
                return DurableExecutionInvocationOutput(
                    status=InvocationStatus.PENDING
                ).to_dict()

            except CheckpointError as e:
                # Checkpoint system is broken - stop background thread and exit immediately
                logger.exception(
                    "Checkpoint system failed",
                    extra=e.build_logger_extras(),
                )
                return handle_checkpoint_error(e).to_dict()
            except InvocationError as e:
                # Non-retryable Durable API errors (e.g., customer configuration issues,
                # 4xx client errors) will never succeed on retry — fail the execution immediately.
                if not e.is_retryable():
                    logger.exception(
                        "Non-retryable Durable API error. Must fail execution without retry.",
                        extra=e.build_logger_extras(),  # type: ignore[attr-defined]
                    )
                    return DurableExecutionInvocationOutput(
                        status=InvocationStatus.FAILED,
                        error=ErrorObject.from_exception(e),
                    ).to_dict()
                logger.exception("Invocation error. Must terminate.")
                # Throw the error to trigger Lambda retry
                raise
            except ExecutionError as e:
                logger.exception("Execution error. Must fail execution without retry.")
                return DurableExecutionInvocationOutput(
                    status=InvocationStatus.FAILED,
                    error=ErrorObject.from_exception(e),
                ).to_dict()
            except Exception as e:
                # all user-space errors go here
                logger.exception("Execution failed")

                result = DurableExecutionInvocationOutput(
                    status=InvocationStatus.FAILED, error=ErrorObject.from_exception(e)
                ).to_dict()

                serialized_result = json.dumps(result)

                if (
                    serialized_result
                    and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT
                ):
                    logger.debug(
                        "Response size (%s bytes) exceeds Lambda limit (%s) bytes). Checkpointing result.",
                        len(serialized_result),
                        LAMBDA_RESPONSE_SIZE_LIMIT,
                    )
                    failed_operation = OperationUpdate.create_execution_fail(
                        error=ErrorObject.from_exception(e)
                    )

                    # Checkpoint large result with blocking (is_sync=True, default).
                    # Must ensure the result is persisted before returning to Lambda.
                    # Large results exceed Lambda response limits and must be stored durably
                    # before the execution completes.
                    try:
                        execution_state.create_checkpoint_sync(failed_operation)
                    except CheckpointError as e:
                        return handle_checkpoint_error(e).to_dict()
                    return DurableExecutionInvocationOutput(
                        status=InvocationStatus.FAILED
                    ).to_dict()

                return result

    return wrapper


def handle_checkpoint_error(error: CheckpointError) -> DurableExecutionInvocationOutput:
    if error.is_retryable():
        raise error from None  # Terminate Lambda immediately and have it be retried
    return DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_exception(error)
    )
