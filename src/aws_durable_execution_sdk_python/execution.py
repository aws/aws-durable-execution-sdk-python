from __future__ import annotations

import contextlib
import functools
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
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
    DurableServiceClient,
    ErrorObject,
    LambdaClient,
    Operation,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus

if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

    import boto3  # type: ignore

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

    def get_execution_operation(self) -> Operation | None:
        if not self.operations:
            # Due to payload size limitations we may have an empty operations list.
            # This will only happen when loading the initial page of results and is
            # expected behaviour. We don't fail, but instead return None
            # as the execution operation does not exist
            msg: str = "No durable operations found in initial execution state."
            logger.debug(msg)
            return None

        candidate = self.operations[0]
        if candidate.operation_type is not OperationType.EXECUTION:
            msg = f"First operation in initial execution state is not an execution operation: {candidate.operation_type}"
            raise DurableExecutionsError(msg)

        return candidate

    def get_input_payload(self) -> str | None:
        # It is possible that backend will not provide an execution operation
        # for the initial page of results.
        if not (operations := self.get_execution_operation()):
            return None
        if not (execution_details := operations.execution_details):
            return None
        return execution_details.input_payload

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "Operations": [op.to_dict() for op in self.operations],
            "NextMarker": self.next_marker,
        }


@dataclass(frozen=True)
class DurableExecutionInvocationInput:
    durable_execution_arn: str
    checkpoint_token: str
    initial_execution_state: InitialExecutionState

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
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_dict(),
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
            service_client=service_client,
        )


class InvocationStatus(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"


@dataclass(frozen=True)
class DurableExecutionInvocationOutput:
    """Representation the DurableExecutionInvocationOutput. This is what the Durable lambda handler returns.

    If the execution has been already completed via an update to the EXECUTION operation via CheckpointDurableExecution,
    payload must be empty for SUCCEEDED/FAILED status.
    """

    status: InvocationStatus
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(
        cls, data: MutableMapping[str, Any]
    ) -> DurableExecutionInvocationOutput:
        """Create an instance from a dictionary.

        Args:
            data: Dictionary with camelCase keys matching the original structure

        Returns:
            A DurableExecutionInvocationOutput instance
        """
        status = InvocationStatus(data.get("Status"))
        error = ErrorObject.from_dict(data["Error"]) if data.get("Error") else None
        return cls(status=status, result=data.get("Result"), error=error)

    def to_dict(self) -> MutableMapping[str, Any]:
        """Convert to a dictionary with the original field names.

        Returns:
            Dictionary with the original camelCase keys
        """
        result: MutableMapping[str, Any] = {"Status": self.status.value}

        if self.result is not None:
            # large payloads return "", because checkpointed already
            result["Result"] = self.result
        if self.error:
            result["Error"] = self.error.to_dict()

        return result

    @classmethod
    def create_succeeded(cls, result: str) -> DurableExecutionInvocationOutput:
        """Create a succeeded invocation output."""
        return cls(status=InvocationStatus.SUCCEEDED, result=result)


# endregion Invocation models


def durable_execution(
    func: Callable[[Any, DurableContext], Any] | None = None,
    *,
    boto3_client: boto3.client | None = None,
) -> Callable[[Any, LambdaContext], Any]:
    # Decorator called with parameters
    if func is None:
        logger.debug("Decorator called with parameters")
        return functools.partial(durable_execution, boto3_client=boto3_client)

    logger.debug("Starting durable execution handler...")

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
                invocation_input = DurableExecutionInvocationInput.from_dict(event)
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
                else LambdaClient.initialize_from_env()
            )

        raw_input_payload: str | None = (
            invocation_input.initial_execution_state.get_input_payload()
        )

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

        execution_state: ExecutionState = ExecutionState(
            durable_execution_arn=invocation_input.durable_execution_arn,
            initial_checkpoint_token=invocation_input.checkpoint_token,
            operations={},
            service_client=service_client,
            # If there are operations other than the initial EXECUTION one, current state is in replay mode
            replay_status=ReplayStatus.REPLAY
            if len(invocation_input.initial_execution_state.operations) > 1
            else ReplayStatus.NEW,
        )

        execution_state.fetch_paginated_operations(
            invocation_input.initial_execution_state.operations,
            invocation_input.checkpoint_token,
            invocation_input.initial_execution_state.next_marker,
        )

        durable_context: DurableContext = DurableContext.from_lambda_context(
            state=execution_state, lambda_context=context
        )

        # Use ThreadPoolExecutor for concurrent execution of user code and background checkpoint processing
        with (
            ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="dex-handler"
            ) as executor,
            contextlib.closing(execution_state) as execution_state,
        ):
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
                else:
                    logger.exception("Checkpoint processing failed")
                # handle the original exception
                if isinstance(bg_error.source_exception, CheckpointError):
                    return handle_checkpoint_error(bg_error.source_exception).to_dict()
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
            except InvocationError:
                logger.exception("Invocation error. Must terminate.")
                # Throw the error to trigger Lambda retry
                raise
            except ExecutionError as e:
                logger.exception("Execution error. Must terminate without retry.")
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
    if error.is_retriable():
        raise error from None  # Terminate Lambda immediately and have it be retried
    return DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_exception(error)
    )
