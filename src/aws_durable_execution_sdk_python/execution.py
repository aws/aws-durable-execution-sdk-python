from __future__ import annotations

import contextlib
import functools
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, cast, Callable

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    BotoClientError,
    CheckpointError,
    ExecutionError,
    InvocationError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    DurableServiceClient,
    ErrorObject,
    LambdaClient,
    Operation,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus

if TYPE_CHECKING:
    from collections.abc import MutableMapping

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
        operations = [
            Operation.from_dict(op) for op in input_dict.get("Operations", [])
        ]
        return InitialExecutionState(
            operations=operations,
            next_marker=input_dict.get("NextMarker", ""),
        )

    @staticmethod
    def from_json_dict(input_dict: MutableMapping[str, Any]) -> InitialExecutionState:
        operations = [
            Operation.from_json_dict(op) for op in input_dict.get("Operations", [])
        ]
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
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_dict(),
        }

    def to_json_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_json_dict(),
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


# endregion Invocation models


def durable_execution(
    func: Callable[[Any, DurableContext], Any] | None = None,
    *,
    boto3_client: Boto3LambdaClient | None = None,
) -> Callable[[Any, LambdaContext], Any]:
    # Decorator called with parameters
    if func is None:
        logger.debug("Decorator called with parameters")
        return functools.partial(durable_execution, boto3_client=boto3_client)
    else:
        logger.debug("Starting durable execution handler...")

        def wrapper(event: Any, context: LambdaContext) -> MutableMapping[str, Any]:
            executor = DurableExecutionExecutor(
                cast(Callable[[Any, DurableContext], Any], func),
                boto3_client,
                event,
                context,
            )
            return executor.execute()

        return wrapper


class DurableExecutionExecutor:
    def __init__(
        self,
        func: Callable[[Any, DurableContext], Any],
        boto3_client: Boto3LambdaClient | None,
        event: Any,
        context: LambdaContext,
    ):
        self.func = func
        self.boto3_client = boto3_client
        self.event = event
        self.context = context
        self.invocation_input = self._parse_invocation_input(event)
        self.service_client = self._parse_service_client(event, boto3_client)

    def _parse_invocation_input(self, event: Any) -> DurableExecutionInvocationInput:
        # event likely only to be DurableExecutionInvocationInputWithClient when directly injected by test framework
        invocation_input: (
            DurableExecutionInvocationInputWithClient | DurableExecutionInvocationInput
        )
        if isinstance(event, DurableExecutionInvocationInputWithClient):
            invocation_input = event
        else:
            try:
                invocation_input = DurableExecutionInvocationInput.from_json_dict(event)
            except (KeyError, TypeError, AttributeError):
                msg = (
                    "Unexpected payload provided to start the durable execution. "
                    "Check your resource configurations to confirm the durability is set."
                )
                # throws ExecutionError to terminate the invocation
                self._handle_execution_output(
                    exception=ExecutionError(msg), retryable=True
                )
                # add a redundant raise to make type checker happy
                raise ExecutionError(msg)

        logger.debug("durableExecutionArn: %s", invocation_input.durable_execution_arn)
        return invocation_input

    @staticmethod
    def _parse_service_client(event, boto3_client):
        if isinstance(event, DurableExecutionInvocationInputWithClient):
            return event.service_client
        elif boto3_client:
            return LambdaClient(boto3_client)
        else:
            # Use custom client if provided, otherwise initialize from environment
            return LambdaClient.initialize_client()

    def execute(self):
        execution_state: ExecutionState = ExecutionState(
            durable_execution_arn=self.invocation_input.durable_execution_arn,
            initial_checkpoint_token=self.invocation_input.checkpoint_token,
            operations={},
            service_client=self.service_client,
            replay_status=ReplayStatus.NEW,
        )

        try:
            execution_state.fetch_paginated_operations(
                self.invocation_input.initial_execution_state.operations,
                self.invocation_input.checkpoint_token,
                self.invocation_input.initial_execution_state.next_marker,
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
            return self._handle_execution_output(
                exception=e, retryable=e.is_retryable()
            )

        execution_state.mark_replaying_if_prior_operations_exist()

        raw_input_payload: str | None = execution_state.get_input_payload()

        # Python RIC LambdaMarshaller just uses standard json deserialization for event
        # https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/main/awslambdaric/lambda_runtime_marshaller.py#L46
        input_event: MutableMapping[str, Any] = {}
        if raw_input_payload and raw_input_payload.strip():
            try:
                input_event = json.loads(raw_input_payload)
            except json.JSONDecodeError as e:
                logger.exception(
                    "Failed to parse input payload as JSON: payload: %r",
                    raw_input_payload,
                )
                self._handle_execution_output(exception=e, retryable=True)

        durable_context: DurableContext = DurableContext.from_lambda_context(
            state=execution_state, lambda_context=self.context
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
                "%s entering user-space...", self.invocation_input.durable_execution_arn
            )
            user_future = executor.submit(self.func, input_event, durable_context)

            logger.debug(
                "%s waiting for user code completion...",
                self.invocation_input.durable_execution_arn,
            )

            try:
                # Background checkpointing errors will propagate through CompletionEvent.wait() as BackgroundThreadError
                result = user_future.result()

                # done with userland
                logger.debug(
                    "%s exiting user-space...",
                    self.invocation_input.durable_execution_arn,
                )
                serialized_result = self._handle_large_result(execution_state, result)

                return self._handle_execution_output(result=serialized_result)

            except BackgroundThreadError as bg_error:
                # Background checkpoint system failed - propagated through CompletionEvent
                # Do not attempt to checkpoint anything, just terminate immediately
                cause = bg_error.source_exception

                if isinstance(cause, BotoClientError):
                    logger.exception(
                        "Checkpoint processing failed",
                        extra=cause.build_logger_extras(),
                    )
                    # Non-retryable Durable API errors (e.g., customer configuration issues,
                    # 4xx client errors) will never succeed on retry — fail the execution immediately.
                    if not cause.is_retryable():
                        logger.exception(
                            "Non-retryable Durable API error from background thread. Must fail execution "
                            "without retry.",
                            extra=cause.build_logger_extras(),
                        )
                else:
                    logger.exception("Checkpoint processing failed")

                retryable = (
                    not isinstance(cause, BotoClientError) or cause.is_retryable()
                )
                return self._handle_execution_output(
                    exception=cause, retryable=retryable
                )

            except SuspendExecution:
                # User code suspended - stop background checkpointing thread
                logger.debug("Suspending execution...")
                return self._handle_execution_output(status=InvocationStatus.PENDING)

            except CheckpointError as e:
                # Checkpoint system is broken - stop background thread and exit immediately
                logger.exception(
                    "Checkpoint system failed",
                    extra=e.build_logger_extras(),
                )
                # Terminate Lambda invocation immediately and have it be retried if retryable
                return self._handle_execution_output(
                    exception=e, retryable=e.is_retryable()
                )
            except InvocationError as e:
                if e.is_retryable():
                    logger.exception("Invocation error. Must terminate.")
                else:
                    # Non-retryable Durable API errors (e.g., customer configuration issues,
                    # 4xx client errors) will never succeed on retry — fail the execution immediately.
                    logger.exception(
                        "Non-retryable Durable API error. Must fail execution without retry.",
                        extra=e.build_logger_extras(),  # type: ignore[attr-defined]
                    )
                return self._handle_execution_output(
                    exception=e, retryable=e.is_retryable()
                )
            except ExecutionError as e:
                logger.exception("Execution error. Must fail execution without retry.")
                return self._handle_execution_output(exception=e)
            except Exception as e:
                # all user-space errors go here
                logger.exception("Execution failed")

                try:
                    error = self._handle_large_error(execution_state, exception=e)
                except CheckpointError as e:
                    # Terminate Lambda invocation immediately and have it be retried if retryable
                    return self._handle_execution_output(
                        exception=e, retryable=e.is_retryable()
                    )

                # fail without an ErrorObject
                return self._handle_execution_output(
                    status=InvocationStatus.FAILED, error=error
                )

    @staticmethod
    def _handle_large_result(execution_state: ExecutionState, result: Any) -> str:
        # large response handling here. Remember if checkpointing to complete, NOT to include
        # payload in response
        serialized_result = json.dumps(result)
        if serialized_result and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT:
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
            execution_state.create_checkpoint(success_operation, is_sync=True)
            return ""

        return serialized_result

    @staticmethod
    def _handle_large_error(
        execution_state: ExecutionState, exception: Exception
    ) -> ErrorObject | None:
        # large response handling here. Remember if checkpointing to complete, NOT to include
        # payload in response
        error = ErrorObject.from_exception(exception)
        serialized_error = json.dumps(error.to_dict())
        if serialized_error and len(serialized_error) > LAMBDA_RESPONSE_SIZE_LIMIT:
            logger.debug(
                "Response size (%s bytes) exceeds Lambda limit (%s) bytes). Checkpointing result.",
                len(serialized_error),
                LAMBDA_RESPONSE_SIZE_LIMIT,
            )
            failed_operation = OperationUpdate.create_execution_fail(error=error)
            # Checkpoint large result with blocking (is_sync=True, default).
            # Must ensure the result is persisted before returning to Lambda.
            # Large results exceed Lambda response limits and must be stored durably
            # before the execution completes.
            execution_state.create_checkpoint_sync(failed_operation)

            # return fail without an ErrorObject
            return None

        return error

    def _handle_execution_output(
        self,
        result: str | None = None,
        error: ErrorObject | None = None,
        exception: Exception | None = None,
        retryable: bool = False,
        status: InvocationStatus | None = None,
    ) -> MutableMapping[str, Any]:
        if exception:
            if retryable:
                # Throw the error to trigger Lambda retry
                raise exception
            else:
                return self._handle_execution_output(
                    result=result,
                    error=ErrorObject.from_exception(exception),
                    status=status,
                )

        if error:
            output = DurableExecutionInvocationOutput(
                status=InvocationStatus.FAILED, result=result, error=error
            )
        elif result is not None:
            output = DurableExecutionInvocationOutput(
                status=InvocationStatus.SUCCEEDED, result=result
            )
        elif status:
            output = DurableExecutionInvocationOutput(status=status)
        else:
            raise ValueError("Unexpected durable execution output")
        return output.to_dict()
