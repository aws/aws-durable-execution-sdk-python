from __future__ import annotations

import contextlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.context import DurableContext, ExecutionState
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

if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

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

    def get_execution_operation(self) -> Operation:
        if len(self.operations) < 1:
            msg: str = "No durable operations found in initial execution state."
            raise DurableExecutionsError(msg)

        candidate = self.operations[0]
        if candidate.operation_type is not OperationType.EXECUTION:
            msg = f"First operation in initial execution state is not an execution operation: {candidate.operation_type}"
            raise DurableExecutionsError(msg)

        return candidate

    def get_input_payload(self) -> str | None:
        # TODO: are these None checks necessary? i.e will there always be execution_details with input_payload
        if execution_details := self.get_execution_operation().execution_details:
            return execution_details.input_payload

        return None

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
    is_local_runner: bool

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
            is_local_runner=input_dict.get("LocalRunner", False),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_dict(),
            "LocalRunner": self.is_local_runner,
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
            is_local_runner=invocation_input.is_local_runner,
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

    @classmethod
    def create_failed(
        cls, error: ErrorObject | None = None
    ) -> DurableExecutionInvocationOutput:
        """Create a failed invocation output."""
        return cls(status=InvocationStatus.FAILED, error=error)

    @classmethod
    def create_pending(cls) -> DurableExecutionInvocationOutput:
        return cls(status=InvocationStatus.PENDING)


# endregion Invocation models


# region execution lifecycle

SUPPRESS = True
RAISE = False


@dataclass(frozen=True)
class DurableExecutionLifecycleStep(contextlib.AbstractContextManager):
    result: ContextVar[DurableExecutionInvocationOutput]
    operation_update: ContextVar[OperationUpdate]

    def set_result_success(self, payload: str) -> bool:
        self.result.set(DurableExecutionInvocationOutput.create_succeeded(payload))
        return SUPPRESS

    def set_result_failed(self, error: ErrorObject | None = None) -> bool:
        self.result.set(DurableExecutionInvocationOutput.create_failed(error))
        return SUPPRESS

    def set_result_failed_from_exception(self, exception: Exception) -> bool:
        return self.set_result_failed(ErrorObject.from_exception(exception))

    def set_result_pending(self) -> bool:
        self.result.set(DurableExecutionInvocationOutput.create_pending())
        return SUPPRESS

    def set_operation_update_success(self, payload: str) -> bool:
        self.operation_update.set(
            OperationUpdate.create_execution_succeed(payload=payload)
        )
        return SUPPRESS

    def set_operation_update_failed(self, error: ErrorObject) -> bool:
        self.operation_update.set(OperationUpdate.create_execution_fail(error=error))
        return SUPPRESS


@dataclass(frozen=True)
class HandleCheckpointErrors(DurableExecutionLifecycleStep):
    def __exit__(self, exc_type, exc_value, *_):
        if isinstance(exc_value, CheckpointError):
            if exc_value.is_retriable():  # then we raise
                return RAISE
            return self.set_result_failed_from_exception(exc_value)
        return RAISE


@dataclass(frozen=True)
class FlushCheckpoints(DurableExecutionLifecycleStep):
    execution_state: ExecutionState

    def __exit__(self, exc_type, exc_value, traceback):
        if operation_update := self.operation_update.get(None):
            self.execution_state.create_checkpoint(operation_update, is_sync=True)


@dataclass(frozen=True)
class EnsureLargeResponsesAreCheckpointed(DurableExecutionLifecycleStep):
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            return RAISE

        result = self.result.get(None)
        if result is None:
            return

        try:
            serialized_result = json.dumps(result.to_dict())
        except Exception as e:  # noqa BLE001
            return self.set_result_failed_from_exception(e)

        if not serialized_result or len(serialized_result) < LAMBDA_RESPONSE_SIZE_LIMIT:
            return

        # we serialized, we now need to create the operation update, and the new result
        match result:
            case DurableExecutionInvocationOutput(
                InvocationStatus.FAILED, None, error
            ) if error is not None:
                self.set_operation_update_failed(error)
                return self.set_result_failed()

            case DurableExecutionInvocationOutput(
                InvocationStatus.SUCCEEDED, result, None
            ):
                self.set_result_success("")
                return self.set_operation_update_success(result)
            case _:
                # invocation status is pending, and size never exceeds Lambda Resp limit
                return
        return


@dataclass(frozen=True)
class HandleBackgroundSystemFailures(DurableExecutionLifecycleStep):
    def __exit__(self, exc_type, exc_value, traceback):
        if isinstance(exc_value, BackgroundThreadError):
            # Background checkpoint system failed - propagated through CompletionEvent
            source = exc_value.source_exception
            if isinstance(source, BotoClientError):
                logger.exception(
                    "Checkpoint processing failed",
                    extra=source.build_logger_extras(),
                )
            else:
                logger.exception("Checkpoint processing failed")
            raise source

        if isinstance(exc_value, BotoClientError):
            logger.exception(
                "Checkpoint system failed",
                extra=exc_value.build_logger_extras(),
            )

        return RAISE


@dataclass(frozen=True)
class MapExecutionErrors(DurableExecutionLifecycleStep):
    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            return

        if isinstance(  # noqa UP038
            exc_value,
            (InvocationError, CheckpointError, BackgroundThreadError),
        ):
            # raise, can't convert to result
            return RAISE

        if isinstance(exc_value, SuspendExecution):
            # User code suspended - stop background checkpointing thread
            logger.debug("Suspending execution...")
            return self.set_result_pending()

        if isinstance(exc_value, ExecutionError):
            logger.exception("Execution error. Must terminate without retry.")
            return self.set_result_failed_from_exception(exc_value)

        return self.set_result_failed_from_exception(exc_value)


def durable_execution(
    func: Callable[[Any, DurableContext], Any],
) -> Callable[[Any, LambdaContext], Any]:
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
            logger.debug("durableExecutionArn: %s", event.get("DurableExecutionArn"))
            invocation_input = DurableExecutionInvocationInput.from_dict(event)

            service_client = (
                LambdaClient.initialize_local_runner_client()
                if invocation_input.is_local_runner
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
        )

        execution_state.fetch_paginated_operations(
            invocation_input.initial_execution_state.operations,
            invocation_input.checkpoint_token,
            invocation_input.initial_execution_state.next_marker,
        )

        durable_context: DurableContext = DurableContext.from_lambda_context(
            state=execution_state, lambda_context=context
        )

        operation_update_var: ContextVar[OperationUpdate] = ContextVar(
            "OperationUpdate"
        )
        result_var: ContextVar[DurableExecutionInvocationOutput] = ContextVar(
            "ExecutionResult"
        )

        # read from the end going up
        exit_stack = contextlib.ExitStack()
        # 6. finally we close checkpoint process
        exit_stack.push(contextlib.closing(execution_state))
        # 5. we then handle the checkpoint errors, retrying on retriable
        # or terminating execution otherwise by setting the result
        exit_stack.push(HandleCheckpointErrors(result_var, operation_update_var))
        # 4. we handle any checkpoint issues that have come up during execution or flushing
        exit_stack.push(
            HandleBackgroundSystemFailures(result_var, operation_update_var)
        )
        # 3. we flush the checkpoints as needed to store
        exit_stack.push(
            FlushCheckpoints(result_var, operation_update_var, execution_state)
        )
        # 2. we then checkpoint large responses that exceed Lambda Response capacity
        exit_stack.push(
            EnsureLargeResponsesAreCheckpointed(result_var, operation_update_var)
        )
        # 1. first we convert Execution errors into output objects, leaving
        # us with checkpoint / background errors and invocation errors
        exit_stack.push(MapExecutionErrors(result_var, operation_update_var))

        # Use ThreadPoolExecutor for concurrent execution of user code and background checkpoint processing
        with (
            ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="dex-handler"
            ) as executor,
            exit_stack,
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

            # Background checkpointing errors will propagate through CompletionEvent.wait() as BackgroundThreadError
            result = user_future.result()

            # done with userland
            logger.debug(
                "%s exiting user-space...",
                invocation_input.durable_execution_arn,
            )
            # happy path terminates here
            serialized_result = json.dumps(result)
            result_var.set(
                DurableExecutionInvocationOutput.create_succeeded(
                    result=serialized_result
                )
            )

        return result_var.get().to_dict()

    return wrapper


def handle_checkpoint_error(error: CheckpointError) -> DurableExecutionInvocationOutput:
    if error.is_retriable():
        raise error from None  # Terminate Lambda immediately and have it be retried
    return DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_exception(error)
    )
