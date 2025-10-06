from __future__ import annotations

import datetime
import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import boto3  # type: ignore

from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    CheckpointError,
)

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier

logger = logging.getLogger(__name__)


# region model
class OperationAction(Enum):
    START = "START"
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"
    RETRY = "RETRY"
    CANCEL = "CANCEL"


class OperationStatus(Enum):
    STARTED = "STARTED"
    PENDING = "PENDING"
    READY = "READY"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    STOPPED = "STOPPED"


class OperationType(Enum):
    EXECUTION = "EXECUTION"
    CONTEXT = "CONTEXT"
    STEP = "STEP"
    WAIT = "WAIT"
    CALLBACK = "CALLBACK"
    INVOKE = "INVOKE"


class OperationSubType(Enum):
    STEP = "Step"
    WAIT = "Wait"
    CALLBACK = "Callback"
    RUN_IN_CHILD_CONTEXT = "RunInChildContext"
    MAP = "Map"
    MAP_ITERATION = "MapIteration"
    PARALLEL = "Parallel"
    PARALLEL_BRANCH = "ParallelBranch"
    WAIT_FOR_CALLBACK = "WaitForCallback"
    WAIT_FOR_CONDITION = "WaitForCondition"
    INVOKE = "Invoke"


@dataclass(frozen=True)
class ExecutionDetails:
    input_payload: str | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ExecutionDetails:
        return cls(input_payload=data.get("InputPayload"))


@dataclass(frozen=True)
class ContextDetails:
    replay_children: bool = False
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ContextDetails:
        error_raw = data.get("Error")
        return cls(
            replay_children=data.get("ReplayChildren", False),
            result=data.get("Result"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )


@dataclass(frozen=True)
class ErrorObject:
    message: str | None
    type: str | None
    data: str | None
    stack_trace: list[str] | None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ErrorObject:
        return cls(
            message=data.get("ErrorMessage"),
            type=data.get("ErrorType"),
            data=data.get("ErrorData"),
            stack_trace=data.get("StackTrace"),
        )

    @classmethod
    def from_exception(cls, exception: Exception) -> ErrorObject:
        return cls(
            message=str(exception),
            type=type(exception).__name__,
            data=None,
            stack_trace=None,
        )

    @classmethod
    def from_message(cls, message: str) -> ErrorObject:
        return cls(
            message=message,
            type=None,
            data=None,
            stack_trace=None,
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {}
        if self.message is not None:
            result["ErrorMessage"] = self.message
        if self.type is not None:
            result["ErrorType"] = self.type
        if self.data is not None:
            result["ErrorData"] = self.data
        if self.stack_trace is not None:
            result["StackTrace"] = self.stack_trace
        return result

    def to_callable_runtime_error(self) -> CallableRuntimeError:
        return CallableRuntimeError(
            message=self.message,
            error_type=self.type,
            data=self.data,
            stack_trace=self.stack_trace,
        )


@dataclass(frozen=True)
class StepDetails:
    attempt: int = 0
    next_attempt_timestamp: datetime.datetime | None = None
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> StepDetails:
        error_raw = data.get("Error")
        return cls(
            attempt=data.get("Attempt", 0),
            next_attempt_timestamp=data.get(
                "NextAttemptTimestamp"
            ),  # TODO: how is this serialized? Unix or ISO 8601?
            result=data.get("Result"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )


@dataclass(frozen=True)
class WaitDetails:
    scheduled_timestamp: datetime.datetime | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> WaitDetails:
        return cls(scheduled_timestamp=data.get("ScheduledTimestamp"))


@dataclass(frozen=True)
class CallbackDetails:
    callback_id: str
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> CallbackDetails:
        error_raw = data.get("Error")
        return cls(
            callback_id=data["CallbackId"],
            result=data.get("Result"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )


@dataclass(frozen=True)
class InvokeDetails:
    durable_execution_arn: str
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> InvokeDetails:
        error_raw = data.get("Error")
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            result=data.get("Result"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )


@dataclass(frozen=True)
class StepOptions:
    next_attempt_delay_seconds: int = 0

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "NextAttemptDelaySeconds": self.next_attempt_delay_seconds,
        }


@dataclass(frozen=True)
class WaitOptions:
    wait_seconds: int = 0

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> WaitOptions:
        return cls(wait_seconds=data.get("WaitSeconds", 0))

    def to_dict(self) -> MutableMapping[str, Any]:
        return {"WaitSeconds": self.wait_seconds}


@dataclass(frozen=True)
class CallbackOptions:
    timeout_seconds: int = 0
    heartbeat_timeout_seconds: int = 0

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "TimeoutSeconds": self.timeout_seconds,
            "HeartbeatTimeoutSeconds": self.heartbeat_timeout_seconds,
        }


@dataclass(frozen=True)
class InvokeOptions:
    function_name: str
    timeout_seconds: int = 0

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {"FunctionName": self.function_name}
        result["TimeoutSeconds"] = self.timeout_seconds
        return result


@dataclass(frozen=True)
class ContextOptions:
    replay_children: bool = False

    def to_dict(self) -> MutableMapping[str, Any]:
        return {"ReplayChildren": self.replay_children}


@dataclass(frozen=True)
class OperationUpdate:
    """Update an Operation. Use this to create a checkpoint.

    See the various create_ factory class methods to instantiate me.
    """

    operation_id: str
    operation_type: OperationType
    action: OperationAction
    parent_id: str | None = None
    name: str | None = None
    sub_type: OperationSubType | None = None
    payload: str | None = None
    error: ErrorObject | None = None
    context_options: ContextOptions | None = None
    step_options: StepOptions | None = None
    wait_options: WaitOptions | None = None
    callback_options: CallbackOptions | None = None
    invoke_options: InvokeOptions | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {
            "Id": self.operation_id,
            "Type": self.operation_type.value,
            "Action": self.action.value,
        }

        if self.parent_id:
            result["ParentId"] = self.parent_id
        if self.name:
            result["Name"] = self.name
        if self.sub_type:
            result["SubType"] = self.sub_type.value
        if self.payload:
            result["Payload"] = self.payload
        if self.error:
            result["Error"] = self.error.to_dict()
        if self.context_options:
            result["ContextOptions"] = self.context_options.to_dict()
        if self.step_options:
            result["StepOptions"] = self.step_options.to_dict()
        if self.wait_options:
            result["WaitOptions"] = self.wait_options.to_dict()
        if self.callback_options:
            result["CallbackOptions"] = self.callback_options.to_dict()
        if self.invoke_options:
            result["InvokeOptions"] = self.invoke_options.to_dict()

        return result

    @classmethod
    def create_callback(
        cls, identifier: OperationIdentifier, callback_options: CallbackOptions
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type:CALLBACK, action:START"""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CALLBACK,
            sub_type=OperationSubType.CALLBACK,
            action=OperationAction.START,
            name=identifier.name,
            callback_options=callback_options,
        )

    # region context
    @classmethod
    def create_context_start(
        cls, identifier: OperationIdentifier, sub_type: OperationSubType
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: CONTEXT, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CONTEXT,
            sub_type=sub_type,
            action=OperationAction.START,
            name=identifier.name,
        )

    @classmethod
    def create_context_succeed(
        cls,
        identifier: OperationIdentifier,
        payload: str,
        sub_type: OperationSubType,
        context_options: ContextOptions | None = None,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: CONTEXT, action: SUCCEED."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CONTEXT,
            sub_type=sub_type,
            action=OperationAction.SUCCEED,
            name=identifier.name,
            payload=payload,
            context_options=context_options,
        )

    @classmethod
    def create_context_fail(
        cls,
        identifier: OperationIdentifier,
        error: ErrorObject,
        sub_type: OperationSubType,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: CONTEXT, action: FAIL."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CONTEXT,
            sub_type=sub_type,
            action=OperationAction.FAIL,
            name=identifier.name,
            error=error,
        )

    # endregion context

    # region execution
    @classmethod
    def create_execution_succeed(cls, payload: str) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: EXECUTION, action: SUCCEED."""
        return cls(
            operation_id=f"execution-result-{datetime.datetime.now(tz=datetime.UTC)}",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
            payload=payload,
        )

    @classmethod
    def create_execution_fail(cls, error: ErrorObject) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: EXECUTION, action: FAIL."""
        return cls(
            operation_id=f"execution-result-{datetime.datetime.now(tz=datetime.UTC)}",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.FAIL,
            error=error,
        )

    # endregion execution

    # region step
    @classmethod
    def create_step_succeed(
        cls, identifier: OperationIdentifier, payload: str
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: SUCCEED."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.SUCCEED,
            name=identifier.name,
            payload=payload,
        )

    @classmethod
    def create_step_fail(
        cls, identifier: OperationIdentifier, error: ErrorObject
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: FAIL."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.FAIL,
            name=identifier.name,
            error=error,
        )

    @classmethod
    def create_step_start(cls, identifier: OperationIdentifier) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.START,
            name=identifier.name,
        )

    @classmethod
    def create_step_retry(
        cls,
        identifier: OperationIdentifier,
        error: ErrorObject,
        next_attempt_delay_seconds: int,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: RETRY."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.RETRY,
            name=identifier.name,
            error=error,
            step_options=StepOptions(
                next_attempt_delay_seconds=next_attempt_delay_seconds
            ),
        )

    # endregion step

    # region invoke
    @classmethod
    def create_invoke_start(
        cls,
        identifier: OperationIdentifier,
        payload: str,
        invoke_options: InvokeOptions,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: INVOKE, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.INVOKE,
            sub_type=OperationSubType.INVOKE,
            action=OperationAction.START,
            name=identifier.name,
            payload=payload,
            invoke_options=invoke_options,
        )

    # endregion invoke

    # region wait for condition
    @classmethod
    def create_wait_for_condition_start(
        cls, identifier: OperationIdentifier
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.START,
            name=identifier.name,
        )

    @classmethod
    def create_wait_for_condition_succeed(
        cls, identifier: OperationIdentifier, payload: str
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: SUCCEED."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.SUCCEED,
            name=identifier.name,
            payload=payload,
        )

    @classmethod
    def create_wait_for_condition_retry(
        cls,
        identifier: OperationIdentifier,
        payload: str,
        next_attempt_delay_seconds: int,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: RETRY."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.RETRY,
            name=identifier.name,
            payload=payload,
            step_options=StepOptions(
                next_attempt_delay_seconds=next_attempt_delay_seconds
            ),
        )

    @classmethod
    def create_wait_for_condition_fail(
        cls, identifier: OperationIdentifier, error: ErrorObject
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: FAIL."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.FAIL,
            name=identifier.name,
            error=error,
        )

    # endregion wait for condition

    # region wait
    @classmethod
    def create_wait_start(
        cls, identifier: OperationIdentifier, wait_options: WaitOptions
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: WAIT, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            action=OperationAction.START,
            name=identifier.name,
            wait_options=wait_options,
        )

    # endregion wait


@dataclass(frozen=True)
class Operation:
    """Represent the Operation type for GetDurableExecutionState and CheckpointDurableExecution."""

    operation_id: str
    operation_type: OperationType
    status: OperationStatus
    parent_id: str | None = None
    name: str | None = None
    start_timestamp: datetime.datetime | None = None
    end_timestamp: datetime.datetime | None = None
    sub_type: OperationSubType | None = None
    execution_details: ExecutionDetails | None = None
    context_details: ContextDetails | None = None
    step_details: StepDetails | None = None
    wait_details: WaitDetails | None = None
    callback_details: CallbackDetails | None = None
    invoke_details: InvokeDetails | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> Operation:
        """Create an Operation instance from a dictionary with the original Smithy model field names.

        Args:
            data: Dictionary with camelCase keys matching the Smithy model

        Returns:
            An Operation instance with snake_case attributes
        """
        operation_type = OperationType(data.get("Type"))
        operation_status = OperationStatus(data.get("Status"))

        sub_type = None
        if sub_type_input := data.get("SubType"):
            sub_type = OperationSubType(sub_type_input)

        execution_details = None
        if execution_details_input := data.get("ExecutionDetails"):
            execution_details = ExecutionDetails.from_dict(execution_details_input)

        context_details = None
        if context_details_input := data.get("ContextDetails"):
            context_details = ContextDetails.from_dict(context_details_input)

        step_details = None
        if step_details_input := data.get("StepDetails"):
            step_details = StepDetails.from_dict(step_details_input)

        wait_details = None
        if wait_details_input := data.get("WaitDetails"):
            wait_details = WaitDetails.from_dict(wait_details_input)

        callback_details = None
        if callback_details_input := data.get("CallbackDetails"):
            callback_details = CallbackDetails.from_dict(callback_details_input)

        invoke_details = None
        if invoke_details_input := data.get("InvokeDetails"):
            invoke_details = InvokeDetails.from_dict(invoke_details_input)

        return cls(
            operation_id=data["Id"],
            operation_type=operation_type,
            status=operation_status,
            parent_id=data.get("ParentId"),
            name=data.get("Name"),
            start_timestamp=data.get("StartTimestamp"),
            end_timestamp=data.get("EndTimestamp"),
            sub_type=sub_type,
            execution_details=execution_details,
            context_details=context_details,
            step_details=step_details,
            wait_details=wait_details,
            callback_details=callback_details,
            invoke_details=invoke_details,
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {
            "Id": self.operation_id,
            "Type": self.operation_type.value,
            "Status": self.status.value,
        }
        if self.parent_id:
            result["ParentId"] = self.parent_id
        if self.name:
            result["Name"] = self.name
        if self.start_timestamp:
            result["StartTimestamp"] = self.start_timestamp
        if self.end_timestamp:
            result["EndTimestamp"] = self.end_timestamp
        if self.sub_type:
            result["SubType"] = self.sub_type.value
        if self.execution_details:
            result["ExecutionDetails"] = {
                "InputPayload": self.execution_details.input_payload
            }
        if self.context_details:
            result["ContextDetails"] = {"Result": self.context_details.result}
        if self.step_details:
            step_dict: MutableMapping[str, Any] = {"Attempt": self.step_details.attempt}
            if self.step_details.next_attempt_timestamp:
                step_dict["NextAttemptTimestamp"] = (
                    self.step_details.next_attempt_timestamp
                )
            if self.step_details.result:
                step_dict["Result"] = self.step_details.result
            if self.step_details.error:
                step_dict["Error"] = self.step_details.error.to_dict()
            result["StepDetails"] = step_dict
        if self.wait_details:
            result["WaitDetails"] = {
                "ScheduledTimestamp": self.wait_details.scheduled_timestamp
            }
        if self.callback_details:
            callback_dict: MutableMapping[str, Any] = {
                "CallbackId": self.callback_details.callback_id
            }
            if self.callback_details.result:
                callback_dict["Result"] = self.callback_details.result
            if self.callback_details.error:
                callback_dict["Error"] = self.callback_details.error.to_dict()
            result["CallbackDetails"] = callback_dict
        if self.invoke_details:
            invoke_dict: MutableMapping[str, Any] = {
                "DurableExecutionArn": self.invoke_details.durable_execution_arn
            }
            if self.invoke_details.result:
                invoke_dict["Result"] = self.invoke_details.result
            if self.invoke_details.error:
                invoke_dict["Error"] = self.invoke_details.error.to_dict()
            result["InvokeDetails"] = invoke_dict
        return result


@dataclass(frozen=True)
class CheckpointUpdatedExecutionState:
    """Representation of the CheckpointUpdatedExecutionState structure of the DEX API."""

    operations: list[Operation] = field(default_factory=list)
    next_marker: str | None = None

    @classmethod
    def from_dict(
        cls, data: MutableMapping[str, Any]
    ) -> CheckpointUpdatedExecutionState:
        """Create an instance from a dictionary with the original Smithy model field names.

        Args:
            data: Dictionary with camelCase keys matching the Smithy model

        Returns:
            Instance of the current class.
        """
        operations = []
        if input_operations := data.get("Operations"):
            operations = [Operation.from_dict(op) for op in input_operations]

        return cls(operations=operations, next_marker=data.get("NextMarker"))


@dataclass(frozen=True)
class CheckpointOutput:
    """Representation of the CheckpointDurableExecutionOutput structure of the DEX CheckpointDurableExecution API."""

    checkpoint_token: str
    new_execution_state: CheckpointUpdatedExecutionState

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> CheckpointOutput:
        """Create an instance from a dictionary with the original Smithy model field names.

        Args:
            data: Dictionary with camelCase keys matching the Smithy model

        Returns:
            A CheckpointDurableExecutionOutput instance.
        """
        new_execution_state = None
        if input_execution_state := data.get("NewExecutionState"):
            new_execution_state = CheckpointUpdatedExecutionState.from_dict(
                input_execution_state
            )
        else:
            # Provide an empty default if not present
            new_execution_state = CheckpointUpdatedExecutionState()

        return cls(
            # TODO: maybe should throw if empty?
            checkpoint_token=data.get("CheckpointToken", ""),
            new_execution_state=new_execution_state,
        )


@dataclass(frozen=True)
class StateOutput:
    """Representation of the GetDurableExecutionStateOutput structure of the DEX GetDurableExecutionState API."""

    operations: list[Operation] = field(default_factory=list)
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> StateOutput:
        """Create a GetDurableExecutionStateOutput instance from a dictionary with the original Smithy model field names.

        Args:
            data: Dictionary with camelCase keys matching the Smithy model

        Returns:
            A GetDurableExecutionStateOutput instance.
        """
        operations = []
        if input_operations := data.get("Operations"):
            operations = [Operation.from_dict(op) for op in input_operations]

        return cls(operations=operations, next_marker=data.get("NextMarker"))


# endregion model


# region client
class DurableServiceClient(Protocol):
    """Durable Service clients must implement this interface."""

    def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput: ...  # pragma: no cover

    def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput: ...  # pragma: no cover

    def stop(
        self, execution_arn: str, payload: bytes | None
    ) -> datetime.datetime: ...  # pragma: no cover


class LambdaClient(DurableServiceClient):
    """Persist durable operations to the Lambda Durable Function APIs."""

    def __init__(self, client: Any) -> None:
        self.client = client

    @staticmethod
    def load_preview_botocore_models() -> None:
        """
        Load boto3 models from the Python path for custom preview client.
        """
        os.environ["AWS_DATA_PATH"] = str(
            Path(__file__).parent.joinpath("botocore", "data")
        )

    @staticmethod
    def initialize_local_runner_client() -> LambdaClient:
        endpoint = os.getenv(
            "DURABLE_LOCAL_RUNNER_ENDPOINT", "http://host.docker.internal:5000"
        )
        region = os.getenv("DURABLE_LOCAL_RUNNER_REGION", "us-west-2")

        # The local runner client needs execute-api as the signing service name,
        # so we have a second `lambdainternal-local` boto model with this.
        LambdaClient.load_preview_botocore_models()
        client = boto3.client(
            "lambdainternal-local",
            endpoint_url=endpoint,
            region_name=region,
        )

        logger.debug(
            "Initialized lambda client with endpoint: '%s', region: '%s'",
            endpoint,
            region,
        )
        return LambdaClient(client=client)

    @staticmethod
    def initialize_from_env() -> LambdaClient:
        LambdaClient.load_preview_botocore_models()
        client = boto3.client(
            "lambdainternal",
        )

        logger.debug("Initialized lambda client")
        return LambdaClient(client=client)

    def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        try:
            params = {
                "DurableExecutionArn": durable_execution_arn,
                "CheckpointToken": checkpoint_token,
                "Updates": [o.to_dict() for o in updates],
            }
            if client_token is not None:
                params["ClientToken"] = client_token

            result: MutableMapping[str, Any] = self.client.checkpoint_durable_execution(
                **params
            )

            return CheckpointOutput.from_dict(result)
        except Exception as e:
            logger.exception("Failed to checkpoint.")
            raise CheckpointError(e) from e

    def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        result: MutableMapping[str, Any] = self.client.get_durable_execution_state(
            DurableExecutionArn=durable_execution_arn,
            CheckpointToken=checkpoint_token,
            Marker=next_marker,
            MaxItems=max_items,
        )
        return StateOutput.from_dict(result)

    def stop(self, execution_arn: str, payload: bytes | None) -> datetime.datetime:
        result: MutableMapping[str, Any] = self.client.stop_durable_execution(
            ExecutionArn=execution_arn, Payload=payload
        )

        # presumably lambda throws if execution_arn not found? this line will throw if stopDate isn't in response
        return result["StopDate"]


# endregion client
