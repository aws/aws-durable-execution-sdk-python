from __future__ import annotations

import copy
import datetime
import logging
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

import boto3
from botocore.config import Config

from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    CheckpointError,
    GetExecutionStateError,
)

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient as Boto3LambdaClient
    from mypy_boto3_lambda.type_defs import (
        CheckpointDurableExecutionResponseTypeDef,
        GetDurableExecutionStateResponseTypeDef,
    )

    from aws_durable_execution_sdk_python.identifier import OperationIdentifier

# Replace with `type` it when dropping support to Python 3.11
ReplayChildren: TypeAlias = bool
OperationPayload: TypeAlias = str
TimeoutSeconds: TypeAlias = int

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
    CHAINED_INVOKE = "CHAINED_INVOKE"


class CallbackTimeoutType(Enum):
    TIMEOUT = "Callback.Timeout"
    HEARTBEAT = "Callback.Heartbeat"


class ChainedInvokeFailedToStartType(Enum):
    FAILED_TO_START = "ChainedInvoke.FailedToStart"


class ChainedInvokeTimeoutType(Enum):
    TIMEOUT = "ChainedInvoke.Timeout"


class ChainedInvokeStopType(Enum):
    STOPPED = "ChainedInvoke.Stopped"


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
    CHAINED_INVOKE = "ChainedInvoke"


@dataclass(frozen=True)
class ExecutionDetails:
    input_payload: str | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ExecutionDetails:
        return cls(input_payload=data.get("InputPayload"))


@dataclass(frozen=True)
class ContextDetails:
    replay_children: ReplayChildren = False
    result: OperationPayload | None = None
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
    result: OperationPayload | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> StepDetails:
        error_raw = data.get("Error")
        return cls(
            attempt=data.get("Attempt", 0),
            next_attempt_timestamp=data.get("NextAttemptTimestamp"),
            result=data.get("Result"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )


@dataclass(frozen=True)
class WaitDetails:
    scheduled_end_timestamp: datetime.datetime | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> WaitDetails:
        return cls(scheduled_end_timestamp=data.get("ScheduledEndTimestamp"))


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
class ChainedInvokeDetails:
    result: str | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ChainedInvokeDetails:
        error_raw = data.get("Error")
        return cls(
            result=data.get("Result"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )


@dataclass(frozen=True)
class StepOptions:
    next_attempt_delay_seconds: int = 0

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> StepOptions:
        return cls(next_attempt_delay_seconds=data.get("NextAttemptDelaySeconds", 0))

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "NextAttemptDelaySeconds": self.next_attempt_delay_seconds,
        }


@dataclass(frozen=True)
class WaitOptions:
    """
    Wait Options provides details regarding suspension.

    As of 2025/10/27:

    - `wait_seconds` accepts values between 1, and 31622400
    - When wait_second seconds does not exist,then we default to 1

    """

    wait_seconds: int = 1

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> WaitOptions:
        return cls(wait_seconds=data.get("WaitSeconds", 1))

    def to_dict(self) -> MutableMapping[str, Any]:
        return {"WaitSeconds": self.wait_seconds}


@dataclass(frozen=True)
class CallbackOptions:
    """
    Callback options provides details about the callback, wrt timeout
    and heartbeat checks.

    As of 2025/10/27:
    - When timeout_seconds == 0, then the callback has no timeout
    - When heartbeat_timeout_seconds == 0, then the callback has no timeout

    - When timeout_seconds is not present, then default is 0
    - When heartbeat_timeout_seconds, then default is 0

    """

    timeout_seconds: TimeoutSeconds = 0
    heartbeat_timeout_seconds: int = 0

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> CallbackOptions:
        return cls(
            timeout_seconds=data.get("TimeoutSeconds", 0),
            heartbeat_timeout_seconds=data.get("HeartbeatTimeoutSeconds", 0),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "TimeoutSeconds": self.timeout_seconds,
            "HeartbeatTimeoutSeconds": self.heartbeat_timeout_seconds,
        }


@dataclass(frozen=True)
class ChainedInvokeOptions:
    """
    As of 2025/10/27:
     - Chained invoke options only contains a function name
    """

    function_name: str
    tenant_id: str | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ChainedInvokeOptions:
        return cls(
            function_name=data["FunctionName"],
            tenant_id=data.get("TenantId"),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {
            "FunctionName": self.function_name,
        }
        if self.tenant_id is not None:
            result["TenantId"] = self.tenant_id

        return result


@dataclass(frozen=True)
class ContextOptions:
    replay_children: ReplayChildren = False

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ContextOptions:
        return cls(replay_children=data.get("ReplayChildren", False))

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
    chained_invoke_options: ChainedInvokeOptions | None = None

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
        if self.chained_invoke_options:
            result["ChainedInvokeOptions"] = self.chained_invoke_options.to_dict()

        return result

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> OperationUpdate:
        """Create OperationUpdate from dictionary data."""
        error = ErrorObject.from_dict(data["Error"]) if data.get("Error") else None

        context_options = None
        if context_data := data.get("ContextOptions"):
            context_options = ContextOptions.from_dict(context_data)

        step_options = None
        if step_data := data.get("StepOptions"):
            step_options = StepOptions.from_dict(step_data)

        wait_options = None
        if wait_data := data.get("WaitOptions"):
            wait_options = WaitOptions.from_dict(wait_data)

        callback_options = None
        if callback_data := data.get("CallbackOptions"):
            callback_options = CallbackOptions.from_dict(callback_data)

        chained_invoke_options = None
        if invoke_data := data.get("ChainedInvokeOptions"):
            chained_invoke_options = ChainedInvokeOptions.from_dict(invoke_data)

        return cls(
            operation_id=data["Id"],
            operation_type=OperationType(data["Type"]),
            action=OperationAction(data["Action"]),
            parent_id=data.get("ParentId"),
            name=data.get("Name"),
            sub_type=OperationSubType(data["SubType"]) if data.get("SubType") else None,
            payload=data.get("Payload"),
            error=error,
            context_options=context_options,
            step_options=step_options,
            wait_options=wait_options,
            callback_options=callback_options,
            chained_invoke_options=chained_invoke_options,
        )

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
            operation_id=f"execution-result-{int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)}",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
            payload=payload,
        )

    @classmethod
    def create_execution_fail(cls, error: ErrorObject) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: EXECUTION, action: FAIL."""
        return cls(
            operation_id=f"execution-result-{int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)}",
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
        chained_invoke_options: ChainedInvokeOptions,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: INVOKE, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CHAINED_INVOKE,
            sub_type=OperationSubType.CHAINED_INVOKE,
            action=OperationAction.START,
            name=identifier.name,
            payload=payload,
            chained_invoke_options=chained_invoke_options,
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


class TimestampConverter:
    """Converter for datetime/Unix timestamp conversions."""

    @staticmethod
    def to_unix_millis(dt: datetime.datetime | None) -> int | None:
        """Convert datetime to Unix timestamp in milliseconds."""
        return int(dt.timestamp() * 1000) if dt else None

    @staticmethod
    def from_unix_millis(ms: int | None) -> datetime.datetime | None:
        """Convert Unix timestamp in milliseconds to datetime."""
        return (
            datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.UTC)
            if ms is not None
            else None
        )


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
    chained_invoke_details: ChainedInvokeDetails | None = None

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

        chained_invoke_details = None
        if chained_invoke_details := data.get("ChainedInvokeDetails"):
            chained_invoke_details = ChainedInvokeDetails.from_dict(
                chained_invoke_details
            )

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
            chained_invoke_details=chained_invoke_details,
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
            result["WaitDetails"] = (
                {"ScheduledEndTimestamp": self.wait_details.scheduled_end_timestamp}
                if self.wait_details.scheduled_end_timestamp
                else {}
            )
        if self.callback_details:
            callback_dict: MutableMapping[str, Any] = {
                "CallbackId": self.callback_details.callback_id
            }
            if self.callback_details.result:
                callback_dict["Result"] = self.callback_details.result
            if self.callback_details.error:
                callback_dict["Error"] = self.callback_details.error.to_dict()
            result["CallbackDetails"] = callback_dict
        if self.chained_invoke_details:
            invoke_dict: MutableMapping[str, Any] = {}
            if self.chained_invoke_details.result:
                invoke_dict["Result"] = self.chained_invoke_details.result
            if self.chained_invoke_details.error:
                invoke_dict["Error"] = self.chained_invoke_details.error.to_dict()
            result["ChainedInvokeDetails"] = invoke_dict
        return result

    def to_json_dict(self) -> MutableMapping[str, Any]:
        """Convert the Operation to a JSON-serializable dictionary.

        Converts datetime objects to millisecond timestamps for JSON compatibility.

        Returns:
            A dictionary with JSON-serializable values
        """
        # Start with the regular to_dict output
        result = self.to_dict()

        # Convert datetime objects to millisecond timestamps
        if ts := result.get("StartTimestamp"):
            result["StartTimestamp"] = TimestampConverter.to_unix_millis(ts)

        if ts := result.get("EndTimestamp"):
            result["EndTimestamp"] = TimestampConverter.to_unix_millis(ts)

        if (step_details := result.get("StepDetails")) and (
            ts := step_details.get("NextAttemptTimestamp")
        ):
            result["StepDetails"]["NextAttemptTimestamp"] = (
                TimestampConverter.to_unix_millis(ts)
            )

        if (wait_details := result.get("WaitDetails")) and (
            ts := wait_details.get("ScheduledEndTimestamp")
        ):
            result["WaitDetails"]["ScheduledEndTimestamp"] = (
                TimestampConverter.to_unix_millis(ts)
            )

        return result

    @classmethod
    def from_json_dict(cls, data: MutableMapping[str, Any]) -> Operation:
        """Create an Operation from a JSON-serializable dictionary.

        Converts millisecond timestamps back to datetime objects.

        Args:
            data: Dictionary with JSON-serializable values (millisecond timestamps)

        Returns:
            An Operation instance with datetime objects
        """
        # Make a copy to avoid modifying the original data
        data_copy = copy.deepcopy(data)

        # Convert millisecond timestamps back to datetime objects
        if ms := data_copy.get("StartTimestamp"):
            data_copy["StartTimestamp"] = TimestampConverter.from_unix_millis(ms)

        if ms := data_copy.get("EndTimestamp"):
            data_copy["EndTimestamp"] = TimestampConverter.from_unix_millis(ms)

        if (step_details := data_copy.get("StepDetails")) and (
            ms := step_details.get("NextAttemptTimestamp")
        ):
            step_details["NextAttemptTimestamp"] = TimestampConverter.from_unix_millis(
                ms
            )

        if (wait_details := data_copy.get("WaitDetails")) and (
            ms := wait_details.get("ScheduledEndTimestamp")
        ):
            wait_details["ScheduledEndTimestamp"] = TimestampConverter.from_unix_millis(
                ms
            )

        # Use the existing from_dict method with the converted data
        return cls.from_dict(data_copy)


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


class LambdaClient(DurableServiceClient):
    """Persist durable operations to the Lambda Durable Function APIs."""

    _cached_boto_client: Boto3LambdaClient | None = None

    def __init__(self, client: Boto3LambdaClient) -> None:
        self.client = client

    @classmethod
    def initialize_client(cls) -> LambdaClient:
        """Initialize or return cached Lambda client.

        Implements lazy initialization with class-level caching to optimize
        Lambda warm starts. The boto3 client is created once and reused across
        invocations, avoiding repeated credential resolution and connection
        pool setup.

        Returns:
            LambdaClient: A new LambdaClient instance wrapping the cached boto3 client.
        """
        if cls._cached_boto_client is None:
            cls._cached_boto_client = boto3.client(
                "lambda",
                config=Config(
                    connect_timeout=5,
                    read_timeout=50,
                ),
            )
        return cls(client=cls._cached_boto_client)

    def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        try:
            optional_params: dict[str, str] = {}
            if client_token is not None:
                optional_params["ClientToken"] = client_token

            result: CheckpointDurableExecutionResponseTypeDef = (
                self.client.checkpoint_durable_execution(
                    DurableExecutionArn=durable_execution_arn,
                    CheckpointToken=checkpoint_token,
                    Updates=cast(Any, [o.to_dict() for o in updates]),
                    **optional_params,  # type: ignore[arg-type]
                )
            )

            return CheckpointOutput.from_dict(cast(MutableMapping[str, Any], result))
        except Exception as e:
            checkpoint_error = CheckpointError.from_exception(e)
            logger.exception(
                "Failed to checkpoint.", extra=checkpoint_error.build_logger_extras()
            )
            raise checkpoint_error from None

    def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        try:
            result: GetDurableExecutionStateResponseTypeDef = (
                self.client.get_durable_execution_state(
                    DurableExecutionArn=durable_execution_arn,
                    CheckpointToken=checkpoint_token,
                    Marker=next_marker,
                    MaxItems=max_items,
                )
            )
            return StateOutput.from_dict(cast(MutableMapping[str, Any], result))
        except Exception as e:
            error = GetExecutionStateError.from_exception(e)
            logger.exception(
                "Failed to get execution state.", extra=error.build_logger_extras()
            )
            raise error from None


# endregion client
