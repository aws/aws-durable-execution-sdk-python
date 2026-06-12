"""Model classes for the web API."""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any

from aws_durable_execution_sdk_python.execution import DurableExecutionInvocationOutput

# Import existing types from the main SDK - REUSE EVERYTHING POSSIBLE
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    CallbackOptions,
    ChainedInvokeDetails,
    ChainedInvokeOptions,
    ContextDetails,
    ContextOptions,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StepDetails,
    StepOptions,
    TimestampConverter,
    WaitDetails,
    WaitOptions,
)
from aws_durable_execution_sdk_python.types import (
    LambdaContext as LambdaContextProtocol,
)
from dateutil.tz import UTC

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


class EventType(Enum):
    """Event types for durable execution events."""

    EXECUTION_STARTED = "ExecutionStarted"
    EXECUTION_SUCCEEDED = "ExecutionSucceeded"
    EXECUTION_FAILED = "ExecutionFailed"
    EXECUTION_TIMED_OUT = "ExecutionTimedOut"
    EXECUTION_STOPPED = "ExecutionStopped"
    CONTEXT_STARTED = "ContextStarted"
    CONTEXT_SUCCEEDED = "ContextSucceeded"
    CONTEXT_FAILED = "ContextFailed"
    WAIT_STARTED = "WaitStarted"
    WAIT_SUCCEEDED = "WaitSucceeded"
    WAIT_CANCELLED = "WaitCancelled"
    STEP_STARTED = "StepStarted"
    STEP_SUCCEEDED = "StepSucceeded"
    STEP_FAILED = "StepFailed"
    CHAINED_INVOKE_STARTED = "ChainedInvokeStarted"
    CHAINED_INVOKE_SUCCEEDED = "ChainedInvokeSucceeded"
    CHAINED_INVOKE_FAILED = "ChainedInvokeFailed"
    CHAINED_INVOKE_TIMED_OUT = "ChainedInvokeTimedOut"
    CHAINED_INVOKE_STOPPED = "ChainedInvokeStopped"
    CALLBACK_STARTED = "CallbackStarted"
    CALLBACK_SUCCEEDED = "CallbackSucceeded"
    CALLBACK_FAILED = "CallbackFailed"
    CALLBACK_TIMED_OUT = "CallbackTimedOut"
    INVOCATION_COMPLETED = "InvocationCompleted"


TERMINAL_STATUSES: set[OperationStatus] = {
    OperationStatus.SUCCEEDED,
    OperationStatus.FAILED,
    OperationStatus.TIMED_OUT,
    OperationStatus.STOPPED,
    OperationStatus.CANCELLED,
}


@dataclass(frozen=True)
class LambdaContext(LambdaContextProtocol):
    """Lambda context for testing."""

    aws_request_id: str
    log_group_name: str | None = None
    log_stream_name: str | None = None
    function_name: str | None = None
    memory_limit_in_mb: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    tenant_id: str | None = None
    client_context: dict | None = None
    identity: dict | None = None

    def get_remaining_time_in_millis(self) -> int:
        return 900000  # 15 minutes default

    def log(self, msg) -> None:
        pass  # No-op for testing


# region web_api_models
# Web API specific models (not in Smithy but needed for web interface)
@dataclass(frozen=True)
class StartDurableExecutionInput:
    """Input for starting a durable execution via web API."""

    account_id: str
    function_name: str
    function_qualifier: str
    execution_name: str
    execution_timeout_seconds: int
    execution_retention_period_days: int
    invocation_id: str | None = None
    trace_fields: dict | None = None
    tenant_id: str | None = None
    input: str | None = None
    lambda_endpoint: str | None = None  # Endpoint for this specific execution

    @classmethod
    def from_dict(cls, data: dict) -> StartDurableExecutionInput:
        # Validate required fields and raise AWS-compliant exceptions
        required_fields = [
            "AccountId",
            "FunctionName",
            "FunctionQualifier",
            "ExecutionName",
            "ExecutionTimeoutSeconds",
            "ExecutionRetentionPeriodDays",
        ]

        for field in required_fields:
            if field not in data:
                msg: str = f"Missing required field: {field}"
                raise InvalidParameterValueException(msg)

        return cls(
            account_id=data["AccountId"],
            function_name=data["FunctionName"],
            function_qualifier=data["FunctionQualifier"],
            execution_name=data["ExecutionName"],
            execution_timeout_seconds=data["ExecutionTimeoutSeconds"],
            execution_retention_period_days=data["ExecutionRetentionPeriodDays"],
            invocation_id=data.get("InvocationId"),
            trace_fields=data.get("TraceFields"),
            tenant_id=data.get("TenantId"),
            input=data.get("Input"),
            lambda_endpoint=data.get("LambdaEndpoint", None),
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "AccountId": self.account_id,
            "FunctionName": self.function_name,
            "FunctionQualifier": self.function_qualifier,
            "ExecutionName": self.execution_name,
            "ExecutionTimeoutSeconds": self.execution_timeout_seconds,
            "ExecutionRetentionPeriodDays": self.execution_retention_period_days,
        }
        if self.invocation_id is not None:
            result["InvocationId"] = self.invocation_id
        if self.trace_fields is not None:
            result["TraceFields"] = self.trace_fields
        if self.tenant_id is not None:
            result["TenantId"] = self.tenant_id
        if self.input is not None:
            result["Input"] = self.input
        if self.lambda_endpoint is not None:
            result["LambdaEndpoint"] = self.lambda_endpoint
        return result

    def get_normalized_input(self):
        """
        Normalize input string to be JSON deserializable.
        Avoid double coding json input.
        """
        # Try to parse once
        try:
            _ = json.loads(self.input)
            return self.input
        except (json.JSONDecodeError, TypeError):
            # Not valid JSON, treat as plain string and encode it
            return json.dumps(self.input)


@dataclass(frozen=True)
class StartDurableExecutionOutput:
    """Output from starting a durable execution via web API."""

    execution_arn: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StartDurableExecutionOutput:
        return cls(execution_arn=data.get("ExecutionArn"))

    def to_dict(self) -> dict[str, Any]:
        result = {}
        if self.execution_arn is not None:
            result["ExecutionArn"] = self.execution_arn
        return result


# endregion web_api_models


# region smithy_api_models
# Smithy-based API models
@dataclass(frozen=True)
class GetDurableExecutionRequest:
    """Request to get durable execution details."""

    durable_execution_arn: str

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionRequest:
        return cls(durable_execution_arn=data["DurableExecutionArn"])

    def to_dict(self) -> dict[str, Any]:
        return {"DurableExecutionArn": self.durable_execution_arn}


@dataclass(frozen=True)
class GetDurableExecutionResponse:
    """Response containing durable execution details."""

    durable_execution_arn: str
    durable_execution_name: str
    function_arn: str
    status: str
    start_timestamp: datetime.datetime
    input_payload: str | None = None
    result: str | None = None
    error: ErrorObject | None = None
    end_timestamp: datetime.datetime | None = None
    version: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionResponse:
        error = None
        if error_data := data.get("Error"):
            error = ErrorObject.from_dict(error_data)

        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            durable_execution_name=data["DurableExecutionName"],
            function_arn=data["FunctionArn"],
            status=data["Status"],
            start_timestamp=data["StartTimestamp"],
            input_payload=data.get("InputPayload"),
            result=data.get("Result"),
            error=error,
            end_timestamp=data.get("EndTimestamp"),
            version=data.get("Version"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutionArn": self.durable_execution_arn,
            "DurableExecutionName": self.durable_execution_name,
            "FunctionArn": self.function_arn,
            "Status": self.status,
            "StartTimestamp": self.start_timestamp,
        }
        if self.input_payload is not None:
            result["InputPayload"] = self.input_payload
        if self.result is not None:
            result["Result"] = self.result
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        if self.end_timestamp is not None:
            result["EndTimestamp"] = self.end_timestamp
        if self.end_timestamp is not None:
            result["EndTimestamp"] = self.end_timestamp
        if self.version is not None:
            result["Version"] = self.version
        return result


@dataclass(frozen=True)
class Execution:
    """Execution summary structure from Smithy model."""

    durable_execution_arn: str
    durable_execution_name: str
    function_arn: str
    status: str
    start_timestamp: datetime.datetime
    end_timestamp: datetime.datetime | None = None

    @classmethod
    def from_dict(cls, data: dict) -> Execution:
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            durable_execution_name=data["DurableExecutionName"],
            function_arn=data.get(
                "FunctionArn", ""
            ),  # Make optional for backward compatibility
            status=data["Status"],
            start_timestamp=data["StartTimestamp"],
            end_timestamp=data.get("EndTimestamp"),
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "DurableExecutionArn": self.durable_execution_arn,
            "DurableExecutionName": self.durable_execution_name,
            "Status": self.status,
            "StartTimestamp": self.start_timestamp,
        }
        if self.function_arn:  # Only include if not empty
            result["FunctionArn"] = self.function_arn
        if self.end_timestamp is not None:
            result["EndTimestamp"] = self.end_timestamp
        return result

    @classmethod
    def from_execution(cls, execution, status: str) -> Execution:
        """Create ExecutionSummary from Execution object."""

        execution_op = execution.get_operation_execution_started()
        return cls(
            durable_execution_arn=execution.durable_execution_arn,
            durable_execution_name=execution.start_input.execution_name,
            function_arn=f"arn:aws:lambda:us-east-1:123456789012:function:{execution.start_input.function_name}",
            status=status,
            start_timestamp=execution_op.start_timestamp
            if execution_op.start_timestamp
            else datetime.datetime.now(datetime.UTC),
            end_timestamp=execution_op.end_timestamp
            if execution_op.end_timestamp
            else None,
        )


@dataclass(frozen=True)
class ListDurableExecutionsRequest:
    """Request to list durable executions."""

    function_name: str | None = None
    function_version: str | None = None
    durable_execution_name: str | None = None
    status_filter: list[str] | None = None
    started_after: str | None = None
    started_before: str | None = None
    marker: str | None = None
    max_items: int = 0
    reverse_order: bool | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsRequest:
        # Handle query parameters that may be lists
        function_name = data.get("FunctionName")
        if isinstance(function_name, list):
            function_name = function_name[0] if function_name else None

        function_version = data.get("FunctionVersion")
        if isinstance(function_version, list):
            function_version = function_version[0] if function_version else None

        durable_execution_name = data.get("DurableExecutionName")
        if isinstance(durable_execution_name, list):
            durable_execution_name = (
                durable_execution_name[0] if durable_execution_name else None
            )

        status_filter = data.get("StatusFilter")
        if isinstance(status_filter, list):
            status_filter = status_filter if status_filter else None
        elif status_filter:
            status_filter = [status_filter]

        started_after = data.get("StartedAfter")
        if isinstance(started_after, list):
            started_after = started_after[0] if started_after else None

        started_before = data.get("StartedBefore")
        if isinstance(started_before, list):
            started_before = started_before[0] if started_before else None

        marker = data.get("Marker")
        if isinstance(marker, list):
            marker = marker[0] if marker else None

        max_items = data.get("MaxItems", 0)
        if isinstance(max_items, list):
            max_items = int(max_items[0]) if max_items else 0

        reverse_order = data.get("ReverseOrder")
        if isinstance(reverse_order, list):
            reverse_order = (
                reverse_order[0].lower() in ("true", "1", "yes")
                if reverse_order
                else None
            )
        elif isinstance(reverse_order, str):
            reverse_order = reverse_order.lower() in ("true", "1", "yes")

        return cls(
            function_name=function_name,
            function_version=function_version,
            durable_execution_name=durable_execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            marker=marker,
            max_items=max_items,
            reverse_order=reverse_order,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.function_name is not None:
            result["FunctionName"] = self.function_name
        if self.function_version is not None:
            result["FunctionVersion"] = self.function_version
        if self.durable_execution_name is not None:
            result["DurableExecutionName"] = self.durable_execution_name
        if self.status_filter is not None:
            result["StatusFilter"] = self.status_filter
        if self.started_after is not None:
            result["StartedAfter"] = self.started_after
        if self.started_before is not None:
            result["StartedBefore"] = self.started_before
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        if self.reverse_order is not None:
            result["ReverseOrder"] = self.reverse_order
        return result


@dataclass(frozen=True)
class ListDurableExecutionsResponse:
    """Response containing list of durable executions."""

    durable_executions: list[Execution]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsResponse:
        executions = [
            Execution.from_dict(exec_data)
            for exec_data in data.get("DurableExecutions", [])
        ]
        return cls(
            durable_executions=executions,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutions": [exe.to_dict() for exe in self.durable_executions]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class StopDurableExecutionRequest:
    """Request to stop a durable execution."""

    durable_execution_arn: str
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StopDurableExecutionRequest:
        error = None
        if error_data := data.get("Error"):
            error = ErrorObject.from_dict(error_data)

        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            error=error,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"DurableExecutionArn": self.durable_execution_arn}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class StopDurableExecutionResponse:
    """Response from stopping a durable execution."""

    stop_timestamp: datetime.datetime

    @classmethod
    def from_dict(cls, data: dict) -> StopDurableExecutionResponse:
        return cls(stop_timestamp=data["StopTimestamp"])

    def to_dict(self) -> dict[str, Any]:
        return {"StopTimestamp": self.stop_timestamp}


@dataclass(frozen=True)
class GetDurableExecutionStateRequest:
    """Request to get durable execution state."""

    durable_execution_arn: str
    checkpoint_token: str
    marker: str | None = None
    max_items: int = 0

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionStateRequest:
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            checkpoint_token=data["CheckpointToken"],
            marker=data.get("Marker"),
            max_items=data.get("MaxItems", 0),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
        }
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        return result


@dataclass(frozen=True)
class GetDurableExecutionStateResponse:
    """Response containing durable execution state operations."""

    operations: list[Operation]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionStateResponse:
        operations = [
            Operation.from_dict(op_data) for op_data in data.get("Operations", [])
        ]
        return cls(
            operations=operations,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "Operations": [op.to_dict() for op in self.operations]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


# endregion smithy_api_models


# region event_structures
# Event-related structures from Smithy model
@dataclass(frozen=True)
class EventInput:
    """Event input structure."""

    payload: str | None = None
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> EventInput:
        return cls(
            payload=data.get("Payload"),
            truncated=data.get("Truncated", False),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Truncated": self.truncated}
        if self.payload is not None:
            result["Payload"] = self.payload
        return result

    @classmethod
    def from_details(
        cls,
        details: ExecutionDetails,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventInput:
        details_input: str | None = details.input_payload if details else None
        payload: str | None = details_input if include else None
        truncated: bool = not include
        return cls(payload=payload, truncated=truncated)

    @classmethod
    def from_start_durable_execution_input(
        cls,
        start_durable_execution_input: StartDurableExecutionInput,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventInput:
        input: str | None = start_durable_execution_input.input
        truncated: bool = not include
        return cls(input, truncated)


@dataclass(frozen=True)
class EventResult:
    """Event result structure."""

    payload: str | None = None
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> EventResult:
        return cls(
            payload=data.get("Payload"),
            truncated=data.get("Truncated", False),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Truncated": self.truncated}
        if self.payload is not None:
            result["Payload"] = self.payload
        return result

    @classmethod
    def from_details(
        cls,
        details: CallbackDetails | StepDetails | ChainedInvokeDetails | ContextDetails,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventResult:
        details_result: str | None = details.result if details else None
        payload: str | None = details_result if include else None
        truncated: bool = not include
        return cls(payload=payload, truncated=truncated)

    @classmethod
    def from_durable_execution_invocation_output(
        cls,
        durable_execution_invocation_output: DurableExecutionInvocationOutput,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventResult:
        truncated: bool = not include
        return cls(durable_execution_invocation_output.result, truncated)


@dataclass(frozen=True)
class EventError:
    """Event error structure."""

    payload: ErrorObject | None = None
    truncated: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> EventError:
        payload = None
        if payload_data := data.get("Payload"):
            payload = ErrorObject.from_dict(payload_data)

        return cls(
            payload=payload,
            truncated=data.get("Truncated", False),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Truncated": self.truncated}
        if self.payload is not None:
            result["Payload"] = self.payload.to_dict()
        return result

    @classmethod
    def from_details(
        cls,
        details: CallbackDetails | StepDetails | ChainedInvokeDetails | ContextDetails,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventError:
        error_object: ErrorObject | None = details.error if details else None
        truncated: bool = not include
        return cls(error_object, truncated)

    @classmethod
    def from_durable_execution_invocation_output(
        cls,
        durable_execution_invocation_output: DurableExecutionInvocationOutput,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventError:
        truncated: bool = not include
        return cls(durable_execution_invocation_output.error, truncated)


@dataclass(frozen=True)
class RetryDetails:
    """Retry details structure."""

    current_attempt: int = 0
    next_attempt_delay_seconds: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> RetryDetails:
        return cls(
            current_attempt=data.get("CurrentAttempt", 0),
            next_attempt_delay_seconds=data.get("NextAttemptDelaySeconds"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CurrentAttempt": self.current_attempt}
        if self.next_attempt_delay_seconds is not None:
            result["NextAttemptDelaySeconds"] = self.next_attempt_delay_seconds
        return result


# Event detail structures
@dataclass(frozen=True)
class ExecutionStartedDetails:
    """Execution started event details."""

    input: EventInput | None = None
    execution_timeout: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionStartedDetails:
        input_data = None
        if input_dict := data.get("Input"):
            input_data = EventInput.from_dict(input_dict)

        return cls(
            input=input_data,
            execution_timeout=data.get("ExecutionTimeout"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.input is not None:
            result["Input"] = self.input.to_dict()
        if self.execution_timeout is not None:
            result["ExecutionTimeout"] = self.execution_timeout
        return result


@dataclass(frozen=True)
class ExecutionSucceededDetails:
    """Execution succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class ExecutionFailedDetails:
    """Execution failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ExecutionTimedOutDetails:
    """Execution timed out event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionTimedOutDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ExecutionStoppedDetails:
    """Execution stopped event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ExecutionStoppedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ContextStartedDetails:
    """Context started event details."""

    @classmethod
    def from_dict(cls, data: dict) -> ContextStartedDetails:  # noqa: ARG003
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {}


@dataclass(frozen=True)
class ContextSucceededDetails:
    """Context succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ContextSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class ContextFailedDetails:
    """Context failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ContextFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class WaitStartedDetails:
    """Wait started event details."""

    duration: int | None = None
    scheduled_end_timestamp: datetime.datetime | None = None

    @classmethod
    def from_dict(cls, data: dict) -> WaitStartedDetails:
        return cls(
            duration=data.get("Duration"),
            scheduled_end_timestamp=data.get("ScheduledEndTimestamp"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.duration is not None:
            result["Duration"] = self.duration
        if self.scheduled_end_timestamp is not None:
            result["ScheduledEndTimestamp"] = self.scheduled_end_timestamp
        return result


@dataclass(frozen=True)
class WaitSucceededDetails:
    """Wait succeeded event details."""

    duration: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> WaitSucceededDetails:
        return cls(duration=data.get("Duration"))

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.duration is not None:
            result["Duration"] = self.duration
        return result


@dataclass(frozen=True)
class WaitCancelledDetails:
    """Wait cancelled event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> WaitCancelledDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class StepStartedDetails:
    """Step started event details."""

    @classmethod
    def from_dict(cls, data: dict) -> StepStartedDetails:  # noqa: ARG003
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {}


@dataclass(frozen=True)
class StepSucceededDetails:
    """Step succeeded event details."""

    result: EventResult | None = None
    retry_details: RetryDetails | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StepSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        retry_details_data = None
        if retry_dict := data.get("RetryDetails"):
            retry_details_data = RetryDetails.from_dict(retry_dict)

        return cls(result=result_data, retry_details=retry_details_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        if self.retry_details is not None:
            result["RetryDetails"] = self.retry_details.to_dict()
        return result


@dataclass(frozen=True)
class StepFailedDetails:
    """Step failed event details."""

    error: EventError | None = None
    retry_details: RetryDetails | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StepFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        retry_details_data = None
        if retry_dict := data.get("RetryDetails"):
            retry_details_data = RetryDetails.from_dict(retry_dict)

        return cls(error=error_data, retry_details=retry_details_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        if self.retry_details is not None:
            result["RetryDetails"] = self.retry_details.to_dict()
        return result


@dataclass(frozen=True)
class ChainedInvokePendingDetails:
    """Chained Invoke Pending event details."""

    input: EventInput | None = None
    function_name: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ChainedInvokePendingDetails:
        input_data = None
        if input_dict := data.get("Input"):
            input_data = EventInput.from_dict(input_dict)

        return cls(
            input=input_data,
            function_name=data.get("FunctionName"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.input is not None:
            result["Input"] = self.input.to_dict()
        if self.function_name is not None:
            result["FunctionName"] = self.function_name
        return result


@dataclass(frozen=True)
class ChainedInvokeStartedDetails:
    """Chained invoke started event details."""

    durable_execution_arn: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ChainedInvokeStartedDetails:
        return cls(
            durable_execution_arn=data.get("DurableExecutionArn"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.durable_execution_arn is not None:
            result["DurableExecutionArn"] = self.durable_execution_arn
        return result


@dataclass(frozen=True)
class ChainedInvokeSucceededDetails:
    """Chained invoke succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ChainedInvokeSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class ChainedInvokeFailedDetails:
    """Chained invoke failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ChainedInvokeFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ChainedInvokeTimedOutDetails:
    """Chained invoke timed out event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ChainedInvokeTimedOutDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class ChainedInvokeStoppedDetails:
    """Chained invoke stopped event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ChainedInvokeStoppedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class CallbackStartedDetails:
    """Callback started event details."""

    callback_id: str | None = None
    heartbeat_timeout: int | None = None
    timeout: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackStartedDetails:
        return cls(
            callback_id=data.get("CallbackId"),
            heartbeat_timeout=data.get("HeartbeatTimeout"),
            timeout=data.get("Timeout"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.callback_id is not None:
            result["CallbackId"] = self.callback_id
        if self.heartbeat_timeout is not None:
            result["HeartbeatTimeout"] = self.heartbeat_timeout
        if self.timeout is not None:
            result["Timeout"] = self.timeout
        return result


@dataclass(frozen=True)
class CallbackSucceededDetails:
    """Callback succeeded event details."""

    result: EventResult | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackSucceededDetails:
        result_data = None
        if result_dict := data.get("Result"):
            result_data = EventResult.from_dict(result_dict)

        return cls(result=result_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.result is not None:
            result["Result"] = self.result.to_dict()
        return result


@dataclass(frozen=True)
class CallbackFailedDetails:
    """Callback failed event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackFailedDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class CallbackTimedOutDetails:
    """Callback timed out event details."""

    error: EventError | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CallbackTimedOutDetails:
        error_data = None
        if error_dict := data.get("Error"):
            error_data = EventError.from_dict(error_dict)

        return cls(error=error_data)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class InvocationCompletedDetails:
    """Invocation completed event details."""

    start_timestamp: datetime.datetime
    end_timestamp: datetime.datetime
    request_id: str

    @classmethod
    def from_dict(cls, data: dict) -> InvocationCompletedDetails:
        return cls(
            start_timestamp=data["StartTimestamp"],
            end_timestamp=data["EndTimestamp"],
            request_id=data["RequestId"],
        )

    @classmethod
    def from_json_dict(cls, data: dict) -> InvocationCompletedDetails:
        """Deserialize from JSON dict with Unix millisecond timestamps."""
        start_ts: datetime.datetime | None = TimestampConverter.from_unix_millis(
            data["StartTimestamp"]
        )  # type: ignore[arg-type]
        end_ts: datetime.datetime | None = TimestampConverter.from_unix_millis(
            data["EndTimestamp"]
        )  # type: ignore[arg-type]

        if start_ts is None or end_ts is None:
            raise InvalidParameterValueException(
                "StartTimestamp and EndTimestamp cannot be null"
            )

        return cls(
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            request_id=data["RequestId"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "StartTimestamp": self.start_timestamp,
            "EndTimestamp": self.end_timestamp,
            "RequestId": self.request_id,
        }

    def to_json_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict with Unix millisecond timestamps."""
        return {
            "StartTimestamp": TimestampConverter.to_unix_millis(self.start_timestamp),
            "EndTimestamp": TimestampConverter.to_unix_millis(self.end_timestamp),
            "RequestId": self.request_id,
        }


# endregion event_structures


@dataclass(frozen=True)
class EventCreationContext:
    operation: Operation
    event_id: int
    durable_execution_arn: str
    start_durable_execution_input: StartDurableExecutionInput
    durable_execution_invocation_output: DurableExecutionInvocationOutput | None = None
    operation_update: OperationUpdate | None = None
    include_execution_data: bool = False  # noqa: FBT001, FBT002

    @classmethod
    def create(
        cls,
        operation: Operation,
        event_id: int,
        durable_execution_arn: str,
        start_input: StartDurableExecutionInput,
        result: DurableExecutionInvocationOutput | None = None,
        operation_update: OperationUpdate | None = None,
        include_execution_data: bool = False,  # noqa: FBT001, FBT002
    ) -> EventCreationContext:
        return cls(
            operation=operation,
            event_id=event_id,
            durable_execution_arn=durable_execution_arn,
            start_durable_execution_input=start_input,
            durable_execution_invocation_output=result,
            operation_update=operation_update,
            include_execution_data=include_execution_data,
        )

    @property
    def sub_type(self) -> str | None:
        return self.operation.sub_type.value if self.operation.sub_type else None

    def get_retry_details(self) -> RetryDetails | None:
        if not self.operation.step_details or not self.operation_update:
            return None

        delay = 0
        if (
            self.operation_update.operation_type == OperationType.STEP
            and self.operation_update.step_options
        ):
            delay = self.operation_update.step_options.next_attempt_delay_seconds

        return RetryDetails(
            current_attempt=self.operation.step_details.attempt,
            next_attempt_delay_seconds=delay,
        )

    @property
    def start_timestamp(self) -> datetime.datetime:
        return (
            self.operation.start_timestamp
            if self.operation.start_timestamp is not None
            else datetime.datetime.now(UTC)
        )

    @property
    def end_timestamp(self) -> datetime.datetime:
        return (
            self.operation.end_timestamp
            if self.operation.end_timestamp is not None
            else datetime.datetime.now(UTC)
        )


# region event_class
@dataclass(frozen=True)
class Event:
    """Event structure from Smithy model."""

    event_type: str
    event_timestamp: datetime.datetime
    sub_type: str | None = None
    event_id: int = 1
    operation_id: str | None = None
    name: str | None = None
    parent_id: str | None = None
    execution_started_details: ExecutionStartedDetails | None = None
    execution_succeeded_details: ExecutionSucceededDetails | None = None
    execution_failed_details: ExecutionFailedDetails | None = None
    execution_timed_out_details: ExecutionTimedOutDetails | None = None
    execution_stopped_details: ExecutionStoppedDetails | None = None
    context_started_details: ContextStartedDetails | None = None
    context_succeeded_details: ContextSucceededDetails | None = None
    context_failed_details: ContextFailedDetails | None = None
    wait_started_details: WaitStartedDetails | None = None
    wait_succeeded_details: WaitSucceededDetails | None = None
    wait_cancelled_details: WaitCancelledDetails | None = None
    step_started_details: StepStartedDetails | None = None
    step_succeeded_details: StepSucceededDetails | None = None
    step_failed_details: StepFailedDetails | None = None
    chained_invoke_pending_details: ChainedInvokePendingDetails | None = None
    chained_invoke_started_details: ChainedInvokeStartedDetails | None = None
    chained_invoke_succeeded_details: ChainedInvokeSucceededDetails | None = None
    chained_invoke_failed_details: ChainedInvokeFailedDetails | None = None
    chained_invoke_timed_out_details: ChainedInvokeTimedOutDetails | None = None
    chained_invoke_stopped_details: ChainedInvokeStoppedDetails | None = None
    callback_started_details: CallbackStartedDetails | None = None
    callback_succeeded_details: CallbackSucceededDetails | None = None
    callback_failed_details: CallbackFailedDetails | None = None
    callback_timed_out_details: CallbackTimedOutDetails | None = None
    invocation_completed_details: InvocationCompletedDetails | None = None

    @classmethod
    def from_dict(cls, data: dict) -> Event:
        # Parse all the detail structures
        execution_started_details = None
        if details_data := data.get("ExecutionStartedDetails"):
            execution_started_details = ExecutionStartedDetails.from_dict(details_data)

        execution_succeeded_details = None
        if details_data := data.get("ExecutionSucceededDetails"):
            execution_succeeded_details = ExecutionSucceededDetails.from_dict(
                details_data
            )

        execution_failed_details = None
        if details_data := data.get("ExecutionFailedDetails"):
            execution_failed_details = ExecutionFailedDetails.from_dict(details_data)

        execution_timed_out_details = None
        if details_data := data.get("ExecutionTimedOutDetails"):
            execution_timed_out_details = ExecutionTimedOutDetails.from_dict(
                details_data
            )

        execution_stopped_details = None
        if details_data := data.get("ExecutionStoppedDetails"):
            execution_stopped_details = ExecutionStoppedDetails.from_dict(details_data)

        context_started_details = None
        if details_data := data.get("ContextStartedDetails"):
            context_started_details = ContextStartedDetails.from_dict(details_data)

        context_succeeded_details = None
        if details_data := data.get("ContextSucceededDetails"):
            context_succeeded_details = ContextSucceededDetails.from_dict(details_data)

        context_failed_details = None
        if details_data := data.get("ContextFailedDetails"):
            context_failed_details = ContextFailedDetails.from_dict(details_data)

        wait_started_details = None
        if details_data := data.get("WaitStartedDetails"):
            wait_started_details = WaitStartedDetails.from_dict(details_data)

        wait_succeeded_details = None
        if details_data := data.get("WaitSucceededDetails"):
            wait_succeeded_details = WaitSucceededDetails.from_dict(details_data)

        wait_cancelled_details = None
        if details_data := data.get("WaitCancelledDetails"):
            wait_cancelled_details = WaitCancelledDetails.from_dict(details_data)

        step_started_details = None
        if details_data := data.get("StepStartedDetails"):
            step_started_details = StepStartedDetails.from_dict(details_data)

        step_succeeded_details = None
        if details_data := data.get("StepSucceededDetails"):
            step_succeeded_details = StepSucceededDetails.from_dict(details_data)

        step_failed_details = None
        if details_data := data.get("StepFailedDetails"):
            step_failed_details = StepFailedDetails.from_dict(details_data)

        chained_invoke_pending_details = None
        if details_data := data.get("ChainedInvokePendingDetails"):
            chained_invoke_pending_details = ChainedInvokePendingDetails.from_dict(
                details_data
            )

        chained_invoke_started_details = None
        if details_data := data.get("ChainedInvokeStartedDetails"):
            chained_invoke_started_details = ChainedInvokeStartedDetails.from_dict(
                details_data
            )

        chained_invoke_succeeded_details = None
        if details_data := data.get("ChainedInvokeSucceededDetails"):
            chained_invoke_succeeded_details = ChainedInvokeSucceededDetails.from_dict(
                details_data
            )

        chained_invoke_failed_details = None
        if details_data := data.get("ChainedInvokeFailedDetails"):
            chained_invoke_failed_details = ChainedInvokeFailedDetails.from_dict(
                details_data
            )

        chained_invoke_timed_out_details = None
        if details_data := data.get("ChainedInvokeTimedOutDetails"):
            chained_invoke_timed_out_details = ChainedInvokeTimedOutDetails.from_dict(
                details_data
            )

        chained_invoke_stopped_details = None
        if details_data := data.get("ChainedInvokeStoppedDetails"):
            chained_invoke_stopped_details = ChainedInvokeStoppedDetails.from_dict(
                details_data
            )

        callback_started_details = None
        if details_data := data.get("CallbackStartedDetails"):
            callback_started_details = CallbackStartedDetails.from_dict(details_data)

        callback_succeeded_details = None
        if details_data := data.get("CallbackSucceededDetails"):
            callback_succeeded_details = CallbackSucceededDetails.from_dict(
                details_data
            )

        callback_failed_details = None
        if details_data := data.get("CallbackFailedDetails"):
            callback_failed_details = CallbackFailedDetails.from_dict(details_data)

        callback_timed_out_details = None
        if details_data := data.get("CallbackTimedOutDetails"):
            callback_timed_out_details = CallbackTimedOutDetails.from_dict(details_data)

        invocation_completed_details = None
        if details_data := data.get("InvocationCompletedDetails"):
            invocation_completed_details = InvocationCompletedDetails.from_dict(
                details_data
            )

        return cls(
            event_type=data["EventType"],
            event_timestamp=data["EventTimestamp"],
            sub_type=data.get("SubType"),
            event_id=data.get("EventId", 1),
            operation_id=data.get("Id"),
            name=data.get("Name"),
            parent_id=data.get("ParentId"),
            execution_started_details=execution_started_details,
            execution_succeeded_details=execution_succeeded_details,
            execution_failed_details=execution_failed_details,
            execution_timed_out_details=execution_timed_out_details,
            execution_stopped_details=execution_stopped_details,
            context_started_details=context_started_details,
            context_succeeded_details=context_succeeded_details,
            context_failed_details=context_failed_details,
            wait_started_details=wait_started_details,
            wait_succeeded_details=wait_succeeded_details,
            wait_cancelled_details=wait_cancelled_details,
            step_started_details=step_started_details,
            step_succeeded_details=step_succeeded_details,
            step_failed_details=step_failed_details,
            chained_invoke_pending_details=chained_invoke_pending_details,
            chained_invoke_started_details=chained_invoke_started_details,
            chained_invoke_succeeded_details=chained_invoke_succeeded_details,
            chained_invoke_failed_details=chained_invoke_failed_details,
            chained_invoke_timed_out_details=chained_invoke_timed_out_details,
            chained_invoke_stopped_details=chained_invoke_stopped_details,
            callback_started_details=callback_started_details,
            callback_succeeded_details=callback_succeeded_details,
            callback_failed_details=callback_failed_details,
            callback_timed_out_details=callback_timed_out_details,
            invocation_completed_details=invocation_completed_details,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "EventType": self.event_type,
            "EventTimestamp": self.event_timestamp,
            "EventId": self.event_id,
        }
        if self.sub_type is not None:
            result["SubType"] = self.sub_type
        if self.operation_id is not None:
            result["Id"] = self.operation_id
        if self.name is not None:
            result["Name"] = self.name
        if self.parent_id is not None:
            result["ParentId"] = self.parent_id
        if self.execution_started_details is not None:
            result["ExecutionStartedDetails"] = self.execution_started_details.to_dict()
        if self.execution_succeeded_details is not None:
            result["ExecutionSucceededDetails"] = (
                self.execution_succeeded_details.to_dict()
            )
        if self.execution_failed_details is not None:
            result["ExecutionFailedDetails"] = self.execution_failed_details.to_dict()
        if self.execution_timed_out_details is not None:
            result["ExecutionTimedOutDetails"] = (
                self.execution_timed_out_details.to_dict()
            )
        if self.execution_stopped_details is not None:
            result["ExecutionStoppedDetails"] = self.execution_stopped_details.to_dict()
        if self.context_started_details is not None:
            result["ContextStartedDetails"] = self.context_started_details.to_dict()
        if self.context_succeeded_details is not None:
            result["ContextSucceededDetails"] = self.context_succeeded_details.to_dict()
        if self.context_failed_details is not None:
            result["ContextFailedDetails"] = self.context_failed_details.to_dict()
        if self.wait_started_details is not None:
            result["WaitStartedDetails"] = self.wait_started_details.to_dict()
        if self.wait_succeeded_details is not None:
            result["WaitSucceededDetails"] = self.wait_succeeded_details.to_dict()
        if self.wait_cancelled_details is not None:
            result["WaitCancelledDetails"] = self.wait_cancelled_details.to_dict()
        if self.step_started_details is not None:
            result["StepStartedDetails"] = self.step_started_details.to_dict()
        if self.step_succeeded_details is not None:
            result["StepSucceededDetails"] = self.step_succeeded_details.to_dict()
        if self.step_failed_details is not None:
            result["StepFailedDetails"] = self.step_failed_details.to_dict()
        if self.chained_invoke_pending_details is not None:
            result["ChainedInvokePendingDetails"] = (
                self.chained_invoke_pending_details.to_dict()
            )
        if self.chained_invoke_started_details is not None:
            result["ChainedInvokeStartedDetails"] = (
                self.chained_invoke_started_details.to_dict()
            )
        if self.chained_invoke_succeeded_details is not None:
            result["ChainedInvokeSucceededDetails"] = (
                self.chained_invoke_succeeded_details.to_dict()
            )
        if self.chained_invoke_failed_details is not None:
            result["ChainedInvokeFailedDetails"] = (
                self.chained_invoke_failed_details.to_dict()
            )
        if self.chained_invoke_timed_out_details is not None:
            result["ChainedInvokeTimedOutDetails"] = (
                self.chained_invoke_timed_out_details.to_dict()
            )
        if self.chained_invoke_stopped_details is not None:
            result["ChainedInvokeStoppedDetails"] = (
                self.chained_invoke_stopped_details.to_dict()
            )
        if self.callback_started_details is not None:
            result["CallbackStartedDetails"] = self.callback_started_details.to_dict()
        if self.callback_succeeded_details is not None:
            result["CallbackSucceededDetails"] = (
                self.callback_succeeded_details.to_dict()
            )
        if self.callback_failed_details is not None:
            result["CallbackFailedDetails"] = self.callback_failed_details.to_dict()
        if self.callback_timed_out_details is not None:
            result["CallbackTimedOutDetails"] = (
                self.callback_timed_out_details.to_dict()
            )
        if self.invocation_completed_details is not None:
            result["InvocationCompletedDetails"] = (
                self.invocation_completed_details.to_dict()
            )
        return result

    # region execution
    @classmethod
    def create_execution_event_started(cls, context: EventCreationContext) -> Event:
        execution_details: ExecutionDetails | None = context.operation.execution_details
        event_input: EventInput | None = (
            EventInput.from_details(execution_details, context.include_execution_data)
            if execution_details
            else None
        )
        execution_timeout: int | None = (
            context.start_durable_execution_input.execution_timeout_seconds
        )

        return cls(
            event_type=EventType.EXECUTION_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_started_details=ExecutionStartedDetails(
                input=event_input,
                execution_timeout=execution_timeout,
            ),
        )

    @classmethod
    def create_execution_event_succeeded(cls, context: EventCreationContext) -> Event:
        result: EventResult | None = (
            EventResult.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_succeeded_details=ExecutionSucceededDetails(result=result),
        )

    @classmethod
    def create_execution_event_failed(cls, context: EventCreationContext) -> Event:
        error: EventError | None = (
            EventError.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                include=context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_failed_details=ExecutionFailedDetails(error=error),
        )

    @classmethod
    def create_execution_event_timed_out(cls, context: EventCreationContext) -> Event:
        error: EventError | None = (
            EventError.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                include=context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_TIMED_OUT.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_timed_out_details=ExecutionTimedOutDetails(error=error),
        )

    @classmethod
    def create_execution_event_stopped(cls, context: EventCreationContext) -> Event:
        error: EventError | None = (
            EventError.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                include=context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_STOPPED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_stopped_details=ExecutionStoppedDetails(error=error),
        )

    @classmethod
    def create_execution_event(cls, context: EventCreationContext) -> Event:
        """Create execution event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_execution_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_execution_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_execution_event_failed(context)
            case OperationStatus.TIMED_OUT:
                return cls.create_execution_event_timed_out(context)
            case OperationStatus.STOPPED:
                return cls.create_execution_event_stopped(context)
            case _:
                msg = f"Operation status {context.operation.status} is not valid for execution operations. Valid statuses are: STARTED, SUCCEEDED, FAILED, TIMED_OUT, STOPPED"
                raise InvalidParameterValueException(msg)

    # endregion execution

    # region context
    @classmethod
    def create_context_event_started(cls, context: EventCreationContext) -> Event:
        return cls(
            event_type=EventType.CONTEXT_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            context_started_details=ContextStartedDetails(),
        )

    @classmethod
    def create_context_event_succeeded(cls, context: EventCreationContext) -> Event:
        context_details: ContextDetails | None = context.operation.context_details
        event_result: EventResult | None = (
            EventResult.from_details(context_details, context.include_execution_data)
            if context_details
            else None
        )
        return cls(
            event_type=EventType.CONTEXT_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            context_succeeded_details=ContextSucceededDetails(result=event_result),
        )

    @classmethod
    def create_context_event_failed(cls, context: EventCreationContext) -> Event:
        context_details: ContextDetails | None = context.operation.context_details
        event_error: EventError | None = (
            EventError.from_details(context_details) if context_details else None
        )
        return cls(
            event_type=EventType.CONTEXT_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            context_failed_details=ContextFailedDetails(error=event_error),
        )

    @classmethod
    def create_context_event(cls, context: EventCreationContext) -> Event:
        """Create context event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_context_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_context_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_context_event_failed(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for context operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, FAILED"
                )
                raise InvalidParameterValueException(msg)

    # endregion context

    # region wait
    @classmethod
    def create_wait_event_started(cls, context: EventCreationContext) -> Event:
        wait_details: WaitDetails | None = context.operation.wait_details
        scheduled_end_timestamp: datetime.datetime | None = (
            wait_details.scheduled_end_timestamp if wait_details else None
        )
        duration: int | None = None
        if (
            wait_details
            and wait_details.scheduled_end_timestamp
            and context.operation.start_timestamp
        ):
            duration = round(
                (
                    wait_details.scheduled_end_timestamp
                    - context.operation.start_timestamp
                ).total_seconds()
            )
        return cls(
            event_type=EventType.WAIT_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            wait_started_details=WaitStartedDetails(
                duration=duration,
                scheduled_end_timestamp=scheduled_end_timestamp,
            ),
        )

    @classmethod
    def create_wait_event_succeeded(cls, context: EventCreationContext) -> Event:
        wait_details: WaitDetails | None = context.operation.wait_details
        duration: int | None = None
        if (
            wait_details
            and wait_details.scheduled_end_timestamp
            and context.operation.start_timestamp
        ):
            duration = round(
                (
                    wait_details.scheduled_end_timestamp - context.start_timestamp
                ).total_seconds()
            )
        return cls(
            event_type=EventType.WAIT_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            wait_succeeded_details=WaitSucceededDetails(duration=duration),
        )

    @classmethod
    def create_wait_event_cancelled(cls, context: EventCreationContext) -> Event:
        error: EventError | None = None
        if (
            context.operation_update
            and context.operation_update.operation_type == OperationType.WAIT
            and context.operation_update.action == OperationAction.CANCEL
        ):
            error = EventError(
                context.operation_update.error, not context.include_execution_data
            )
        return cls(
            event_type=EventType.WAIT_CANCELLED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            wait_cancelled_details=WaitCancelledDetails(error=error),
        )

    @classmethod
    def create_wait_event(cls, context: EventCreationContext) -> Event:
        """Create wait event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_wait_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_wait_event_succeeded(context)
            case OperationStatus.CANCELLED:
                return cls.create_wait_event_cancelled(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for wait operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, CANCELLED"
                )
                raise InvalidParameterValueException(msg)

    # endregion wait

    # region step
    @classmethod
    def create_step_event_started(cls, context: EventCreationContext) -> Event:
        return cls(
            event_type=EventType.STEP_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            step_started_details=StepStartedDetails(),
        )

    @classmethod
    def create_step_event_succeeded(cls, context: EventCreationContext) -> Event:
        step_details: StepDetails | None = context.operation.step_details
        event_result: EventResult | None = (
            EventResult.from_details(step_details, context.include_execution_data)
            if step_details
            else None
        )
        return cls(
            event_type=EventType.STEP_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            step_succeeded_details=StepSucceededDetails(
                result=event_result,
                retry_details=context.get_retry_details(),
            ),
        )

    @classmethod
    def create_step_event_failed(cls, context: EventCreationContext) -> Event:
        step_details: StepDetails | None = context.operation.step_details
        event_error: EventError | None = (
            EventError.from_details(
                step_details, include=context.include_execution_data
            )
            if step_details
            else None
        )
        return cls(
            event_type=EventType.STEP_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            step_failed_details=StepFailedDetails(
                error=event_error,
                retry_details=context.get_retry_details(),
            ),
        )

    @classmethod
    def create_step_event(cls, context: EventCreationContext) -> Event:
        """Create step event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_step_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_step_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_step_event_failed(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for step operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, FAILED"
                )
                raise InvalidParameterValueException(msg)

    # endregion step

    # region chained_invoke
    @classmethod
    def create_chained_invoke_event_pending(
        cls, context: EventCreationContext
    ) -> Event:
        input: EventInput = EventInput.from_start_durable_execution_input(
            context.start_durable_execution_input, context.include_execution_data
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_pending_details=ChainedInvokePendingDetails(
                input=input,
                function_name=context.start_durable_execution_input.function_name,
            ),
        )

    @classmethod
    def create_chained_invoke_event_started(
        cls, context: EventCreationContext
    ) -> Event:
        return cls(
            event_type=EventType.CHAINED_INVOKE_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_started_details=ChainedInvokeStartedDetails(
                durable_execution_arn=context.durable_execution_arn
            ),
        )

    @classmethod
    def create_chained_invoke_event_succeeded(
        cls, context: EventCreationContext
    ) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_result: EventResult | None = (
            EventResult.from_details(
                chained_invoke_details, context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_succeeded_details=ChainedInvokeSucceededDetails(
                result=event_result
            ),
        )

    @classmethod
    def create_chained_invoke_event_failed(cls, context: EventCreationContext) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_error: EventError | None = (
            EventError.from_details(
                chained_invoke_details, include=context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_failed_details=ChainedInvokeFailedDetails(error=event_error),
        )

    @classmethod
    def create_chained_invoke_event_timed_out(
        cls, context: EventCreationContext
    ) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_error: EventError | None = (
            EventError.from_details(
                chained_invoke_details, include=context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_TIMED_OUT.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_timed_out_details=ChainedInvokeTimedOutDetails(
                error=event_error
            ),
        )

    @classmethod
    def create_chained_invoke_event_stopped(
        cls, context: EventCreationContext
    ) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_error: EventError | None = (
            EventError.from_details(
                chained_invoke_details, include=context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_STOPPED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_stopped_details=ChainedInvokeStoppedDetails(
                error=event_error
            ),
        )

    @classmethod
    def create_chained_invoke_event(cls, context: EventCreationContext) -> Event:
        """Create chained invoke event based on action."""
        match context.operation.status:
            case OperationStatus.PENDING:
                return cls.create_chained_invoke_event_pending(context)
            case OperationStatus.STARTED:
                return cls.create_chained_invoke_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_chained_invoke_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_chained_invoke_event_failed(context)
            case OperationStatus.TIMED_OUT:
                return cls.create_chained_invoke_event_timed_out(context)
            case OperationStatus.STOPPED:
                return cls.create_chained_invoke_event_stopped(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for chained invoke operations. Valid statuses are: "
                    f"STARTED, SUCCEEDED, FAILED, TIMED_OUT, STOPPED"
                )
                raise InvalidParameterValueException(msg)

    # endregion chained_invoke

    # region callback
    @classmethod
    def create_callback_event_started(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        callback_id: str | None = (
            callback_details.callback_id if callback_details else None
        )
        callback_options: CallbackOptions | None = (
            context.operation_update.callback_options
            if context.operation_update
            else None
        )
        timeout: int | None = (
            callback_options.timeout_seconds if callback_options else None
        )
        heartbeat_timeout: int | None = (
            callback_options.heartbeat_timeout_seconds if callback_options else None
        )
        return cls(
            event_type=EventType.CALLBACK_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_started_details=CallbackStartedDetails(
                callback_id=callback_id,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
            ),
        )

    @classmethod
    def create_callback_event_succeeded(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        event_result: EventResult | None = (
            EventResult.from_details(callback_details, context.include_execution_data)
            if callback_details
            else None
        )
        return cls(
            event_type=EventType.CALLBACK_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_succeeded_details=CallbackSucceededDetails(result=event_result),
        )

    @classmethod
    def create_callback_event_failed(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        event_error: EventError | None = (
            EventError.from_details(callback_details) if callback_details else None
        )
        return cls(
            event_type=EventType.CALLBACK_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_failed_details=CallbackFailedDetails(error=event_error),
        )

    @classmethod
    def create_callback_event_timed_out(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        event_error: EventError | None = (
            EventError.from_details(callback_details) if callback_details else None
        )
        return cls(
            event_type=EventType.CALLBACK_TIMED_OUT.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_timed_out_details=CallbackTimedOutDetails(error=event_error),
        )

    @classmethod
    def create_callback_event(cls, context: EventCreationContext) -> Event:
        """Create callback event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_callback_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_callback_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_callback_event_failed(context)
            case OperationStatus.TIMED_OUT:
                return cls.create_callback_event_timed_out(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for callback operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, FAILED, TIMED_OUT"
                )
                raise InvalidParameterValueException(msg)

    # endregion callback

    # region invocation_completed
    @classmethod
    def create_invocation_completed(
        cls,
        event_id: int,
        event_timestamp: datetime.datetime,
        start_timestamp: datetime.datetime,
        end_timestamp: datetime.datetime,
        request_id: str,
    ) -> Event:
        """Create invocation completed event."""
        return cls(
            event_type=EventType.INVOCATION_COMPLETED.value,
            event_timestamp=event_timestamp,
            event_id=event_id,
            invocation_completed_details=InvocationCompletedDetails(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                request_id=request_id,
            ),
        )

    # endregion invocation_completed

    @classmethod
    def create_event_started(cls, context: EventCreationContext) -> Event:
        """Convert operation to started event."""
        if context.operation.start_timestamp is None:
            msg: str = "Operation start timestamp cannot be None when converting to started event"
            raise InvalidParameterValueException(msg)

        match context.operation.operation_type:
            case OperationType.EXECUTION:
                return cls.create_execution_event_started(context)
            case OperationType.CONTEXT:
                return cls.create_context_event_started(context)
            case OperationType.WAIT:
                return cls.create_wait_event_started(context)
            case OperationType.STEP:
                return cls.create_step_event_started(context)
            case OperationType.CHAINED_INVOKE:
                return cls.create_chained_invoke_event_started(context)
            case OperationType.CALLBACK:
                return cls.create_callback_event_started(context)
            case _:
                msg = f"Unknown operation type: {context.operation.operation_type}"
                raise InvalidParameterValueException(msg)

    @classmethod
    def from_event_with_id(cls, event: Event, event_id: int) -> Event:
        """Create a new Event from an existing event with updated event_id."""
        return cls(
            event_type=event.event_type,
            event_timestamp=event.event_timestamp,
            sub_type=event.sub_type,
            event_id=event_id,
            operation_id=event.operation_id,
            name=event.name,
            parent_id=event.parent_id,
            execution_started_details=event.execution_started_details,
            execution_succeeded_details=event.execution_succeeded_details,
            execution_failed_details=event.execution_failed_details,
            execution_timed_out_details=event.execution_timed_out_details,
            execution_stopped_details=event.execution_stopped_details,
            context_started_details=event.context_started_details,
            context_succeeded_details=event.context_succeeded_details,
            context_failed_details=event.context_failed_details,
            wait_started_details=event.wait_started_details,
            wait_succeeded_details=event.wait_succeeded_details,
            wait_cancelled_details=event.wait_cancelled_details,
            step_started_details=event.step_started_details,
            step_succeeded_details=event.step_succeeded_details,
            step_failed_details=event.step_failed_details,
            chained_invoke_pending_details=event.chained_invoke_pending_details,
            chained_invoke_started_details=event.chained_invoke_started_details,
            chained_invoke_succeeded_details=event.chained_invoke_succeeded_details,
            chained_invoke_failed_details=event.chained_invoke_failed_details,
            chained_invoke_timed_out_details=event.chained_invoke_timed_out_details,
            chained_invoke_stopped_details=event.chained_invoke_stopped_details,
            callback_started_details=event.callback_started_details,
            callback_succeeded_details=event.callback_succeeded_details,
            callback_failed_details=event.callback_failed_details,
            callback_timed_out_details=event.callback_timed_out_details,
        )

    @classmethod
    def create_event_terminated(cls, context: EventCreationContext) -> Event:
        """Convert operation to finished event."""
        operation: Operation = context.operation
        if operation.end_timestamp is None:
            msg: str = "Operation end timestamp cannot be None when converting to finished event"
            raise InvalidParameterValueException(msg)

        if operation.status not in TERMINAL_STATUSES:
            msg = f"Operation status must be one of SUCCEEDED, FAILED, TIMED_OUT, STOPPED, or CANCELLED. Got: {operation.status}"
            raise InvalidParameterValueException(msg)

        match operation.operation_type:
            case OperationType.EXECUTION:
                return cls.create_execution_event(context)
            case OperationType.CONTEXT:
                return cls.create_context_event(context)
            case OperationType.WAIT:
                return cls.create_wait_event(context)
            case OperationType.STEP:
                return cls.create_step_event(context)
            case OperationType.CHAINED_INVOKE:
                return cls.create_chained_invoke_event(context)
            case OperationType.CALLBACK:
                return cls.create_callback_event(context)
            case _:
                msg = f"Unknown operation type: {operation.operation_type}"
                raise InvalidParameterValueException(msg)


# endregion event_class


# region history_models
@dataclass(frozen=True)
class HistoryEventTypeConfig:
    """Configuration for how to process a specific event type."""

    operation_type: OperationType | None
    operation_status: OperationStatus | None
    is_start_event: bool
    is_end_event: bool
    has_result: bool  # Whether this event type contains result/error data


# Mapping of event types to their processing configuration
# This matches the TypeScript historyEventTypes constant
HISTORY_EVENT_TYPES: dict[str, HistoryEventTypeConfig] = {
    "ExecutionStarted": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "ExecutionFailed": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "ExecutionStopped": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.STOPPED,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "ExecutionSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "ExecutionTimedOut": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.TIMED_OUT,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "CallbackStarted": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "CallbackFailed": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "CallbackSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "CallbackTimedOut": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.TIMED_OUT,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ContextStarted": HistoryEventTypeConfig(
        operation_type=OperationType.CONTEXT,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "ContextFailed": HistoryEventTypeConfig(
        operation_type=OperationType.CONTEXT,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ContextSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.CONTEXT,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeStarted": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "ChainedInvokeFailed": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeTimedOut": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.TIMED_OUT,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeCancelled": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.CANCELLED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "StepStarted": HistoryEventTypeConfig(
        operation_type=OperationType.STEP,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "StepFailed": HistoryEventTypeConfig(
        operation_type=OperationType.STEP,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "StepSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.STEP,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "WaitStarted": HistoryEventTypeConfig(
        operation_type=OperationType.WAIT,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=True,
    ),
    "WaitSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.WAIT,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "WaitCancelled": HistoryEventTypeConfig(
        operation_type=OperationType.WAIT,
        operation_status=OperationStatus.CANCELLED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    # TODO: add support for populating invocation information from InvocationCompleted event
    "InvocationCompleted": HistoryEventTypeConfig(
        operation_type=None,
        operation_status=None,
        is_start_event=False,
        is_end_event=False,
        has_result=True,
    ),
}


def events_to_operations(events: list[Event]) -> list[Operation]:
    """Convert a list of history events into operations.

    This function processes raw history events and groups them by operation ID,
    creating comprehensive operation objects following the TypeScript pattern from
    aws-durable-execution-sdk-js-testing.

    Multiple events for the same operation_id are merged together, with each event
    contributing its specific fields (e.g., CallbackStarted provides callback_id,
    CallbackSucceeded provides result).

    Args:
        events: List of history events to process

    Returns:
        List of operations, one per unique operation ID

    Raises:
        InvalidParameterValueException: When required fields are missing from an event

    Note:
        InvocationCompleted events are currently skipped as they don't represent
        operations. Future enhancement: populate invocation information from these
        events (TODO).
    """
    operations_map: dict[str, Operation] = {}

    for event in events:
        if not event.event_type:
            msg = "Missing required 'event_type' field in event"
            raise InvalidParameterValueException(msg)

        # Get event type configuration
        event_config: HistoryEventTypeConfig | None = HISTORY_EVENT_TYPES.get(
            event.event_type
        )
        if not event_config:
            msg = f"Unknown event type: {event.event_type}"
            raise InvalidParameterValueException(msg)

        # TODO: add support for populating invocation information from InvocationCompleted event
        if event.event_type == "InvocationCompleted":
            continue

        if not event.operation_id:
            msg = f"Missing required 'operation_id' field in event {event.event_id}"
            raise InvalidParameterValueException(msg)

        # Get previous operation if it exists
        previous_operation: Operation | None = operations_map.get(event.operation_id)

        # Get operation type and status from configuration
        operation_type: OperationType = (
            event_config.operation_type or OperationType.EXECUTION
        )
        status: OperationStatus = (
            event_config.operation_status or OperationStatus.PENDING
        )

        # Parse sub_type
        sub_type: OperationSubType | None = None
        if event.sub_type:
            try:
                sub_type = OperationSubType(event.sub_type)
            except ValueError as e:
                raise InvalidParameterValueException(str(e)) from e

        # Create base operation
        operation = Operation(
            operation_id=event.operation_id,
            operation_type=operation_type,
            status=status,
            name=event.name,
            parent_id=event.parent_id,
            sub_type=sub_type,
            start_timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
        )

        # Merge with previous operation if it exists
        # Most fields are immutable, so they get preserved from previous events
        if previous_operation:
            operation = replace(
                operation,
                name=operation.name or previous_operation.name,
                parent_id=operation.parent_id or previous_operation.parent_id,
                sub_type=operation.sub_type or previous_operation.sub_type,
                start_timestamp=previous_operation.start_timestamp,
                end_timestamp=previous_operation.end_timestamp,
                execution_details=previous_operation.execution_details,
                context_details=previous_operation.context_details,
                step_details=previous_operation.step_details,
                wait_details=previous_operation.wait_details,
                callback_details=previous_operation.callback_details,
                chained_invoke_details=previous_operation.chained_invoke_details,
            )

        # Set timestamps based on event configuration
        if event_config.is_start_event:
            operation = replace(operation, start_timestamp=event.event_timestamp)
        if event_config.is_end_event:
            operation = replace(operation, end_timestamp=event.event_timestamp)

        # Add operation-specific details incrementally
        # Each event type contributes only the fields it has

        # EXECUTION details
        if (
            operation_type == OperationType.EXECUTION
            and event.execution_started_details
            and event.execution_started_details.input
        ):
            operation = replace(
                operation,
                execution_details=ExecutionDetails(
                    input_payload=event.execution_started_details.input.payload
                ),
            )

        # CALLBACK details - merge callback_id, result, and error from different events
        if operation_type == OperationType.CALLBACK:
            existing_cb: CallbackDetails | None = operation.callback_details
            callback_id: str = existing_cb.callback_id if existing_cb else ""
            result: str | None = existing_cb.result if existing_cb else None
            error: ErrorObject | None = existing_cb.error if existing_cb else None

            # CallbackStarted provides callback_id
            if event.callback_started_details:
                callback_id = event.callback_started_details.callback_id or callback_id

            # CallbackSucceeded provides result
            if (
                event.callback_succeeded_details
                and event.callback_succeeded_details.result
            ):
                result = event.callback_succeeded_details.result.payload

            # CallbackFailed provides error
            if event.callback_failed_details and event.callback_failed_details.error:
                error = event.callback_failed_details.error.payload

            # CallbackTimedOut provides error
            if (
                event.callback_timed_out_details
                and event.callback_timed_out_details.error
            ):
                error = event.callback_timed_out_details.error.payload

            operation = replace(
                operation,
                callback_details=CallbackDetails(
                    callback_id=callback_id,
                    result=result,
                    error=error,
                ),
            )

        # STEP details - only update if this event type has result data
        if operation_type == OperationType.STEP and event_config.has_result:
            existing_step: StepDetails | None = operation.step_details
            result_val: str | None = existing_step.result if existing_step else None
            error_val: ErrorObject | None = (
                existing_step.error if existing_step else None
            )
            attempt: int = existing_step.attempt if existing_step else 0
            next_attempt_ts: datetime.datetime | None = (
                existing_step.next_attempt_timestamp if existing_step else None
            )

            # StepSucceeded provides result
            if event.step_succeeded_details:
                if event.step_succeeded_details.result:
                    result_val = event.step_succeeded_details.result.payload
                if event.step_succeeded_details.retry_details:
                    attempt = event.step_succeeded_details.retry_details.current_attempt

            # StepFailed provides error and retry details
            if event.step_failed_details:
                if event.step_failed_details.error:
                    error_val = event.step_failed_details.error.payload
                if event.step_failed_details.retry_details:
                    attempt = event.step_failed_details.retry_details.current_attempt
                    if (
                        event.step_failed_details.retry_details.next_attempt_delay_seconds
                        is not None
                    ):
                        next_attempt_ts = event.event_timestamp + datetime.timedelta(
                            seconds=event.step_failed_details.retry_details.next_attempt_delay_seconds
                        )

            operation = replace(
                operation,
                step_details=StepDetails(
                    result=result_val,
                    error=error_val,
                    attempt=attempt,
                    next_attempt_timestamp=next_attempt_ts,
                ),
            )

        # WAIT details
        if operation_type == OperationType.WAIT and event.wait_started_details:
            operation = replace(
                operation,
                wait_details=WaitDetails(
                    scheduled_end_timestamp=event.wait_started_details.scheduled_end_timestamp
                ),
            )

        # CONTEXT details - only update if this event type has result data (matching TypeScript hasResult)
        if operation_type == OperationType.CONTEXT and event_config.has_result:
            if (
                event.context_succeeded_details
                and event.context_succeeded_details.result
            ):
                operation = replace(
                    operation,
                    context_details=ContextDetails(
                        result=event.context_succeeded_details.result.payload,
                        error=None,
                    ),
                )
            elif event.context_failed_details and event.context_failed_details.error:
                operation = replace(
                    operation,
                    context_details=ContextDetails(
                        result=None,
                        error=event.context_failed_details.error.payload,
                    ),
                )

        # CHAINED_INVOKE details - only update if this event type has result data (matching TypeScript hasResult)
        if operation_type == OperationType.CHAINED_INVOKE and event_config.has_result:
            if (
                event.chained_invoke_succeeded_details
                and event.chained_invoke_succeeded_details.result
            ):
                operation = replace(
                    operation,
                    chained_invoke_details=ChainedInvokeDetails(
                        result=event.chained_invoke_succeeded_details.result.payload,
                        error=None,
                    ),
                )
            elif (
                event.chained_invoke_failed_details
                and event.chained_invoke_failed_details.error
            ):
                operation = replace(
                    operation,
                    chained_invoke_details=ChainedInvokeDetails(
                        result=None,
                        error=event.chained_invoke_failed_details.error.payload,
                    ),
                )

        # Store in map
        operations_map[event.operation_id] = operation

    return list(operations_map.values())


@dataclass(frozen=True)
class GetDurableExecutionHistoryRequest:
    """Request to get durable execution history."""

    durable_execution_arn: str
    include_execution_data: bool | None = None
    reverse_order: bool | None = None
    marker: str | None = None
    max_items: int = 0

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionHistoryRequest:
        return cls(
            durable_execution_arn=data["DurableExecutionArn"],
            include_execution_data=data.get("IncludeExecutionData"),
            reverse_order=data.get("ReverseOrder"),
            marker=data.get("Marker"),
            max_items=data.get("MaxItems", 0),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"DurableExecutionArn": self.durable_execution_arn}
        if self.include_execution_data is not None:
            result["IncludeExecutionData"] = self.include_execution_data
        if self.reverse_order is not None:
            result["ReverseOrder"] = self.reverse_order
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        return result


@dataclass(frozen=True)
class GetDurableExecutionHistoryResponse:
    """Response containing durable execution history events."""

    events: list[Event]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionHistoryResponse:
        events = [Event.from_dict(event_data) for event_data in data.get("Events", [])]
        return cls(
            events=events,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"Events": [event.to_dict() for event in self.events]}
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class ListDurableExecutionsByFunctionRequest:
    """Request to list durable executions by function."""

    function_name: str
    qualifier: str | None = None
    durable_execution_name: str | None = None
    status_filter: list[str] | None = None
    started_after: str | None = None
    started_before: str | None = None
    marker: str | None = None
    max_items: int = 0
    reverse_order: bool | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsByFunctionRequest:
        # Handle query parameters that may be lists
        function_name = data.get("FunctionName")
        if isinstance(function_name, list):
            function_name = function_name[0] if function_name else ""
        elif not function_name:
            function_name = ""

        qualifier = data.get("Qualifier") or data.get("functionVersion")
        if isinstance(qualifier, list):
            qualifier = qualifier[0] if qualifier else None

        durable_execution_name = data.get("DurableExecutionName") or data.get(
            "executionName"
        )
        if isinstance(durable_execution_name, list):
            durable_execution_name = (
                durable_execution_name[0] if durable_execution_name else None
            )

        status_filter = data.get("StatusFilter") or data.get("statusFilter")
        if isinstance(status_filter, list):
            status_filter = status_filter if status_filter else None
        elif status_filter:
            status_filter = [status_filter]

        started_after = data.get("StartedAfter") or data.get("startedAfter")
        if isinstance(started_after, list):
            started_after = started_after[0] if started_after else None

        started_before = data.get("StartedBefore") or data.get("startedBefore")
        if isinstance(started_before, list):
            started_before = started_before[0] if started_before else None

        marker = data.get("Marker") or data.get("marker")
        if isinstance(marker, list):
            marker = marker[0] if marker else None

        max_items = data.get("MaxItems") or data.get("maxItems", 0)
        if isinstance(max_items, list):
            max_items = int(max_items[0]) if max_items else 0

        reverse_order = data.get("ReverseOrder") or data.get("reverseOrder")
        if isinstance(reverse_order, list):
            reverse_order = (
                reverse_order[0].lower() in ("true", "1", "yes")
                if reverse_order
                else None
            )
        elif isinstance(reverse_order, str):
            reverse_order = reverse_order.lower() in ("true", "1", "yes")

        return cls(
            function_name=function_name,
            qualifier=qualifier,
            durable_execution_name=durable_execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            marker=marker,
            max_items=max_items,
            reverse_order=reverse_order,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"FunctionName": self.function_name}
        if self.qualifier is not None:
            result["Qualifier"] = self.qualifier
        if self.durable_execution_name is not None:
            result["DurableExecutionName"] = self.durable_execution_name
        if self.status_filter is not None:
            result["StatusFilter"] = self.status_filter
        if self.started_after is not None:
            result["StartedAfter"] = self.started_after
        if self.started_before is not None:
            result["StartedBefore"] = self.started_before
        if self.marker is not None:
            result["Marker"] = self.marker
        if self.max_items is not None:
            result["MaxItems"] = self.max_items
        if self.reverse_order is not None:
            result["ReverseOrder"] = self.reverse_order
        return result


@dataclass(frozen=True)
class ListDurableExecutionsByFunctionResponse:
    """Response containing list of durable executions by function."""

    durable_executions: list[Execution]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ListDurableExecutionsByFunctionResponse:
        executions = [
            Execution.from_dict(exec_data)
            for exec_data in data.get("DurableExecutions", [])
        ]
        return cls(
            durable_executions=executions,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutions": [exe.to_dict() for exe in self.durable_executions]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


# endregion history_models


# region callback_models
# Callback-related models
@dataclass(frozen=True)
class SendDurableExecutionCallbackSuccessRequest:
    """Request to send callback success."""

    callback_id: str
    result: bytes | None = None

    @classmethod
    def from_dict(cls, data: dict) -> SendDurableExecutionCallbackSuccessRequest:
        return cls(
            callback_id=data["CallbackId"],
            result=data.get("Result"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CallbackId": self.callback_id}
        if self.result is not None:
            result["Result"] = self.result
        return result


@dataclass(frozen=True)
class SendDurableExecutionCallbackSuccessResponse:
    """Response from sending callback success."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackFailureRequest:
    """Request to send callback failure."""

    callback_id: str
    error: ErrorObject | None = None

    @classmethod
    def from_dict(
        cls, data: dict, callback_id: str
    ) -> SendDurableExecutionCallbackFailureRequest:
        error = ErrorObject.from_dict(data) if data else None

        return cls(
            callback_id=callback_id,
            error=error,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CallbackId": self.callback_id}
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class SendDurableExecutionCallbackFailureResponse:
    """Response from sending callback failure."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackHeartbeatRequest:
    """Request to send callback heartbeat."""

    callback_id: str

    @classmethod
    def from_dict(cls, data: dict) -> SendDurableExecutionCallbackHeartbeatRequest:
        return cls(callback_id=data["CallbackId"])

    def to_dict(self) -> dict[str, Any]:
        return {"CallbackId": self.callback_id}


@dataclass(frozen=True)
class SendDurableExecutionCallbackHeartbeatResponse:
    """Response from sending callback heartbeat."""


# endregion callback_models


# region checkpoint_models
# Checkpoint-related models
@dataclass(frozen=True)
class CheckpointUpdatedExecutionState:
    """Updated execution state from checkpoint."""

    operations: list[Operation]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CheckpointUpdatedExecutionState:
        operations = [
            Operation.from_dict(op_data) for op_data in data.get("Operations", [])
        ]
        return cls(
            operations=operations,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "Operations": [op.to_dict() for op in self.operations]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class CheckpointDurableExecutionRequest:
    """Request to checkpoint a durable execution."""

    durable_execution_arn: str
    checkpoint_token: str
    updates: list[OperationUpdate] | None = None
    client_token: str | None = None

    @classmethod
    def from_dict(
        cls, data: dict, durable_execution_arn: str
    ) -> CheckpointDurableExecutionRequest:
        updates = None
        if updates_data := data.get("Updates"):
            updates = []
            for update_data in updates_data:
                # Map dictionary fields to OperationUpdate constructor parameters
                operation_update = OperationUpdate(
                    operation_id=update_data["Id"],
                    operation_type=OperationType(update_data["Type"]),
                    action=OperationAction(update_data["Action"]),
                    parent_id=update_data.get("ParentId"),
                    name=update_data.get("Name"),
                    sub_type=OperationSubType(update_data["SubType"])
                    if update_data.get("SubType")
                    else None,
                    payload=update_data.get("Payload"),
                    error=ErrorObject.from_dict(update_data["Error"])
                    if update_data.get("Error")
                    else None,
                    context_options=ContextOptions.from_dict(
                        update_data["ContextOptions"]
                    )
                    if update_data.get("ContextOptions")
                    else None,
                    step_options=StepOptions.from_dict(update_data["StepOptions"])
                    if update_data.get("StepOptions")
                    else None,
                    wait_options=WaitOptions.from_dict(update_data["WaitOptions"])
                    if update_data.get("WaitOptions")
                    else None,
                    callback_options=CallbackOptions.from_dict(
                        update_data["CallbackOptions"]
                    )
                    if update_data.get("CallbackOptions")
                    else None,
                    chained_invoke_options=ChainedInvokeOptions.from_dict(
                        update_data["ChainedInvokeOptions"]
                    )
                    if update_data.get("ChainedInvokeOptions")
                    else None,
                )
                updates.append(operation_update)

        return cls(
            durable_execution_arn=durable_execution_arn,
            checkpoint_token=data["CheckpointToken"],
            updates=updates,
            client_token=data.get("ClientToken"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
        }
        if self.updates is not None:
            result["Updates"] = [update.to_dict() for update in self.updates]
        if self.client_token is not None:
            result["ClientToken"] = self.client_token
        return result


@dataclass(frozen=True)
class CheckpointDurableExecutionResponse:
    """Response from checkpointing a durable execution."""

    checkpoint_token: str
    new_execution_state: CheckpointUpdatedExecutionState | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CheckpointDurableExecutionResponse:
        new_execution_state = None
        if state_data := data.get("NewExecutionState"):
            new_execution_state = CheckpointUpdatedExecutionState.from_dict(state_data)

        return cls(
            checkpoint_token=data["CheckpointToken"],
            new_execution_state=new_execution_state,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CheckpointToken": self.checkpoint_token}
        if self.new_execution_state is not None:
            result["NewExecutionState"] = self.new_execution_state.to_dict()
        return result


# endregion checkpoint_models


# region error_models
# Error response structure for consistent error handling
@dataclass(frozen=True)
class ErrorResponse:
    """Structured error response for web service operations."""

    error_type: str
    error_message: str
    error_code: str | None = None
    request_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> ErrorResponse:
        """Create ErrorResponse from dictionary.

        Args:
            data: Dictionary containing error data

        Returns:
            ErrorResponse: The error response object
        """
        error_data = data.get("error", data)  # Support both nested and flat structures
        return cls(
            error_type=error_data["type"],
            error_message=error_data["message"],
            error_code=error_data.get("code"),
            request_id=error_data.get("requestId"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert ErrorResponse to dictionary.

        Returns:
            dict: Dictionary representation of the error response
        """
        error_data: dict[str, Any] = {
            "type": self.error_type,
            "message": self.error_message,
        }

        if self.error_code is not None:
            error_data["code"] = self.error_code
        if self.request_id is not None:
            error_data["requestId"] = self.request_id

        return {"error": error_data}


# endregion error_models
