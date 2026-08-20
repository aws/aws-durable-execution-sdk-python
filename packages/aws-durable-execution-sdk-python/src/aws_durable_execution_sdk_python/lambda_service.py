from __future__ import annotations

import builtins
import copy
import datetime
import functools
import logging
import re
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, TypeAlias, cast

import boto3
import botocore.session
from botocore.config import Config

from aws_durable_execution_sdk_python.__about__ import __version__
from aws_durable_execution_sdk_python.exceptions import (
    CheckpointError,
    DurableOperationError,
    GetExecutionStateError,
    SerDesError,
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


@functools.lru_cache(maxsize=1)
def lambda_function_name_matcher() -> tuple[re.Pattern[str], int]:
    """The allowed shape and length for a Lambda function name, read from botocore."""
    shape = (
        botocore.session.get_session()
        .get_service_model("lambda")
        .shape_for("NamespacedFunctionName")
    )
    return re.compile(shape.metadata["pattern"]), shape.metadata["max"]


def _is_in_var_dir(module_file: str = __file__) -> bool:
    """Return True if this SDK is installed under /var/lang/.

    Lambda bundled Python runtimes install packages at
    /var/lang/lib/pythonX.Y/site-packages/.
    """
    return module_file.startswith("/var/lang/")


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
    DISTRIBUTED_MAP = "DISTRIBUTED_MAP"

    @classmethod
    def from_sub_type(cls, sub_type: OperationSubType) -> OperationType:
        match sub_type:
            case OperationSubType.STEP | OperationSubType.WAIT_FOR_CONDITION:
                return OperationType.STEP
            case OperationSubType.WAIT:
                return OperationType.WAIT
            case OperationSubType.CHAINED_INVOKE:
                return OperationType.CHAINED_INVOKE
            case OperationSubType.CALLBACK:
                return OperationType.CALLBACK
            case OperationSubType.DISTRIBUTED_MAP:
                return OperationType.DISTRIBUTED_MAP
            case (
                OperationSubType.WAIT_FOR_CALLBACK
                | OperationSubType.RUN_IN_CHILD_CONTEXT
                | OperationSubType.MAP
                | OperationSubType.MAP_ITERATION
                | OperationSubType.PARALLEL
                | OperationSubType.PARALLEL_BRANCH
            ):
                return OperationType.CONTEXT
            case _:
                raise ValueError(f"Unknown operation sub-type {sub_type}")


class CallbackTimeoutType(Enum):
    TIMEOUT = "Callback.Timeout"
    HEARTBEAT = "Callback.Heartbeat"


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
    DISTRIBUTED_MAP = "DistributedMap"


class InvocationStatus(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"

    # Used internally only: the invocation failed and the backend will retry
    RETRY = "RETRY"


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
    def create_retry(cls, error: ErrorObject) -> DurableExecutionInvocationOutput:
        """Create a failed invocation output."""
        return cls(status=InvocationStatus.RETRY, error=error)


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


def _qualified_error_type(exception: BaseException) -> str:
    """Return the fully-qualified class name for use as the wire ErrorType.

    Builtins (e.g. ValueError) are left unqualified; everything else is
    prefixed with its module path.
    """
    cls_: type[BaseException] = type(exception)
    module: str = "" if cls_.__module__ == "builtins" else f"{cls_.__module__}."
    return f"{module}{cls_.__qualname__}"


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
        # SerDesError and subclasses pin to the base discriminator so replay
        # always reconstructs them as SerDesError.
        if isinstance(exception, SerDesError):
            return cls(
                message=exception.message,
                type=f"{SerDesError.__module__}.{SerDesError.__qualname__}",
                data=exception.data,
                stack_trace=exception.stack_trace,
            )
        # The wire ErrorType is the fully-qualified class name, with builtins
        # left unqualified for brevity.
        wire_type: str = _qualified_error_type(exception)
        if isinstance(exception, DurableOperationError):
            return cls(
                message=exception.message,
                type=wire_type,
                data=exception.data,
                stack_trace=exception.stack_trace,
            )
        return cls(
            message=str(exception),
            type=wire_type,
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

    def to_durable_operation_error(self) -> DurableOperationError:
        return DurableOperationError.from_error_fields(
            error_type=self.type,
            message=self.message,
            data=self.data,
            stack_trace=self.stack_trace,
        )

    def raise_as_operation_error(
        self, operation_error_cls: builtins.type[DurableOperationError]
    ) -> NoReturn:
        """Raise the operation's typed error reconstructed from this ErrorObject.

        Used by both the first-run terminal-failure path and replay, so the
        surfaced error is identical (a durable-execution determinism guarantee):
        the wrapper is ``operation_error_cls`` (or ``SerDesError`` for a serdes
        failure) carrying this object's ``error_type``/``data``/``stack_trace``,
        and ``__cause__`` is the escaping error rebuilt via the registry (a typed
        subclass when known, else the base ``DurableOperationError``).
        """
        cause: DurableOperationError = DurableOperationError.from_error_fields(
            error_type=self.type,
            message=self.message,
            data=self.data,
            stack_trace=self.stack_trace,
        )
        # A serdes failure surfaces as SerDesError regardless of the operation
        # kind, so it is catchable as itself on both first run and replay.
        if self.type == f"{SerDesError.__module__}.{SerDesError.__qualname__}":
            raise SerDesError(
                message=self.message,
                error_type=self.type,
                data=self.data,
                stack_trace=self.stack_trace,
            ) from cause
        raise operation_error_cls(
            message=self.message,
            error_type=self.type,
            data=self.data,
            stack_trace=self.stack_trace,
        ) from cause


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
class DistributedMapResultItemWire:
    """Wire representation of a single map run item's outcome."""

    item_id: str
    status: str
    output: Any | None = None
    error: ErrorObject | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapResultItemWire:
        error_raw = data.get("Error")
        return cls(
            item_id=data.get("ItemId", ""),
            status=data.get("Status", ""),
            output=data.get("Output"),
            error=ErrorObject.from_dict(error_raw) if error_raw else None,
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {
            "ItemId": self.item_id,
            "Status": self.status,
        }
        if self.output is not None:
            result["Output"] = self.output
        if self.error is not None:
            result["Error"] = self.error.to_dict()
        return result


@dataclass(frozen=True)
class DistributedMapDetails:
    status: str
    completion_reason: str
    distributed_map_run_arn: str | None = None
    completion_details: str | None = None
    total_count: int | None = None
    success_count: int = 0
    failure_count: int = 0
    unprocessed_count: int = 0
    results: tuple[DistributedMapResultItemWire, ...] | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapDetails:
        results_raw = data.get("Results")
        return cls(
            status=data["Status"],
            completion_reason=data["CompletionReason"],
            distributed_map_run_arn=data.get("DistributedMapRunArn"),
            completion_details=data.get("CompletionDetails"),
            total_count=data.get("TotalCount"),
            success_count=data.get("SuccessCount", 0),
            failure_count=data.get("FailureCount", 0),
            unprocessed_count=data.get("UnprocessedCount", 0),
            results=tuple(
                DistributedMapResultItemWire.from_dict(item) for item in results_raw
            )
            if results_raw is not None
            else None,
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
class DistributedMapSourceWire:
    """Wire representation of a map run source."""

    source_type: str
    max_items: int | None = None
    inline_items: tuple[Any, ...] | None = None
    s3_config: MutableMapping[str, Any] | None = None
    reader_config: MutableMapping[str, Any] | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {"Type": self.source_type}
        if self.source_type == "INLINE":
            result["InlineSourceConfig"] = {"Items": list(self.inline_items or ())}
        elif self.source_type == "S3" and self.s3_config is not None:
            result["S3SourceConfig"] = dict(self.s3_config)
        elif self.source_type == "READER_FUNCTION" and self.reader_config is not None:
            result["ReaderFunctionSourceConfig"] = dict(self.reader_config)
        if self.max_items is not None:
            result["MaxItemsToRead"] = self.max_items
        return result

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapSourceWire:
        source_type = data.get("Type", "INLINE")
        inline_cfg = data.get("InlineSourceConfig") or {}
        return cls(
            source_type=source_type,
            max_items=data.get("MaxItemsToRead"),
            inline_items=tuple(inline_cfg.get("Items", ()))
            if source_type == "INLINE"
            else None,
            s3_config=data.get("S3SourceConfig"),
            reader_config=data.get("ReaderFunctionSourceConfig"),
        )


@dataclass(frozen=True)
class DistributedMapProcessorWire:
    """Wire representation of a map run processor."""

    function_name: str
    function_response_types: tuple[str, ...] | None = None
    batch_size: int | None = None
    max_retry_attempts: int | None = None
    max_retry_duration_seconds: int | None = None
    durable_execution_name_prefix: str | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {"FunctionName": self.function_name}
        if self.function_response_types:
            result["FunctionResponseTypes"] = list(self.function_response_types)
        if self.batch_size is not None:
            result["BatchSize"] = self.batch_size
        if self.max_retry_attempts is not None:
            result["MaxRetryAttempts"] = self.max_retry_attempts
        if self.max_retry_duration_seconds is not None:
            result["MaxRetryDurationSeconds"] = self.max_retry_duration_seconds
        if self.durable_execution_name_prefix is not None:
            result["DurableExecutionNamePrefix"] = self.durable_execution_name_prefix
        return result

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapProcessorWire:
        response_types = data.get("FunctionResponseTypes")
        return cls(
            function_name=data.get("FunctionName", ""),
            function_response_types=tuple(response_types) if response_types else None,
            batch_size=data.get("BatchSize"),
            max_retry_attempts=data.get("MaxRetryAttempts"),
            max_retry_duration_seconds=data.get("MaxRetryDurationSeconds"),
            durable_execution_name_prefix=data.get("DurableExecutionNamePrefix"),
        )


@dataclass(frozen=True)
class DistributedMapCompletionConfigWire:
    """Wire representation of a map run completion (failure-tolerance) config."""

    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: float | None = None
    minimum_sample_size: int | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {}
        if self.tolerated_failure_count is not None:
            result["ToleratedFailureCount"] = self.tolerated_failure_count
        if self.tolerated_failure_percentage is not None:
            result["ToleratedFailurePercentage"] = self.tolerated_failure_percentage
        if self.minimum_sample_size is not None:
            result["MinimumSampleSize"] = self.minimum_sample_size
        return result

    @classmethod
    def from_dict(
        cls, data: MutableMapping[str, Any]
    ) -> DistributedMapCompletionConfigWire:
        return cls(
            tolerated_failure_count=data.get("ToleratedFailureCount"),
            tolerated_failure_percentage=data.get("ToleratedFailurePercentage"),
            minimum_sample_size=data.get("MinimumSampleSize"),
        )


@dataclass(frozen=True)
class DistributedMapDestinationWire:
    """Wire representation of a map run destination config."""

    on_success: MutableMapping[str, Any] | None = None
    on_failure: MutableMapping[str, Any] | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {}
        if self.on_success is not None:
            result["OnSuccess"] = dict(self.on_success)
        if self.on_failure is not None:
            result["OnFailure"] = dict(self.on_failure)
        return result

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapDestinationWire:
        return cls(
            on_success=data.get("OnSuccess"),
            on_failure=data.get("OnFailure"),
        )


@dataclass(frozen=True)
class DistributedMapResultCollectionWire:
    """Wire representation of the map run result-collection setting."""

    mode: str

    def to_dict(self) -> MutableMapping[str, Any]:
        return {"Mode": self.mode}

    @classmethod
    def from_dict(
        cls, data: MutableMapping[str, Any]
    ) -> DistributedMapResultCollectionWire:
        return cls(mode=data.get("Mode", "NONE"))


@dataclass(frozen=True)
class DistributedMapOptions:
    """Configuration options for starting a map run."""

    max_concurrency: int
    source: DistributedMapSourceWire
    processor: DistributedMapProcessorWire
    destination: DistributedMapDestinationWire | None = None
    completion_config: DistributedMapCompletionConfigWire | None = None
    result_collection: DistributedMapResultCollectionWire | None = None
    timeout_seconds: int | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapOptions:
        source_raw = data.get("Source") or {}
        processor_raw = data.get("Processor") or {}
        destination_raw = data.get("Destination")
        completion_raw = data.get("CompletionConfig")
        result_collection_raw = data.get("ResultCollection")
        return cls(
            max_concurrency=data["MaxConcurrency"],
            source=DistributedMapSourceWire.from_dict(source_raw),
            processor=DistributedMapProcessorWire.from_dict(processor_raw),
            destination=DistributedMapDestinationWire.from_dict(destination_raw)
            if destination_raw is not None
            else None,
            completion_config=DistributedMapCompletionConfigWire.from_dict(
                completion_raw
            )
            if completion_raw is not None
            else None,
            result_collection=DistributedMapResultCollectionWire.from_dict(
                result_collection_raw
            )
            if result_collection_raw is not None
            else None,
            timeout_seconds=data.get("TimeoutSeconds"),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {
            "MaxConcurrency": self.max_concurrency,
            "Source": self.source.to_dict(),
            "Processor": self.processor.to_dict(),
        }
        if self.destination is not None:
            result["Destination"] = self.destination.to_dict()
        if self.completion_config is not None:
            result["CompletionConfig"] = self.completion_config.to_dict()
        if self.result_collection is not None:
            result["ResultCollection"] = self.result_collection.to_dict()
        if self.timeout_seconds is not None:
            result["TimeoutSeconds"] = self.timeout_seconds
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
    distributed_map_options: DistributedMapOptions | None = None

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
        if self.payload is not None:
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
        if self.distributed_map_options:
            result["DistributedMapOptions"] = self.distributed_map_options.to_dict()

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

        distributed_map_options = None
        if distributed_map_options_data := data.get("DistributedMapOptions"):
            distributed_map_options = DistributedMapOptions.from_dict(
                distributed_map_options_data
            )

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
            distributed_map_options=distributed_map_options,
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
        payload: str | None,
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
        cls, identifier: OperationIdentifier, payload: str | None
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
        payload: str | None,
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

    # region map run
    @classmethod
    def create_distributed_map_start(
        cls,
        identifier: OperationIdentifier,
        distributed_map_options: DistributedMapOptions,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: DISTRIBUTED_MAP, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.DISTRIBUTED_MAP,
            sub_type=OperationSubType.DISTRIBUTED_MAP,
            action=OperationAction.START,
            name=identifier.name,
            distributed_map_options=distributed_map_options,
        )

    # endregion map run

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
        cls, identifier: OperationIdentifier, payload: str | None
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
        payload: str | None,
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
    distributed_map_details: DistributedMapDetails | None = None

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

        distributed_map_details = None
        if distributed_map_details_input := data.get("DistributedMapDetails"):
            distributed_map_details = DistributedMapDetails.from_dict(
                distributed_map_details_input
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
            distributed_map_details=distributed_map_details,
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
            context_dict: MutableMapping[str, Any] = {
                "Result": self.context_details.result,
            }
            if self.context_details.error:
                context_dict["Error"] = self.context_details.error.to_dict()
            if self.context_details.replay_children:
                context_dict["ReplayChildren"] = self.context_details.replay_children
            result["ContextDetails"] = context_dict
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
        if self.distributed_map_details:
            distributed_map_details_dict: MutableMapping[str, Any] = {
                "Status": self.distributed_map_details.status,
                "CompletionReason": self.distributed_map_details.completion_reason,
                "SuccessCount": self.distributed_map_details.success_count,
                "FailureCount": self.distributed_map_details.failure_count,
                "UnprocessedCount": self.distributed_map_details.unprocessed_count,
            }
            if self.distributed_map_details.distributed_map_run_arn:
                distributed_map_details_dict["DistributedMapRunArn"] = (
                    self.distributed_map_details.distributed_map_run_arn
                )
            if self.distributed_map_details.completion_details:
                distributed_map_details_dict["CompletionDetails"] = (
                    self.distributed_map_details.completion_details
                )
            if self.distributed_map_details.total_count is not None:
                distributed_map_details_dict["TotalCount"] = (
                    self.distributed_map_details.total_count
                )
            if self.distributed_map_details.results is not None:
                distributed_map_details_dict["Results"] = [
                    item.to_dict() for item in self.distributed_map_details.results
                ]
            result["DistributedMapDetails"] = distributed_map_details_dict
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

    # None on the terminal checkpoint that ends the execution.
    checkpoint_token: str | None
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
            checkpoint_token=data.get("CheckpointToken"),
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
                    user_agent_extra=f"aws-durable-execution-sdk-python/{__version__}{'-bundled' if _is_in_var_dir() else ''}",
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
        # A checkpoint token is required. Raise a clear, retryable error (so the
        # invocation re-drives) rather than letting the client reject an empty
        # value with an opaque validation error.
        if not checkpoint_token:
            raise CheckpointError("Cannot checkpoint without a checkpoint token.")
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
        # A checkpoint token is required. Raise a clear, retryable error (so the
        # invocation re-drives) rather than letting the client reject an empty
        # value with an opaque validation error.
        if not checkpoint_token:
            raise GetExecutionStateError(
                "Cannot get execution state without a checkpoint token."
            )
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
