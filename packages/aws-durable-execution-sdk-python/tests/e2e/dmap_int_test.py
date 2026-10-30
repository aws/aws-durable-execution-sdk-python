"""Integration tests for ctx.distributed_map through a full durable_execution invocation.

Drives the real suspend/resume flow: the first invocation starts the DISTRIBUTED_MAP
operation and suspends (PENDING); a replay invocation with the operation
completed (carrying DistributedMapDetails) resolves to the summary/result. The backend
is mocked; no emulator or real service is involved.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python.dmap.handlers import (
    ItemFailure,
    distributed_map_item_handler,
)
from aws_durable_execution_sdk_python.dmap.models import DistributedMapResult
from aws_durable_execution_sdk_python.config import (
    DistributedMapResultConfig,
    DistributedMapProcessor,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.exceptions import (
    CheckpointError,
    DurableOperationError,
    ValidationError,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationStatus,
    OperationType,
)
from tests.test_helpers import operation_id_sequence


def _lambda_context():
    ctx = Mock()
    ctx.aws_request_id = "test-request-id"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    ctx.invoked_function_arn = "test-arn"
    ctx.tenant_id = None
    return ctx


_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:test-func:1"
    "/durable-execution/exec-001/inv-001"
)


def _initial_event():
    return {
        "DurableExecutionArn": _ARN,
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                }
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _replay_event(distributed_map_details: dict):
    distributed_map_id = next(operation_id_sequence())
    return distributed_map_id, {
        "DurableExecutionArn": _ARN,
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                },
                {
                    "Id": distributed_map_id,
                    "Type": "DISTRIBUTED_MAP",
                    "SubType": "DistributedMap",
                    "Status": "SUCCEEDED",
                    "DistributedMapDetails": distributed_map_details,
                },
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _tracking_checkpoint():
    """Checkpoint mock that records created operations as STARTED."""
    calls: list = []
    operations = [
        Operation(
            operation_id="execution-1",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]

    def mock_checkpoint(
        durable_execution_arn, checkpoint_token, updates, client_token="token"
    ):  # noqa: S107
        calls.append(updates)
        for update in updates:
            operations.append(
                Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    sub_type=update.sub_type,
                    name=update.name,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                )
            )
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=operations.copy()
            ),
        )

    return calls, mock_checkpoint


def _run(handler, event):
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        _calls, mock_checkpoint = _tracking_checkpoint()
        mock_client.checkpoint = mock_checkpoint
        return handler(event, _lambda_context())


def test_map_run_suspends_then_resumes_with_summary():
    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        summary = context.distributed_map(
            ["a", "b"],
            DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=2,
        )
        return {
            "status": summary.status.value,
            "success": summary.success_count,
            "failure": summary.failure_count,
            "distributed_map_run_arn": summary.distributed_map_run_arn,
        }

    # First invocation suspends.
    first = _run(handler, _initial_event())
    assert first["Status"] == InvocationStatus.PENDING.value

    # Replay with the run completed.
    _map_run_id, replay_event = _replay_event(
        {
            "SourceExhausted": True,
            "SuccessCount": 2,
            "FailureCount": 0,
            "UnprocessedCount": 0,
            "TotalCount": 2,
            "DistributedMapRunArn": "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1/distributed-map-run/abc",
        }
    )
    replay = _run(handler, replay_event)
    assert replay["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(replay["Result"])
    assert data == {
        "status": "SUCCEEDED",
        "success": 2,
        "failure": 0,
        "distributed_map_run_arn": "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1/distributed-map-run/abc",
    }


def test_map_run_result_config_returns_items():
    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        result: DistributedMapResult[Any] = context.distributed_map(
            ["a", "b"],
            DistributedMapProcessor.report_item_outcomes("proc"),
            max_concurrency=2,
            config=DistributedMapResultConfig(),
        )
        assert isinstance(result, DistributedMapResult)
        return {
            "results": result.get_results(),
            "errors": [
                e.message if e is not None else None for e in result.get_errors()
            ],
        }

    _map_run_id, replay_event = _replay_event(
        {
            "SourceExhausted": True,
            "SuccessCount": 1,
            "FailureCount": 1,
            "UnprocessedCount": 0,
            "TotalCount": 2,
            "Results": [
                {"ItemId": "0", "Status": "SUCCEEDED", "Output": "5"},
                {
                    "ItemId": "1",
                    "Status": "FAILED",
                    "Error": {"ErrorType": "E", "ErrorMessage": "boom"},
                },
            ],
        }
    )
    replay = _run(handler, replay_event)
    assert replay["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(replay["Result"])
    assert data == {"results": [5], "errors": ["boom"]}


def test_map_run_throw_if_error_fails_execution():
    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        summary = context.distributed_map(
            ["a"],
            DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
        )
        summary.throw_if_error()
        return {"status": summary.status.value}

    _map_run_id, replay_event = _replay_event(
        {
            "SourceExhausted": False,
            "Error": {
                "ErrorType": "DistributedMapFailure",
                "ErrorMessage": "failure tolerance exceeded",
            },
            "SuccessCount": 0,
            "FailureCount": 1,
            "UnprocessedCount": 0,
            "TotalCount": 1,
        }
    )
    replay = _run(handler, replay_event)
    assert replay["Status"] == InvocationStatus.FAILED.value


def test_item_error_data_and_stack_trace_reach_the_parent_result():
    """An item handler's error members survive the whole round trip.

    The processor wrapper writes them as camelCase errorData/stackTrace, the
    service re-emits them as PascalCase ErrorData/StackTrace, and the parent
    reads them back off DistributedMapResult. Each hop is tested on its own
    elsewhere; this test joins them. The service hop is bridged with the SDK's
    own two shapes rather than hand-written keys, so the test fails if either
    side's key names drift.

    The error is raised as a DurableOperationError because ErrorObject only
    carries data and a stack trace for that type and for SerDesError.
    """

    def process(_item: Any) -> Any:
        raise DurableOperationError(
            message="boom",
            error_type="Inner",
            data="payload",
            stack_trace=["frame one", "frame two"],
        )

    response = distributed_map_item_handler(process)(
        {"records": [{"itemId": "0", "body": "1"}]}, None
    )
    failure = ItemFailure.from_dict(response["batchItemFailures"][0])
    assert failure.error is not None

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        result: DistributedMapResult[Any] = context.distributed_map(
            ["a"],
            DistributedMapProcessor.report_item_outcomes("proc"),
            max_concurrency=1,
            config=DistributedMapResultConfig(),
        )
        assert isinstance(result, DistributedMapResult)
        [err] = result.get_errors()
        assert err is not None
        return {
            "type": err.type,
            "message": err.message,
            "data": err.data,
            "stack_trace": err.stack_trace,
        }

    _map_run_id, replay_event = _replay_event(
        {
            "SourceExhausted": True,
            "SuccessCount": 0,
            "FailureCount": 1,
            "UnprocessedCount": 0,
            "TotalCount": 1,
            "Results": [
                {
                    "ItemId": failure.item_identifier,
                    "Status": "FAILED",
                    "Error": failure.error.to_dict(),
                }
            ],
        }
    )
    replay = _run(handler, replay_event)
    assert replay["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(replay["Result"]) == {
        "type": f"{DurableOperationError.__module__}.{DurableOperationError.__qualname__}",
        "message": "boom",
        "data": "payload",
        "stack_trace": ["frame one", "frame two"],
    }


def _rejecting_checkpoint(message: str):
    """Checkpoint mock that rejects a DISTRIBUTED_MAP start the way the service does.

    It raises the CheckpointError that LambdaClient builds from a 400
    InvalidParameterValueException, so everything after that point is the real
    path: the checkpoint thread wraps it in BackgroundThreadError and the
    invocation handler decides the outcome.
    """
    calls: list = []
    rejection = CheckpointError.from_exception(
        ClientError(
            {
                "Error": {
                    "Code": "InvalidParameterValueException",
                    "Message": message,
                },
                "ResponseMetadata": {
                    "RequestId": "req-1",
                    "HostId": "",
                    "HTTPStatusCode": 400,
                    "HTTPHeaders": {},
                    "RetryAttempts": 0,
                },
            },
            "CheckpointDurableExecution",
        )
    )

    def mock_checkpoint(
        durable_execution_arn, checkpoint_token, updates, client_token="token"
    ):  # noqa: S107
        calls.append(updates)
        if any(u.operation_type is OperationType.DISTRIBUTED_MAP for u in updates):
            raise rejection
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(operations=[]),
        )

    return calls, mock_checkpoint


def test_start_rejected_by_the_service_fails_the_execution():
    """A rejected START fails the execution, as a rejected checkpoint does anywhere.

    The SDK does not turn the rejection into a ValidationError. The failure is
    raised on the checkpoint thread, so customer code cannot catch it, and the
    invocation reports FAILED carrying the service's message.
    """
    message = "Source.MaxItemsToRead is out of range"
    caught: list = []

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        try:
            context.distributed_map(
                ["a"],
                DistributedMapProcessor.report_batch_outcome("proc"),
                max_concurrency=1,
            )
        except Exception as exc:  # noqa: BLE001
            caught.append(type(exc).__name__)
        return {"reached_the_end": True}

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        calls, mock_checkpoint = _rejecting_checkpoint(message)
        mock_client.checkpoint = mock_checkpoint
        result = handler(_initial_event(), _lambda_context())

    assert result["Status"] == InvocationStatus.FAILED.value
    assert message in result["Error"]["ErrorMessage"]
    assert caught == []
    # One request was sent, carrying the rejected START. Nothing was recorded.
    assert len(calls) == 1
    assert [u.operation_type for u in calls[0]] == [OperationType.DISTRIBUTED_MAP]


def test_configuration_the_sdk_rejects_itself_stays_catchable():
    """A configuration error the SDK finds is a catchable ValidationError.

    The SDK validates what it can before checkpointing, so the error is raised
    at the call site with nothing sent. Customer code catches it and the
    invocation carries on to a later durable operation.
    """

    @durable_step
    def finish(_step_context: StepContext) -> str:
        return "done"

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        caught = None
        try:
            context.distributed_map(
                ["a"],
                DistributedMapProcessor.report_batch_outcome("proc"),
                max_concurrency=0,
            )
        except ValidationError as exc:
            caught = str(exc)
        return {"caught": caught, "stepped": context.step(finish())}

    result = _run(handler, _initial_event())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert "max_concurrency must be greater than zero" in data["caught"]
    assert data["stepped"] == "done"
