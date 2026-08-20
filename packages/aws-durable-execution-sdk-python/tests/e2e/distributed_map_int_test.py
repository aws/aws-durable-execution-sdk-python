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

from aws_durable_execution_sdk_python.concurrency.models import DistributedMapResult
from aws_durable_execution_sdk_python.config import (
    DistributedMapConfig,
    DistributedMapProcessor,
)
from aws_durable_execution_sdk_python.context import DurableContext
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
                    "Status": "SUCCEEDED",
                    "ParentId": "execution-1",
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
            "distributed_map_id": summary.distributed_map_id,
        }

    # First invocation suspends.
    first = _run(handler, _initial_event())
    assert first["Status"] == InvocationStatus.PENDING.value

    # Replay with the run completed.
    _map_run_id, replay_event = _replay_event(
        {
            "Status": "SUCCEEDED",
            "CompletionReason": "ALL_COMPLETED",
            "SuccessCount": 2,
            "FailureCount": 0,
            "UnprocessedCount": 0,
            "TotalCount": 2,
            "DistributedMapRunArn": "arn:aws:lambda:us-east-1:123456789012:map-run:abc",
        }
    )
    replay = _run(handler, replay_event)
    assert replay["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(replay["Result"])
    assert data == {
        "status": "SUCCEEDED",
        "success": 2,
        "failure": 0,
        "distributed_map_id": "abc",
    }


def test_map_run_collect_results_returns_items():
    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        result = context.distributed_map(
            ["a", "b"],
            DistributedMapProcessor.report_item_results("proc"),
            max_concurrency=2,
            config=DistributedMapConfig(collect_results=True),
        )
        assert isinstance(result, DistributedMapResult)
        return {
            "results": result.get_results(),
            "errors": [e.error_message for e in result.get_errors()],
        }

    _map_run_id, replay_event = _replay_event(
        {
            "Status": "SUCCEEDED",
            "CompletionReason": "ALL_COMPLETED",
            "SuccessCount": 1,
            "FailureCount": 1,
            "UnprocessedCount": 0,
            "TotalCount": 2,
            "Results": [
                {"ItemId": "0", "Status": "SUCCEEDED", "Output": 5},
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
            "Status": "FAILED",
            "CompletionReason": "FAILURE_TOLERANCE_EXCEEDED",
            "SuccessCount": 0,
            "FailureCount": 1,
            "UnprocessedCount": 0,
            "TotalCount": 1,
        }
    )
    replay = _run(handler, replay_event)
    assert replay["Status"] == InvocationStatus.FAILED.value
