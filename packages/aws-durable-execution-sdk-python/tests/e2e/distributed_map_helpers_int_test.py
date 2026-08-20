"""Integration tests for the durable distributed map authoring wrappers.

The durable variants are ``@durable_execution`` handlers; they run through the
full invocation harness with a mocked checkpoint backend.
"""

from __future__ import annotations

import json
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.distributed_map_helpers import (
    create_distributed_map_batch_handler_with_durable_execution,
    create_distributed_map_item_handler_with_durable_execution,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,  # noqa: F401  (ensures decorator import path is valid)
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationStatus,
    OperationType,
)

_ARN = "arn:aws:lambda:us-east-1:123456789012:function:proc:1/durable-execution/execution-1"


def _lambda_context():
    ctx = Mock()
    ctx.aws_request_id = "test-request-id"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    ctx.invoked_function_arn = "test-arn"
    ctx.tenant_id = None
    return ctx


def _event(records: list[dict]):
    return {
        "DurableExecutionArn": _ARN,
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {
                        "InputPayload": json.dumps({"records": records})
                    },
                }
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _run(handler, records: list[dict]):
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

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint
        return handler(_event(records), _lambda_context())


def test_durable_item_handler_reports_results():
    handler = create_distributed_map_item_handler_with_durable_execution(
        lambda _ctx, item: item * 2
    )
    result = _run(handler, [{"itemId": "0", "body": 2}, {"itemId": "1", "body": 3}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemResults"] == [
        {"itemIdentifier": "0", "output": 4},
        {"itemIdentifier": "1", "output": 6},
    ]
    assert data["batchItemFailures"] == []


def test_durable_item_handler_captures_failure():
    def process(_ctx, item):
        if item == "bad":
            msg = "boom"
            raise ValueError(msg)
        return item

    handler = create_distributed_map_item_handler_with_durable_execution(process)
    result = _run(
        handler, [{"itemId": "0", "body": "ok"}, {"itemId": "1", "body": "bad"}]
    )
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemResults"] == [{"itemIdentifier": "0", "output": "ok"}]
    assert data["batchItemFailures"][0]["itemIdentifier"] == "1"
    assert data["batchItemFailures"][0]["error"]["errorType"] == "ValueError"


def test_durable_batch_handler_returns_value():
    handler = create_distributed_map_batch_handler_with_durable_execution(
        lambda _ctx, items: {"count": len(items)}
    )
    result = _run(handler, [{"itemId": "0", "body": 1}, {"itemId": "1", "body": 2}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) == {"count": 2}


def test_durable_item_handler_failures_form():
    handler = create_distributed_map_item_handler_with_durable_execution(
        lambda _ctx, item: item, report="failures"
    )
    result = _run(handler, [{"itemId": "0", "body": 1}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data == {"batchItemFailures": []}
