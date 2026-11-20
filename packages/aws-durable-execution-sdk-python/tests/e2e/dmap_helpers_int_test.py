"""Integration tests for the distributed map helpers inside durable handlers.

Each test defines a ``@durable_execution`` handler that calls a helper, and runs
it through the full invocation harness with a mocked checkpoint backend.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.dmap.helpers import (
    process_distributed_map_batch,
    process_distributed_map_items,
)
from aws_durable_execution_sdk_python.exceptions import DurableOperationError
from aws_durable_execution_sdk_python.filesystem_serdes import FileSystemSerDes
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
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext

_RUN_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/exec1/invoke1/distributed-map-run/abc"
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
                        "InputPayload": json.dumps(
                            {"distributedMapRunArn": _RUN_ARN, "records": records}
                        )
                    },
                }
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _mock_checkpoint():
    """Return a checkpoint stub that accepts every update and echoes the state."""
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

    return mock_checkpoint


def _run(handler, records: list[dict]):
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = _mock_checkpoint()
        return handler(_event(records), _lambda_context())


def _items_handler(func, **options):
    """Build a durable handler that returns process_distributed_map_items."""

    @durable_execution
    def handler(event, context):
        return process_distributed_map_items(context, event, func, **options)

    return handler


def _batch_handler(func, **options):
    """Build a durable handler that calls process_distributed_map_batch.

    ``func`` receives the handler's own DurableContext first.
    """

    @durable_execution
    def handler(event, context):
        return process_distributed_map_batch(
            event, lambda items, batch_ctx: func(context, items, batch_ctx), **options
        )

    return handler


def test_items_helper_reports_results():
    handler = _items_handler(lambda _context, item, _ctx: item * 2)
    result = _run(handler, [{"itemId": "0", "body": "2"}, {"itemId": "1", "body": "3"}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "4"},
        {"itemIdentifier": "1", "output": "6"},
    ]
    assert data["batchItemFailures"] == []


def test_items_helper_captures_failure():
    def process(_context, item, _ctx):
        if item == "bad":
            msg = "boom"
            raise ValueError(msg)
        return item

    handler = _items_handler(process)
    result = _run(
        handler, [{"itemId": "0", "body": '"ok"'}, {"itemId": "1", "body": '"bad"'}]
    )
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemResults"] == [{"itemIdentifier": "0", "output": '"ok"'}]
    assert data["batchItemFailures"][0]["itemIdentifier"] == "1"
    assert data["batchItemFailures"][0]["error"]["errorType"] == "ValueError"


def test_items_helper_forwards_every_error_member():
    """The branch error's data and stack trace reach the service, not just type and message."""

    def process(_context, _item, _ctx):
        raise DurableOperationError(
            message="boom",
            error_type="Inner",
            data="payload",
            stack_trace=["frame one", "frame two"],
        )

    handler = _items_handler(process)
    result = _run(handler, [{"itemId": "0", "body": "1"}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])

    error = data["batchItemFailures"][0]["error"]
    # A branch error is checkpointed and rebuilt, which normalizes errorType to
    # the raised class's qualified name.
    assert error["errorType"] == (
        f"{DurableOperationError.__module__}.{DurableOperationError.__qualname__}"
    )
    assert error["errorMessage"] == "boom"
    assert error["errorData"] == "payload"
    assert error["stackTrace"] == ["frame one", "frame two"]


def test_batch_helper_records_no_output():
    """A batch processor reports no output, so the return value is not forwarded."""
    seen: list = []

    def process(_context, items, _ctx):
        seen.extend(items)
        return {"count": len(items)}

    handler = _batch_handler(process)
    result = _run(handler, [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert json.loads(result["Result"]) is None
    assert seen == [1, 2]


def test_items_helper_fails_only_the_item_whose_output_cannot_be_serialized():
    """A result serdes failure is that item's failure, not the batch's."""

    class _RejectsBad(SerDes[Any]):
        def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
            if value == "bad":
                msg = "cannot serialize"
                raise TypeError(msg)
            return json.dumps(value)

        def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
            return json.loads(data)

    handler = _items_handler(
        lambda _context, item, _ctx: item, result_serdes=_RejectsBad()
    )
    result = _run(
        handler, [{"itemId": "0", "body": '"ok"'}, {"itemId": "1", "body": '"bad"'}]
    )
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemResults"] == [{"itemIdentifier": "0", "output": '"ok"'}]
    assert data["batchItemFailures"] == [
        {
            "itemIdentifier": "1",
            "error": {"errorType": "TypeError", "errorMessage": "cannot serialize"},
        }
    ]


def test_items_helper_serializes_a_custom_result_type():
    """Only the result serdes can write this output, so it must run before the checkpoint."""

    class _Order:
        def __init__(self, order_id: str) -> None:
            self.order_id = order_id

    class _OrderSerDes(SerDes[Any]):
        def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
            return json.dumps({"orderId": value.order_id})

        def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
            return _Order(json.loads(data)["orderId"])

    handler = _items_handler(
        lambda _context, item, _ctx: _Order(item), result_serdes=_OrderSerDes()
    )
    result = _run(handler, [{"itemId": "0", "body": '"o-1"'}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemFailures"] == []
    assert data["batchItemResults"] == [
        {"itemIdentifier": "0", "output": '{"orderId": "o-1"}'}
    ]


def test_items_helper_omits_the_output_of_a_handler_returning_none():
    handler = _items_handler(lambda _context, _item, _ctx: None)
    result = _run(handler, [{"itemId": "0", "body": "1"}])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data == {
        "batchItemFailures": [],
        "batchItemResults": [{"itemIdentifier": "0"}],
    }


def test_items_helper_runs_items_one_at_a_time_by_default():
    lock = threading.Lock()
    in_flight = 0
    peak = 0

    def process(_context, item, _ctx):
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        # Hold the branch so that a wider bound would overlap this item with the
        # next one, which is what makes the peak below meaningful.
        time.sleep(0.02)
        with lock:
            in_flight -= 1
        return item

    handler = _items_handler(process)
    result = _run(handler, [{"itemId": str(i), "body": "1"} for i in range(4)])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert peak == 1


def test_items_helper_honors_requested_concurrency():
    # Each item blocks until all four have arrived, so the batch only finishes
    # if the map really runs four items at once. At a smaller bound the first
    # item waits out the timeout and every item fails.
    barrier = threading.Barrier(4, timeout=5)

    def process(_context, item, _ctx):
        barrier.wait()
        return item

    handler = _items_handler(process, concurrency=4)
    result = _run(handler, [{"itemId": str(i), "body": str(i)} for i in range(4)])
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    data = json.loads(result["Result"])
    assert data["batchItemFailures"] == []
    assert data["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "0"},
        {"itemIdentifier": "1", "output": "1"},
        {"itemIdentifier": "2", "output": "2"},
        {"itemIdentifier": "3", "output": "3"},
    ]


def test_items_helper_stores_each_output_under_its_own_key(tmp_path):
    """A serdes that stores values under their context keeps every output."""
    fs = FileSystemSerDes(str(tmp_path))
    handler = _items_handler(
        lambda _context, item, _ctx: f"out-{item}", result_serdes=fs
    )
    records = [{"itemId": f"item-{i}", "body": json.dumps(i)} for i in range(3)]
    result = _run(handler, records)
    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    outputs = [r["output"] for r in json.loads(result["Result"])["batchItemResults"]]
    assert len(set(outputs)) == 3
    assert [fs.deserialize(o, SerDesContext()) for o in outputs] == [
        "out-0",
        "out-1",
        "out-2",
    ]


@pytest.mark.parametrize("batch", [False, True], ids=["item", "batch"])
def test_helper_reads_and_writes_each_value_under_its_own_context(batch):
    lock = threading.Lock()
    contexts: list[SerDesContext] = []

    class _Recording(SerDes[Any]):
        def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
            with lock:
                contexts.append(serdes_context)
            return json.dumps(value)

        def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:
            with lock:
                contexts.append(serdes_context)
            return json.loads(data)

    serdes = _Recording()
    if batch:
        handler = _batch_handler(
            lambda _context, _items, _ctx: None, item_serdes=serdes
        )
    else:
        handler = _items_handler(
            lambda _context, item, _ctx: item, item_serdes=serdes, result_serdes=serdes
        )
    _run(handler, [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}])
    assert (
        len({c.operation_id for c in contexts}) == len(contexts) == (2 if batch else 4)
    )
