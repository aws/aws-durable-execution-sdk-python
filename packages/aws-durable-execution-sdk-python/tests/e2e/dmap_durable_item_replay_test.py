"""A durable item handler resumes after a wait without rerunning a completed item."""

from __future__ import annotations

import json
import threading
from collections import Counter
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dmap.handlers import (
    durable_distributed_map_item_handler,
)
from aws_durable_execution_sdk_python.types import DistributedMapItemContext
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)

_RUN_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/exec1/invoke1/distributed-map-run/abc"
)

_LOCK = threading.Lock()
_CALLS: dict[str, Counter[str]] = {"func": Counter(), "serialize": Counter()}


def _count(kind: str, item: str) -> None:
    with _LOCK:
        _CALLS[kind][item] += 1


class _TaggingSerDes(SerDes[str]):
    """Prefixes each output so a double encoding would be visible."""

    def serialize(self, value: str, serdes_context: SerDesContext) -> str:  # noqa: ARG002
        _count("serialize", value)
        return f"tag:{value}"

    def deserialize(self, data: str, serdes_context: SerDesContext) -> str:  # noqa: ARG002
        return data.removeprefix("tag:")


def _process(ctx: DurableContext, item: str, _ctx: DistributedMapItemContext) -> str:
    _count("func", item)
    if item == "b":
        ctx.wait(Duration.from_seconds(1), name="item-b-wait")
    return item


handler = durable_distributed_map_item_handler(
    _process, result_serdes=_TaggingSerDes(), concurrency=2
)


def test_durable_item_handler_resumes_after_wait_without_rerunning_a_completed_sibling() -> (
    None
):
    for counter in _CALLS.values():
        counter.clear()
    event = {
        "distributedMapRunArn": _RUN_ARN,
        "records": [
            {"itemId": "id-a", "body": json.dumps("a")},
            {"itemId": "id-b", "body": json.dumps("b")},
        ],
    }

    with DurableFunctionTestRunner(handler=handler) as runner:
        result: DurableFunctionTestResult = runner.run(
            input=json.dumps(event), execution_timeout=15
        )

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    response: dict[str, Any] = json.loads(result.result)
    assert response["batchItemFailures"] == []
    assert sorted(response["batchItemResults"], key=lambda r: r["itemIdentifier"]) == [
        {"itemIdentifier": "id-a", "output": "tag:a"},
        {"itemIdentifier": "id-b", "output": "tag:b"},
    ]
    assert _CALLS["func"]["a"] == 1
    assert _CALLS["serialize"]["a"] == 1
    assert _CALLS["func"]["b"] > 1
