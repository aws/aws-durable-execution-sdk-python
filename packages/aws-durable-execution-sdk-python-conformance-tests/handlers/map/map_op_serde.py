"""9-19: Map with an operation-level (whole-result) serdes - serialize on a fresh operation."""

from typing import Any

from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import JsonSerDes, SerDes, SerDesContext


class OpSerDes(SerDes):
    """Whole-result serde: 'OPSERDE:<comma-joined results>' round-trip (real, non-identity)."""

    def serialize(self, value: BatchResult, _: SerDesContext) -> str:
        return "OPSERDE:" + ",".join(value.get_results())

    def deserialize(self, payload: str, _: SerDesContext) -> BatchResult:
        vals = payload[len("OPSERDE:") :].split(",")
        items = [
            BatchItem(index=i, status=BatchItemStatus.SUCCEEDED, result=v, error=None)
            for i, v in enumerate(vals)
        ]
        return BatchResult(all=items, completion_reason=CompletionReason.ALL_COMPLETED)


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return item.upper()


@durable_execution
def handler(_event: Any, context: DurableContext) -> list:
    # serdes=OpSerDes serializes the whole BatchResult; item_serdes=JsonSerDes keeps
    # per-item serialization on the default so the op-serde applies to the batch only.
    result = context.map(
        ["x", "y"],
        map_fn,
        name="op-serde",
        config=MapConfig(
            max_concurrency=1, serdes=OpSerDes(), item_serdes=JsonSerDes()
        ),
    )
    return result.get_results()
