"""process_distributed_map_items in a durable handler resumes after a wait.

Also covers the helpers composed with other durable operations in the same
handler, run through DurableFunctionTestRunner.
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, NestingType
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dmap.helpers import (
    process_distributed_map_batch,
    process_distributed_map_items,
)
from aws_durable_execution_sdk_python.exceptions import DurableOperationError
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import OperationType
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python.types import (
    DistributedMapBatchContext,
    DistributedMapItemContext,
)
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)

_RUN_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/exec1/invoke1/distributed-map-run/abc"
)

_LOCK = threading.Lock()
_CALLS: dict[str, Counter[str]] = {
    "func": Counter(),
    "step": Counter(),
    "serialize": Counter(),
}


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
    ctx.step(lambda _: _count("step", item), name="item-step")
    if item == "b":
        ctx.wait(Duration.from_seconds(1), name="item-b-wait")
    return item


def _event(items: list[str]) -> str:
    return json.dumps(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [
                {"itemId": f"id-{item}", "body": json.dumps(item)} for item in items
            ],
        }
    )


def _items_handler(nesting_type: NestingType) -> Any:
    @durable_execution
    def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
        return process_distributed_map_items(
            context,
            event,
            _process,
            result_serdes=_TaggingSerDes(),
            concurrency=2,
            nesting_type=nesting_type,
        )

    return handler


def _run_and_check_response(nesting_type: NestingType) -> DurableFunctionTestResult:
    for counter in _CALLS.values():
        counter.clear()

    with DurableFunctionTestRunner(handler=_items_handler(nesting_type)) as runner:
        result: DurableFunctionTestResult = runner.run(
            input=_event(["a", "b"]), execution_timeout=15
        )

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    response: dict[str, Any] = json.loads(result.result)
    assert response["batchItemFailures"] == []
    assert sorted(response["batchItemResults"], key=lambda r: r["itemIdentifier"]) == [
        {"itemIdentifier": "id-a", "output": "tag:a"},
        {"itemIdentifier": "id-b", "output": "tag:b"},
    ]
    return result


def test_flat_item_helper_reruns_a_completed_items_function_but_not_its_steps() -> None:
    """Under FLAT, the resume after item b's wait replays item a's function.

    The function body and the result serializer of item a each run again. The
    step inside item a does not, because its result is checkpointed.
    """
    _run_and_check_response(NestingType.FLAT)

    assert _CALLS["func"]["a"] == 2
    assert _CALLS["serialize"]["a"] == 2
    assert _CALLS["step"]["a"] == 1
    assert _CALLS["func"]["b"] == 2
    assert _CALLS["step"]["b"] == 1


def test_nested_item_helper_resumes_without_rerunning_a_completed_item() -> None:
    """Under NESTED, item a's function and result serializer each run once."""
    _run_and_check_response(NestingType.NESTED)

    assert _CALLS["func"]["a"] == 1
    assert _CALLS["serialize"]["a"] == 1
    assert _CALLS["step"]["a"] == 1
    assert _CALLS["func"]["b"] == 2
    assert _CALLS["step"]["b"] == 1


def _walk(operations: list[Any]) -> list[Any]:
    found: list[Any] = []
    for operation in operations:
        found.append(operation)
        found.extend(_walk(getattr(operation, "child_operations", [])))
    return found


def test_flat_item_helper_records_no_per_item_context_operation() -> None:
    """Under FLAT the item steps sit directly under the Map operation."""
    result = _run_and_check_response(NestingType.FLAT)

    contexts = [
        op
        for op in _walk(result.operations)
        if op.operation_type is OperationType.CONTEXT
    ]
    assert [op.sub_type for op in contexts] == ["Map"]


def test_nested_item_helper_records_one_context_operation_per_item() -> None:
    """Under NESTED each item gets its own MapIteration context operation."""
    result = _run_and_check_response(NestingType.NESTED)

    contexts = [
        op
        for op in _walk(result.operations)
        if op.operation_type is OperationType.CONTEXT
    ]
    assert [op.sub_type for op in contexts] == ["Map", "MapIteration", "MapIteration"]


def test_item_helper_runs_after_a_step_in_the_same_handler() -> None:
    step_runs: list[str] = []

    @durable_execution
    def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
        def before_items(_: Any) -> str:
            step_runs.append("before")
            return "pre"

        prefix = context.step(before_items, name="before-items")
        response = process_distributed_map_items(
            context, event, lambda _ctx, item, _item_ctx: f"{prefix}-{item}"
        )
        return response

    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=_event(["a", "b"]), execution_timeout=15)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    response = json.loads(result.result)
    assert response["batchItemFailures"] == []
    assert response["batchItemResults"] == [
        {"itemIdentifier": "id-a", "output": '"pre-a"'},
        {"itemIdentifier": "id-b", "output": '"pre-b"'},
    ]
    assert step_runs == ["before"]


def test_batch_helper_in_a_durable_handler_uses_the_handlers_own_context() -> None:
    seen: list[Any] = []

    @durable_execution
    def handler(event: dict[str, Any], context: DurableContext) -> None:
        def process(items: list[Any], batch_ctx: DistributedMapBatchContext) -> None:
            seen.append((items, batch_ctx.item_ids))
            context.step(lambda _: len(items), name="count-batch")

        return process_distributed_map_batch(event, process)

    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=_event(["a", "b"]), execution_timeout=15)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    assert json.loads(result.result) is None
    assert seen == [(["a", "b"], ("id-a", "id-b"))]
    steps = [op for op in result.operations if op.operation_type is OperationType.STEP]
    assert [op.name for op in steps] == ["count-batch"]


def test_flat_item_helper_forwards_error_data_to_the_response() -> None:
    """Each failed item's errorData and stackTrace reach the response under FLAT."""

    def process(
        _ctx: DurableContext, _item: str, _item_ctx: DistributedMapItemContext
    ) -> str:
        raise DurableOperationError(
            message="boom",
            error_type="Inner",
            data="payload",
            stack_trace=["frame one", "frame two"],
        )

    @durable_execution
    def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
        return process_distributed_map_items(
            context, event, process, nesting_type=NestingType.FLAT
        )

    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=_event(["a"]), execution_timeout=15)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    response = json.loads(result.result)
    assert response["batchItemResults"] == []
    assert response["batchItemFailures"] == [
        {
            "itemIdentifier": "id-a",
            "error": {
                "errorType": (
                    f"{DurableOperationError.__module__}."
                    f"{DurableOperationError.__qualname__}"
                ),
                "errorMessage": "boom",
                "errorData": "payload",
                "stackTrace": ["frame one", "frame two"],
            },
        }
    ]

    contexts = [
        op
        for op in _walk(result.operations)
        if op.operation_type is OperationType.CONTEXT
    ]
    assert "MapIteration" not in [op.sub_type for op in contexts]
