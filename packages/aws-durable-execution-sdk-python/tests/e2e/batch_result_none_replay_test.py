"""None-valued batch results remain visible across checkpoint replay."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

import pytest
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


def _map_item(
    _context: DurableContext,
    item: int | None,
    _index: int,
    _items: Sequence[int | None],
) -> int | None:
    return item


def _parallel_branch(value: int | None) -> Callable[[DurableContext], int | None]:
    def branch(_context: DurableContext) -> int | None:
        return value

    return branch


@durable_execution
def _batch_handler(event: dict[str, str], context: DurableContext) -> dict[str, Any]:
    batch: BatchResult[int | None]
    if event["kind"] == "map":
        batch = context.map(
            [None, 10, None, 30],
            _map_item,
            name="none-result-map",
        )
    else:
        batch = context.parallel(
            [
                _parallel_branch(None),
                _parallel_branch(10),
                _parallel_branch(None),
                _parallel_branch(30),
            ],
            name="none-result-parallel",
        )

    # Suspend after the batch. The next invocation reads the BatchResult from
    # its parent-context checkpoint before these accessors run.
    context.wait(Duration.from_seconds(1), name="after-batch")
    return {
        "success_count": batch.success_count,
        "succeeded_indexes": [item.index for item in batch.succeeded()],
        "results": batch.get_results(),
    }


@pytest.mark.parametrize("kind", ["map", "parallel"])
def test_none_results_survive_checkpoint_replay(kind: str) -> None:
    with DurableFunctionTestRunner(handler=_batch_handler) as runner:
        result: DurableFunctionTestResult = runner.run(
            input=json.dumps({"kind": kind}),
            execution_timeout=15,
        )

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    assert json.loads(result.result) == {
        "success_count": 4,
        "succeeded_indexes": [0, 1, 2, 3],
        "results": [None, 10, None, 30],
    }
