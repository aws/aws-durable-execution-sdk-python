"""DAG conformance 10-6: task that is a map over a fixed item list (flat map).

squares(map over [1, 2]; each item one step -> item*item => [1, 4]) ->
sum(step[dep squares] -> sum of successful results = 5). The map's native Map op
is checkpointed directly under the Dag container (flat, name-based).
max_concurrency=1 (both DAG and map) for a deterministic history. Returns the
canonical summary defined by test-requirements/dag/10-6.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        def square(ctx: DurableContext, item: int, _index: int, _items: Any) -> int:
            return ctx.step(lambda _sc: item * item, name="square")

        squares = d.map(
            [1, 2], square, name="squares", config=MapConfig(max_concurrency=1)
        )
        d.step(
            lambda deps, sc: sum(deps[squares].get_results()),
            deps=[squares],
            name="sum",
        )

    result = context.dag(register, name="mapdag", config=DagConfig(max_concurrency=1))
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "sum": result.results["sum"].result,
    }
