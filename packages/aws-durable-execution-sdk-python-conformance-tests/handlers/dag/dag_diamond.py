"""DAG conformance 10-1: diamond fan-out/fan-in (all tasks complete).

fetch(10) -> {ta(=fetch+1=11), tb(=fetch*2=20)} -> merge(=ta+tb=31).
max_concurrency=1 for a deterministic topological order. Returns the canonical
cross-language summary defined by test-requirements/dag/10-1.yaml.
"""

from __future__ import annotations

from typing import Any

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
        fetch = d.step(lambda deps, sc: 10, name="fetch")
        ta = d.step(lambda deps, sc: deps[fetch] + 1, deps=[fetch], name="ta")
        tb = d.step(lambda deps, sc: deps[fetch] * 2, deps=[fetch], name="tb")
        d.step(lambda deps, sc: deps[ta] + deps[tb], deps=[ta, tb], name="merge")

    result = context.dag(register, name="diamond", config=DagConfig(max_concurrency=1))
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "merge": result.results["merge"].result,
    }
