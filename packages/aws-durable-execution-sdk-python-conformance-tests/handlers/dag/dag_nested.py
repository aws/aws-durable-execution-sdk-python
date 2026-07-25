"""DAG conformance 10-9: task that is itself a nested DAG / sub-dag (flat).

pre(step->1) -> sub(nested dag[dep pre]: n1->2, n2[dep n1]->n1+3=5) ->
post(step[dep sub] -> nested n2 result * 10 = 50). The nested DAG's native Dag
op is checkpointed directly under the outer Dag container (flat, name-based).
max_concurrency=1 at both DAG levels for a deterministic topological order.
Returns the canonical summary defined by test-requirements/dag/10-9.yaml.
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
        pre = d.step(lambda deps, sc: 1, name="pre")

        def sub_register(sd: DagContext) -> None:
            n1 = sd.step(lambda deps, sc: 2, name="n1")
            sd.step(lambda deps, sc: deps[n1] + 3, deps=[n1], name="n2")

        sub = d.dag(
            sub_register,
            deps=[pre],
            name="sub",
            config=DagConfig(max_concurrency=1),
        )
        d.step(
            lambda deps, sc: deps[sub].get_result("n2") * 10,
            deps=[sub],
            name="post",
        )

    result = context.dag(register, name="outerdag", config=DagConfig(max_concurrency=1))
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "post": result.results["post"].result,
    }
