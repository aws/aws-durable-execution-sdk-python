"""DAG conformance 10-5: task that is a runInChildContext (flat child container).

seed(step->1) -> group(runInChildContext[dep seed]: inner-a->2, inner-b->3,
returns 5) -> done(step[dep group]->group*2=10). The child's native
RunInChildContext op is checkpointed directly under the Dag container (flat,
name-based). max_concurrency=1 for a deterministic topological order. Returns
the canonical summary defined by test-requirements/dag/10-5.yaml.
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
        seed = d.step(lambda deps, sc: 1, name="seed")

        def group_body(deps, child: DurableContext) -> int:
            seed_val = deps[seed]
            a = child.step(lambda _sc: seed_val + 1, name="inner-a")
            b = child.step(lambda _sc: seed_val + 2, name="inner-b")
            return a + b

        group = d.run_in_child_context(group_body, deps=[seed], name="group")
        d.step(lambda deps, sc: deps[group] * 2, deps=[group], name="done")

    result = context.dag(register, name="childdag", config=DagConfig(max_concurrency=1))
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "group": result.results["group"].result,
        "done": result.results["done"].result,
    }
