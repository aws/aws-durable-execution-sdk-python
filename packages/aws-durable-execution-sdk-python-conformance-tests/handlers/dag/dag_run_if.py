"""DAG conformance 10-3: per-task conditional execution (run_if).

classify returns "review". publish/review/block each depend on classify and are
guarded by a run_if predicate that runs the branch only when classify's result
equals the branch's own name. Only review runs; publish and block are SKIPPED and
emit no events. Returns the canonical summary defined by
test-requirements/dag/10-3.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution

_BRANCHES = ("publish", "review", "block")


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
        classify = d.step(lambda deps, sc: "review", name="classify")
        for branch in _BRANCHES:
            d.step(
                (lambda name: (lambda deps, sc: name))(branch),
                deps=[classify],
                name=branch,
                run_if=(lambda name: (lambda deps: deps[classify] == name))(branch),
            )

    result = context.dag(register, name="runif", config=DagConfig(max_concurrency=1))
    statuses = {name: te.status.value for name, te in result.results.items()}
    branch = next(
        name for name in _BRANCHES if statuses.get(name) == "SUCCEEDED"
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": statuses,
        "counts": _counts(result),
        "branch": branch,
    }
