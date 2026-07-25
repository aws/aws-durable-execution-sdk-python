"""DAG conformance 10-4: in-graph Wait task (suspend and resume).

start -> pause(Wait 5s) -> finish. pause suspends the whole invocation without
compute charges until the wait elapses, then resumes in a fresh invocation.
finish returns "resumed", proving the DAG ran across the suspend/resume boundary.
Returns the canonical summary defined by test-requirements/dag/10-4.yaml.
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
        start = d.step(lambda deps, sc: "started", name="start")
        pause = d.wait(5, deps=[start], name="pause")
        d.step(lambda deps, sc: "resumed", deps=[pause], name="finish")

    result = context.dag(
        register, name="waitresume", config=DagConfig(max_concurrency=1)
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "marker": result.results["finish"].result,
    }
