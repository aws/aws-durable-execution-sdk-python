"""DAG conformance 10-19: custom result-based completion. A rules-engine
predicate short-circuits the moment any task's SUCCEEDED result carries a
REJECT verdict -- expressible only because the custom-completion predicate can
inspect task RESULTS, not just aggregate counts.

A DAG "rulesengine" with max-concurrency 1 and a linear chain of three step
tasks: r1 -> r2 -> r3, each returning a verdict dict. r1 -> ACCEPT, r2 ->
REJECT, r3 (never runs) -> ACCEPT.

The DAG's completion_config is a custom predicate (DagCustomCompletionConfig),
not a threshold: after every settlement it receives a live DagCompletionStatus
snapshot and inspects every SUCCEEDED item's result for a REJECT verdict. The
moment it sees one, it returns complete_dag(FAILED). r3 is never started and
is absent from the results map. The DAG completes with
CUSTOM_COMPLETION_FAILED -- a dedicated reason distinct from
COMPLETED_WITH_FAILURES, since no individual task FAILED. throw_if_error()
MUST still raise in this case (the contract keys off completion_reason too,
not failure_count alone).

Returns the canonical summary defined by test-requirements/dag/10-19.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import (
    DagCompletionOutcome,
    DagCompletionStatus,
    DagConfig,
    DagContext,
    DagCustomCompletionConfig,
    DagResult,
    TaskStatus,
    complete_dag,
    continue_dag,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


def _should_complete(status: DagCompletionStatus) -> Any:
    rejected = any(
        item.status is TaskStatus.SUCCEEDED
        and isinstance(item.result, dict)
        and item.result.get("verdict") == "REJECT"
        for item in status.items
    )
    if rejected:
        return complete_dag(DagCompletionOutcome.FAILED)
    return continue_dag()


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        r1 = d.step(lambda deps, sc: {"verdict": "ACCEPT"}, name="r1")
        r2 = d.step(
            lambda deps, sc: {"verdict": "REJECT"}, deps=[r1], name="r2"
        )
        d.step(lambda deps, sc: {"verdict": "ACCEPT"}, deps=[r2], name="r3")

    result = context.dag(
        register,
        name="rulesengine",
        config=DagConfig(
            max_concurrency=1,
            completion_config=DagCustomCompletionConfig(_should_complete),
        ),
    )
    return {
        "reason": result.completion_reason.value,
        "counts": _counts(result),
        "r1": result.get_result("r1"),
        "r2": result.get_result("r2"),
    }
