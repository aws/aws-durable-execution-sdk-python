"""DAG conformance 10-12: a throwing run_if aborts the DAG with a typed error.

Serial (max_concurrency=1), so this scenario keeps full history assertions.

gate (root) returns 1. guarded depends on gate and its run_if THROWS ("predicate
boom") — a defect in deterministic code, not a business outcome. Per the runIf
abort contract the scheduler neither records guarded FAILED nor SKIPPED: it
aborts, starts no further tasks, and context.dag() fails with DagPredicateError.
guarded's body ("ran") is never invoked. refund has an ordering-only edge
.after(guarded) with an ALL_FAILED trigger and returns "refunded"; it MUST NOT
run — the whole point of the abort contract is that a predicate defect does not
drive compensation.

The handler does NOT catch the error: the DagPredicateError propagates so the
execution FAILS. The error-type token differs per language, so the canonical
YAML wildcards it. Returns nothing on the success path (never reached).
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import (
    DagConfig,
    DagContext,
    DagResult,
    TriggerRule,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


def _boom(_deps: Any) -> bool:
    raise RuntimeError("predicate boom")


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        gate = d.step(lambda deps, sc: 1, name="gate")
        guarded = d.step(
            lambda deps, sc: "ran", deps=[gate], name="guarded", run_if=_boom
        )
        d.step(lambda deps, sc: "refunded", name="refund").after(guarded).trigger_rule(
            TriggerRule.ALL_FAILED
        )

    # A throwing run_if aborts the DAG: this raises DagPredicateError, failing
    # the execution. The code below is unreachable and exists only to mirror the
    # other handlers' summary shape.
    result = context.dag(register, name="abortdag", config=DagConfig(max_concurrency=1))
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
    }
