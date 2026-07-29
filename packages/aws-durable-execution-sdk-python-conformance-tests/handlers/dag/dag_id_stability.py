"""DAG conformance 10-20: task-id stability across independently forced completion orders.

Identical shape to 10-13's overlap (DagConcurrentOverlap) -- root -> {a, b} ->
{afterA, afterB} -> merge, max_concurrency unset -- except which sibling
sleeps longer is driven by event["swap"]: swap=False makes a finish first;
swap=True makes b finish first. Both invocations register the SAME task names
in the SAME order every time -- only the RUNTIME completion order changes.

This is the harness-level counterpart to 10-13: 10-13 proves out-of-order
completion doesn't fail the execution (an INDIRECT proof of name-based ids,
since a counter-based scheme would trip the SDK's own replay-consistency
check). This scenario is invoked TWICE by a dedicated script
(id_stability.py, not the normal single-invocation validator) with swap
flipped between runs, and asserts each task's Id field in the captured
execution history is IDENTICAL across both runs -- the direct proof that ids
are derived from the task name, not from completion order or a counter.

Returns the canonical summary defined by test-requirements/dag/10-20.yaml.
"""

from __future__ import annotations

import time
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    swap = bool((event or {}).get("swap", False))

    def a_body(_deps: Any, _sc: Any) -> str:
        time.sleep(2 if swap else 0.2)
        return "A"

    def b_body(_deps: Any, _sc: Any) -> str:
        time.sleep(0.2 if swap else 2)
        return "B"

    def register(d: DagContext) -> None:
        root = d.step(lambda deps, sc: 1, name="root")
        a_h = d.step(a_body, deps=[root], name="a")
        b_h = d.step(b_body, deps=[root], name="b")
        after_a = d.step(lambda deps, sc: deps[a_h] + "a", deps=[a_h], name="afterA")
        after_b = d.step(lambda deps, sc: deps[b_h] + "b", deps=[b_h], name="afterB")
        d.step(
            lambda deps, sc: deps[after_a] + deps[after_b],
            deps=[after_a, after_b],
            name="merge",
        )

    result = context.dag(register, name="idstabilitydag")
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "merge": result.results["merge"].result,
    }
