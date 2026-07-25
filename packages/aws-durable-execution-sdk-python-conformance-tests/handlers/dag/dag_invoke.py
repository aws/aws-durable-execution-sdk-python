"""DAG conformance 10-10: task that is an invoke of another Lambda (flat).

prep(step->21) -> call(invoke[dep prep]: target echoes the payload -> 21) ->
done(step[dep call] -> call*2 = 42). The invoke's native Invoke op is
checkpointed directly under the Dag container (flat, name-based). The target is
the shared echo function (``invoke.target_echo``) reached via the
``TARGET_FUNCTION_NAME`` environment variable and returns whatever it receives,
so ``call`` resolves to the integer ``prep`` produced. max_concurrency=1 for a
deterministic topological order.

This scenario suspends and resumes: the invoke completes in a later invocation.

Returns the canonical summary defined by test-requirements/dag/10-10.yaml.
"""

from __future__ import annotations

import os
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
    function_name = os.environ["TARGET_FUNCTION_NAME"]

    def register(d: DagContext) -> None:
        prep = d.step(lambda deps, sc: 21, name="prep")
        call = d.invoke(
            function_name,
            lambda deps: deps[prep],
            deps=[prep],
            name="call",
        )
        d.step(lambda deps, sc: deps[call] * 2, deps=[call], name="done")

    result = context.dag(
        register, name="invokedag", config=DagConfig(max_concurrency=1)
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "call": result.results["call"].result,
        "done": result.results["done"].result,
    }
