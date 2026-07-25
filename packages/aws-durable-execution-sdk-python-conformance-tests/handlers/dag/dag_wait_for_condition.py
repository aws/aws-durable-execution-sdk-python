"""DAG conformance 10-8: task that is a waitForCondition (flat WaitForCondition).

poll(waitForCondition: initial state 0, +1 per poll, stop at 2 => returns 2) ->
done(step[dep poll] -> poll*5 = 10). The waitForCondition's native
WaitForCondition op is checkpointed directly under the Dag container (flat,
name-based); the first poll suspends and the DAG resumes across the
suspend/resume boundary. max_concurrency=1. Returns the canonical summary
defined by test-requirements/dag/10-8.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


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
        def check(_deps, state: int, _ctx: WaitForConditionCheckContext) -> int:
            return state + 1

        def wait_strategy(state: int, _attempt: int) -> WaitForConditionDecision:
            if state >= 2:
                return WaitForConditionDecision.stop_polling()
            return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

        poll = d.wait_for_condition(
            check,
            WaitForConditionConfig(wait_strategy=wait_strategy, initial_state=0),
            name="poll",
        )
        d.step(lambda deps, sc: deps[poll] * 5, deps=[poll], name="done")

    result = context.dag(register, name="wfcdag", config=DagConfig(max_concurrency=1))
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "poll": result.results["poll"].result,
        "done": result.results["done"].result,
    }
