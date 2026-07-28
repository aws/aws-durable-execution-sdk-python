"""DAG conformance 10-14: inverted readiness across a suspend.

max_concurrency is UNSET. This is the replay-flip case: two Wait tasks are in
flight when the invocation suspends, and the downstream pair becomes ready in
the reverse of registration order across different invocations.

root returns 1. slow is an 8s Wait and fast is a 2s Wait, both depending on root;
slow is registered FIRST. Both waits start in the first invocation, so the
invocation suspends with TWO tasks in flight and resumes twice. afterSlow
(registered FIRST) has an ordering-only edge .after(slow) and returns "S";
afterFast has .after(fast) and returns "F". Because fast's timer fires first,
afterFast becomes ready — and starts — one invocation before afterSlow, the
inversion of registration order that a counter-based id scheme cannot survive.
merge depends on [afterSlow, afterFast] and returns "SF".

Timers, not races, decide the order, so the outcome is deterministic; the gap
between the two waits is 6s (>> the ~4s floor). No peak-concurrency assertion is
possible or needed here — the waits are not user code and the suspend boundary
is the point.

Returns the canonical summary defined by test-requirements/dag/10-14.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.config import Duration
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
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        root = d.step(lambda deps, sc: 1, name="root")
        slow = d.wait(Duration.from_seconds(8), deps=[root], name="slow")  # registered FIRST
        fast = d.wait(Duration.from_seconds(2), deps=[root], name="fast")
        after_slow = d.step(lambda deps, sc: "S", name="afterSlow").after(
            slow
        )  # registered FIRST
        after_fast = d.step(lambda deps, sc: "F", name="afterFast").after(fast)
        d.step(
            lambda deps, sc: deps[after_slow] + deps[after_fast],
            deps=[after_slow, after_fast],
            name="merge",
        )

    result = context.dag(register, name="suspenddag")
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "merge": result.results["merge"].result,
    }
