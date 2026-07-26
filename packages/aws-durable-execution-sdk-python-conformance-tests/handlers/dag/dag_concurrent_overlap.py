"""DAG conformance 10-13: real overlap of two tasks inside one invocation.

max_concurrency is UNSET, so ready siblings run concurrently on real threads.

root returns 1. slow and fast both depend on root; slow (registered FIRST)
sleeps ~2s in-body, fast sleeps ~200ms — so fast finishes first even though it
was registered second. afterSlow (registered FIRST) depends on slow and returns
deps.slow + "s" ("Ss"); afterFast depends on fast and returns deps.fast + "f"
("Ff"). Because fast is ready and completes before slow, afterFast starts before
afterSlow — the inversion of registration order versus start order that a
counter-based id scheme cannot survive (it would hand out different ids on
replay and terminate the execution with a replay-consistency error). merge
depends on [afterSlow, afterFast] and returns "SsFf".

Peak-concurrency instrumentation: a shared counter is incremented on entry to
slow/fast and decremented on exit, tracking the maximum observed. Python runs
tasks on real OS threads, so the counter is guarded by a lock. It is returned as
peakConcurrency (expected 2); without it the scenario would silently go vacuous
if a future change serialized the scheduler.

Returns the canonical summary defined by test-requirements/dag/10-13.yaml.
"""

from __future__ import annotations

import threading
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
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    # Shared peak-concurrency tracker. Bodies run on real threads, so mutation
    # is serialized by a lock; ``current`` is the live in-flight count and
    # ``peak`` the maximum ever observed simultaneously.
    tracker = {"current": 0, "peak": 0}
    lock = threading.Lock()

    def _enter() -> None:
        with lock:
            tracker["current"] += 1
            tracker["peak"] = max(tracker["peak"], tracker["current"])

    def _leave() -> None:
        with lock:
            tracker["current"] -= 1

    def slow(_deps: Any, _sc: Any) -> str:
        _enter()
        try:
            time.sleep(2)
            return "S"
        finally:
            _leave()

    def fast(_deps: Any, _sc: Any) -> str:
        _enter()
        try:
            time.sleep(0.2)
            return "F"
        finally:
            _leave()

    def register(d: DagContext) -> None:
        root = d.step(lambda deps, sc: 1, name="root")
        slow_h = d.step(slow, deps=[root], name="slow")  # registered FIRST
        fast_h = d.step(fast, deps=[root], name="fast")
        after_slow = d.step(  # registered FIRST
            lambda deps, sc: deps[slow_h] + "s", deps=[slow_h], name="afterSlow"
        )
        after_fast = d.step(
            lambda deps, sc: deps[fast_h] + "f", deps=[fast_h], name="afterFast"
        )
        d.step(
            lambda deps, sc: deps[after_slow] + deps[after_fast],
            deps=[after_slow, after_fast],
            name="merge",
        )

    result = context.dag(register, name="overlapdag")
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "merge": result.results["merge"].result,
        "peakConcurrency": tracker["peak"],
    }
