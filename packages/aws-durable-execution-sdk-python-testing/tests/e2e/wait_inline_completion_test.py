"""End-to-end test proving a wait inside a parallel branch completes inline.

Demonstrates that the local runner matches the real service: a wait
started in one parallel branch completes while its sibling is still
working, within a single invocation. Without the fix, the wait would
only complete after the invocation returns and a second invocation
re-checks it.
"""

from __future__ import annotations

import json
import time
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.types import StepContext

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


# Wall-clock seconds branch B keeps working. Must exceed the modeled
# wait plus the SDK's branch-retry overhead so branch A resumes while
# B is still running.
_SIBLING_WORK_SECONDS: float = 3.0

# Modeled wait duration for branch A (minimum 1 second).
_WAIT_SECONDS: int = 1


@durable_step
def do_work(step_context: StepContext, seconds: float) -> str:  # noqa: ARG001
    """Simulate long-running work by sleeping."""
    time.sleep(seconds)
    return "work-done"


# Timeline collected during the execution for post-hoc assertions.
_timeline: list[tuple[float, str]] = []


def _record(start: float, msg: str) -> None:
    _timeline.append((time.time() - start, msg))


@durable_execution
def parallel_wait_and_work(event: Any, context: DurableContext) -> list[str]:  # noqa: ARG001
    """Two parallel branches: A waits, B does work."""
    start: float = time.time()
    _record(start, "INVOCATION START")

    def branch_a(ctx: DurableContext) -> str:
        _record(start, "A: before wait")
        ctx.wait(Duration.from_seconds(_WAIT_SECONDS))
        _record(start, "A: after wait")
        return "a-done"

    def branch_b(ctx: DurableContext) -> str:
        _record(start, "B: start work")
        ctx.step(do_work(_SIBLING_WORK_SECONDS))
        _record(start, "B: finished work")
        return "b-done"

    batch = context.parallel(functions=[branch_a, branch_b], name="test-parallel")
    results: list[str] = batch.get_results()
    _record(start, "parallel returned")
    return results


@pytest.mark.parametrize("skip_time", [True, False])
def test_wait_completes_inline_during_parallel_invocation(skip_time: bool) -> None:  # noqa: FBT001
    """A wait in one parallel branch completes while the sibling is still
    working, within a single invocation.

    The wait's completion is delivered through a checkpoint response
    while the invocation is in flight, so the waiting branch resumes
    without an extra invocation. Runs on both the skip clock and the
    wall clock.
    """
    _timeline.clear()

    with DurableFunctionTestRunner(
        handler=parallel_wait_and_work,
        execution_timeout=15,
        skip_time=skip_time,
    ) as runner:
        result: DurableFunctionTestResult = runner.run(input="x")

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(["a-done", "b-done"])

    # Branch A must resume from its wait BEFORE branch B finishes its
    # work. This proves the wait completed inline while the sibling was
    # still working.
    a_after_wait_times = [t for t, msg in _timeline if msg == "A: after wait"]
    b_finished_times = [t for t, msg in _timeline if msg == "B: finished work"]

    assert len(a_after_wait_times) >= 1, "Branch A never resumed after wait"
    assert len(b_finished_times) >= 1, "Branch B never finished"

    a_after_wait: float = a_after_wait_times[0]
    b_finished: float = b_finished_times[0]

    assert a_after_wait < b_finished, (
        f"Branch A resumed at {a_after_wait:.2f}s but B finished at "
        f"{b_finished:.2f}s — wait did not complete inline"
    )

    # Only ONE invocation should be recorded. Look for exactly one
    # "INVOCATION START" event.
    invocation_starts = [msg for _, msg in _timeline if msg == "INVOCATION START"]
    assert len(invocation_starts) == 1, (
        f"Expected 1 invocation, got {len(invocation_starts)}"
    )
