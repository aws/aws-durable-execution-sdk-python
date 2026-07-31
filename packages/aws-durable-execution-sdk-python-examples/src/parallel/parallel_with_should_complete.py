"""Example demonstrating parallel with a custom should_complete predicate.

Uses an index-based quorum rule: the batch completes when branch A (index 0)
succeeds OR both branches B (index 1) and C (index 2) succeed. This shows
how the items snapshot enables dependency-style completion logic.
"""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    CompletionStatus,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def _quorum_predicate(status: CompletionStatus) -> bool:
    """Complete when branch A succeeds OR both B and C succeed."""
    if not status.items:
        return False
    branch_a_ok: bool = status.items[0].is_succeeded
    branch_b_ok: bool = len(status.items) > 1 and status.items[1].is_succeeded
    branch_c_ok: bool = len(status.items) > 2 and status.items[2].is_succeeded
    return branch_a_ok or (branch_b_ok and branch_c_ok)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Run parallel branches with a quorum-based completion predicate."""
    config: ParallelConfig = ParallelConfig(
        max_concurrency=3,
        completion_config=CompletionConfig(should_complete=_quorum_predicate),
    )

    functions = [
        # Branch A - slow task
        lambda ctx: ctx.step(lambda _: "Branch A done", name="branch-a"),
        # Branch B - fast task
        lambda ctx: ctx.step(lambda _: "Branch B done", name="branch-b"),
        # Branch C - fast task
        lambda ctx: ctx.step(lambda _: "Branch C done", name="branch-c"),
        # Branch D - never needed if quorum met
        lambda ctx: ctx.step(lambda _: "Branch D done", name="branch-d"),
    ]

    results = context.parallel(
        functions=functions,
        name="quorum-branches",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "completion_reason": results.completion_reason.value,
        "results": results.get_results(),
    }
