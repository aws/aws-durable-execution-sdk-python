"""Replay-aware logging across concurrent (parallel) branch replays.

Each parallel branch runs in its own child context with its own replay status.
A branch that contains a `wait` suspends and is replayed independently of the
other branches and of the parent. This example demonstrates that the
replay-aware logger de-duplicates per branch:

- A branch's "before wait" log is emitted once (on the invocation where the
  branch first reaches its wait) and de-duplicated on the branch's replay.
- A branch's "after wait" log is emitted once, as new work, when that branch
  resumes.
- The branches stagger their waits so they resume on different invocations,
  making the per-branch independence visible in CloudWatch.

Deploy and invoke asynchronously, then inspect the logs. Each `branch` field
identifies the emitting branch so you can confirm each line appears exactly
once across all invocations.
"""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, ParallelConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_parallel_branch,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def _make_branch(name: str, wait_seconds: int):
    """Build a parallel branch that logs around its own wait boundary."""

    @durable_parallel_branch(name=name)
    def branch(ctx: DurableContext) -> str:
        # Before the branch's wait. On this branch's replay invocation this is
        # de-duplicated because the branch is still replaying when it reaches
        # here. This line is the load-bearing case: there is no inner operation
        # before it, so the branch's child context relies on starting in the
        # correct replay status.
        ctx.logger.info(
            "branch start (before wait)",
            extra={"branch": name, "is_replaying": ctx.is_replaying()},
        )

        # The branch's own replay boundary. Different per branch so the branches
        # resume on different invocations.
        ctx.wait(duration=Duration.from_seconds(wait_seconds), name=f"{name}_wait")

        # After the branch's wait: emitted as new work on the branch's replay.
        ctx.logger.info(
            "branch resumed (after wait)",
            extra={"branch": name, "is_replaying": ctx.is_replaying()},
        )

        return ctx.step(lambda _: f"{name}-done", name=f"{name}_finalize")

    return branch


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Run concurrent branches that each log around their own wait."""
    context.logger.info(
        "workflow started",
        extra={"is_replaying": context.is_replaying()},
    )

    results: list[str] = context.parallel(
        functions=[
            _make_branch("alpha", 3)(),
            _make_branch("bravo", 6)(),
            _make_branch("charlie", 9)(),
        ],
        name="concurrent_branches",
        config=ParallelConfig(max_concurrency=3),
    ).get_results()

    context.logger.info(
        "workflow completed",
        extra={"results": results, "is_replaying": context.is_replaying()},
    )

    return {"results": results}
