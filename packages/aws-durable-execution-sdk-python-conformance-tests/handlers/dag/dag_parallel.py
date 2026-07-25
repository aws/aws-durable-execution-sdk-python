"""DAG conformance 10-7: task that is a parallel of two named branches (flat).

fork(parallel of branches left->"L", right->"R") -> join(step[dep fork]). The
parallel's native Parallel op is checkpointed directly under the Dag container
(flat, name-based). max_concurrency=1 (both DAG and parallel) for a
deterministic history.

Aggregate-only join: ``join`` reads ONLY the aggregate ParallelResult /
BatchResult handed to it as the dep value and returns ``"<succeeded>/<size>"``
(``"2/2"``). It does not read individual branch values, keeping the scenario
expressible in every SDK (Java's ``ParallelResult`` is aggregate-only, and a
durable-op read from inside a step body is illegal there). Reading child branch
values is still covered by 10-6 (map).

Returns the canonical summary defined by test-requirements/dag/10-7.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelBranch, ParallelConfig
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
    def register(d: DagContext) -> None:
        def left(ctx: DurableContext) -> str:
            return ctx.step(lambda _sc: "L", name="left-step")

        def right(ctx: DurableContext) -> str:
            return ctx.step(lambda _sc: "R", name="right-step")

        fork = d.parallel(
            [
                ParallelBranch(func=left, name="left"),
                ParallelBranch(func=right, name="right"),
            ],
            name="fork",
            config=ParallelConfig(max_concurrency=1),
        )

        def join(deps, _sc) -> str:
            aggregate = deps[fork]
            return f"{aggregate.success_count}/{aggregate.total_count}"

        d.step(join, deps=[fork], name="join")

    result = context.dag(
        register, name="paralleldag", config=DagConfig(max_concurrency=1)
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "join": result.results["join"].result,
    }
