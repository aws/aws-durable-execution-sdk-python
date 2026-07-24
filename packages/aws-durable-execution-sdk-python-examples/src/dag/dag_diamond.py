"""DAG example: diamond topology with typed dependency access.

Demonstrates ``context.dag()`` with a fan-out/fan-in ("diamond") graph where
downstream tasks read upstream results via the typed ``deps[handle]`` accessor.

.. warning::
   Uses the EXPERIMENTAL ``context.dag()`` API.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution


def _summarize(result: DagResult) -> dict[str, Any]:
    """Build a JSON-serializable summary of a DagResult for assertion."""
    return {
        "completion_reason": result.completion_reason.value,
        "counts": {
            "success": result.success_count,
            "failure": result.failure_count,
            "skipped": result.skipped_count,
            "total": result.total_count,
        },
        "tasks": {
            name: {
                "status": te.status.value,
                "skip_reason": te.skip_reason.value if te.skip_reason else None,
                "result": te.result,
            }
            for name, te in result.results.items()
        },
    }


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Run a diamond DAG: fetch -> (ta, tb) -> merge."""

    def register(d: DagContext) -> None:
        fetch = d.step(lambda deps, sc: 10, name="fetch")
        ta = d.step(lambda deps, sc: deps[fetch] + 1, deps=[fetch], name="ta")
        tb = d.step(lambda deps, sc: deps[fetch] * 2, deps=[fetch], name="tb")
        d.step(lambda deps, sc: deps[ta] + deps[tb], deps=[ta, tb], name="merge")

    result = context.dag(register, name="diamond")
    return _summarize(result)
