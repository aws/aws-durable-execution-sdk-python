"""DAG example: conditional branching with ``run_if``.

A ``classify`` task produces a category; three downstream branches each declare a
``run_if`` predicate so only the matching branch runs and the others are SKIPPED
with reason RUN_IF_PREDICATE. The category defaults to ``"review"`` and can be
overridden via ``{"category": "publish" | "review" | "block"}``.

.. warning::
   Uses the EXPERIMENTAL ``context.dag()`` API.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution


def _summarize(result: DagResult) -> dict[str, Any]:
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
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Run a run_if-branching DAG selecting exactly one downstream branch."""
    category = event.get("category", "review") if isinstance(event, dict) else "review"

    def register(d: DagContext) -> None:
        classify = d.step(lambda deps, sc: category, name="classify")
        d.step(
            lambda deps, sc: "published",
            deps=[classify],
            name="publish",
            run_if=lambda deps: deps[classify] == "publish",
        )
        d.step(
            lambda deps, sc: "reviewed",
            deps=[classify],
            name="review",
            run_if=lambda deps: deps[classify] == "review",
        )
        d.step(
            lambda deps, sc: "blocked",
            deps=[classify],
            name="block",
            run_if=lambda deps: deps[classify] == "block",
        )

    result = context.dag(register, name="run_if_branching")
    return _summarize(result)
