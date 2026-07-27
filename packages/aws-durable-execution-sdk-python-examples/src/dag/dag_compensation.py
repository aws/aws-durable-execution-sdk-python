"""DAG example: compensation via trigger rules.

Demonstrates the saga/compensation pattern with ``context.dag()`` trigger rules:
- ``fulfill`` runs only if ``charge`` SUCCEEDED (ALL_SUCCESS, the default)
- ``refund`` runs only if ``charge`` FAILED (ALL_FAILED)
- ``audit`` always runs once ``charge`` is terminal (ALL_DONE)

By default ``charge`` fails, so the DAG drains to COMPLETED_WITH_FAILURES with
``refund`` + ``audit`` succeeding and ``fulfill`` skipped. Pass
``{"charge_ok": true}`` to exercise the success path.

.. warning::
   Uses the EXPERIMENTAL ``context.dag()`` API.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagContext, DagResult, TriggerRule
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryPresets


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


def _charge_declined(_deps: Any, _sc: Any) -> Any:
    raise RuntimeError("charge declined")


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Run a compensation DAG driven by the terminal state of ``charge``."""
    charge_ok = bool(event.get("charge_ok", False)) if isinstance(event, dict) else False

    def register(d: DagContext) -> None:
        # Disable retries on the charge so an intentional failure terminates promptly.
        no_retry = StepConfig(retry_strategy=RetryPresets.none())
        if charge_ok:
            charge = d.step(lambda deps, sc: "charged", name="charge", config=no_retry)
        else:
            charge = d.step(_charge_declined, name="charge", config=no_retry)
        d.step(lambda deps, sc: "fulfilled", name="fulfill").after(charge)
        d.step(lambda deps, sc: "refunded", name="refund").after(charge).trigger_rule(
            TriggerRule.ALL_FAILED
        )
        d.step(lambda deps, sc: "audited", name="audit").after(charge).trigger_rule(
            TriggerRule.ALL_DONE
        )

    result = context.dag(
        register,
        name="compensation",
    )
    return _summarize(result)
