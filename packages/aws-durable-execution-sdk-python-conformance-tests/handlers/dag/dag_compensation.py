"""DAG conformance 10-2: trigger-rule compensation (COMPLETED_WITH_FAILURES).

charge (root) always fails. fulfill uses the default (ALL_SUCCESS) trigger, so it
is SKIPPED. refund uses ALL_FAILED and runs ("refunded"). audit uses ALL_DONE and
runs ("logged"). charge exhausts the DAG default retry policy before failing
terminally, so the DAG drains to COMPLETED_WITH_FAILURES without throwing.
Returns the canonical summary defined by test-requirements/dag/10-2.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import (
    DagConfig,
    DagContext,
    DagResult,
    TriggerRule,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


def _charge_declined(_deps: Any, _sc: Any) -> Any:
    raise RuntimeError("payment declined")


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        charge = d.step(_charge_declined, name="charge")
        d.step(lambda deps, sc: "fulfilled", name="fulfill").after(charge)
        d.step(lambda deps, sc: "refunded", name="refund").after(charge).trigger_rule(
            TriggerRule.ALL_FAILED
        )
        d.step(lambda deps, sc: "logged", name="audit").after(charge).trigger_rule(
            TriggerRule.ALL_DONE
        )

    result = context.dag(
        register, name="compensation", config=DagConfig(max_concurrency=1)
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
    }
