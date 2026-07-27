"""DAG conformance 10-18: compensation dependency read on a FAILED upstream is
ABSENT, not present (the deps-nullability contract).

A DAG "compensate" with two step tasks: charge -> audit.

- ``charge`` is a root step that ALWAYS fails. Its retry strategy is disabled
  (max_attempts=1) so it ends FAILED deterministically after a single attempt
  (exactly one StepFailed).
- ``audit`` depends on ``charge`` via an INLINE (typed) dependency and uses the
  ALL_DONE trigger rule, so it runs even though ``charge`` FAILED and receives
  ``charge`` in its resolved deps map. Its body reads its dependency's result
  for ``charge``: a dependency that did not SUCCEED resolves to ``None``
  (absent), never a stale/fabricated value. ``audit`` returns ``"absent"`` when
  it observes ``None`` and ``"present"`` otherwise.

The DAG drains to COMPLETED_WITH_FAILURES without throwing: ``charge`` FAILED,
``audit`` SUCCEEDED with result ``"absent"``. Returns the canonical summary
defined by test-requirements/dag/10-18.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import (
    Duration,
    JitterStrategy,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import (
    DagConfig,
    DagContext,
    DagResult,
    TriggerRule,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


def _charge_fails(_deps: Any, _sc: Any) -> Any:
    raise RuntimeError("charge failed")


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    # Single attempt (no retry) so charge ends FAILED deterministically.
    no_retry = create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=1,
            initial_delay=Duration.from_seconds(1),
            backoff_rate=1,
            jitter_strategy=JitterStrategy.NONE,
        )
    )

    def register(d: DagContext) -> None:
        charge = d.step(
            _charge_fails,
            name="charge",
            config=StepConfig(retry_strategy=no_retry),
        )
        # Inline dep on charge + ALL_DONE: audit runs and reads charge. A failed
        # dependency's value is absent (None), so audit returns "absent".
        d.step(
            lambda deps, sc: "absent" if deps.get(charge) is None else "present",
            deps=[charge],
            name="audit",
        ).trigger_rule(TriggerRule.ALL_DONE)

    result = context.dag(
        register, name="compensate", config=DagConfig(max_concurrency=1)
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "audit": result.results["audit"].result,
    }
