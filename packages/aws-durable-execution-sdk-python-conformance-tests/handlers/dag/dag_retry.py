"""DAG conformance 10-16: per-task retry inside a DAG (DagRetry).

A DAG where a task retries and eventually succeeds, proving that a retried
task's result flows downstream normally:

- ``flaky`` is a step with a per-task retry strategy allowing at least three
  attempts and no meaningful backoff. Its body reads the 1-based attempt number
  from the step context (``sc.attempt``). It throws while the attempt is not yet
  the third, and returns the attempt number (``3``) on the third attempt.
- ``after`` is a step depending on ``flaky`` that returns ``flaky``'s result
  doubled (``6``).

``flaky`` must end SUCCEEDED (not FAILED) and ``after`` must run (not skip),
which is exactly what a broken retry inside a DAG would break. ``max_concurrency``
is 1 for a deterministic single-lane order.

Handler returns ``{"flaky": <flaky result>, "after": <after result>}`` — i.e.
``{"flaky": 3, "after": 6}`` — the canonical values pinned by
test-requirements/dag/10-16.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import (
    Duration,
    JitterStrategy,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext, StepContext
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def _flaky(_deps: Any, sc: StepContext) -> int:
    # Read the SDK's built-in durable attempt counter (1-based) from the step
    # context. Fail until the third attempt, then return the attempt number.
    if sc.attempt < 3:
        msg = f"attempt {sc.attempt} not yet the third"
        raise RuntimeError(msg)
    return sc.attempt


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    # At least three attempts, no backoff delay worth waiting on (the step
    # executor floors any retry delay at 1 second regardless).
    retry_strategy = create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=5,
            initial_delay=Duration.from_seconds(1),
            backoff_rate=1,
            jitter_strategy=JitterStrategy.NONE,
        )
    )

    def register(d: DagContext) -> None:
        flaky = d.step(
            _flaky,
            name="flaky",
            config=StepConfig(retry_strategy=retry_strategy),
        )
        d.step(lambda deps, sc: deps[flaky] * 2, deps=[flaky], name="after")

    result: DagResult = context.dag(
        register, name="retrydag", config=DagConfig(max_concurrency=1)
    )
    return {
        "flaky": result.results["flaky"].result,
        "after": result.results["after"].result,
    }
