# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Long retry delay for OTel requirement otel-long-running-2."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration, JitterStrategy, StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)
from common import long_delay_seconds, otel_plugin, require_scenario


@durable_step
def succeed_after_long_retry(step_context: StepContext) -> str:
    if step_context.attempt == 1:
        raise RuntimeError("Intentional first-attempt failure")
    return "retried"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "long-retry")
    retry_strategy = create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=2,
            initial_delay=Duration.from_seconds(long_delay_seconds(event)),
            backoff_rate=1.0,
            jitter_strategy=JitterStrategy.NONE,
            retryable_error_types=[RuntimeError],
        )
    )
    return context.step(
        succeed_after_long_retry(),
        name="otel-long-retry",
        config=StepConfig(retry_strategy=retry_strategy),
    )
