# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Terminal execution failure scenario for OTel requirement otel-invocation-4."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)
from common import otel_plugin, require_scenario


@durable_step
def fail_terminally(_step_context: StepContext) -> None:
    raise RuntimeError("Intentional terminal failure")


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "terminal-failure")
    retry_strategy = create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=1,
            retryable_error_types=[RuntimeError],
        )
    )
    context.step(
        fail_terminally(),
        name="otel-terminal-failure",
        config=StepConfig(retry_strategy=retry_strategy),
    )
