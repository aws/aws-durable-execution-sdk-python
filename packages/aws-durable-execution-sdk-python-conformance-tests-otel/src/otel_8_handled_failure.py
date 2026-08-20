# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Handled step failure scenario for OTel requirement otel-invocation-8."""

from __future__ import annotations

import contextlib
from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    StepError,
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
def fail_and_handle(_step_context: StepContext) -> None:
    raise RuntimeError("Intentional handled failure")


@durable_step
def recover_after_failure(_step_context: StepContext) -> str:
    return "recovered"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "handled-failure")
    retry_strategy = create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=1,
            retryable_error_types=[RuntimeError],
        )
    )
    with contextlib.suppress(StepError):
        context.step(
            fail_and_handle(),
            name="otel-handled-failure",
            config=StepConfig(retry_strategy=retry_strategy),
        )
    return context.step(
        recover_after_failure(),
        name="otel-recovery-step",
    )
