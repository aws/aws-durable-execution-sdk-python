# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Long durable wait for OTel requirement otel-long-running-1."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration
from common import long_delay_seconds, otel_plugin, require_scenario


@durable_step
def complete_after_long_wait(_step_context: StepContext) -> str:
    return "resumed"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "long-wait")
    context.wait(
        Duration.from_seconds(long_delay_seconds(event)),
        name="otel-long-wait",
    )
    return context.step(
        complete_after_long_wait(),
        name="otel-after-long-wait",
    )
