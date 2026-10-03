# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Normal successful replay of a completed step for OTel case 21."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration
from common import otel_plugin, require_scenario


@durable_step
def before_wait(_step_context: StepContext) -> str:
    return "before"


@durable_step
def after_wait(_step_context: StepContext) -> str:
    return "after"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "completed-step-replay")
    before = context.step(before_wait(), name="otel-before-wait")
    context.wait(Duration.from_seconds(1), name="otel-replay-wait")
    after = context.step(after_wait(), name="otel-after-wait")
    return f"{before}-{after}"
