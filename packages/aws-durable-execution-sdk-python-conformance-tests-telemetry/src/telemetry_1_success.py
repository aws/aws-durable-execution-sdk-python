# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Successful single-step execution for requirement telemetry-1.

The handler uses the real SDK API only. It builds no record and calls no
exporter method. A missing or wrong record is the plugin's defect, and that
defect is the signal the suite measures.
"""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python_telemetry_exporter import (
    CloudWatchLogsExporter,
    ContentConfig,
    DurableExecutionTelemetryConfig,
    durable_execution_telemetry,
)


# The template sets this variable. Without it the exporter has no target.
# So the module fails at import instead of dropping records silently.
log_group_name = os.environ.get("TELEMETRY_LOG_GROUP_NAME")
if not log_group_name:
    raise RuntimeError("TELEMETRY_LOG_GROUP_NAME must be set")


@durable_step
def increment(_step_context: StepContext, value: int) -> int:
    return value + 1


@durable_execution(
    plugins=[
        durable_execution_telemetry(
            DurableExecutionTelemetryConfig(
                exporters=[CloudWatchLogsExporter(log_group_name=log_group_name)],
                emit_mode="on-complete",
                sampling_rate=1,
                operation_detail="top-level",
                content=ContentConfig(input=True, output=True),
            )
        )
    ]
)
def handler(event: dict[str, Any], context: DurableContext) -> dict[str, int]:
    result: int = context.step(increment(event["value"]), name="increment")
    return {"value": result}
