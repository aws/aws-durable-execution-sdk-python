# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed single-step execution for requirement telemetry-2.

The step ``charge`` raises ``card declined``. The retry strategy declines, so
the step fails on its first attempt. The handler catches the ``StepError`` and
raises ``payment failed`` from it. So the execution fails, and the live
exception chain has three levels: the handler error, the ``StepError``, and the
``DurableOperationError`` the SDK rebuilt from the checkpointed step error.

telemetry-2 pins the messages ``card declined`` and ``payment failed``. Do not
change them.

The handler uses the real SDK API only. It builds no record and calls no
exporter method. A missing or wrong record is the plugin's defect, and that
defect is the signal the suite measures.
"""

from __future__ import annotations

import os
from typing import Any, NoReturn

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    StepError,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import RetryPresets
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
def charge(_step_context: StepContext) -> NoReturn:
    raise RuntimeError("card declined")


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
def handler(_event: dict[str, Any], context: DurableContext) -> None:
    try:
        context.step(
            charge(),
            name="charge",
            config=StepConfig(retry_strategy=RetryPresets.none()),
        )
    except StepError as error:
        raise RuntimeError("payment failed") from error
