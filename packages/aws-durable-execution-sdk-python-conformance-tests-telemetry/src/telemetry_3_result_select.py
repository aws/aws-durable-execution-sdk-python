# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Per-operation result selection for requirement telemetry-3.

The step ``fetch-user`` returns ``{userId, email, token}``. The plugin exports
operation results only for operations that opt in through an
``OperationOverride.result`` transform. The override below returns a new
object that holds ``userId`` alone. So the record's
``operationsByName.fetch-user.result`` is exactly ``{"userId": "u-123"}``.
``email`` and ``token`` never reach CloudWatch Logs.

The transform receives the step's checkpointed result decoded to a plain JSON
value. The plugin decodes the SDK's own serialized form before the call. So
the transform indexes a ``dict``, not a string and not a serializer envelope.

The handler also sets ``content.input=False`` and ``content.output=False``.
telemetry-3 requires that the record carry no ``input`` and no ``output`` key.
The execution result still holds all three keys, so the core
``ExpectedResult`` check proves the transform removed data from the record,
not from the step.

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
    ContentOperations,
    DurableExecutionTelemetryConfig,
    OperationOverride,
    durable_execution_telemetry,
)


# The template sets this variable. Without it the exporter has no target.
# So the module fails at import instead of dropping records silently.
log_group_name = os.environ.get("TELEMETRY_LOG_GROUP_NAME")
if not log_group_name:
    raise RuntimeError("TELEMETRY_LOG_GROUP_NAME must be set")


@durable_step
def fetch_user(_step_context: StepContext, user_id: str) -> dict[str, str]:
    return {"userId": user_id, "email": "test@example.com", "token": "tok-456"}


def select_user_id(result: Any) -> dict[str, Any]:
    """Keep ``userId`` and drop every other key of the step result."""
    return {"userId": result["userId"]}


@durable_execution(
    plugins=[
        durable_execution_telemetry(
            DurableExecutionTelemetryConfig(
                exporters=[CloudWatchLogsExporter(log_group_name=log_group_name)],
                emit_mode="on-complete",
                sampling_rate=1,
                operation_detail="top-level",
                content=ContentConfig(
                    input=False,
                    output=False,
                    operations=ContentOperations(
                        overrides=[
                            OperationOverride(
                                operation_name="fetch-user", result=select_user_id
                            )
                        ]
                    ),
                ),
            )
        )
    ]
)
def handler(event: dict[str, Any], context: DurableContext) -> dict[str, str]:
    result: dict[str, str] = context.step(
        fetch_user(event["userId"]), name="fetch-user"
    )
    return result
