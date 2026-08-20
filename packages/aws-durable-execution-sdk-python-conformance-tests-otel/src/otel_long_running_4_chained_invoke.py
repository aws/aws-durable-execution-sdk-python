# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Long chained invoke for OTel requirement otel-long-running-4."""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration
from common import long_delay_seconds, otel_plugin, require_scenario


@durable_execution(plugins=[otel_plugin()])
def handler(
    event: dict[str, Any],
    context: DurableContext,
) -> dict[str, Any]:
    require_scenario(event, "long-chained-invoke")
    return context.invoke(
        function_name=os.environ["OTEL_INVOKE_TARGET_FUNCTION_NAME"],
        payload=event,
        name="otel-long-invoke",
    )


@durable_execution(plugins=[otel_plugin()])
def target_handler(
    event: dict[str, Any],
    context: DurableContext,
) -> dict[str, Any]:
    require_scenario(event, "long-chained-invoke")
    context.wait(
        Duration.from_seconds(long_delay_seconds(event)),
        name="otel-long-invoke-target-wait",
    )
    return event
