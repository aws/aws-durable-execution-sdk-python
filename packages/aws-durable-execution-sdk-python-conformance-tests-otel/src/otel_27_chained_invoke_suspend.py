# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Chained invoke with child suspension for both OTel view case 27 requirements."""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration
from common import otel_plugin_factory, require_scenario


@durable_execution(plugins=[otel_plugin_factory()])
def handler(
    event: dict[str, Any],
    context: DurableContext,
) -> dict[str, Any]:
    require_scenario(event, "chained-invoke-suspend")
    return context.invoke(
        function_name=os.environ["OTEL_INVOKE_TARGET_FUNCTION_NAME"],
        payload=event,
        name="otel-suspended-invoke",
    )


@durable_execution(plugins=[otel_plugin_factory()])
def target_handler(
    event: dict[str, Any],
    context: DurableContext,
) -> dict[str, Any]:
    require_scenario(event, "chained-invoke-suspend")
    context.wait(
        Duration.from_seconds(1),
        name="otel-suspended-target-wait",
    )
    return event
