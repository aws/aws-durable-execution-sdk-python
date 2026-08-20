# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Chained-invoke scenario for OTel requirement otel-invocation-11."""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from common import otel_plugin, require_scenario


@durable_execution(plugins=[otel_plugin()])
def handler(
    event: dict[str, Any],
    context: DurableContext,
) -> dict[str, Any]:
    require_scenario(event, "chained-invoke")
    return context.invoke(
        function_name=os.environ["OTEL_INVOKE_TARGET_FUNCTION_NAME"],
        payload=event,
        name="otel-invoke",
    )


@durable_execution(plugins=[otel_plugin()])
def target_handler(
    event: dict[str, Any],
    _context: DurableContext,
) -> dict[str, Any]:
    return event
