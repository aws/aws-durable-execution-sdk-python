# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed chained-invoke scenario for OTel requirement otel-invocation-18."""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from common import otel_plugin, require_scenario


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "chained-invoke-failure")
    context.invoke(
        function_name=os.environ["OTEL_INVOKE_TARGET_FUNCTION_NAME"],
        payload=event,
        name="otel-failed-invoke",
    )


@durable_execution(plugins=[otel_plugin()])
def target_handler(
    _event: dict[str, Any],
    _context: DurableContext,
) -> None:
    raise RuntimeError("Intentional chained-invoke target failure")
