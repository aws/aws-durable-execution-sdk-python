# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Wait-for-callback scenario for OTel requirement otel-invocation-10."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.types import WaitForCallbackContext
from common import otel_plugin, require_scenario


def submit_callback(
    _callback_id: str,
    _context: WaitForCallbackContext,
) -> None:
    return None


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "wait-for-callback")
    return context.wait_for_callback(
        submit_callback,
        name="otel-callback",
    )
