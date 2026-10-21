# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Chained-invoke scenario for OTel requirement otel-invocation-11."""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import WaitForCallbackConfig
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.types import WaitForCallbackContext
from common import otel_plugin, require_scenario
from chain_wire_observer import install_checkpoint_observer


def _submit(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """The conformance driver completes this callback after suspension."""
    return None


@durable_execution(plugins=[otel_plugin()])
def handler(
    event: dict[str, Any],
    context: DurableContext,
) -> dict[str, Any]:
    require_scenario(event, "chained-invoke")
    install_checkpoint_observer()
    result: dict[str, Any] = context.invoke(
        function_name=os.environ["OTEL_INVOKE_TARGET_FUNCTION_NAME"],
        payload=event,
        name="otel-invoke",
    )
    context.wait_for_callback(
        _submit,
        name="otel-invoke-replay-barrier",
        config=WaitForCallbackConfig(serdes=JsonSerDes()),
    )
    return result


@durable_execution(plugins=[otel_plugin()])
def target_handler(
    event: dict[str, Any],
    _context: DurableContext,
) -> dict[str, Any]:
    return event
