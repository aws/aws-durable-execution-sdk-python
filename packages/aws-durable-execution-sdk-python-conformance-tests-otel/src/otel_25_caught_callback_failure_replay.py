# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Catch a real callback failure, then replay it before a successful return."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import WaitForCallbackConfig
from aws_durable_execution_sdk_python.exceptions import CallbackExternalError
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.types import LambdaContext, WaitForCallbackContext
from common import otel_plugin, require_scenario


def submit_callback(_callback_id: str, _context: WaitForCallbackContext) -> None:
    return None


@durable_step
def observe_failure(_context: StepContext) -> str:
    return "caught"


def _run(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "caught-callback-failure-replay")
    config = WaitForCallbackConfig(serdes=JsonSerDes())
    try:
        context.wait_for_callback(
            submit_callback, name="otel-failed-callback", config=config
        )
    except CallbackExternalError:
        # wait_for_callback restores the callback-typed cause on both first
        # consumption and replay of its persisted failed child context.
        pass
    else:
        raise AssertionError("Expected the callback to fail")

    observed = context.step(observe_failure(), name="otel-failed-callback-observed")
    one = context.wait_for_callback(
        submit_callback, name="otel-failed-callback-barrier-one", config=config
    )
    two = context.wait_for_callback(
        submit_callback, name="otel-failed-callback-barrier-two", config=config
    )
    return "/".join((observed, one, two))


_durable_handler = durable_execution(_run, plugins=[otel_plugin()])


def handler(event: Any, context: LambdaContext) -> Any:
    return _durable_handler(event, context)
