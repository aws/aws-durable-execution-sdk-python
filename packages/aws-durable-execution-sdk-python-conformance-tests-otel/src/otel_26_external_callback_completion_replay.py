# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Public callback completion followed by two controlled replay barriers."""

from __future__ import annotations

from typing import Any, cast

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    WaitForCallbackConfig,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.types import WaitForCallbackContext
from common import otel_plugin, require_scenario


def submit_callback(_callback_id: str, _context: WaitForCallbackContext) -> None:
    return None


@durable_step
def observe_target(_context: StepContext, result: str) -> str:
    return result


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "external-callback-completion-replay")
    config = WaitForCallbackConfig(serdes=JsonSerDes())
    target = cast(
        str,
        context.create_callback(
            name="otel-external-target", config=CallbackConfig(serdes=JsonSerDes())
        ).result(),
    )
    observed = context.step(
        observe_target(target), name="otel-external-target-observed"
    )
    one = context.wait_for_callback(
        submit_callback, name="otel-external-barrier-one", config=config
    )
    two = context.wait_for_callback(
        submit_callback, name="otel-external-barrier-two", config=config
    )
    return "/".join((observed, one, two))
