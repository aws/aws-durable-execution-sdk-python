# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Wait-for-condition scenario for OTel requirement otel-invocation-9."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from common import otel_plugin, require_scenario


def increment_condition(
    state: int,
    _context: WaitForConditionCheckContext,
) -> int:
    return state + 1


def stop_after_second_attempt(
    state: int,
    _attempt: int,
) -> WaitForConditionDecision:
    if state >= 2:
        return WaitForConditionDecision.stop_polling()
    return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> int:
    require_scenario(event, "wait-for-condition")
    return context.wait_for_condition(
        check=increment_condition,
        name="otel-condition",
        config=WaitForConditionConfig(
            initial_state=0,
            wait_strategy=stop_after_second_attempt,
        ),
    )
