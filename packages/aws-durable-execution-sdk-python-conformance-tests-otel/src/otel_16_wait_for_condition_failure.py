# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed wait-for-condition scenario for OTel requirement otel-invocation-16."""

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


def fail_condition_check(
    _state: int,
    _context: WaitForConditionCheckContext,
) -> int:
    raise RuntimeError("Intentional condition check failure")


def continue_condition(
    _state: int,
    _attempt: int,
) -> WaitForConditionDecision:
    return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "wait-for-condition-failure")
    context.wait_for_condition(
        check=fail_condition_check,
        name="otel-failed-condition",
        config=WaitForConditionConfig(
            initial_state=0,
            wait_strategy=continue_condition,
        ),
    )
