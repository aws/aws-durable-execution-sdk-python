# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Child-context hierarchy scenario for OTel requirement otel-invocation-5."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
    durable_with_child_context,
)
from common import otel_plugin, require_scenario


@durable_step
def complete_child_step(_step_context: StepContext) -> str:
    return "child-complete"


@durable_with_child_context
def run_child_workflow(context: DurableContext) -> str:
    return context.step(complete_child_step(), name="otel-child-step")


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "child-context")
    return context.run_in_child_context(
        run_child_workflow(),
        name="otel-child-context",
    )
