# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Successful execution shared by OTel view case 1."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from common import otel_plugin, require_scenario


@durable_step
def complete_successfully(_step_context: StepContext) -> str:
    return "success"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "success")
    return context.step(complete_successfully(), name="otel-success")
