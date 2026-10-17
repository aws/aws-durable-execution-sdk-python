# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""A real invocation retry for OTel status-mapping case 24."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.exceptions import InvocationError
from common import otel_plugin, require_scenario


@durable_step
def before_invocation_retry(_step_context: StepContext) -> str:
    return "saved"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "invocation-retry-status")
    # Capture this at entry: consuming the completed step can end replay mode.
    entered_replay = context.is_replaying()
    context.step(before_invocation_retry(), name="otel-before-invocation-retry")
    if not entered_replay:
        raise InvocationError("Conformance invocation retry after a completed step")
    return "retry-complete"
