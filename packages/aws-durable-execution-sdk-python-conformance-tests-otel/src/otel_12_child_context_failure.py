# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed child-context scenario for OTel requirement otel-invocation-12."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)
from common import otel_plugin, require_scenario


@durable_with_child_context
def fail_child_context(_context: DurableContext) -> None:
    raise RuntimeError("Intentional child-context failure")


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "child-context-failure")
    context.run_in_child_context(
        fail_child_context(),
        name="otel-failed-child-context",
    )
