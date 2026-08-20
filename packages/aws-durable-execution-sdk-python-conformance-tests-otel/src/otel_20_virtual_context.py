# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Virtual child-context scenario for OTel requirement otel-invocation-20."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.config import ChildConfig
from common import otel_plugin, require_scenario


@durable_with_child_context
def run_virtual_context(_context: DurableContext) -> str:
    return "virtual-complete"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "virtual-context")
    return context.run_in_child_context(
        run_virtual_context(),
        name="otel-virtual-context",
        config=ChildConfig(is_virtual=True),
    )
