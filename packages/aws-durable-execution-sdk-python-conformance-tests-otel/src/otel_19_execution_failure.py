# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed execution scenario for OTel requirement otel-invocation-19."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from common import otel_plugin, require_scenario


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], _context: DurableContext) -> None:
    require_scenario(event, "execution-failure")
    raise RuntimeError("Intentional execution failure")
