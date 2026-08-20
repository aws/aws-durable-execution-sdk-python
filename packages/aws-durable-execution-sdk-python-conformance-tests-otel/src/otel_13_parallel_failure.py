# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed parallel scenario for OTel requirement otel-invocation-13."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_parallel_branch,
)
from aws_durable_execution_sdk_python.config import ParallelConfig
from common import otel_plugin, require_scenario


@durable_parallel_branch(name="otel-failed-parallel-branch")
def fail_parallel_branch(_context: DurableContext) -> None:
    raise RuntimeError("Intentional parallel branch failure")


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "parallel-failure")
    result = context.parallel(
        functions=[fail_parallel_branch()],
        name="otel-failed-parallel",
        config=ParallelConfig(max_concurrency=1),
    )
    result.throw_if_error()
