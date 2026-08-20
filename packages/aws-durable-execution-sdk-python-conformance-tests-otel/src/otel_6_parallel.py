# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Parallel hierarchy scenario for OTel requirement otel-invocation-6."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_parallel_branch,
    durable_step,
)
from aws_durable_execution_sdk_python.config import ParallelConfig
from common import otel_plugin, require_scenario


@durable_step
def complete_parallel_step(_step_context: StepContext, label: str) -> str:
    return label


@durable_parallel_branch(name="otel-parallel-branch-a")
def run_parallel_branch_a(context: DurableContext) -> str:
    return context.step(
        complete_parallel_step("a"),
        name="otel-parallel-step-a",
    )


@durable_parallel_branch(name="otel-parallel-branch-b")
def run_parallel_branch_b(context: DurableContext) -> str:
    return context.step(
        complete_parallel_step("b"),
        name="otel-parallel-step-b",
    )


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> list[str]:
    require_scenario(event, "parallel-hierarchy")
    return context.parallel(
        functions=[
            run_parallel_branch_a(),
            run_parallel_branch_b(),
        ],
        name="otel-parallel",
        config=ParallelConfig(max_concurrency=1),
    ).get_results()
