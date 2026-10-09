# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Observe the real active SDK context inside public user callbacks."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import (
    Duration,
    MapConfig,
    ParallelBranch,
    ParallelConfig,
)
from common import otel_plugin, require_scenario
from opentelemetry import trace


def probe(label: str) -> None:
    if not trace.get_current_span().get_span_context().is_valid:
        raise RuntimeError(f"No active span for conformance.{label}")
    span = trace.get_tracer("aws-durable-execution-conformance").start_span(
        f"conformance.{label}", attributes={"conformance.callback": label}
    )
    span.end()


def step_body(_context: StepContext) -> str:
    probe("step")
    return "step"


def child_step(_context: StepContext) -> str:
    probe("child-step")
    return "child-step"


def child_body(context: DurableContext) -> str:
    probe("child")
    context.step(child_step, name="otel-context-child-step")
    probe("child-restored")
    return "child"


def parallel_step_a(_context: StepContext) -> str:
    probe("parallel-step-a")
    return "a"


def parallel_step_b(_context: StepContext) -> str:
    probe("parallel-step-b")
    return "b"


def parallel_a(context: DurableContext) -> str:
    probe("parallel-a")
    return context.step(parallel_step_a, name="otel-context-branch-step-a")


def parallel_b(context: DurableContext) -> str:
    probe("parallel-b")
    return context.step(parallel_step_b, name="otel-context-branch-step-b")


def iteration_name(_item: int, index: int) -> str:
    return ("otel-context-iteration-0", "otel-context-iteration-1")[index]


def mapper(
    context: DurableContext, item: int, index: int, _items: Sequence[int]
) -> int:
    probe(("map-0", "map-1")[index])

    def map_step(_step_context: StepContext) -> int:
        probe(("map-step-0", "map-step-1")[index])
        return item

    return context.step(
        map_step, name=("otel-context-map-step-0", "otel-context-map-step-1")[index]
    )


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "user-function-context")
    probe("handler")
    context.step(step_body, name="otel-context-step")
    context.run_in_child_context(child_body, name="otel-context-child")
    context.parallel(
        [
            ParallelBranch(parallel_a, name="otel-context-branch-a"),
            ParallelBranch(parallel_b, name="otel-context-branch-b"),
        ],
        name="otel-context-parallel",
        config=ParallelConfig(max_concurrency=2),
    )
    context.map(
        [0, 1],
        mapper,
        name="otel-context-map",
        config=MapConfig(max_concurrency=2, item_namer=iteration_name),
    )
    probe("handler-restored")
    context.wait(Duration.from_seconds(1), name="otel-context-resume")
    probe("handler-after-resume")
    return "context-complete"
