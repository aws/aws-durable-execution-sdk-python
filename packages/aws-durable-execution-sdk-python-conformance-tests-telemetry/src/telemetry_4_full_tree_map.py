# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Full-tree operation detail for requirement telemetry-4.

The handler maps over the event ``[1, 2]`` with ``maxConcurrency=1``. The item
namer names each iteration ``item-<item>``. Each iteration runs one step named
``greet-<item>``. The plugin runs with ``operation_detail="full-tree"``. So the
record's ``operationsByName`` holds the map context ``process-items``, the two
iteration contexts ``item-1`` and ``item-2``, and the two steps ``greet-1`` and
``greet-2``. Under ``top-level`` detail the record would hold only
``process-items``.

The handler uses the real SDK API only. It builds no record and calls no
exporter method. A missing or wrong record is the plugin's defect, and that
defect is the signal the suite measures.
"""

from __future__ import annotations

import os
from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python_telemetry_exporter import (
    CloudWatchLogsExporter,
    ContentConfig,
    DurableExecutionTelemetryConfig,
    durable_execution_telemetry,
)


# The template sets this variable. Without it the exporter has no target.
# So the module fails at import instead of dropping records silently.
log_group_name = os.environ.get("TELEMETRY_LOG_GROUP_NAME")
if not log_group_name:
    raise RuntimeError("TELEMETRY_LOG_GROUP_NAME must be set")


@durable_step
def greet(_step_context: StepContext, item: int) -> str:
    return f"Hello, {item}!"


def map_fn(ctx: DurableContext, item: int, _index: int, _items: Any) -> str:
    result: str = ctx.step(greet(item), name=f"greet-{item}")
    return result


@durable_execution(
    plugins=[
        durable_execution_telemetry(
            DurableExecutionTelemetryConfig(
                exporters=[CloudWatchLogsExporter(log_group_name=log_group_name)],
                emit_mode="on-complete",
                sampling_rate=1,
                operation_detail="full-tree",
                content=ContentConfig(input=True, output=True),
            )
        )
    ]
)
def handler(event: list[int], context: DurableContext) -> list[str]:
    results: list[str] = context.map(
        event,
        map_fn,
        name="process-items",
        config=MapConfig(
            max_concurrency=1,
            item_namer=lambda item, index: f"item-{item}",
        ),
    ).get_results()
    return results
