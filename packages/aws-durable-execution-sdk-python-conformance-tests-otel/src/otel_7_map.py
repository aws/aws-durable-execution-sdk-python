# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Map hierarchy scenario for OTel requirement otel-invocation-7."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import MapConfig
from common import otel_plugin, require_scenario


@durable_step
def double_map_item(_step_context: StepContext, item: int) -> int:
    return item * 2


def process_map_item(
    context: DurableContext,
    item: int,
    index: int,
    _items: Sequence[int],
) -> int:
    return context.step(
        double_map_item(item),
        name=f"otel-map-step-{index}",
    )


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> list[int]:
    require_scenario(event, "map-hierarchy")
    return context.map(
        inputs=[1, 2],
        func=process_map_item,
        name="otel-map",
        config=MapConfig(
            item_namer=lambda _item, index: f"otel-map-iteration-{index}",
            max_concurrency=1,
        ),
    ).get_results()
