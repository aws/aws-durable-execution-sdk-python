# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed map scenario for OTel requirement otel-invocation-14."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import MapConfig
from common import otel_plugin, require_scenario


def fail_map_item(
    _context: DurableContext,
    _item: int,
    _index: int,
    _items: Sequence[int],
) -> None:
    raise RuntimeError("Intentional map iteration failure")


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "map-failure")
    result = context.map(
        inputs=[1],
        func=fail_map_item,
        name="otel-failed-map",
        config=MapConfig(
            item_namer=lambda _item, index: f"otel-failed-map-iteration-{index}",
            max_concurrency=1,
        ),
    )
    result.throw_if_error()
