"""Static type checks for the distributed map authoring surface.

``assert_type`` is checked by mypy and returns its value unchanged at runtime,
so every test below asserts nothing beyond building the value it checks. Each
test is annotated, because mypy skips the body of an unannotated function.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, assert_type

from aws_durable_execution_sdk_python.config import (
    DistributedMapProcessor,
    DistributedMapResultConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dmap.handlers import (
    ReaderPage,
    distributed_map_batch_handler,
    distributed_map_item_handler,
    distributed_map_reader,
    durable_distributed_map_batch_handler,
    durable_distributed_map_item_handler,
)
from aws_durable_execution_sdk_python.dmap.models import DistributedMapResult
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python.types import LambdaContext


@dataclass(frozen=True)
class _Order:
    """A result type the default serdes cannot write."""

    order_id: str


class _OrderSerDes(SerDes[_Order]):
    """A serdes that fixes the result type of a map run that uses it."""

    def serialize(self, value: _Order, _serdes_context: SerDesContext) -> str:
        return value.order_id

    def deserialize(self, data: str, _serdes_context: SerDesContext) -> _Order:
        return _Order(order_id=data)


def _check_result_config_carries_its_output_type(ctx: DurableContext) -> None:
    """Checked by mypy only. A map run needs a live execution, so nothing calls this."""
    result = ctx.distributed_map(
        ["a"],
        DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=1,
        config=DistributedMapResultConfig(result_serdes=_OrderSerDes()),
    )
    assert_type(result, DistributedMapResult[_Order])
    assert_type(result.get_results(), list[_Order | None])


def test_item_handler_returns_a_lambda_handler() -> None:
    bare = distributed_map_item_handler(lambda item: item)
    assert_type(bare, Callable[[dict[str, Any], Any], dict[str, Any]])
    configured = distributed_map_item_handler(concurrency=2)(lambda item: item)
    assert_type(configured, Callable[[dict[str, Any], Any], dict[str, Any]])


def test_batch_handler_returns_a_lambda_handler() -> None:
    bare = distributed_map_batch_handler(lambda _items: None)
    assert_type(bare, Callable[[dict[str, Any], Any], None])
    configured = distributed_map_batch_handler()(lambda _items: None)
    assert_type(configured, Callable[[dict[str, Any], Any], None])


def test_reader_returns_a_lambda_handler() -> None:
    bare = distributed_map_reader(lambda _state, _max_items: ReaderPage())
    assert_type(bare, Callable[[dict[str, Any], Any], dict[str, Any]])
    configured = distributed_map_reader()(lambda _state, _max_items: ReaderPage())
    assert_type(configured, Callable[[dict[str, Any], Any], dict[str, Any]])


def test_durable_item_handler_returns_a_lambda_handler() -> None:
    bare = durable_distributed_map_item_handler(lambda _ctx, item: item)
    assert_type(bare, Callable[[Any, LambdaContext], Any])
    configured = durable_distributed_map_item_handler(concurrency=2)(
        lambda _ctx, item: item
    )
    assert_type(configured, Callable[[Any, LambdaContext], Any])


def test_durable_batch_handler_returns_a_lambda_handler() -> None:
    bare = durable_distributed_map_batch_handler(lambda _ctx, _items: None)
    assert_type(bare, Callable[[Any, LambdaContext], Any])
    configured = durable_distributed_map_batch_handler()(lambda _ctx, _items: None)
    assert_type(configured, Callable[[Any, LambdaContext], Any])
