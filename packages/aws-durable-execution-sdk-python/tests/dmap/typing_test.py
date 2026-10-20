"""Static type checks for the distributed map authoring surface.

``assert_type`` is checked by mypy and returns its value unchanged at runtime,
so every test below asserts nothing beyond building the value it checks. Each
test is annotated, because mypy skips the body of an unannotated function.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, assert_type

from aws_durable_execution_sdk_python.config import (
    DistributedMapProcessor,
    DistributedMapResultConfig,
    NestingType,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dmap.helpers import (
    ReaderPage,
    process_distributed_map_batch,
    process_distributed_map_items,
    read_distributed_map_page,
)
from aws_durable_execution_sdk_python.dmap.models import DistributedMapResult
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python.logger import Logger
from aws_durable_execution_sdk_python.types import (
    DistributedMapBatchContext,
    DistributedMapItemContext,
    DistributedMapReaderContext,
    LoggerInterface,
)


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


_RUN_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/exec1/invoke1/distributed-map-run/abc"
)
_EVENT: dict[str, Any] = {
    "distributedMapRunArn": _RUN_ARN,
    "records": [{"itemId": "0", "body": "1"}],
    "maxItemsPerPage": 10,
}


def test_process_items_non_durable_form_returns_the_response() -> None:
    def item_func(item: Any, ctx: DistributedMapItemContext) -> Any:
        assert_type(item, Any)
        assert_type(ctx, DistributedMapItemContext)
        assert_type(ctx.logger, LoggerInterface)
        return item

    response = process_distributed_map_items(_EVENT, item_func)
    assert_type(response, dict[str, Any])
    configured = process_distributed_map_items(
        _EVENT, item_func, item_serdes=None, result_serdes=None, concurrency=2
    )
    assert_type(configured, dict[str, Any])


def test_process_items_durable_form_passes_the_durable_context_to_func() -> None:
    def item_func(
        context: DurableContext, item: Any, ctx: DistributedMapItemContext
    ) -> Any:
        assert_type(item, Any)
        assert_type(ctx, DistributedMapItemContext)
        return context.step(lambda _step_ctx: item, name="read")

    def run(context: DurableContext) -> None:
        durable = process_distributed_map_items(
            context, _EVENT, item_func, concurrency=2, nesting_type=NestingType.NESTED
        )
        assert_type(durable, dict[str, Any])

    assert callable(run)


def test_process_items_durable_func_receives_the_durable_logger_type() -> None:
    """The item function sees the durable context's Logger and the item context's LoggerInterface.

    The Python DurableContext has one logger type, Logger, so there is no
    customer logger type parameter to flow. A customer logger installed with
    set_logger is wrapped by that Logger, and the item context's logger is typed
    LoggerInterface.
    """

    def item_func(
        context: DurableContext, _item: Any, ctx: DistributedMapItemContext
    ) -> Any:
        assert_type(context.logger, Logger)
        assert_type(ctx.logger, LoggerInterface)

    def run(context: DurableContext) -> None:
        assert_type(
            process_distributed_map_items(context, _EVENT, item_func),
            dict[str, Any],
        )

    assert callable(run)


def test_process_batch_returns_none() -> None:
    def batch_func(items: list[Any], ctx: DistributedMapBatchContext) -> None:
        assert_type(items, list[Any])
        assert_type(ctx, DistributedMapBatchContext)
        assert_type(ctx.item_ids, tuple[str, ...])

    assert_type(process_distributed_map_batch(_EVENT, batch_func), None)
    assert_type(
        process_distributed_map_batch(_EVENT, batch_func, item_serdes=None), None
    )


def test_read_page_returns_the_response() -> None:
    def read_func(
        state: Any, max_items_per_page: int, ctx: DistributedMapReaderContext
    ) -> ReaderPage:
        assert_type(state, Any)
        assert_type(max_items_per_page, int)
        assert_type(ctx, DistributedMapReaderContext)
        return ReaderPage()

    assert_type(read_distributed_map_page(_EVENT, read_func), dict[str, Any])
    assert_type(
        read_distributed_map_page(
            _EVENT, read_func, item_serdes=None, state_serdes=None
        ),
        dict[str, Any],
    )
