"""Authoring helpers for distributed map processor and reader Lambda functions.

Wrappers that own the request/response format so customers can write a plain
function to process items/batches or read pages. Exported from the package root.
"""

from __future__ import annotations

import functools
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, overload

from aws_durable_execution_sdk_python.config import (
    BatchItemStatus,
    CompletionConfig,
    MapConfig,
)
from aws_durable_execution_sdk_python.constants import READER_STATE_LIMIT_BYTES
from aws_durable_execution_sdk_python.exceptions import ValidationError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPluginFactory
from aws_durable_execution_sdk_python.serdes import (
    DEFAULT_JSON_SERDES,
    SerDes,
    SerDesContext,
)


if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient as Boto3LambdaClient

    from aws_durable_execution_sdk_python.types import LambdaContext


@dataclass(frozen=True)
class ReaderPage:
    """A page returned by a reader function: its items and the next state."""

    items: list[Any] = field(default_factory=list)
    next_state: Any | None = None


@dataclass(frozen=True)
class ProcessorRecord:
    """One item of the batch handed to a processor function."""

    item_id: str
    body: Any = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ProcessorRecord:
        item_id = data.get("itemId")
        # The wrappers copy this id onto every outcome they report, so the
        # service matches each outcome back to its item by it. An empty id
        # would report the outcome against no item at all.
        if not isinstance(item_id, str) or not item_id:
            msg = (
                "expected each distributed map processor record to have a "
                "string 'itemId'"
            )
            raise ValidationError(msg)
        return cls(item_id=item_id, body=data.get("body"))


@dataclass(frozen=True)
class ProcessorEvent:
    """The event a processor function is invoked with, one batch of items."""

    records: list[ProcessorRecord] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ProcessorEvent:
        records = data.get("records")
        if not isinstance(records, list):
            msg = "expected a distributed map processor envelope with a 'records' list"
            raise ValidationError(msg)
        return cls(records=[ProcessorRecord.from_dict(r) for r in records])


@dataclass(frozen=True)
class ItemResult:
    """One item's output, reported by an item_outcomes processor."""

    item_identifier: str
    output: str | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {"itemIdentifier": self.item_identifier}
        if self.output is not None:
            result["output"] = self.output
        return result


@dataclass(frozen=True)
class ItemFailure:
    """One item's failure, reported by an item_outcomes processor."""

    item_identifier: str
    error: ErrorObject | None = None

    @classmethod
    def from_exception(cls, item_identifier: str, exc: Exception) -> ItemFailure:
        return cls(
            item_identifier=item_identifier, error=ErrorObject.from_exception(exc)
        )

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ItemFailure:
        error = data.get("error")
        return cls(
            item_identifier=data.get("itemIdentifier", ""),
            error=ErrorObject(
                message=error.get("errorMessage"),
                type=error.get("errorType"),
                data=error.get("errorData"),
                stack_trace=error.get("stackTrace"),
            )
            if error
            else None,
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {"itemIdentifier": self.item_identifier}
        if self.error is None:
            return result
        # The processor response envelope is camelCase, so ErrorObject.to_dict
        # cannot be reused here. Every member is optional and the service
        # stores each one unchanged, so an absent member is omitted.
        error: MutableMapping[str, Any] = {}
        if self.error.type is not None:
            error["errorType"] = self.error.type
        if self.error.message is not None:
            error["errorMessage"] = self.error.message
        if self.error.data is not None:
            error["errorData"] = self.error.data
        if self.error.stack_trace is not None:
            error["stackTrace"] = self.error.stack_trace
        if error:
            result["error"] = error
        return result


@dataclass(frozen=True)
class ItemHandlerResponse:
    """What an item handler returns: a failure or a result for each item."""

    failures: list[ItemFailure] = field(default_factory=list)
    results: list[ItemResult] = field(default_factory=list)

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "batchItemFailures": [f.to_dict() for f in self.failures],
            "batchItemResults": [r.to_dict() for r in self.results],
        }


@dataclass(frozen=True)
class ReaderEvent:
    """The event a reader function is invoked with."""

    max_items_per_page: int
    state: str | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> ReaderEvent:
        max_items_per_page = data.get("maxItemsPerPage")
        if not isinstance(max_items_per_page, int):
            msg = (
                "expected a distributed map reader envelope with an integer "
                "'maxItemsPerPage'"
            )
            raise ValidationError(msg)
        return cls(max_items_per_page=max_items_per_page, state=data.get("state"))


@dataclass(frozen=True)
class ReaderResponse:
    """What a reader function returns. A None ``next_state`` exhausts the source."""

    items: list[str] = field(default_factory=list)
    next_state: str | None = None

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {"items": self.items}
        if self.next_state is not None:
            result["nextState"] = self.next_state
        return result


def _to_item(serdes: SerDes[Any], body: str, ctx: SerDesContext) -> Any:
    """Recover a typed item from a record body."""
    return serdes.deserialize(body, ctx)


def _to_output(serdes: SerDes[Any], value: Any, ctx: SerDesContext) -> str | None:
    """Serialize an item's output, or None when the handler returned nothing."""
    if value is None:
        return None
    return serdes.serialize(value, ctx)


def _validate_concurrency(concurrency: int) -> None:
    if concurrency < 1:
        msg = f"concurrency must be at least 1, got: {concurrency}"
        raise ValidationError(msg)


@overload
def distributed_map_item_handler(
    func: Callable[[Any], Any],
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
) -> Callable[[dict[str, Any], Any], dict[str, Any]]: ...


@overload
def distributed_map_item_handler(
    func: None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
) -> Callable[
    [Callable[[Any], Any]], Callable[[dict[str, Any], Any], dict[str, Any]]
]: ...


def distributed_map_item_handler(
    func: Callable[[Any], Any] | None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
) -> Any:
    """Wrap a Lambda to be used as an item_outcomes processor.

    ``func`` takes one item and returns its output or raises. The decorated name
    becomes the Lambda handler and takes ``(event, context)``, so name it
    ``handler``. An item whose handler returns ``None`` succeeds with no output.

    Pair this with ``DistributedMapProcessor.report_item_outcomes``. Paired with
    ``report_batch_outcome`` instead, the service ignores the response this
    returns, so every item in the batch is marked succeeded, including the items
    whose handler raised.

    Items in a batch are processed one at a time. Raise ``concurrency`` to run
    that many items at once, which requires ``func`` to be safe to call from
    several threads.
    """
    if func is None:
        return functools.partial(
            distributed_map_item_handler,
            item_serdes=item_serdes,
            result_serdes=result_serdes,
            concurrency=concurrency,
        )
    _validate_concurrency(concurrency)
    serdes = item_serdes or DEFAULT_JSON_SERDES
    out_serdes = result_serdes or DEFAULT_JSON_SERDES

    def handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
        ctx = SerDesContext()
        records = ProcessorEvent.from_dict(event).records

        def run(record: ProcessorRecord) -> str | None:
            # Serializing here, not after the loop, keeps a serialization
            # failure on the one item whose output could not be serialized.
            return _to_output(out_serdes, func(_to_item(serdes, record.body, ctx)), ctx)

        outputs: list[str | None] = [None] * len(records)
        errors: list[Exception | None] = [None] * len(records)
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(run, r): i for i, r in enumerate(records)}
            for future, i in futures.items():
                try:
                    outputs[i] = future.result()
                except Exception as exc:  # noqa: BLE001
                    errors[i] = exc

        results: list[ItemResult] = []
        failures: list[ItemFailure] = []
        for i, record in enumerate(records):
            err = errors[i]
            if err is not None:
                failures.append(ItemFailure.from_exception(record.item_id, err))
            else:
                results.append(
                    ItemResult(item_identifier=record.item_id, output=outputs[i])
                )

        response = ItemHandlerResponse(failures=failures, results=results)
        return dict(response.to_dict())

    return handler


@overload
def distributed_map_batch_handler(
    func: Callable[[list[Any]], None],
    *,
    item_serdes: SerDes[Any] | None = None,
) -> Callable[[dict[str, Any], Any], None]: ...


@overload
def distributed_map_batch_handler(
    func: None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
) -> Callable[[Callable[[list[Any]], None]], Callable[[dict[str, Any], Any], None]]: ...


def distributed_map_batch_handler(
    func: Callable[[list[Any]], None] | None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
) -> Any:
    """Wrap a Lambda to be used as a batch processor.

    ``func`` takes the whole batch of items. Returning succeeds every item;
    raising fails every item. The decorated name becomes the Lambda handler and
    takes ``(event, context)``, so name it ``handler``.

    A batch processor reports no per-item outcome, so it records no output. Use
    ``distributed_map_item_handler`` to report an output per item.
    """
    if func is None:
        return functools.partial(distributed_map_batch_handler, item_serdes=item_serdes)
    serdes = item_serdes or DEFAULT_JSON_SERDES

    def handler(event: dict[str, Any], _context: Any = None) -> None:
        ctx = SerDesContext()
        func(
            [
                _to_item(serdes, record.body, ctx)
                for record in ProcessorEvent.from_dict(event).records
            ]
        )

    return handler


@overload
def distributed_map_reader(
    func: Callable[[Any, int], ReaderPage],
    *,
    item_serdes: SerDes[Any] | None = None,
    state_serdes: SerDes[Any] | None = None,
) -> Callable[[dict[str, Any], Any], dict[str, Any]]: ...


@overload
def distributed_map_reader(
    func: None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    state_serdes: SerDes[Any] | None = None,
) -> Callable[
    [Callable[[Any, int], ReaderPage]],
    Callable[[dict[str, Any], Any], dict[str, Any]],
]: ...


def distributed_map_reader(
    func: Callable[[Any, int], ReaderPage] | None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    state_serdes: SerDes[Any] | None = None,
) -> Any:
    """Wrap a Lambda to be used as a reader source.

    ``func`` takes the current state and the maximum items this page may
    return, and returns a ReaderPage. A ``next_state`` of ``None`` signals the
    source is exhausted. The decorated name becomes the Lambda handler and
    takes ``(event, context)``, so name it ``handler``.

    ``item_serdes`` serializes each item of the page. It must be the same serdes
    the processor is configured with, since the processor deserializes every
    item body with its own ``item_serdes``.
    """
    if func is None:
        return functools.partial(
            distributed_map_reader,
            item_serdes=item_serdes,
            state_serdes=state_serdes,
        )
    item_sd = item_serdes or DEFAULT_JSON_SERDES
    state_sd = state_serdes or DEFAULT_JSON_SERDES

    def handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
        ctx = SerDesContext()
        reader_event = ReaderEvent.from_dict(event)
        state = (
            state_sd.deserialize(reader_event.state, ctx)
            if reader_event.state is not None
            else None
        )

        page = func(state, reader_event.max_items_per_page)
        if len(page.items) > reader_event.max_items_per_page:
            msg = (
                f"reader returned {len(page.items)} items, exceeding "
                f"maxItemsPerPage {reader_event.max_items_per_page}"
            )
            raise ValidationError(msg)

        items = [item_sd.serialize(item, ctx) for item in page.items]

        next_state: str | None = None
        if page.next_state is not None:
            next_state = state_sd.serialize(page.next_state, ctx)
            if len(next_state.encode("utf-8")) > READER_STATE_LIMIT_BYTES:
                msg = f"reader next_state exceeds the {READER_STATE_LIMIT_BYTES // 1024} KB limit"
                raise ValidationError(msg)
            # next_state becomes the next call's input state, so returning the
            # input state repeats the same position instead of advancing. The
            # service rejects this as a source failure, empty page included.
            if next_state == reader_event.state:
                msg = (
                    "reader next_state must differ from the state it was called "
                    "with, or the next page repeats the same position"
                )
                raise ValidationError(msg)

        return dict(ReaderResponse(items=items, next_state=next_state).to_dict())

    return handler


@overload
def durable_distributed_map_item_handler(
    func: Callable[[Any, Any], Any],
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPluginFactory] | None = None,
) -> Callable[[Any, LambdaContext], Any]: ...


@overload
def durable_distributed_map_item_handler(
    func: None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPluginFactory] | None = None,
) -> Callable[[Callable[[Any, Any], Any]], Callable[[Any, LambdaContext], Any]]: ...


def durable_distributed_map_item_handler(
    func: Callable[[Any, Any], Any] | None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPluginFactory] | None = None,
) -> Any:
    """Durable variant of the item handler. ``func`` receives (context, item).

    The Lambda must be deployed as a durable function. The decorated name becomes
    the Lambda handler and takes ``(event, context)``, so name it ``handler``.
    An item whose handler returns ``None`` succeeds with no output.

    Pair this with ``DistributedMapProcessor.report_item_outcomes``. Paired with
    ``report_batch_outcome`` instead, the service ignores the response this
    returns, so every item in the batch is marked succeeded, including the items
    whose handler raised.

    Items in a batch are processed one at a time. Raise ``concurrency`` to run
    that many items at once.

    ``boto3_client`` and ``plugins`` are passed to ``durable_execution``, so a
    durable processor can supply its own client or register plugins.
    """
    if func is None:
        return functools.partial(
            durable_distributed_map_item_handler,
            item_serdes=item_serdes,
            result_serdes=result_serdes,
            concurrency=concurrency,
            boto3_client=boto3_client,
            plugins=plugins,
        )
    _validate_concurrency(concurrency)
    serdes = item_serdes or DEFAULT_JSON_SERDES
    out_serdes = result_serdes or DEFAULT_JSON_SERDES

    def inner(event: dict[str, Any], context: Any) -> dict[str, Any]:
        ctx = SerDesContext()
        records = ProcessorEvent.from_dict(event).records

        def per_item(
            inner_ctx: Any, body: Any, _index: int, _inputs: Any
        ) -> str | None:
            # Serializing here, not after the batch, keeps a serialization
            # failure on the one item whose output could not be serialized.
            return _to_output(
                out_serdes, func(inner_ctx, _to_item(serdes, body, ctx)), ctx
            )

        batch = context.map(
            [record.body for record in records],
            per_item,
            config=MapConfig(
                max_concurrency=concurrency,
                completion_config=CompletionConfig.all_completed(),
            ),
        )

        results: list[ItemResult] = []
        failures: list[ItemFailure] = []
        for bi in batch.all:
            item_id = records[bi.index].item_id
            if bi.status is BatchItemStatus.SUCCEEDED:
                results.append(ItemResult(item_identifier=item_id, output=bi.result))
            else:
                failures.append(ItemFailure(item_identifier=item_id, error=bi.error))

        response = ItemHandlerResponse(failures=failures, results=results)
        return dict(response.to_dict())

    return durable_execution(inner, boto3_client=boto3_client, plugins=plugins)


@overload
def durable_distributed_map_batch_handler(
    func: Callable[[Any, list[Any]], None],
    *,
    item_serdes: SerDes[Any] | None = None,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPluginFactory] | None = None,
) -> Callable[[Any, LambdaContext], Any]: ...


@overload
def durable_distributed_map_batch_handler(
    func: None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPluginFactory] | None = None,
) -> Callable[
    [Callable[[Any, list[Any]], None]], Callable[[Any, LambdaContext], Any]
]: ...


def durable_distributed_map_batch_handler(
    func: Callable[[Any, list[Any]], None] | None = None,
    *,
    item_serdes: SerDes[Any] | None = None,
    boto3_client: Boto3LambdaClient | None = None,
    plugins: list[DurableInstrumentationPluginFactory] | None = None,
) -> Any:
    """Durable variant of the batch handler. ``func`` receives (context, items).

    The Lambda must be deployed as a durable function. The decorated name becomes
    the Lambda handler and takes ``(event, context)``, so name it ``handler``.

    A batch processor reports no per-item outcome, so it records no output. Use
    ``durable_distributed_map_item_handler`` to report an output per item.

    ``boto3_client`` and ``plugins`` are passed to ``durable_execution``, so a
    durable processor can supply its own client or register plugins.
    """
    if func is None:
        return functools.partial(
            durable_distributed_map_batch_handler,
            item_serdes=item_serdes,
            boto3_client=boto3_client,
            plugins=plugins,
        )
    serdes = item_serdes or DEFAULT_JSON_SERDES

    def inner(event: dict[str, Any], context: Any) -> None:
        ctx = SerDesContext()
        func(
            context,
            [
                _to_item(serdes, record.body, ctx)
                for record in ProcessorEvent.from_dict(event).records
            ],
        )

    return durable_execution(inner, boto3_client=boto3_client, plugins=plugins)
