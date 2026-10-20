"""Authoring helpers for distributed map processor and reader Lambda functions.

Helpers that own the request/response format. The customer calls one inside
their own Lambda handler to process items/batches or read pages. Exported from
the package root.

These helpers deserialize values under a different context than the one they
were serialized with, so the serdes must satisfy the context requirement
documented in ``aws_durable_execution_sdk_python.serdes``.
"""

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, overload

from aws_durable_execution_sdk_python.config import (
    BatchItemStatus,
    CompletionConfig,
    MapConfig,
    NestingType,
)
from aws_durable_execution_sdk_python.constants import READER_STATE_LIMIT_BYTES
from aws_durable_execution_sdk_python.dmap.models import DistributedMapRunArn
from aws_durable_execution_sdk_python.exceptions import ValidationError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.logger import Logger
from aws_durable_execution_sdk_python.serdes import (
    DEFAULT_JSON_SERDES,
    SerDes,
    SerDesContext,
)
from aws_durable_execution_sdk_python.types import (
    DistributedMapBatchContext,
    DistributedMapItemContext,
    DistributedMapReaderContext,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import LoggerInterface


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReaderPage:
    """A page returned by a reader function: its items and the next state."""

    items: list[Any] = field(default_factory=list)
    next_state: Any | None = None


@dataclass(frozen=True)
class DistributedMapProcessorRecord:
    """One item in a processor event's batch.

    ``item_id`` is the service-assigned id of the item. ``body`` is the item's
    serialized body.
    """

    item_id: str
    body: Any = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapProcessorRecord:
        item_id = data.get("itemId")
        # The helpers copy this id onto every outcome they report, so the
        # service matches each outcome back to its item by it. An empty id
        # would report the outcome against no item at all.
        if not isinstance(item_id, str) or not item_id:
            msg = (
                "expected each distributed map processor record to have a "
                "string 'itemId'"
            )
            raise ValidationError(msg)
        return cls(item_id=item_id, body=data.get("body"))


def _run_arn_from(data: MutableMapping[str, Any]) -> DistributedMapRunArn:
    """Parse and validate the envelope's 'distributedMapRunArn'."""
    arn = data.get("distributedMapRunArn")
    run_arn = DistributedMapRunArn.from_arn(arn) if isinstance(arn, str) else None
    if run_arn is None:
        msg = "expected a distributed map envelope with a valid 'distributedMapRunArn'"
        raise ValidationError(msg)
    return run_arn


@dataclass(frozen=True)
class DistributedMapProcessorEvent:
    """The event the service sends to a processor Lambda.

    ``distributed_map_run_arn`` is the ARN of the distributed map run.
    ``records`` is the batch of items to process.
    """

    distributed_map_run_arn: DistributedMapRunArn
    records: list[DistributedMapProcessorRecord] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapProcessorEvent:
        records = data.get("records")
        if not isinstance(records, list):
            msg = "expected a distributed map processor envelope with a 'records' list"
            raise ValidationError(msg)
        return cls(
            distributed_map_run_arn=_run_arn_from(data),
            records=[DistributedMapProcessorRecord.from_dict(r) for r in records],
        )


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
        error = _error_fields(self.error)
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
class DistributedMapReaderEvent:
    """The event the service sends to a reader Lambda.

    ``distributed_map_run_arn`` is the ARN of the distributed map run.
    ``state`` is the serialized state, absent on the first call and the previous
    call's next state afterwards. ``max_items_per_page`` is the maximum number
    of items the page may contain.
    """

    distributed_map_run_arn: DistributedMapRunArn
    max_items_per_page: int
    state: str | None = None

    @classmethod
    def from_dict(cls, data: MutableMapping[str, Any]) -> DistributedMapReaderEvent:
        max_items_per_page = data.get("maxItemsPerPage")
        if not isinstance(max_items_per_page, int):
            msg = (
                "expected a distributed map reader envelope with an integer "
                "'maxItemsPerPage'"
            )
            raise ValidationError(msg)
        return cls(
            distributed_map_run_arn=_run_arn_from(data),
            max_items_per_page=max_items_per_page,
            state=data.get("state"),
        )


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


def _stamped_logger(base: LoggerInterface, **extra: str) -> Logger:
    """A logger that adds the map run's identifiers to every line it writes.

    The customer function gets these for free, so it never has to carry the run
    ARN or item id into its own log calls. A durable helper passes its branch
    logger as the base, so that logger's own fields and replay de-duplication
    are kept; a non-durable helper has no owning context and passes this module's
    logger, which always logs.
    """
    return Logger(logger=base, default_extra=extra)


def _item_context(
    run_arn: DistributedMapRunArn,
    item_id: str,
    base: LoggerInterface | None = None,
) -> DistributedMapItemContext:
    """Build the context for one item of a processor batch."""
    run = run_arn.to_arn()
    return DistributedMapItemContext(
        logger=_stamped_logger(
            base if base is not None else _LOGGER,
            distributedMapRunArn=run,
            itemId=item_id,
        ),
        distributed_map_run_arn=run,
        item_id=item_id,
    )


def _batch_context(
    run_arn: DistributedMapRunArn,
    item_ids: tuple[str, ...],
    base: LoggerInterface | None = None,
) -> DistributedMapBatchContext:
    """Build the context for a whole processor batch.

    The ids are index-aligned with the items the function receives.
    """
    run = run_arn.to_arn()
    return DistributedMapBatchContext(
        logger=_stamped_logger(
            base if base is not None else _LOGGER, distributedMapRunArn=run
        ),
        distributed_map_run_arn=run,
        item_ids=item_ids,
    )


def _error_fields(error: ErrorObject | None) -> MutableMapping[str, Any]:
    """An error's camelCase wire fields, used in the response and in the log line.

    The processor response envelope is camelCase, so ErrorObject.to_dict cannot
    be reused. Every member is optional, so an absent member is omitted. All
    four are written because the model accepts them, but the service keeps only
    the type and the message for the parent.
    """
    if error is None:
        return {}
    fields: MutableMapping[str, Any] = {}
    if error.type is not None:
        fields["errorType"] = error.type
    if error.message is not None:
        fields["errorMessage"] = error.message
    if error.data is not None:
        fields["errorData"] = error.data
    if error.stack_trace is not None:
        fields["stackTrace"] = error.stack_trace
    return fields


def _reader_context(run_arn: DistributedMapRunArn) -> DistributedMapReaderContext:
    """Build the context for one reader page."""
    run = run_arn.to_arn()
    return DistributedMapReaderContext(
        logger=_stamped_logger(_LOGGER, distributedMapRunArn=run),
        distributed_map_run_arn=run,
    )


def _process_items_non_durable(
    event: MutableMapping[str, Any],
    func: Callable[[Any, DistributedMapItemContext], Any],
    item_serdes: SerDes[Any] | None,
    result_serdes: SerDes[Any] | None,
    concurrency: int,
) -> dict[str, Any]:
    _validate_concurrency(concurrency)
    serdes = item_serdes or DEFAULT_JSON_SERDES
    out_serdes = result_serdes or DEFAULT_JSON_SERDES
    processor_event = DistributedMapProcessorEvent.from_dict(event)
    run_arn = processor_event.distributed_map_run_arn
    records = processor_event.records

    def run(record: DistributedMapProcessorRecord) -> str | None:
        item = _to_item(
            serdes, record.body, run_arn.serdes_context("body", record.item_id)
        )
        # Serializing here, not after the loop, keeps a serialization
        # failure on the one item whose output could not be serialized.
        return _to_output(
            out_serdes,
            func(item, _item_context(run_arn, record.item_id)),
            run_arn.serdes_context("output", record.item_id),
        )

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
            failure = ItemFailure.from_exception(record.item_id, err)
            _item_context(run_arn, record.item_id).logger.error(
                "distributed map item failed", extra=_error_fields(failure.error)
            )
            failures.append(failure)
        else:
            results.append(
                ItemResult(item_identifier=record.item_id, output=outputs[i])
            )

    response = ItemHandlerResponse(failures=failures, results=results)
    return dict(response.to_dict())


def _process_items_durable(
    context: Any,
    event: MutableMapping[str, Any],
    func: Callable[[Any, Any, DistributedMapItemContext], Any],
    item_serdes: SerDes[Any] | None,
    result_serdes: SerDes[Any] | None,
    concurrency: int,
    nesting_type: NestingType,
) -> dict[str, Any]:
    _validate_concurrency(concurrency)
    serdes = item_serdes or DEFAULT_JSON_SERDES
    out_serdes = result_serdes or DEFAULT_JSON_SERDES
    processor_event = DistributedMapProcessorEvent.from_dict(event)
    run_arn = processor_event.distributed_map_run_arn
    records = processor_event.records

    def per_item(inner_ctx: Any, body: Any, index: int, _inputs: Any) -> str | None:
        item_id = records[index].item_id
        item = _to_item(serdes, body, run_arn.serdes_context("body", item_id))
        # Serializing here, not after the batch, keeps a serialization
        # failure on the one item whose output could not be serialized.
        return _to_output(
            out_serdes,
            func(
                inner_ctx,
                item,
                _item_context(run_arn, item_id, inner_ctx.logger),
            ),
            run_arn.serdes_context("output", item_id),
        )

    batch = context.map(
        [record.body for record in records],
        per_item,
        config=MapConfig(
            max_concurrency=concurrency,
            nesting_type=nesting_type,
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
            _item_context(run_arn, item_id, context.logger).logger.error(
                "distributed map item failed", extra=_error_fields(bi.error)
            )
            failures.append(ItemFailure(item_identifier=item_id, error=bi.error))

    response = ItemHandlerResponse(failures=failures, results=results)
    return dict(response.to_dict())


_ITEM_OPTIONS = ("item_serdes", "result_serdes", "concurrency")


def _reject_unknown_options(kwargs: dict[str, Any], accepted: tuple[str, ...]) -> None:
    for name in kwargs:
        if name not in accepted:
            msg = (
                f"process_distributed_map_items got an unexpected keyword "
                f"argument '{name}' for this form, accepted: {', '.join(accepted)}"
            )
            raise ValidationError(msg)


@overload
def process_distributed_map_items(
    event: MutableMapping[str, Any],
    func: Callable[[Any, DistributedMapItemContext], Any],
    /,
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
) -> dict[str, Any]:
    """Process the items of a distributed map batch in a non-durable processor.

    Call this inside the handler of a non-durable processor Lambda. Pass that
    Lambda's name to ``DistributedMapProcessor.report_item_outcomes`` in the
    parent's ``ctx.distributed_map`` call.

    The handler must return the result of this function. The service reads each
    item's outcome from the handler's return value. A handler that returns
    nothing makes the service mark every item succeeded, including the items
    whose ``func`` raised.

    For each failed item, this function logs one error line through the item's
    logger.

    ``func`` receives ``(item, DistributedMapItemContext)`` and returns the
    item's output or raises. The context names the run and the item. An item
    whose ``func`` returns ``None`` succeeds with no output. Items in a batch
    are processed one at a time. Raise ``concurrency`` to run that many items
    at once, which requires ``func``, ``item_serdes`` and ``result_serdes`` to
    be safe to call from several threads.

    Args:
        event: The event the Lambda was invoked with.
        func: Called once per item.
        item_serdes: Deserializes each item. Defaults to JSON.
        result_serdes: Serializes each item's output. Defaults to JSON.
        concurrency: Maximum number of items to process concurrently.

    Example:
        def handler(event, context):
            return process_distributed_map_items(
                event, lambda item, item_ctx: item["id"], concurrency=5
            )
    """


@overload
def process_distributed_map_items(
    context: Any,
    event: MutableMapping[str, Any],
    func: Callable[[Any, Any, DistributedMapItemContext], Any],
    /,
    *,
    item_serdes: SerDes[Any] | None = None,
    result_serdes: SerDes[Any] | None = None,
    concurrency: int = 1,
    nesting_type: NestingType = NestingType.FLAT,
) -> dict[str, Any]:
    """Process the items of a distributed map batch in a durable processor.

    Call this inside the ``@durable_execution`` handler of a durable processor
    Lambda, passing the handler's ``DurableContext`` first. Pass that Lambda's
    name to ``DistributedMapProcessor.report_item_outcomes`` in the parent's
    ``ctx.distributed_map`` call.

    The handler must return the result of this function. The service reads each
    item's outcome from the handler's return value. A handler that returns
    nothing makes the service mark every item succeeded, including the items
    whose ``func`` raised.

    For each failed item, this function logs one error line through the item's
    logger.

    ``func`` receives ``(durable_context, item, DistributedMapItemContext)`` and
    returns the item's output or raises. Each item runs through ``context.map``,
    so ``durable_context`` is the item's own context. An item whose ``func``
    returns ``None`` succeeds with no output. Items in a batch are processed
    one at a time. Raise ``concurrency`` to run that many items at once.

    Args:
        context: The handler's ``DurableContext``.
        event: The event the Lambda was invoked with.
        func: Called once per item.
        item_serdes: Deserializes each item. Defaults to JSON.
        result_serdes: Serializes each item's output. Defaults to JSON.
        concurrency: Maximum number of items to process concurrently.
        nesting_type: How child operations should inherit context from their
            parent.
            - NESTED: Each item runs in its own isolated context
            - FLAT: All items share the same parent context (default)

    Example:
        @durable_execution
        def handler(event, context):
            return process_distributed_map_items(
                context,
                event,
                lambda ctx, item, item_ctx: ctx.step(
                    lambda _: item["id"], name="read-id"
                ),
                nesting_type=NestingType.NESTED,
            )
    """


def process_distributed_map_items(*args: Any, **kwargs: Any) -> dict[str, Any]:
    # The two forms are told apart by how many positional arguments the call has.
    if len(args) == 2:
        _reject_unknown_options(kwargs, _ITEM_OPTIONS)
        return _process_items_non_durable(
            args[0],
            args[1],
            kwargs.get("item_serdes"),
            kwargs.get("result_serdes"),
            kwargs.get("concurrency", 1),
        )
    if len(args) == 3:
        _reject_unknown_options(kwargs, (*_ITEM_OPTIONS, "nesting_type"))
        return _process_items_durable(
            args[0],
            args[1],
            args[2],
            kwargs.get("item_serdes"),
            kwargs.get("result_serdes"),
            kwargs.get("concurrency", 1),
            kwargs.get("nesting_type", NestingType.FLAT),
        )
    msg = (
        "process_distributed_map_items expects (event, func) for a "
        "non-durable processor or (context, event, func) for a durable "
        f"processor, got {len(args)} positional arguments"
    )
    raise ValidationError(msg)


def process_distributed_map_batch(
    event: MutableMapping[str, Any],
    func: Callable[[list[Any], DistributedMapBatchContext], None],
    *,
    item_serdes: SerDes[Any] | None = None,
) -> None:
    """Process a whole distributed map batch with one call to ``func``.

    Call this inside the handler of a batch processor Lambda, durable or
    non-durable. Pass that Lambda's name to
    ``DistributedMapProcessor.report_batch_outcome`` in the parent's
    ``ctx.distributed_map`` call.

    The service reads the batch outcome from the handler's return value and from
    any error the handler raises, so the handler must return the result of this
    function. The result is ``None``.

    ``func`` receives ``(items, DistributedMapBatchContext)``, where the
    context's ``item_ids`` are index-aligned with the items. Returning succeeds
    every item. Raising fails every item. This function does not pass a durable
    context to ``func``. In a durable handler, use the handler's own
    ``DurableContext`` inside ``func``.

    In Python, an exception raised by ``func`` propagates out of this function
    and fails the invocation, whether or not the handler returns the result.

    A batch processor reports no per-item outcome, so it records no output. Use
    ``process_distributed_map_items`` to report an output per item.

    Args:
        event: The event the Lambda was invoked with.
        func: Called once with every item of the batch.
        item_serdes: Deserializes each item. Defaults to JSON.

    Example:
        def handler(event, context):
            return process_distributed_map_batch(
                event, lambda items, batch_ctx: save_all(items)
            )
    """
    serdes = item_serdes or DEFAULT_JSON_SERDES
    processor_event = DistributedMapProcessorEvent.from_dict(event)
    run_arn = processor_event.distributed_map_run_arn
    func(
        [
            _to_item(
                serdes, record.body, run_arn.serdes_context("body", record.item_id)
            )
            for record in processor_event.records
        ],
        _batch_context(
            run_arn,
            tuple(record.item_id for record in processor_event.records),
        ),
    )


def read_distributed_map_page(
    event: MutableMapping[str, Any],
    func: Callable[[Any, int, DistributedMapReaderContext], ReaderPage],
    *,
    item_serdes: SerDes[Any] | None = None,
    state_serdes: SerDes[Any] | None = None,
) -> dict[str, Any]:
    """Read one page of a distributed map source and return it in service format.

    Call this inside the handler of a reader Lambda. Pass that Lambda's name to
    ``DistributedMapSource.reader_function`` in the parent's ``ctx.distributed_map`` call.

    The handler must return the result of this function. The service reads the
    page from the handler's return value.

    ``func`` receives ``(state, max_items_per_page, DistributedMapReaderContext)``
    and returns a ``ReaderPage``. A ``next_state`` of ``None`` signals the source
    is exhausted.

    ``item_serdes`` serializes each item of the page. It must be the same serdes
    the processor is configured with, because the processor deserializes every
    item body with its own ``item_serdes``. Neither it nor ``state_serdes`` may
    depend on getting back the context it wrote under.

    Args:
        event: The event the Lambda was invoked with.
        func: Called once per page.
        item_serdes: Serializes each item of the page. Defaults to JSON.
        state_serdes: Serializes and deserializes the paging state. Defaults to
            JSON.

    Example:
        def handler(event, context):
            return read_distributed_map_page(
                event,
                lambda state, max_items, reader_ctx: ReaderPage(
                    items=fetch(state, max_items), next_state=None
                ),
            )
    """
    item_sd = item_serdes or DEFAULT_JSON_SERDES
    state_sd = state_serdes or DEFAULT_JSON_SERDES
    reader_event = DistributedMapReaderEvent.from_dict(event)
    run_arn = reader_event.distributed_map_run_arn
    state = (
        state_sd.deserialize(
            reader_event.state,
            run_arn.serdes_context("state", reader_event.state),
        )
        if reader_event.state is not None
        else None
    )

    page = func(state, reader_event.max_items_per_page, _reader_context(run_arn))
    if len(page.items) > reader_event.max_items_per_page:
        msg = (
            f"reader returned {len(page.items)} items, exceeding "
            f"maxItemsPerPage {reader_event.max_items_per_page}"
        )
        raise ValidationError(msg)

    items = [
        item_sd.serialize(
            item, run_arn.serdes_context("item", reader_event.state, index)
        )
        for index, item in enumerate(page.items)
    ]

    next_state: str | None = None
    if page.next_state is not None:
        next_state = state_sd.serialize(
            page.next_state,
            run_arn.serdes_context("next-state", reader_event.state),
        )
        if len(next_state.encode("utf-8")) > READER_STATE_LIMIT_BYTES:
            msg = f"reader next_state exceeds the {READER_STATE_LIMIT_BYTES // 1024} KB limit"
            raise ValidationError(msg)
        # next_state becomes the next call's input state, so returning the
        # input state repeats the same position instead of advancing. The
        # service rejects this as a source failure, empty page included.
        # This check and the service compare the serialized strings. A
        # serdes that returns a pointer, such as FileSystemSerDes, writes
        # each state under its own key. So a repeated state gets a new
        # pointer, and neither this check nor the service detects it.
        if next_state == reader_event.state:
            msg = (
                "reader next_state must differ from the state it was called "
                "with, or the next page repeats the same position"
            )
            raise ValidationError(msg)

    return dict(ReaderResponse(items=items, next_state=next_state).to_dict())
