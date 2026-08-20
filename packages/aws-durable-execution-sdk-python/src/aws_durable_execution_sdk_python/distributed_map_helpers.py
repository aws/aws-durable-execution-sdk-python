"""Authoring helpers for distributed map processor and reader Lambda functions.

Wrappers that own the request/response format so customers can write a plain
function to process items/batches or read pages. Imported explicitly from this
module, not the main package.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from aws_durable_execution_sdk_python.concurrency.models import BatchItemStatus
from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.exceptions import ValidationError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import (
    DEFAULT_JSON_SERDES,
    SerDes,
    SerDesContext,
)

_CTX = SerDesContext()
_READER_STATE_LIMIT = 32 * 1024


@dataclass(frozen=True)
class ReaderPage:
    """A page returned by a reader function: its items and the next state."""

    items: list[Any] = field(default_factory=list)
    next_state: Any | None = None


def _to_item(serdes: SerDes[Any], body: Any) -> Any:
    """Recover a typed item from a record body (a JSON value)."""
    return serdes.deserialize(json.dumps(body), _CTX)


def _to_json_value(serdes: SerDes[Any], value: Any) -> Any:
    """Serialize a value and return it as a JSON value for the wire."""
    return json.loads(serdes.serialize(value, _CTX))


def _error_entry(item_id: str, exc: BaseException) -> dict[str, Any]:
    return {
        "itemIdentifier": item_id,
        "error": {"errorType": type(exc).__name__, "errorMessage": str(exc)},
    }


def _validate_report(report: str) -> None:
    if report not in ("results", "failures"):
        msg = f"report must be 'results' or 'failures', got: {report!r}"
        raise ValidationError(msg)


def create_distributed_map_item_handler(
    func: Callable[[Any], Any],
    *,
    item_serdes: SerDes[Any] | None = None,
    concurrency: int | None = None,
    report: Literal["results", "failures"] = "results",
) -> Callable[..., dict[str, Any]]:
    """Wrap a Lambda to be used as a report_item_results processor.

    Pass ``report="failures"`` for a report_failed_items processor. ``func``
    takes one item and returns its output or raises.
    """
    _validate_report(report)
    serdes = item_serdes or DEFAULT_JSON_SERDES

    def handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
        records: list[dict[str, Any]] = event["records"]

        def run(record: dict[str, Any]) -> Any:
            return func(_to_item(serdes, record["body"]))

        outputs: list[Any] = [None] * len(records)
        errors: list[BaseException | None] = [None] * len(records)
        with ThreadPoolExecutor(max_workers=concurrency or len(records) or 1) as pool:
            futures = {pool.submit(run, r): i for i, r in enumerate(records)}
            for future, i in futures.items():
                try:
                    outputs[i] = future.result()
                except Exception as exc:  # noqa: BLE001
                    errors[i] = exc

        results: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for i, record in enumerate(records):
            item_id = record["itemId"]
            err = errors[i]
            if err is not None:
                failures.append(_error_entry(item_id, err))
            elif report == "results":
                results.append(
                    {
                        "itemIdentifier": item_id,
                        "output": _to_json_value(serdes, outputs[i]),
                    }
                )

        if report == "failures":
            return {"batchItemFailures": failures}
        return {"batchItemResults": results, "batchItemFailures": failures}

    return handler


def create_distributed_map_batch_handler(
    func: Callable[[list[Any]], Any],
    *,
    item_serdes: SerDes[Any] | None = None,
) -> Callable[..., Any]:
    """Wrap a Lambda to be used as a report_batch_outcome processor.

    ``func`` takes the whole batch of items. Returning succeeds every item;
    raising fails every item.
    """
    serdes = item_serdes or DEFAULT_JSON_SERDES

    def handler(event: dict[str, Any], _context: Any = None) -> Any:
        items = [_to_item(serdes, record["body"]) for record in event["records"]]
        return func(items)

    return handler


def create_distributed_map_reader(
    func: Callable[[Any], ReaderPage],
    *,
    state_serdes: SerDes[Any] | None = None,
) -> Callable[..., dict[str, Any]]:
    """Wrap a Lambda to be used as a reader source.

    ``func`` takes the current state and returns a ReaderPage. A ``next_state``
    of ``None`` signals the source is exhausted.
    """
    serdes = state_serdes or DEFAULT_JSON_SERDES

    def handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
        raw_state = event.get("state")
        state = serdes.deserialize(raw_state, _CTX) if raw_state is not None else None
        max_items: int = event["maxItems"]

        page = func(state)
        if len(page.items) > max_items:
            msg = f"reader returned {len(page.items)} items, exceeding maxItems {max_items}"
            raise ValueError(msg)

        response: dict[str, Any] = {"items": list(page.items)}
        if page.next_state is not None:
            next_state = serdes.serialize(page.next_state, _CTX)
            if len(next_state.encode("utf-8")) > _READER_STATE_LIMIT:
                msg = f"reader next_state exceeds the {_READER_STATE_LIMIT // 1024} KB limit"
                raise ValueError(msg)
            response["nextState"] = next_state
        return response

    return handler


def _item_response(
    records: list[dict[str, Any]],
    outputs: dict[int, Any],
    failures: list[dict[str, Any]],
    serdes: SerDes[Any],
    report: str,
) -> dict[str, Any]:
    """Build the item-handler response from per-index outputs and failures."""
    if report == "failures":
        return {"batchItemFailures": failures}
    results = [
        {"itemIdentifier": records[i]["itemId"], "output": _to_json_value(serdes, out)}
        for i, out in outputs.items()
    ]
    return {"batchItemResults": results, "batchItemFailures": failures}


def create_distributed_map_item_handler_with_durable_execution(
    func: Callable[..., Any],
    *,
    item_serdes: SerDes[Any] | None = None,
    report: Literal["results", "failures"] = "results",
) -> Callable[..., dict[str, Any]]:
    """Durable variant of the item handler; ``func`` receives (context, item)."""
    _validate_report(report)
    serdes = item_serdes or DEFAULT_JSON_SERDES

    @durable_execution
    def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
        records: list[dict[str, Any]] = event["records"]

        def per_item(ctx: Any, body: Any, _index: int, _inputs: Any) -> Any:
            return func(ctx, _to_item(serdes, body))

        batch = context.map(
            [record["body"] for record in records],
            per_item,
            config=MapConfig(completion_config=CompletionConfig.all_completed()),
        )

        outputs: dict[int, Any] = {}
        failures: list[dict[str, Any]] = []
        for bi in batch.all:
            item_id = records[bi.index]["itemId"]
            if bi.status is BatchItemStatus.SUCCEEDED:
                outputs[bi.index] = bi.result
            else:
                err = bi.error
                failures.append(
                    {
                        "itemIdentifier": item_id,
                        "error": {
                            "errorType": (err.type or "") if err else "",
                            "errorMessage": (err.message or "") if err else "",
                        },
                    }
                )
        return _item_response(records, outputs, failures, serdes, report)

    return handler


def create_distributed_map_batch_handler_with_durable_execution(
    func: Callable[..., Any],
    *,
    item_serdes: SerDes[Any] | None = None,
) -> Callable[..., Any]:
    """Durable variant of the batch handler; ``func`` receives (context, items)."""
    serdes = item_serdes or DEFAULT_JSON_SERDES

    @durable_execution
    def handler(event: dict[str, Any], context: Any) -> Any:
        items = [_to_item(serdes, record["body"]) for record in event["records"]]
        return func(context, items)

    return handler
