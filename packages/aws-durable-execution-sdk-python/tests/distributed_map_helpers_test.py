"""Unit tests for the non-durable distributed map authoring wrappers."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.distributed_map_helpers import (
    ReaderPage,
    create_distributed_map_batch_handler,
    create_distributed_map_item_handler,
    create_distributed_map_item_handler_with_durable_execution,
    create_distributed_map_reader,
)
from aws_durable_execution_sdk_python.exceptions import ValidationError


def test_item_handler_invalid_report_rejected():
    with pytest.raises(ValidationError, match="report must be"):
        create_distributed_map_item_handler(lambda x: x, report="bogus")


def test_durable_item_handler_invalid_report_rejected():
    with pytest.raises(ValidationError, match="report must be"):
        create_distributed_map_item_handler_with_durable_execution(
            lambda _ctx, item: item, report="bogus"
        )


def test_item_handler_reports_results_in_order():
    handler = create_distributed_map_item_handler(lambda x: x * 2)
    resp = handler(
        {"records": [{"itemId": "0", "body": 2}, {"itemId": "1", "body": 3}]}
    )
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": 4},
        {"itemIdentifier": "1", "output": 6},
    ]
    assert resp["batchItemFailures"] == []


def test_item_handler_captures_failures():
    def process(x):
        if x == "bad":
            msg = "boom"
            raise ValueError(msg)
        return x

    handler = create_distributed_map_item_handler(process)
    resp = handler(
        {"records": [{"itemId": "0", "body": "ok"}, {"itemId": "1", "body": "bad"}]}
    )
    assert resp["batchItemResults"] == [{"itemIdentifier": "0", "output": "ok"}]
    assert resp["batchItemFailures"] == [
        {
            "itemIdentifier": "1",
            "error": {"errorType": "ValueError", "errorMessage": "boom"},
        }
    ]


def test_item_handler_failures_form_reports_only_failures():
    handler = create_distributed_map_item_handler(lambda x: x, report="failures")
    resp = handler({"records": [{"itemId": "0", "body": 1}]})
    assert resp == {"batchItemFailures": []}


def test_batch_handler_success_and_propagates_error():
    seen: list = []
    handler = create_distributed_map_batch_handler(
        lambda items: seen.extend(items) or "done"
    )
    assert (
        handler({"records": [{"itemId": "0", "body": 1}, {"itemId": "1", "body": 2}]})
        == "done"
    )
    assert seen == [1, 2]

    def boom(_items):
        msg = "batch failed"
        raise RuntimeError(msg)

    failing = create_distributed_map_batch_handler(boom)
    with pytest.raises(RuntimeError, match="batch failed"):
        failing({"records": [{"itemId": "0", "body": 1}]})


def test_reader_returns_items_and_next_state_then_exhausts():
    def read(state):
        if state is None:
            return ReaderPage(items=[1, 2], next_state={"page": 1})
        return ReaderPage(items=[3])

    handler = create_distributed_map_reader(read)
    first = handler({"state": None, "maxItems": 10})
    assert first["items"] == [1, 2]
    assert first["nextState"] == '{"page": 1}'

    second = handler({"state": '{"page": 1}', "maxItems": 10})
    assert second["items"] == [3]
    assert "nextState" not in second


def test_reader_rejects_page_over_max_items():
    handler = create_distributed_map_reader(lambda _s: ReaderPage(items=[1, 2, 3]))
    with pytest.raises(ValueError, match="exceeding maxItems"):
        handler({"state": None, "maxItems": 2})


def test_reader_rejects_oversized_next_state():
    handler = create_distributed_map_reader(
        lambda _s: ReaderPage(items=[1], next_state="x" * 40_000)
    )
    with pytest.raises(ValueError, match="32 KB limit"):
        handler({"state": None, "maxItems": 10})
