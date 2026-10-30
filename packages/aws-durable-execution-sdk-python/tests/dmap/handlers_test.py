"""Unit tests for the non-durable distributed map authoring wrappers."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import pytest

from aws_durable_execution_sdk_python.dmap.handlers import (
    ItemFailure,
    ReaderPage,
    distributed_map_batch_handler,
    distributed_map_item_handler,
    durable_distributed_map_item_handler,
    distributed_map_reader,
)
from aws_durable_execution_sdk_python.exceptions import SerDesError, ValidationError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class _PrefixSerDes(SerDes[Any]):
    """A serdes whose prefix shows which of a wrapper's serdes options ran."""

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix

    def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
        return self.prefix + json.dumps(value)

    def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
        return json.loads(data.removeprefix(self.prefix))


def test_item_handler_rejects_concurrency_below_one():
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        distributed_map_item_handler(lambda x: x, concurrency=0)
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        distributed_map_item_handler(lambda x: x, concurrency=-1)


def test_durable_item_handler_rejects_concurrency_below_one():
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        durable_distributed_map_item_handler(lambda _ctx, item: item, concurrency=0)
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        durable_distributed_map_item_handler(lambda _ctx, item: item, concurrency=-1)


def test_item_handler_runs_items_one_at_a_time_by_default():
    lock = threading.Lock()
    in_flight = 0
    peak = 0

    def process(x):
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        # Hold the worker so that a wider pool would overlap this item with the
        # next one, which is what makes the peak below meaningful.
        time.sleep(0.02)
        with lock:
            in_flight -= 1
        return x

    handler = distributed_map_item_handler(process)
    handler({"records": [{"itemId": str(i), "body": "1"} for i in range(4)]}, None)
    assert peak == 1


def test_item_handler_honors_requested_concurrency():
    # Each item blocks until all four have arrived, so the batch only finishes
    # if the pool really runs four items at once. At a smaller pool size the
    # first item waits out the timeout and every item fails.
    barrier = threading.Barrier(4, timeout=5)

    def process(x):
        barrier.wait()
        return x

    handler = distributed_map_item_handler(process, concurrency=4)
    resp = handler(
        {"records": [{"itemId": str(i), "body": str(i)} for i in range(4)]}, None
    )
    assert resp["batchItemFailures"] == []
    # The four items are released together, so completion order is arbitrary
    # while the reported results stay in item order.
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "0"},
        {"itemIdentifier": "1", "output": "1"},
        {"itemIdentifier": "2", "output": "2"},
        {"itemIdentifier": "3", "output": "3"},
    ]


def test_item_handler_reports_results_in_order():
    handler = distributed_map_item_handler(lambda x: x * 2)
    resp = handler(
        {"records": [{"itemId": "0", "body": "2"}, {"itemId": "1", "body": "3"}]}, None
    )
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "4"},
        {"itemIdentifier": "1", "output": "6"},
    ]
    assert resp["batchItemFailures"] == []


def test_item_handler_captures_failures():
    def process(x):
        if x == "bad":
            msg = "boom"
            raise ValueError(msg)
        return x

    handler = distributed_map_item_handler(process)
    resp = handler(
        {
            "records": [
                {"itemId": "0", "body": '"ok"'},
                {"itemId": "1", "body": '"bad"'},
            ]
        },
        None,
    )
    assert resp["batchItemResults"] == [{"itemIdentifier": "0", "output": '"ok"'}]
    assert resp["batchItemFailures"] == [
        {
            "itemIdentifier": "1",
            "error": {"errorType": "ValueError", "errorMessage": "boom"},
        }
    ]


def test_item_handler_reports_every_member_of_a_rich_error():
    """An error carrying data and a stack trace reaches the service with all four members."""

    def process(_x: Any) -> Any:
        raise SerDesError(
            message="bad payload",
            error_type="ignored, SerDesError pins its own type",
            data="payload",
            stack_trace=["frame one", "frame two"],
        )

    handler = distributed_map_item_handler(process)
    resp = handler({"records": [{"itemId": "0", "body": "1"}]}, None)

    error = resp["batchItemFailures"][0]["error"]
    assert error["errorMessage"] == "bad payload"
    assert error["errorData"] == "payload"
    assert error["stackTrace"] == ["frame one", "frame two"]


def test_item_handler_omits_error_members_the_exception_lacks():
    """A plain exception carries no data or trace, so neither key is written."""

    def process(_x: Any) -> Any:
        msg = "boom"
        raise ValueError(msg)

    handler = distributed_map_item_handler(process)
    resp = handler({"records": [{"itemId": "0", "body": "1"}]}, None)

    assert resp["batchItemFailures"][0]["error"] == {
        "errorType": "ValueError",
        "errorMessage": "boom",
    }


def test_item_failure_wire_round_trip_keeps_every_member():
    failure = ItemFailure(
        item_identifier="7",
        error=ErrorObject(
            message="boom", type="E", data="payload", stack_trace=["frame"]
        ),
    )
    parsed = ItemFailure.from_dict(failure.to_dict())
    assert parsed == failure


def test_item_handler_omits_the_output_of_a_handler_returning_none():
    handler = distributed_map_item_handler(lambda _x: None)
    resp = handler({"records": [{"itemId": "0", "body": "1"}]}, None)
    assert resp == {
        "batchItemFailures": [],
        "batchItemResults": [{"itemIdentifier": "0"}],
    }


def test_item_handler_fails_only_the_item_whose_output_cannot_be_serialized():
    """A result serdes failure is that item's failure, not the batch's."""

    class _RejectsBad(SerDes[Any]):
        def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
            if value == "bad":
                msg = "cannot serialize"
                raise TypeError(msg)
            return json.dumps(value)

        def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
            return json.loads(data)

    handler = distributed_map_item_handler(lambda x: x, result_serdes=_RejectsBad())
    resp = handler(
        {
            "records": [
                {"itemId": "0", "body": '"ok"'},
                {"itemId": "1", "body": '"bad"'},
            ]
        },
        None,
    )
    assert resp["batchItemResults"] == [{"itemIdentifier": "0", "output": '"ok"'}]
    assert resp["batchItemFailures"] == [
        {
            "itemIdentifier": "1",
            "error": {"errorType": "TypeError", "errorMessage": "cannot serialize"},
        }
    ]


def test_batch_handler_success_and_propagates_error():
    seen: list = []

    def process(items):
        seen.extend(items)
        return "ignored"

    handler = distributed_map_batch_handler(process)
    # A batch processor reports no output, so the return value is not forwarded.
    assert (
        handler(
            {"records": [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}]},
            None,
        )
        is None
    )
    assert seen == [1, 2]

    def boom(_items):
        msg = "batch failed"
        raise RuntimeError(msg)

    failing = distributed_map_batch_handler(boom)
    with pytest.raises(RuntimeError, match="batch failed"):
        failing({"records": [{"itemId": "0", "body": "1"}]})


def test_reader_returns_items_and_next_state_then_exhausts():
    def read(state, max_items_per_page):
        if state is None:
            return ReaderPage(items=[1, 2][:max_items_per_page], next_state={"page": 1})
        return ReaderPage(items=[3])

    handler = distributed_map_reader(read)
    first = handler({"state": None, "maxItemsPerPage": 10}, None)
    assert first["items"] == ["1", "2"]
    assert first["nextState"] == '{"page": 1}'

    second = handler({"state": '{"page": 1}', "maxItemsPerPage": 10}, None)
    assert second["items"] == ["3"]
    assert "nextState" not in second


def test_reader_serializes_items_with_the_item_serdes():
    """An item serdes encodes each item, so the processor's can decode it."""

    class _UpperSerDes(SerDes[Any]):
        def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
            return json.dumps(value.upper())

        def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
            return json.loads(data).lower()

    serdes = _UpperSerDes()
    reader = distributed_map_reader(
        lambda _s, _max: ReaderPage(items=["a", "b"]), item_serdes=serdes
    )
    page = reader({"state": None, "maxItemsPerPage": 10}, None)
    assert page["items"] == ['"A"', '"B"']

    seen = []
    processor = distributed_map_batch_handler(seen.extend, item_serdes=serdes)
    processor(
        {
            "records": [
                {"itemId": str(i), "body": body} for i, body in enumerate(page["items"])
            ]
        },
        None,
    )
    assert seen == ["a", "b"]


def test_reader_item_serdes_is_separate_from_the_state_serdes():
    """Items use the item serdes and next_state uses the state serdes."""
    handler = distributed_map_reader(
        lambda state, _max: ReaderPage(items=[state], next_state="s"),
        item_serdes=_PrefixSerDes("item:"),
        state_serdes=_PrefixSerDes("state:"),
    )
    page = handler({"state": 'state:"in"', "maxItemsPerPage": 10}, None)
    assert page["items"] == ['item:"in"']
    assert page["nextState"] == 'state:"s"'


def test_reader_receives_the_page_bound():
    """The page bound reaches func, so a reader can page to it."""
    handler = distributed_map_reader(
        lambda _s, max_items_per_page: ReaderPage(items=list(range(max_items_per_page)))
    )
    assert handler({"state": None, "maxItemsPerPage": 3}, None)["items"] == [
        "0",
        "1",
        "2",
    ]


def test_reader_rejects_page_over_max_items_per_page():
    handler = distributed_map_reader(lambda _s, _max: ReaderPage(items=[1, 2, 3]))
    with pytest.raises(ValidationError, match="exceeding maxItemsPerPage"):
        handler({"state": None, "maxItemsPerPage": 2}, None)


def test_reader_rejects_oversized_next_state():
    handler = distributed_map_reader(
        lambda _s, _max: ReaderPage(items=[1], next_state="x" * 40_000)
    )
    with pytest.raises(ValidationError, match="32 KB limit"):
        handler({"state": None, "maxItemsPerPage": 10}, None)


@pytest.mark.parametrize("items", [[1], []], ids=["with_items", "empty_page"])
def test_reader_rejects_an_unchanged_next_state(items):
    """The service fails the run over this, so the wrapper rejects it at the source."""
    handler = distributed_map_reader(
        lambda s, _max: ReaderPage(items=items, next_state=s)
    )
    with pytest.raises(ValidationError, match="must differ from the state"):
        handler({"state": '{"page": 1}', "maxItemsPerPage": 10}, None)


def test_reader_allows_an_advanced_next_state():
    handler = distributed_map_reader(
        lambda _s, _max: ReaderPage(items=[], next_state={"page": 2})
    )
    page = handler({"state": '{"page": 1}', "maxItemsPerPage": 10}, None)
    assert page == {"items": [], "nextState": '{"page": 2}'}


def test_item_handler_rejects_non_processor_envelope():
    handler = distributed_map_item_handler(lambda x: x)
    with pytest.raises(ValidationError, match="processor envelope"):
        handler({}, None)


@pytest.mark.parametrize(
    "record",
    [
        {"body": "1"},
        {"itemId": None, "body": "1"},
        {"itemId": "", "body": "1"},
        {"itemId": 7, "body": "1"},
    ],
    ids=["absent", "null", "empty", "not-a-string"],
)
def test_item_handler_rejects_a_record_with_no_item_id(record):
    """The wrapper reports each outcome against its item id, so it must have one."""
    handler = distributed_map_item_handler(lambda x: x)
    with pytest.raises(ValidationError, match="string 'itemId'"):
        handler({"records": [record]}, None)


def test_batch_handler_rejects_a_record_with_no_item_id():
    """The batch wrapper validates its invocation input the same way."""
    handler = distributed_map_batch_handler(lambda items: items)
    with pytest.raises(ValidationError, match="string 'itemId'"):
        handler({"records": [{"body": "1"}]}, None)


def test_item_handler_rejects_a_batch_when_only_one_record_lacks_an_item_id():
    """One unusable record fails the invocation rather than the batch running partly."""
    calls = []
    handler = distributed_map_item_handler(calls.append)
    with pytest.raises(ValidationError, match="string 'itemId'"):
        handler({"records": [{"itemId": "0", "body": "1"}, {"body": "2"}]}, None)
    assert calls == []


def test_reader_rejects_non_reader_envelope():
    handler = distributed_map_reader(lambda _state, _max: ReaderPage(items=[]))
    with pytest.raises(ValidationError, match="reader envelope"):
        handler({"state": None}, None)


# The decorator-with-arguments form (@wrapper(...)), which returns a partial
# before it sees the function. Each option below changes an observable result,
# so a partial that dropped an option would fail rather than pass.
# ============================================================================


def test_item_handler_applies_options_given_to_the_decorator_form():
    # Both items block until the other arrives, so the batch only finishes if
    # concurrency really reached the pool.
    barrier = threading.Barrier(2, timeout=5)

    @distributed_map_item_handler(result_serdes=_PrefixSerDes("out:"), concurrency=2)
    def handler(item):
        barrier.wait()
        return item

    resp = handler(
        {"records": [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}]}, None
    )
    assert resp["batchItemFailures"] == []
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "out:1"},
        {"itemIdentifier": "1", "output": "out:2"},
    ]


def test_batch_handler_applies_options_given_to_the_decorator_form():
    seen: list = []

    @distributed_map_batch_handler(item_serdes=_PrefixSerDes("in:"))
    def handler(items):
        seen.extend(items)

    assert handler({"records": [{"itemId": "0", "body": "in:1"}]}, None) is None
    assert seen == [1]


def test_reader_applies_options_given_to_the_decorator_form():
    @distributed_map_reader(
        item_serdes=_PrefixSerDes("item:"), state_serdes=_PrefixSerDes("state:")
    )
    def handler(state, _max_items_per_page):
        return ReaderPage(items=[state], next_state="s")

    page = handler({"state": 'state:"in"', "maxItemsPerPage": 10}, None)
    assert page["items"] == ['item:"in"']
    assert page["nextState"] == 'state:"s"'
