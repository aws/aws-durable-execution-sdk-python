"""Unit tests for the distributed map authoring wrappers."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

import pytest

from aws_durable_execution_sdk_python import (
    DistributedMapBatchContext,
    DistributedMapItemContext,
    DistributedMapReaderContext,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dmap.handlers import (
    ItemFailure,
    ReaderPage,
    distributed_map_batch_handler,
    distributed_map_item_handler,
    durable_distributed_map_batch_handler,
    durable_distributed_map_item_handler,
    distributed_map_reader,
)
from aws_durable_execution_sdk_python.exceptions import SerDesError, ValidationError
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.filesystem_serdes import FileSystemSerDes
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.logger import Logger
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

_RUN_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/exec1/invoke1/distributed-map-run/abc"
)


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
        distributed_map_item_handler(lambda x, _ctx: x, concurrency=0)
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        distributed_map_item_handler(lambda x, _ctx: x, concurrency=-1)


def test_durable_item_handler_rejects_concurrency_below_one():
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        durable_distributed_map_item_handler(
            lambda _context, item, _ctx: item, concurrency=0
        )
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        durable_distributed_map_item_handler(
            lambda _context, item, _ctx: item, concurrency=-1
        )


def test_item_handler_runs_items_one_at_a_time_by_default():
    lock = threading.Lock()
    in_flight = 0
    peak = 0

    def process(x, _ctx):
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
    handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [{"itemId": str(i), "body": "1"} for i in range(4)],
        },
        None,
    )
    assert peak == 1


def test_item_handler_honors_requested_concurrency():
    # Each item blocks until all four have arrived, so the batch only finishes
    # if the pool really runs four items at once. At a smaller pool size the
    # first item waits out the timeout and every item fails.
    barrier = threading.Barrier(4, timeout=5)

    def process(x, _ctx):
        barrier.wait()
        return x

    handler = distributed_map_item_handler(process, concurrency=4)
    resp = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [{"itemId": str(i), "body": str(i)} for i in range(4)],
        },
        None,
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
    handler = distributed_map_item_handler(lambda x, _ctx: x * 2)
    resp = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [{"itemId": "0", "body": "2"}, {"itemId": "1", "body": "3"}],
        },
        None,
    )
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "4"},
        {"itemIdentifier": "1", "output": "6"},
    ]
    assert resp["batchItemFailures"] == []


def test_item_handler_captures_failures():
    def process(x, _ctx):
        if x == "bad":
            msg = "boom"
            raise ValueError(msg)
        return x

    handler = distributed_map_item_handler(process)
    resp = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [
                {"itemId": "0", "body": '"ok"'},
                {"itemId": "1", "body": '"bad"'},
            ],
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

    def process(_x: Any, _ctx: Any) -> Any:
        raise SerDesError(
            message="bad payload",
            error_type="ignored, SerDesError pins its own type",
            data="payload",
            stack_trace=["frame one", "frame two"],
        )

    handler = distributed_map_item_handler(process)
    resp = handler(
        {"distributedMapRunArn": _RUN_ARN, "records": [{"itemId": "0", "body": "1"}]},
        None,
    )

    error = resp["batchItemFailures"][0]["error"]
    assert error["errorMessage"] == "bad payload"
    assert error["errorData"] == "payload"
    assert error["stackTrace"] == ["frame one", "frame two"]


def test_item_handler_omits_error_members_the_exception_lacks():
    """A plain exception carries no data or trace, so neither key is written."""

    def process(_x: Any, _ctx: Any) -> Any:
        msg = "boom"
        raise ValueError(msg)

    handler = distributed_map_item_handler(process)
    resp = handler(
        {"distributedMapRunArn": _RUN_ARN, "records": [{"itemId": "0", "body": "1"}]},
        None,
    )

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
    handler = distributed_map_item_handler(lambda _x, _ctx: None)
    resp = handler(
        {"distributedMapRunArn": _RUN_ARN, "records": [{"itemId": "0", "body": "1"}]},
        None,
    )
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

    handler = distributed_map_item_handler(
        lambda x, _ctx: x, result_serdes=_RejectsBad()
    )
    resp = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [
                {"itemId": "0", "body": '"ok"'},
                {"itemId": "1", "body": '"bad"'},
            ],
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

    def process(items, _ctx):
        seen.extend(items)
        return "ignored"

    handler = distributed_map_batch_handler(process)
    # A batch processor reports no output, so the return value is not forwarded.
    assert (
        handler(
            {
                "distributedMapRunArn": _RUN_ARN,
                "records": [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}],
            },
            None,
        )
        is None
    )
    assert seen == [1, 2]

    def boom(_items, _ctx):
        msg = "batch failed"
        raise RuntimeError(msg)

    failing = distributed_map_batch_handler(boom)
    with pytest.raises(RuntimeError, match="batch failed"):
        failing(
            {
                "distributedMapRunArn": _RUN_ARN,
                "records": [{"itemId": "0", "body": "1"}],
            }
        )


def test_reader_returns_items_and_next_state_then_exhausts():
    def read(state, max_items_per_page, _ctx):
        if state is None:
            return ReaderPage(items=[1, 2][:max_items_per_page], next_state={"page": 1})
        return ReaderPage(items=[3])

    handler = distributed_map_reader(read)
    first = handler(
        {"distributedMapRunArn": _RUN_ARN, "state": None, "maxItemsPerPage": 10}, None
    )
    assert first["items"] == ["1", "2"]
    assert first["nextState"] == '{"page": 1}'

    second = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "state": '{"page": 1}',
            "maxItemsPerPage": 10,
        },
        None,
    )
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
        lambda _s, _max, _ctx: ReaderPage(items=["a", "b"]), item_serdes=serdes
    )
    page = reader(
        {"distributedMapRunArn": _RUN_ARN, "state": None, "maxItemsPerPage": 10}, None
    )
    assert page["items"] == ['"A"', '"B"']

    seen = []
    processor = distributed_map_batch_handler(
        lambda items, _ctx: seen.extend(items), item_serdes=serdes
    )
    processor(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [
                {"itemId": str(i), "body": body} for i, body in enumerate(page["items"])
            ],
        },
        None,
    )
    assert seen == ["a", "b"]


def test_reader_item_serdes_is_separate_from_the_state_serdes():
    """Items use the item serdes and next_state uses the state serdes."""
    handler = distributed_map_reader(
        lambda state, _max, _ctx: ReaderPage(items=[state], next_state="s"),
        item_serdes=_PrefixSerDes("item:"),
        state_serdes=_PrefixSerDes("state:"),
    )
    page = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "state": 'state:"in"',
            "maxItemsPerPage": 10,
        },
        None,
    )
    assert page["items"] == ['item:"in"']
    assert page["nextState"] == 'state:"s"'


def test_reader_receives_the_page_bound():
    """The page bound reaches func, so a reader can page to it."""
    handler = distributed_map_reader(
        lambda _s, max_items_per_page, _ctx: ReaderPage(
            items=list(range(max_items_per_page))
        )
    )
    assert handler(
        {"distributedMapRunArn": _RUN_ARN, "state": None, "maxItemsPerPage": 3}, None
    )["items"] == [
        "0",
        "1",
        "2",
    ]


def test_reader_rejects_page_over_max_items_per_page():
    handler = distributed_map_reader(lambda _s, _max, _ctx: ReaderPage(items=[1, 2, 3]))
    with pytest.raises(ValidationError, match="exceeding maxItemsPerPage"):
        handler(
            {"distributedMapRunArn": _RUN_ARN, "state": None, "maxItemsPerPage": 2},
            None,
        )


def test_reader_rejects_oversized_next_state():
    handler = distributed_map_reader(
        lambda _s, _max, _ctx: ReaderPage(items=[1], next_state="x" * 40_000)
    )
    with pytest.raises(ValidationError, match="32 KB limit"):
        handler(
            {"distributedMapRunArn": _RUN_ARN, "state": None, "maxItemsPerPage": 10},
            None,
        )


@pytest.mark.parametrize("items", [[1], []], ids=["with_items", "empty_page"])
def test_reader_rejects_an_unchanged_next_state(items):
    """The service fails the run over this, so the wrapper rejects it at the source."""
    handler = distributed_map_reader(
        lambda s, _max, _ctx: ReaderPage(items=items, next_state=s)
    )
    with pytest.raises(ValidationError, match="must differ from the state"):
        handler(
            {
                "distributedMapRunArn": _RUN_ARN,
                "state": '{"page": 1}',
                "maxItemsPerPage": 10,
            },
            None,
        )


def test_reader_allows_an_advanced_next_state():
    handler = distributed_map_reader(
        lambda _s, _max, _ctx: ReaderPage(items=[], next_state={"page": 2})
    )
    page = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "state": '{"page": 1}',
            "maxItemsPerPage": 10,
        },
        None,
    )
    assert page == {"items": [], "nextState": '{"page": 2}'}


def test_item_handler_rejects_non_processor_envelope():
    handler = distributed_map_item_handler(lambda x, _ctx: x)
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
    handler = distributed_map_item_handler(lambda x, _ctx: x)
    with pytest.raises(ValidationError, match="string 'itemId'"):
        handler({"distributedMapRunArn": _RUN_ARN, "records": [record]}, None)


def test_batch_handler_rejects_a_record_with_no_item_id():
    """The batch wrapper validates its invocation input the same way."""
    handler = distributed_map_batch_handler(lambda items, _ctx: items)
    with pytest.raises(ValidationError, match="string 'itemId'"):
        handler({"distributedMapRunArn": _RUN_ARN, "records": [{"body": "1"}]}, None)


def test_item_handler_rejects_a_batch_when_only_one_record_lacks_an_item_id():
    """One unusable record fails the invocation rather than the batch running partly."""
    calls = []
    handler = distributed_map_item_handler(lambda item, _ctx: calls.append(item))
    with pytest.raises(ValidationError, match="string 'itemId'"):
        handler(
            {
                "distributedMapRunArn": _RUN_ARN,
                "records": [{"itemId": "0", "body": "1"}, {"body": "2"}],
            },
            None,
        )
    assert calls == []


def test_reader_rejects_non_reader_envelope():
    handler = distributed_map_reader(lambda _state, _max, _ctx: ReaderPage(items=[]))
    with pytest.raises(ValidationError, match="reader envelope"):
        handler({"distributedMapRunArn": _RUN_ARN, "state": None}, None)


# The decorator-with-arguments form (@wrapper(...)), which returns a partial
# before it sees the function. Each option below changes an observable result,
# so a partial that dropped an option would fail rather than pass.
# ============================================================================


def test_item_handler_applies_options_given_to_the_decorator_form():
    # Both items block until the other arrives, so the batch only finishes if
    # concurrency really reached the pool.
    barrier = threading.Barrier(2, timeout=5)

    @distributed_map_item_handler(result_serdes=_PrefixSerDes("out:"), concurrency=2)
    def handler(item, _ctx):
        barrier.wait()
        return item

    resp = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "records": [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}],
        },
        None,
    )
    assert resp["batchItemFailures"] == []
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "out:1"},
        {"itemIdentifier": "1", "output": "out:2"},
    ]


def test_batch_handler_applies_options_given_to_the_decorator_form():
    seen: list = []

    @distributed_map_batch_handler(item_serdes=_PrefixSerDes("in:"))
    def handler(items, _ctx):
        seen.extend(items)

    assert (
        handler(
            {
                "distributedMapRunArn": _RUN_ARN,
                "records": [{"itemId": "0", "body": "in:1"}],
            },
            None,
        )
        is None
    )
    assert seen == [1]


def test_reader_applies_options_given_to_the_decorator_form():
    @distributed_map_reader(
        item_serdes=_PrefixSerDes("item:"), state_serdes=_PrefixSerDes("state:")
    )
    def handler(state, _max_items_per_page, _ctx):
        return ReaderPage(items=[state], next_state="s")

    page = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "state": 'state:"in"',
            "maxItemsPerPage": 10,
        },
        None,
    )
    assert page["items"] == ['item:"in"']
    assert page["nextState"] == 'state:"s"'


# Each value a wrapper serializes gets its own serdes context, so a serdes that
# stores values under their context, such as FileSystemSerDes, keeps them all.
# ============================================================================


class _RecordingSerDes(SerDes[Any]):
    """A JSON serdes that records every context it is called with."""

    def __init__(self) -> None:
        self.contexts: list[SerDesContext] = []
        self._lock = threading.Lock()

    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        with self._lock:
            self.contexts.append(serdes_context)
        return json.dumps(value)

    def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:
        with self._lock:
            self.contexts.append(serdes_context)
        return json.loads(data)


def _records(count: int) -> list[dict[str, Any]]:
    return [{"itemId": f"item-{i}", "body": json.dumps(i)} for i in range(count)]


def _read(fs: FileSystemSerDes, data: str) -> Any:
    return fs.deserialize(data, SerDesContext())


def test_item_handler_stores_each_output_under_its_own_key(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    handler = distributed_map_item_handler(lambda i, _ctx: f"out-{i}", result_serdes=fs)
    resp = handler({"distributedMapRunArn": _RUN_ARN, "records": _records(3)}, None)
    outputs = [r["output"] for r in resp["batchItemResults"]]
    assert len(set(outputs)) == 3
    assert [_read(fs, o) for o in outputs] == ["out-0", "out-1", "out-2"]


def test_item_handler_stores_concurrent_outputs_under_their_own_keys(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    handler = distributed_map_item_handler(
        lambda i, _ctx: f"out-{i}", result_serdes=fs, concurrency=4
    )
    resp = handler({"distributedMapRunArn": _RUN_ARN, "records": _records(8)}, None)
    outputs = [r["output"] for r in resp["batchItemResults"]]
    assert [_read(fs, o) for o in outputs] == [f"out-{i}" for i in range(8)]


def test_item_handler_keeps_the_outputs_of_another_run(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    first_run = distributed_map_item_handler(lambda _i, _ctx: "first", result_serdes=fs)
    second_run = distributed_map_item_handler(
        lambda _i, _ctx: "second", result_serdes=fs
    )
    other_run_arn = _RUN_ARN.replace("/abc", "/def")
    first = first_run({"distributedMapRunArn": _RUN_ARN, "records": _records(1)}, None)
    second_run({"distributedMapRunArn": other_run_arn, "records": _records(1)}, None)
    assert _read(fs, first["batchItemResults"][0]["output"]) == "first"


def test_item_handler_reuses_the_key_when_an_item_is_retried(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    handler = distributed_map_item_handler(lambda i, _ctx: f"out-{i}", result_serdes=fs)
    event = {"distributedMapRunArn": _RUN_ARN, "records": _records(2)}
    first = handler(event, None)
    assert handler(event, None) == first
    outputs = [r["output"] for r in first["batchItemResults"]]
    assert [_read(fs, o) for o in outputs] == ["out-0", "out-1"]


@pytest.mark.parametrize("batch", [False, True], ids=["item", "batch"])
def test_processor_reads_and_writes_each_value_under_its_own_context(batch):
    serdes = _RecordingSerDes()
    event = {"distributedMapRunArn": _RUN_ARN, "records": _records(2)}
    if batch:
        distributed_map_batch_handler(lambda _items, _ctx: None, item_serdes=serdes)(
            event, None
        )
    else:
        distributed_map_item_handler(
            lambda i, _ctx: i, item_serdes=serdes, result_serdes=serdes
        )(event, None)
    keys = {(c.durable_execution_arn, c.operation_id) for c in serdes.contexts}
    assert len(keys) == len(serdes.contexts) == (2 if batch else 4)
    assert {arn for arn, _ in keys} == {_RUN_ARN.split("/distributed-map-run/")[0]}


def test_reader_stores_items_and_next_state_under_their_own_keys(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))

    def read(state, _max, _ctx):
        page = 0 if state is None else state["page"]
        return ReaderPage(
            items=[f"r{page}-0", f"r{page}-1"], next_state={"page": page + 1}
        )

    handler = distributed_map_reader(read, item_serdes=fs, state_serdes=fs)
    first = handler({"distributedMapRunArn": _RUN_ARN, "maxItemsPerPage": 10}, None)
    assert len({*first["items"], first["nextState"]}) == 3
    assert [_read(fs, i) for i in first["items"]] == ["r0-0", "r0-1"]
    assert _read(fs, first["nextState"]) == {"page": 1}

    second = handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "state": first["nextState"],
            "maxItemsPerPage": 10,
        },
        None,
    )
    assert [_read(fs, i) for i in second["items"]] == ["r1-0", "r1-1"]
    assert _read(fs, second["nextState"]) == {"page": 2}
    # Page 2 leaves page 1's items and state in place.
    assert [_read(fs, i) for i in first["items"]] == ["r0-0", "r0-1"]
    assert _read(fs, first["nextState"]) == {"page": 1}


def test_reader_reads_and_writes_each_value_under_its_own_context():
    serdes = _RecordingSerDes()
    handler = distributed_map_reader(
        lambda _s, _max, _ctx: ReaderPage(items=[1, 2], next_state="next"),
        item_serdes=serdes,
        state_serdes=serdes,
    )
    handler(
        {"distributedMapRunArn": _RUN_ARN, "state": '"in"', "maxItemsPerPage": 10},
        None,
    )
    keys = {(c.durable_execution_arn, c.operation_id) for c in serdes.contexts}
    assert len(keys) == len(serdes.contexts) == 4


def test_reader_state_is_read_under_a_different_context_than_it_was_written():
    """next_state is written as one call's output and read as the next call's input.

    So the write context and the read context always differ. Pins the context
    requirement documented in the serdes module.
    """
    serdes = _RecordingSerDes()
    # The next state has to advance, or the wrapper rejects it for repeating
    # the position it was called with.
    handler = distributed_map_reader(
        lambda state, _max, _ctx: ReaderPage(
            items=[], next_state="page-2" if state else "page-1"
        ),
        state_serdes=serdes,
    )

    first = handler({"distributedMapRunArn": _RUN_ARN, "maxItemsPerPage": 10}, None)
    # No input state on the first call, so the only context is the write.
    assert len(serdes.contexts) == 1
    wrote = serdes.contexts[0]

    serdes.contexts.clear()
    handler(
        {
            "distributedMapRunArn": _RUN_ARN,
            "state": first["nextState"],
            "maxItemsPerPage": 10,
        },
        None,
    )
    # The input state is deserialized before anything else is written.
    read = serdes.contexts[0]

    assert read.durable_execution_arn == wrote.durable_execution_arn
    assert read.operation_id != wrote.operation_id


@pytest.mark.parametrize(
    "run_arn",
    [None, 7, "not-an-arn"],
    ids=["absent", "not-a-string", "malformed"],
)
@pytest.mark.parametrize(
    "envelope",
    [
        {"records": [{"itemId": "0", "body": "1"}]},
        {"state": None, "maxItemsPerPage": 10},
    ],
    ids=["processor", "reader"],
)
def test_wrappers_reject_an_envelope_without_a_valid_run_arn(run_arn, envelope):
    event = dict(envelope)
    if run_arn is not None:
        event["distributedMapRunArn"] = run_arn
    if "records" in envelope:
        handlers = [
            distributed_map_item_handler(lambda x, _ctx: x),
            distributed_map_batch_handler(lambda _items, _ctx: None),
        ]
    else:
        handlers = [distributed_map_reader(lambda _s, _max, _ctx: ReaderPage())]
    for handler in handlers:
        with pytest.raises(ValidationError, match="'distributedMapRunArn'"):
            handler(event, None)


# Each wrapper passes the customer function a context as its last argument. It
# carries the run ARN, and the item id or ids for a processor.
# ============================================================================

# The execution name and the invocation id are both "distributed-map-run", so
# splitting on the first separator instead of the last gives the wrong run id.
_RUN_ARN_WITH_SEPARATOR_NAMES = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/distributed-map-run/distributed-map-run"
    "/distributed-map-run/abc"
)

_ITEM_IDS = ["id-a", "id-b", "id-c"]


def _records_named_by_id(item_ids: list[str]) -> list[dict[str, Any]]:
    """Records whose body is the item's own id, so an item shows which id it has."""
    return [{"itemId": i, "body": json.dumps(i)} for i in item_ids]


def test_item_handler_passes_each_item_its_run_arn_and_id() -> None:
    seen: dict[str, DistributedMapItemContext] = {}

    def process(item: str, ctx: DistributedMapItemContext) -> None:
        seen[item] = ctx

    handler = distributed_map_item_handler(process)
    handler(
        {"distributedMapRunArn": _RUN_ARN, "records": _records_named_by_id(_ITEM_IDS)},
        None,
    )
    assert sorted(seen) == _ITEM_IDS
    for item, ctx in seen.items():
        assert ctx.item_id == item
        assert ctx.distributed_map_run_arn == _RUN_ARN


def test_item_handler_logger_stamps_the_run_arn_and_item_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A plain wrapper has no durable logger, so it logs through the module logger."""

    def process(_item: Any, ctx: DistributedMapItemContext) -> None:
        ctx.logger.info("processing")

    handler = distributed_map_item_handler(process)
    with caplog.at_level(logging.INFO):
        handler(
            {
                "distributedMapRunArn": _RUN_ARN,
                "records": _records_named_by_id(["id-a"]),
            },
            None,
        )
    (record,) = [r for r in caplog.records if r.getMessage() == "processing"]
    assert record.name == "aws_durable_execution_sdk_python.dmap.handlers"
    assert record.__dict__["distributedMapRunArn"] == _RUN_ARN
    assert record.__dict__["itemId"] == "id-a"


def test_batch_handler_item_ids_are_index_aligned_with_the_items() -> None:
    seen: list[tuple[list[str], DistributedMapBatchContext]] = []

    def process(items: list[str], ctx: DistributedMapBatchContext) -> None:
        seen.append((items, ctx))

    handler = distributed_map_batch_handler(process)
    handler(
        {"distributedMapRunArn": _RUN_ARN, "records": _records_named_by_id(_ITEM_IDS)},
        None,
    )
    ((items, ctx),) = seen
    assert items == _ITEM_IDS
    assert ctx.item_ids == tuple(_ITEM_IDS)
    assert ctx.distributed_map_run_arn == _RUN_ARN


def test_reader_passes_its_run_arn() -> None:
    seen: list[DistributedMapReaderContext] = []

    def read(_state: Any, _max: int, ctx: DistributedMapReaderContext) -> ReaderPage:
        seen.append(ctx)
        return ReaderPage()

    handler = distributed_map_reader(read)
    handler(
        {"distributedMapRunArn": _RUN_ARN, "state": None, "maxItemsPerPage": 10}, None
    )
    ((ctx,),) = [seen]
    assert ctx.distributed_map_run_arn == _RUN_ARN


@pytest.mark.parametrize(
    "run_arn",
    [_RUN_ARN, _RUN_ARN_WITH_SEPARATOR_NAMES],
    ids=["plain", "separator-in-names"],
)
def test_every_wrapper_passes_the_envelope_run_arn_unchanged(run_arn: str) -> None:
    """The wrappers parse the run ARN and rebuild it, which must restore the input."""
    seen: list[str] = []
    processor_event = {
        "distributedMapRunArn": run_arn,
        "records": _records_named_by_id(["id-a"]),
    }
    distributed_map_item_handler(
        lambda _item, ctx: seen.append(ctx.distributed_map_run_arn)
    )(processor_event, None)
    distributed_map_batch_handler(
        lambda _items, ctx: seen.append(ctx.distributed_map_run_arn)
    )(processor_event, None)

    def read(_state: Any, _max: int, ctx: DistributedMapReaderContext) -> ReaderPage:
        seen.append(ctx.distributed_map_run_arn)
        return ReaderPage()

    distributed_map_reader(read)(
        {"distributedMapRunArn": run_arn, "state": None, "maxItemsPerPage": 10}, None
    )
    assert seen == [run_arn, run_arn, run_arn]


def _run_durable(handler: Any, item_ids: list[str]) -> None:
    """Run a durable wrapper on one processor batch and require it to succeed."""
    event = {
        "distributedMapRunArn": _RUN_ARN_WITH_SEPARATOR_NAMES,
        "records": _records_named_by_id(item_ids),
    }
    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=json.dumps(event), execution_timeout=15)
    assert result.status is InvocationStatus.SUCCEEDED


def test_durable_item_handler_passes_each_item_its_run_arn_and_id() -> None:
    """The context's logger wraps the branch logger, so replay de-duplication is kept."""
    lock = threading.Lock()
    seen: dict[str, tuple[DurableContext, DistributedMapItemContext]] = {}

    def process(
        context: DurableContext, item: str, ctx: DistributedMapItemContext
    ) -> None:
        with lock:
            seen[item] = (context, ctx)

    _run_durable(durable_distributed_map_item_handler(process), _ITEM_IDS)
    assert sorted(seen) == _ITEM_IDS
    for item, (context, ctx) in seen.items():
        assert ctx.item_id == item
        assert ctx.distributed_map_run_arn == _RUN_ARN_WITH_SEPARATOR_NAMES
        assert isinstance(ctx.logger, Logger)
        assert ctx.logger.get_logger() is context.logger


def test_durable_batch_handler_item_ids_are_index_aligned_with_the_items() -> None:
    """The context's logger wraps the execution logger, so replay de-duplication is kept."""
    seen: list[tuple[DurableContext, list[str], DistributedMapBatchContext]] = []

    def process(
        context: DurableContext, items: list[str], ctx: DistributedMapBatchContext
    ) -> None:
        seen.append((context, items, ctx))

    _run_durable(durable_distributed_map_batch_handler(process), _ITEM_IDS)
    ((context, items, ctx),) = seen
    assert items == _ITEM_IDS
    assert ctx.item_ids == tuple(_ITEM_IDS)
    assert ctx.distributed_map_run_arn == _RUN_ARN_WITH_SEPARATOR_NAMES
    assert isinstance(ctx.logger, Logger)
    assert ctx.logger.get_logger() is context.logger
