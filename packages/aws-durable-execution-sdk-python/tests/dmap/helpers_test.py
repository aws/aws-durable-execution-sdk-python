"""Unit tests for the distributed map authoring helpers."""

from __future__ import annotations

import json
import logging
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest

from aws_durable_execution_sdk_python import (
    DistributedMapBatchContext,
    DistributedMapItemContext,
    DistributedMapReaderContext,
)
from aws_durable_execution_sdk_python.config import (
    BatchItemStatus,
    CompletionConfig,
    MapConfig,
    NestingType,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dmap.helpers import (
    ItemFailure,
    ReaderPage,
    process_distributed_map_batch,
    process_distributed_map_items,
    read_distributed_map_page,
)
from aws_durable_execution_sdk_python.exceptions import (
    SerDesError,
    StepError,
    ValidationError,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
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
    """A serdes whose prefix shows which of a helper's serdes options ran."""

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix

    def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
        return self.prefix + json.dumps(value)

    def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
        return json.loads(data.removeprefix(self.prefix))


def _processor_event(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {"distributedMapRunArn": _RUN_ARN, "records": records}


def _reader_event(
    state: str | None = None, max_items_per_page: int = 10
) -> dict[str, Any]:
    return {
        "distributedMapRunArn": _RUN_ARN,
        "state": state,
        "maxItemsPerPage": max_items_per_page,
    }


# Which of the two process_distributed_map_items forms a call selects, and the
# rejection of a call that matches neither.
# ============================================================================


class _RecordingMapContext:
    """A stand-in DurableContext whose map records its call and runs no item."""

    def __init__(self) -> None:
        self.configs: list[MapConfig] = []
        self.inputs: list[list[Any]] = []

    def map(self, inputs: list[Any], _func: Any, config: MapConfig) -> Any:
        self.inputs.append(inputs)
        self.configs.append(config)
        return SimpleNamespace(all=[])


def test_process_items_selects_the_non_durable_form_for_a_callable_second_argument():
    seen: list[Any] = []
    response = process_distributed_map_items(
        _processor_event([{"itemId": "0", "body": "1"}]),
        lambda item, _ctx: seen.append(item),
    )
    assert seen == [1]
    assert response["batchItemResults"] == [{"itemIdentifier": "0"}]


def test_process_items_selects_the_durable_form_for_a_non_callable_second_argument():
    context = _RecordingMapContext()
    response = process_distributed_map_items(
        context,
        _processor_event([{"itemId": "0", "body": "1"}]),
        lambda _context, item, _ctx: item,
    )
    assert context.inputs == [["1"]]
    assert response == {"batchItemFailures": [], "batchItemResults": []}


@pytest.mark.parametrize(
    "args",
    [(), ({},), ({}, {}, None, None)],
    ids=["none", "event-only", "four-arguments"],
)
def test_process_items_rejects_a_call_matching_neither_form(args):
    with pytest.raises(
        ValidationError, match=r"\(event, func\).*\(context, event, func\)"
    ):
        process_distributed_map_items(*args)


def test_process_items_rejects_func_passed_as_a_keyword():
    with pytest.raises(
        ValidationError, match=r"\(event, func\).*\(context, event, func\)"
    ):
        process_distributed_map_items(  # type: ignore[call-overload]
            _processor_event([]), func=lambda item, _ctx: item
        )


def test_process_items_non_durable_form_rejects_nesting_type():
    with pytest.raises(ValidationError, match="nesting_type"):
        process_distributed_map_items(  # type: ignore[call-overload]
            _processor_event([]),
            lambda item, _ctx: item,
            nesting_type=NestingType.NESTED,
        )


@pytest.mark.parametrize("durable", [False, True], ids=["non-durable", "durable"])
def test_process_items_rejects_an_unknown_keyword(durable):
    args: tuple[Any, ...] = (
        (_RecordingMapContext(), _processor_event([]), lambda c, i, x: i)
        if durable
        else (_processor_event([]), lambda i, x: i)
    )
    with pytest.raises(ValidationError, match="concurrenc"):
        process_distributed_map_items(*args, concurrenc=5)  # type: ignore[call-overload]


def test_process_items_durable_form_defaults_to_flat_nesting():
    context = _RecordingMapContext()
    process_distributed_map_items(
        context,
        _processor_event([{"itemId": "0", "body": "1"}]),
        lambda _context, item, _ctx: item,
    )
    assert context.configs[0].nesting_type is NestingType.FLAT


def test_process_items_durable_form_passes_nesting_nested_when_asked():
    context = _RecordingMapContext()
    process_distributed_map_items(
        context,
        _processor_event([{"itemId": "0", "body": "1"}]),
        lambda _context, item, _ctx: item,
        nesting_type=NestingType.NESTED,
    )
    assert context.configs[0].nesting_type is NestingType.NESTED


def test_process_items_durable_form_passes_concurrency_as_max_concurrency():
    context = _RecordingMapContext()
    process_distributed_map_items(
        context,
        _processor_event([{"itemId": "0", "body": "1"}]),
        lambda _context, item, _ctx: item,
        concurrency=3,
    )
    assert context.configs[0].max_concurrency == 3


def test_process_items_durable_form_passes_completion_config_all_completed():
    context = _RecordingMapContext()
    process_distributed_map_items(
        context,
        _processor_event([{"itemId": "0", "body": "1"}]),
        lambda _context, item, _ctx: item,
    )
    assert context.configs[0].completion_config == CompletionConfig.all_completed()


def test_process_items_rejects_concurrency_below_one_when_called():
    """The non-durable form raises when it is called, before any item runs."""
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        process_distributed_map_items(
            _processor_event([]), lambda x, _ctx: x, concurrency=0
        )
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        process_distributed_map_items(
            _processor_event([]), lambda x, _ctx: x, concurrency=-1
        )


def test_process_items_durable_form_rejects_concurrency_below_one_when_called():
    """The durable form raises when it is called, before context.map runs."""
    context = _RecordingMapContext()
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        process_distributed_map_items(
            context,
            _processor_event([]),
            lambda _context, item, _ctx: item,
            concurrency=0,
        )
    with pytest.raises(ValidationError, match="concurrency must be at least 1"):
        process_distributed_map_items(
            context,
            _processor_event([]),
            lambda _context, item, _ctx: item,
            concurrency=-1,
        )
    assert context.configs == []


def test_process_items_runs_items_one_at_a_time_by_default():
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

    process_distributed_map_items(
        _processor_event([{"itemId": str(i), "body": "1"} for i in range(4)]),
        process,
    )
    assert peak == 1


def test_process_items_honors_requested_concurrency():
    # Each item blocks until all four have arrived, so the batch only finishes
    # if the pool really runs four items at once. At a smaller pool size the
    # first item waits out the timeout and every item fails.
    barrier = threading.Barrier(4, timeout=5)

    def process(x, _ctx):
        barrier.wait()
        return x

    resp = process_distributed_map_items(
        _processor_event([{"itemId": str(i), "body": str(i)} for i in range(4)]),
        process,
        concurrency=4,
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


def test_process_items_reports_results_in_order():
    resp = process_distributed_map_items(
        _processor_event([{"itemId": "0", "body": "2"}, {"itemId": "1", "body": "3"}]),
        lambda x, _ctx: x * 2,
    )
    assert resp["batchItemResults"] == [
        {"itemIdentifier": "0", "output": "4"},
        {"itemIdentifier": "1", "output": "6"},
    ]
    assert resp["batchItemFailures"] == []


def test_process_items_captures_failures():
    def process(x, _ctx):
        if x == "bad":
            msg = "boom"
            raise ValueError(msg)
        return x

    resp = process_distributed_map_items(
        _processor_event(
            [
                {"itemId": "0", "body": '"ok"'},
                {"itemId": "1", "body": '"bad"'},
            ]
        ),
        process,
    )
    assert resp["batchItemResults"] == [{"itemIdentifier": "0", "output": '"ok"'}]
    assert resp["batchItemFailures"] == [
        {
            "itemIdentifier": "1",
            "error": {"errorType": "ValueError", "errorMessage": "boom"},
        }
    ]


def test_process_items_reports_every_member_of_a_rich_error():
    """An error carrying data and a stack trace reaches the service with all four members."""

    def process(_x: Any, _ctx: Any) -> Any:
        raise SerDesError(
            message="bad payload",
            error_type="ignored, SerDesError pins its own type",
            data="payload",
            stack_trace=["frame one", "frame two"],
        )

    resp = process_distributed_map_items(
        _processor_event([{"itemId": "0", "body": "1"}]), process
    )

    error = resp["batchItemFailures"][0]["error"]
    assert error["errorMessage"] == "bad payload"
    assert error["errorData"] == "payload"
    assert error["stackTrace"] == ["frame one", "frame two"]


def test_process_items_omits_error_members_the_exception_lacks():
    """A plain exception carries no data or trace, so neither key is written."""

    def process(_x: Any, _ctx: Any) -> Any:
        msg = "boom"
        raise ValueError(msg)

    resp = process_distributed_map_items(
        _processor_event([{"itemId": "0", "body": "1"}]), process
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


def test_process_items_omits_the_output_of_a_function_returning_none():
    resp = process_distributed_map_items(
        _processor_event([{"itemId": "0", "body": "1"}]), lambda _x, _ctx: None
    )
    assert resp == {
        "batchItemFailures": [],
        "batchItemResults": [{"itemIdentifier": "0"}],
    }


def test_process_items_fails_only_the_item_whose_output_cannot_be_serialized():
    """A result serdes failure is that item's failure, not the batch's."""

    class _RejectsBad(SerDes[Any]):
        def serialize(self, value: Any, _serdes_context: SerDesContext) -> str:
            if value == "bad":
                msg = "cannot serialize"
                raise TypeError(msg)
            return json.dumps(value)

        def deserialize(self, data: str, _serdes_context: SerDesContext) -> Any:
            return json.loads(data)

    resp = process_distributed_map_items(
        _processor_event(
            [
                {"itemId": "0", "body": '"ok"'},
                {"itemId": "1", "body": '"bad"'},
            ]
        ),
        lambda x, _ctx: x,
        result_serdes=_RejectsBad(),
    )
    assert resp["batchItemResults"] == [{"itemIdentifier": "0", "output": '"ok"'}]
    assert resp["batchItemFailures"] == [
        {
            "itemIdentifier": "1",
            "error": {"errorType": "TypeError", "errorMessage": "cannot serialize"},
        }
    ]


def test_process_batch_succeeds_and_propagates_error():
    seen: list = []

    def process(items, _ctx):
        seen.extend(items)
        return "ignored"

    # A batch processor reports no output, so the return value is not forwarded.
    assert (
        process_distributed_map_batch(
            _processor_event(
                [{"itemId": "0", "body": "1"}, {"itemId": "1", "body": "2"}]
            ),
            process,
        )
        is None
    )
    assert seen == [1, 2]

    def boom(_items, _ctx):
        msg = "batch failed"
        raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="batch failed"):
        process_distributed_map_batch(
            _processor_event([{"itemId": "0", "body": "1"}]), boom
        )


def test_reader_returns_items_and_next_state_then_exhausts():
    def read(state, max_items_per_page, _ctx):
        if state is None:
            return ReaderPage(items=[1, 2][:max_items_per_page], next_state={"page": 1})
        return ReaderPage(items=[3])

    first = read_distributed_map_page(_reader_event(), read)
    assert first["items"] == ["1", "2"]
    assert first["nextState"] == '{"page": 1}'

    second = read_distributed_map_page(_reader_event(state='{"page": 1}'), read)
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
    page = read_distributed_map_page(
        _reader_event(),
        lambda _s, _max, _ctx: ReaderPage(items=["a", "b"]),
        item_serdes=serdes,
    )
    assert page["items"] == ['"A"', '"B"']

    seen = []
    process_distributed_map_batch(
        _processor_event(
            [{"itemId": str(i), "body": body} for i, body in enumerate(page["items"])]
        ),
        lambda items, _ctx: seen.extend(items),
        item_serdes=serdes,
    )
    assert seen == ["a", "b"]


def test_reader_item_serdes_is_separate_from_the_state_serdes():
    """Items use the item serdes and next_state uses the state serdes."""
    page = read_distributed_map_page(
        _reader_event(state='state:"in"'),
        lambda state, _max, _ctx: ReaderPage(items=[state], next_state="s"),
        item_serdes=_PrefixSerDes("item:"),
        state_serdes=_PrefixSerDes("state:"),
    )
    assert page["items"] == ['item:"in"']
    assert page["nextState"] == 'state:"s"'


def test_reader_receives_the_page_bound():
    """The page bound reaches func, so a reader can page to it."""
    page = read_distributed_map_page(
        _reader_event(max_items_per_page=3),
        lambda _s, max_items_per_page, _ctx: ReaderPage(
            items=list(range(max_items_per_page))
        ),
    )
    assert page["items"] == ["0", "1", "2"]


def test_reader_rejects_page_over_max_items_per_page():
    with pytest.raises(ValidationError, match="exceeding maxItemsPerPage"):
        read_distributed_map_page(
            _reader_event(max_items_per_page=2),
            lambda _s, _max, _ctx: ReaderPage(items=[1, 2, 3]),
        )


@pytest.mark.parametrize("items", [[1], []], ids=["with_items", "empty_page"])
def test_reader_rejects_an_unchanged_next_state(items):
    """The service fails the run over this, so the helper rejects it at the source."""
    with pytest.raises(ValidationError, match="must differ from the state"):
        read_distributed_map_page(
            _reader_event(state='{"page": 1}'),
            lambda s, _max, _ctx: ReaderPage(items=items, next_state=s),
        )


def test_reader_allows_an_advanced_next_state():
    page = read_distributed_map_page(
        _reader_event(state='{"page": 1}'),
        lambda _s, _max, _ctx: ReaderPage(items=[], next_state={"page": 2}),
    )
    assert page == {"items": [], "nextState": '{"page": 2}'}


def test_process_items_rejects_non_processor_envelope():
    with pytest.raises(ValidationError, match="processor envelope"):
        process_distributed_map_items({}, lambda x, _ctx: x)


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
def test_process_items_rejects_a_record_with_no_item_id(record):
    """The helper reports each outcome against its item id, so it must have one."""
    with pytest.raises(ValidationError, match="string 'itemId'"):
        process_distributed_map_items(_processor_event([record]), lambda x, _ctx: x)


def test_process_batch_rejects_a_record_with_no_item_id():
    """The batch helper validates its invocation input the same way."""
    with pytest.raises(ValidationError, match="string 'itemId'"):
        process_distributed_map_batch(
            _processor_event([{"body": "1"}]), lambda items, _ctx: items
        )


def test_process_items_rejects_a_batch_when_only_one_record_lacks_an_item_id():
    """One unusable record fails the invocation rather than the batch running partly."""
    calls = []
    with pytest.raises(ValidationError, match="string 'itemId'"):
        process_distributed_map_items(
            _processor_event([{"itemId": "0", "body": "1"}, {"body": "2"}]),
            lambda item, _ctx: calls.append(item),
        )
    assert calls == []


def test_reader_rejects_non_reader_envelope():
    with pytest.raises(ValidationError, match="reader envelope"):
        read_distributed_map_page(
            {"distributedMapRunArn": _RUN_ARN, "state": None},
            lambda _state, _max, _ctx: ReaderPage(items=[]),
        )


# Each value a helper serializes gets its own serdes context, so a serdes that
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


def test_process_items_stores_each_output_under_its_own_key(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    resp = process_distributed_map_items(
        _processor_event(_records(3)), lambda i, _ctx: f"out-{i}", result_serdes=fs
    )
    outputs = [r["output"] for r in resp["batchItemResults"]]
    assert len(set(outputs)) == 3
    assert [_read(fs, o) for o in outputs] == ["out-0", "out-1", "out-2"]


def test_process_items_stores_concurrent_outputs_under_their_own_keys(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    resp = process_distributed_map_items(
        _processor_event(_records(8)),
        lambda i, _ctx: f"out-{i}",
        result_serdes=fs,
        concurrency=4,
    )
    outputs = [r["output"] for r in resp["batchItemResults"]]
    assert [_read(fs, o) for o in outputs] == [f"out-{i}" for i in range(8)]


def test_process_items_keeps_the_outputs_of_another_run(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    other_run_arn = _RUN_ARN.replace("/abc", "/def")
    first = process_distributed_map_items(
        _processor_event(_records(1)), lambda _i, _ctx: "first", result_serdes=fs
    )
    process_distributed_map_items(
        {"distributedMapRunArn": other_run_arn, "records": _records(1)},
        lambda _i, _ctx: "second",
        result_serdes=fs,
    )
    assert _read(fs, first["batchItemResults"][0]["output"]) == "first"


def test_process_items_reuses_the_key_when_an_item_is_retried(tmp_path):
    fs = FileSystemSerDes(str(tmp_path))
    event = _processor_event(_records(2))

    def call() -> dict[str, Any]:
        return process_distributed_map_items(
            event, lambda i, _ctx: f"out-{i}", result_serdes=fs
        )

    first = call()
    assert call() == first
    outputs = [r["output"] for r in first["batchItemResults"]]
    assert [_read(fs, o) for o in outputs] == ["out-0", "out-1"]


@pytest.mark.parametrize("batch", [False, True], ids=["item", "batch"])
def test_processor_reads_and_writes_each_value_under_its_own_context(batch):
    serdes = _RecordingSerDes()
    event = _processor_event(_records(2))
    if batch:
        process_distributed_map_batch(
            event, lambda _items, _ctx: None, item_serdes=serdes
        )
    else:
        process_distributed_map_items(
            event, lambda i, _ctx: i, item_serdes=serdes, result_serdes=serdes
        )
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

    first = read_distributed_map_page(
        {"distributedMapRunArn": _RUN_ARN, "maxItemsPerPage": 10},
        read,
        item_serdes=fs,
        state_serdes=fs,
    )
    assert len({*first["items"], first["nextState"]}) == 3
    assert [_read(fs, i) for i in first["items"]] == ["r0-0", "r0-1"]
    assert _read(fs, first["nextState"]) == {"page": 1}

    second = read_distributed_map_page(
        _reader_event(state=first["nextState"]),
        read,
        item_serdes=fs,
        state_serdes=fs,
    )
    assert [_read(fs, i) for i in second["items"]] == ["r1-0", "r1-1"]
    assert _read(fs, second["nextState"]) == {"page": 2}
    # Page 2 leaves page 1's items and state in place.
    assert [_read(fs, i) for i in first["items"]] == ["r0-0", "r0-1"]
    assert _read(fs, first["nextState"]) == {"page": 1}


def test_reader_reads_and_writes_each_value_under_its_own_context():
    serdes = _RecordingSerDes()
    read_distributed_map_page(
        _reader_event(state='"in"'),
        lambda _s, _max, _ctx: ReaderPage(items=[1, 2], next_state="next"),
        item_serdes=serdes,
        state_serdes=serdes,
    )
    keys = {(c.durable_execution_arn, c.operation_id) for c in serdes.contexts}
    assert len(keys) == len(serdes.contexts) == 4


def test_reader_state_is_read_under_a_different_context_than_it_was_written():
    """next_state is written as one call's output and read as the next call's input.

    So the write context and the read context always differ. Pins the context
    requirement documented in the serdes module.
    """
    serdes = _RecordingSerDes()

    # The next state has to advance, or the helper rejects it for repeating
    # the position it was called with.
    def read(state, _max, _ctx):
        return ReaderPage(items=[], next_state="page-2" if state else "page-1")

    first = read_distributed_map_page(
        {"distributedMapRunArn": _RUN_ARN, "maxItemsPerPage": 10},
        read,
        state_serdes=serdes,
    )
    # No input state on the first call, so the only context is the write.
    assert len(serdes.contexts) == 1
    wrote = serdes.contexts[0]

    serdes.contexts.clear()
    read_distributed_map_page(
        _reader_event(state=first["nextState"]), read, state_serdes=serdes
    )
    # The input state is deserialized before anything else is written.
    read_context = serdes.contexts[0]

    assert read_context.durable_execution_arn == wrote.durable_execution_arn
    assert read_context.operation_id != wrote.operation_id


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
def test_helpers_reject_an_envelope_without_a_valid_run_arn(run_arn, envelope):
    event = dict(envelope)
    if run_arn is not None:
        event["distributedMapRunArn"] = run_arn
    if "records" in envelope:
        calls = [
            lambda: process_distributed_map_items(event, lambda x, _ctx: x),
            lambda: process_distributed_map_batch(event, lambda _items, _ctx: None),
        ]
    else:
        calls = [
            lambda: read_distributed_map_page(
                event, lambda _s, _max, _ctx: ReaderPage()
            )
        ]
    for call in calls:
        with pytest.raises(ValidationError, match="'distributedMapRunArn'"):
            call()


# Each helper passes the customer function a context as its last argument. It
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


def test_process_items_passes_each_item_its_run_arn_and_id() -> None:
    seen: dict[str, DistributedMapItemContext] = {}

    def process(item: str, ctx: DistributedMapItemContext) -> None:
        seen[item] = ctx

    process_distributed_map_items(
        _processor_event(_records_named_by_id(_ITEM_IDS)), process
    )
    assert sorted(seen) == _ITEM_IDS
    for item, ctx in seen.items():
        assert ctx.item_id == item
        assert ctx.distributed_map_run_arn == _RUN_ARN


def test_process_items_logger_stamps_the_run_arn_and_item_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The non-durable form has no durable logger, so it logs through the module logger."""

    def process(_item: Any, ctx: DistributedMapItemContext) -> None:
        ctx.logger.info("processing")

    with caplog.at_level(logging.INFO):
        process_distributed_map_items(
            _processor_event(_records_named_by_id(["id-a"])), process
        )
    (record,) = [r for r in caplog.records if r.getMessage() == "processing"]
    assert record.name == "aws_durable_execution_sdk_python.dmap.helpers"
    assert record.__dict__["distributedMapRunArn"] == _RUN_ARN
    assert record.__dict__["itemId"] == "id-a"


def test_process_batch_item_ids_are_index_aligned_with_the_items() -> None:
    seen: list[tuple[list[str], DistributedMapBatchContext]] = []

    def process(items: list[str], ctx: DistributedMapBatchContext) -> None:
        seen.append((items, ctx))

    process_distributed_map_batch(
        _processor_event(_records_named_by_id(_ITEM_IDS)), process
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

    read_distributed_map_page(_reader_event(), read)
    ((ctx,),) = [seen]
    assert ctx.distributed_map_run_arn == _RUN_ARN


@pytest.mark.parametrize(
    "run_arn",
    [_RUN_ARN, _RUN_ARN_WITH_SEPARATOR_NAMES],
    ids=["plain", "separator-in-names"],
)
def test_every_helper_passes_the_envelope_run_arn_unchanged(run_arn: str) -> None:
    """The helpers parse the run ARN and rebuild it, which must restore the input."""
    seen: list[str] = []
    processor_event = {
        "distributedMapRunArn": run_arn,
        "records": _records_named_by_id(["id-a"]),
    }
    process_distributed_map_items(
        processor_event, lambda _item, ctx: seen.append(ctx.distributed_map_run_arn)
    )
    process_distributed_map_batch(
        processor_event, lambda _items, ctx: seen.append(ctx.distributed_map_run_arn)
    )

    def read(_state: Any, _max: int, ctx: DistributedMapReaderContext) -> ReaderPage:
        seen.append(ctx.distributed_map_run_arn)
        return ReaderPage()

    read_distributed_map_page(
        {"distributedMapRunArn": run_arn, "state": None, "maxItemsPerPage": 10}, read
    )
    assert seen == [run_arn, run_arn, run_arn]


def _run_durable(handler: Any, item_ids: list[str]) -> None:
    """Run a durable handler on one processor batch and require it to succeed."""
    event = {
        "distributedMapRunArn": _RUN_ARN_WITH_SEPARATOR_NAMES,
        "records": _records_named_by_id(item_ids),
    }
    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=json.dumps(event), execution_timeout=15)
    assert result.status is InvocationStatus.SUCCEEDED


def test_durable_process_items_passes_each_item_its_run_arn_and_id() -> None:
    """The context's logger wraps the branch logger, so replay de-duplication is kept."""
    lock = threading.Lock()
    seen: dict[str, tuple[DurableContext, DistributedMapItemContext]] = {}

    def process(
        context: DurableContext, item: str, ctx: DistributedMapItemContext
    ) -> None:
        with lock:
            seen[item] = (context, ctx)

    @durable_execution
    def handler(event: dict[str, Any], context: DurableContext) -> Any:
        return process_distributed_map_items(context, event, process)

    _run_durable(handler, _ITEM_IDS)
    assert sorted(seen) == _ITEM_IDS
    for item, (context, ctx) in seen.items():
        assert ctx.item_id == item
        assert ctx.distributed_map_run_arn == _RUN_ARN_WITH_SEPARATOR_NAMES
        assert isinstance(ctx.logger, Logger)
        assert ctx.logger.get_logger() is context.logger


def test_process_batch_in_a_durable_handler_keeps_item_ids_aligned() -> None:
    """The batch helper takes no durable context, so its logger wraps the module logger."""
    seen: list[tuple[list[str], DistributedMapBatchContext]] = []

    def process(items: list[str], ctx: DistributedMapBatchContext) -> None:
        seen.append((items, ctx))

    @durable_execution
    def handler(event: dict[str, Any], _context: DurableContext) -> Any:
        return process_distributed_map_batch(event, process)

    _run_durable(handler, _ITEM_IDS)
    ((items, ctx),) = seen
    assert items == _ITEM_IDS
    assert ctx.item_ids == tuple(_ITEM_IDS)
    assert ctx.distributed_map_run_arn == _RUN_ARN_WITH_SEPARATOR_NAMES
    assert isinstance(ctx.logger, Logger)


# A failed item writes one error line through the item's logger, so a log query
# can find it. The line carries the error the helper reports to the service.
# ============================================================================


def _error_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno == logging.ERROR]


def _failure_stub_context(error: ErrorObject) -> Any:
    """A stand-in DurableContext whose map reports item 0 failed and item 1 succeeded."""
    return SimpleNamespace(
        logger=Logger(logging.getLogger("durable-test"), {}),
        map=lambda _inputs, _func, config: SimpleNamespace(  # noqa: ARG005
            all=[
                SimpleNamespace(
                    index=0, status=BatchItemStatus.FAILED, error=error, result=None
                ),
                SimpleNamespace(
                    index=1, status=BatchItemStatus.SUCCEEDED, error=None, result="1"
                ),
            ]
        ),
    )


_TWO_RECORDS = [{"itemId": "id-a", "body": "1"}, {"itemId": "id-b", "body": "1"}]


def test_process_items_logs_one_error_line_for_a_failed_item(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def process(item: int, ctx: DistributedMapItemContext) -> int:
        if ctx.item_id == "id-a":
            msg = "boom"
            raise ValueError(msg)
        return item

    with caplog.at_level(logging.ERROR):
        response = process_distributed_map_items(
            _processor_event(_TWO_RECORDS), process
        )
    (record,) = _error_records(caplog)
    reported = response["batchItemFailures"][0]["error"]
    assert record.getMessage() == "distributed map item failed"
    assert record.__dict__["distributedMapRunArn"] == _RUN_ARN
    assert record.__dict__["itemId"] == "id-a"
    assert record.__dict__["errorType"] == reported["errorType"] == "ValueError"
    assert record.__dict__["errorMessage"] == reported["errorMessage"] == "boom"
    assert not hasattr(record, "errorData")
    assert not hasattr(record, "stackTrace")


def test_process_items_error_line_carries_error_data_and_stack_trace(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def process(_item: int, _ctx: DistributedMapItemContext) -> None:
        raise StepError("boom", "ValueError", "payload", ["line 1", "line 2"])

    with caplog.at_level(logging.ERROR):
        process_distributed_map_items(_processor_event(_TWO_RECORDS[:1]), process)
    (record,) = _error_records(caplog)
    assert record.__dict__["errorData"] == "payload"
    assert record.__dict__["stackTrace"] == ["line 1", "line 2"]


def test_process_items_logs_no_error_line_when_every_item_succeeds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG):
        process_distributed_map_items(
            _processor_event(_TWO_RECORDS), lambda item, _ctx: item
        )
    assert _error_records(caplog) == []


def test_durable_process_items_logs_one_error_line_for_a_failed_item(
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = ErrorObject(message="boom", type="ValueError", data=None, stack_trace=None)
    with caplog.at_level(logging.ERROR):
        response = process_distributed_map_items(
            _failure_stub_context(error),
            _processor_event(_TWO_RECORDS),
            lambda _context, item, _ctx: item,
        )
    (record,) = _error_records(caplog)
    reported = response["batchItemFailures"][0]["error"]
    assert record.getMessage() == "distributed map item failed"
    assert record.__dict__["distributedMapRunArn"] == _RUN_ARN
    assert record.__dict__["itemId"] == "id-a"
    assert record.__dict__["errorType"] == reported["errorType"] == "ValueError"
    assert record.__dict__["errorMessage"] == reported["errorMessage"] == "boom"
    assert not hasattr(record, "errorData")
    assert not hasattr(record, "stackTrace")


def test_durable_process_items_error_line_carries_error_data_and_stack_trace(
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = ErrorObject(
        message="boom", type="ValueError", data="payload", stack_trace=["line 1"]
    )
    with caplog.at_level(logging.ERROR):
        process_distributed_map_items(
            _failure_stub_context(error),
            _processor_event(_TWO_RECORDS),
            lambda _context, item, _ctx: item,
        )
    (record,) = _error_records(caplog)
    assert record.__dict__["errorData"] == "payload"
    assert record.__dict__["stackTrace"] == ["line 1"]


def test_durable_process_items_logs_no_error_line_when_every_item_succeeds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    context = SimpleNamespace(
        logger=Logger(logging.getLogger("durable-test"), {}),
        map=lambda _inputs, _func, config: SimpleNamespace(  # noqa: ARG005
            all=[
                SimpleNamespace(
                    index=0, status=BatchItemStatus.SUCCEEDED, error=None, result="1"
                )
            ]
        ),
    )
    with caplog.at_level(logging.DEBUG):
        process_distributed_map_items(
            context, _processor_event(_TWO_RECORDS[:1]), lambda _c, item, _ctx: item
        )
    assert _error_records(caplog) == []
