# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Error shape and opted-in operation result decoding in emitted records."""

from __future__ import annotations

import base64
import dataclasses
import datetime
import decimal
import json
import uuid
from typing import Any

import pytest
from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationEndInfo,
    OperationInfo,
    OperationType,
)
from aws_durable_execution_sdk_python.serdes import EXTENDED_TYPES_SERDES

from aws_durable_execution_sdk_python_telemetry_exporter import (
    ContentConfig,
    ContentOperations,
    DurableExecutionTelemetryConfig,
    OperationOverride,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_telemetry_exporter.operations_index import (
    with_operations_by_name,
)
from aws_durable_execution_sdk_python_telemetry_exporter.plugin import (
    _decode_checkpoint_result,
    _to_json_compatible,
)

ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1"
T0 = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
T1 = datetime.datetime(2026, 1, 1, 0, 0, 1, tzinfo=datetime.UTC)
STEP_ERROR = "aws_durable_execution_sdk_python.exceptions.StepError"


class CaptureExporter:
    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self.records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> Any:  # noqa: PLR6301
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        return None


def _op(name: str, *, result=None, error=None, status=OperationStatus.SUCCEEDED):
    return OperationEndInfo(
        operation_id=name,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=name,
        parent_id=None,
        start_time=T0,
        is_replayed=False,
        status=status,
        end_time=T1,
        result=result,
        error=error,
        attempt=1,
    )


def _emit(
    ops: list[OperationInfo],
    *,
    error: ErrorObject | None = None,
    content: ContentConfig | None = None,
) -> dict[str, Any]:
    exporter = CaptureExporter()
    factory = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], content=content)
    )
    operations = {op.operation_id: op for op in ops}
    start = InvocationStartInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=T0,
        execution_input=None,
        operations=operations,
    )
    plugin = factory.create_plugin(start)
    plugin.on_invocation_start(start)
    plugin.on_invocation_end(
        InvocationEndInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=T0,
            status=InvocationStatus.FAILED if error else InvocationStatus.SUCCEEDED,
            error=error,
            execution_result=None,
            operations=operations,
        )
    )
    assert len(exporter.records) == 1
    return exporter.records[0]


def _err(type_name: str | None, message: str | None, data: str | None = None):
    return ErrorObject(message=message, type=type_name, data=data, stack_trace=None)


# -- error shape ---------------------------------------------------------------


def test_hook_errors_carry_no_cause():
    # The plugin hooks expose ErrorObject only; it has no cause field, so a
    # Python record can never carry ``cause``.
    fields = {f.name for f in dataclasses.fields(ErrorObject)}
    assert fields == {"message", "type", "data", "stack_trace"}
    assert "error" in {f.name for f in dataclasses.fields(InvocationEndInfo)}
    assert "error" in {f.name for f in dataclasses.fields(OperationEndInfo)}


def test_error_shape_and_key_order_everywhere():
    record = _emit(
        [
            _op(
                "charge",
                status=OperationStatus.FAILED,
                error=_err("PaymentError", "card declined", data='{"code":1}'),
            )
        ],
        error=_err(STEP_ERROR, "card declined", data="ignored"),
    )
    assert list(record["error"]) == ["name", "message"]
    # Names are the SDK's ErrorType, unchanged.
    assert record["error"] == {"name": STEP_ERROR, "message": "card declined"}
    op_error = record["operations"][0]["error"]
    assert list(op_error) == ["name", "message"]
    assert op_error == {"name": "PaymentError", "message": "card declined"}
    by_name = with_operations_by_name(record)["operationsByName"]["charge"]["error"]
    assert list(by_name) == ["name", "message"]
    assert by_name == op_error
    # ErrorData is never turned into a cause.
    assert "cause" not in json.dumps(record)


def test_error_with_missing_type_and_message_keeps_both_keys():
    record = _emit([], error=_err(None, None))
    assert list(record["error"]) == ["name", "message"]
    assert record["error"] == {"name": None, "message": None}


def test_include_errors_false_removes_operation_errors_everywhere():
    record = _emit(
        [_op("charge", status=OperationStatus.FAILED, error=_err("E", "m"))],
        error=_err(STEP_ERROR, "m"),
        content=ContentConfig(operations=ContentOperations(include_errors=False)),
    )
    assert "error" not in record["operations"][0]
    assert "error" not in with_operations_by_name(record)["operationsByName"]["charge"]
    assert record["error"] == {"name": STEP_ERROR, "message": "m"}


# -- result decoding -----------------------------------------------------------


def _results(*ops: OperationInfo) -> dict[str, Any]:
    overrides = [OperationOverride(op.name or "", result=lambda r: r) for op in ops]
    record = _emit(
        list(ops),
        content=ContentConfig(operations=ContentOperations(overrides=overrides)),
    )
    return {op["name"]: op.get("result") for op in record["operations"]}


def _envelope(value: Any) -> str:
    raw = EXTENDED_TYPES_SERDES.serialize(value, None)  # type: ignore[arg-type]
    assert set(json.loads(raw)) == {"t", "v"}
    return raw


def test_envelope_result_decodes_to_plain_json():
    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    value = {
        "amount": decimal.Decimal("12.50"),
        "count": decimal.Decimal(3),
        "when": datetime.datetime(2026, 1, 2, 3, 4, 5, 6000, tzinfo=datetime.UTC),
        "day": datetime.date(2026, 1, 2),
        "id": uid,
        "blob": b"\x00\xffhi",
        "pair": (1, "a"),
        "nested": [{"x": decimal.Decimal("0.1")}],
    }
    results = _results(_op("r", result=_envelope(value)))
    assert results["r"] == {
        "amount": 12.5,
        "count": 3,
        "when": "2026-01-02T03:04:05.006000+00:00",
        "day": "2026-01-02",
        "id": str(uid),
        "blob": base64.b64encode(b"\x00\xffhi").decode(),
        "pair": [1, "a"],
        "nested": [{"x": 0.1}],
    }
    json.dumps(results["r"], allow_nan=False)


def test_batch_result_becomes_to_dict():
    batch = BatchResult(
        all=[BatchItem(0, BatchItemStatus.SUCCEEDED, result=decimal.Decimal(2))],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )
    decoded = _decode_checkpoint_result(_envelope(batch))
    expected = batch.to_dict()
    expected["all"][0]["result"] = 2
    assert decoded == json.loads(json.dumps(expected))


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("12", 12),
        ("-0", 0),
        ("-0.0", 0),
        ("1.10", 1.1),
        ("0.1", 0.1),
        ("9007199254740991", 9007199254740991),
        ("-9007199254740991", -9007199254740991),
        ("9007199254740992", 9007199254740992.0),
        ("9007199254740993", "9007199254740993"),
        ("1E+20", 1e20),
        ("1E+125", 1e125),
        ("1E+126", "1E+126"),
        ("1E-130", 1e-130),
        ("1E-131", "1E-131"),
        ("1E+299", "1E+299"),
        ("1E+300", "1E+300"),
        ("1" * 39, "1" * 39),
        ("12345678901234567.89", "12345678901234567.89"),
        ("1E+5000", "1E+5000"),
        ("1E+1000000", "1E+1000000"),
        ("1E-1000000", "1E-1000000"),
        ("NaN", None),
        ("sNaN", None),
        ("Infinity", None),
        ("-Infinity", None),
    ],
)
def test_decimal_conversion(text, expected):
    raw = _envelope({"v": decimal.Decimal(text)})
    decoded = _decode_checkpoint_result(raw)
    assert decoded == {"v": expected}
    assert type(decoded["v"]) is type(expected)


class _CountingDecimal(decimal.Decimal):
    int_calls = 0

    def __int__(self) -> int:
        type(self).int_calls += 1
        return super().__int__()


@pytest.mark.parametrize(
    "text", ["1E+16", "1E+299", "1E+5000", "1E+1000000", "1" * 4000]
)
def test_out_of_range_integral_decimal_never_builds_an_int(text):
    # Counts int() conversions instead of timing them: a Decimal past the
    # 2**53 - 1 bound is rejected on its exponent before any int is built.
    _CountingDecimal.int_calls = 0
    assert _to_json_compatible([_CountingDecimal(text)]) == [
        float(text) if text == "1E+16" else text
    ]
    assert _CountingDecimal.int_calls == 0
    assert _to_json_compatible([_CountingDecimal("12")]) == [12]
    assert _CountingDecimal.int_calls == 1


def test_non_finite_plain_json_becomes_null():
    assert _decode_checkpoint_result("[NaN, Infinity, 1.5]") == [None, None, 1.5]
    assert _decode_checkpoint_result('{"a": [1e999, -1e999]}') == {"a": [None, None]}


def test_plain_json_skips_the_tree_walk(monkeypatch):
    from aws_durable_execution_sdk_python_telemetry_exporter import plugin

    calls: list[Any] = []
    real = plugin._to_json_compatible

    def spy(value):
        calls.append(value)
        return real(value)

    monkeypatch.setattr(plugin, "_to_json_compatible", spy)
    assert _decode_checkpoint_result('{"a": [1, 2.5, "x", null, true]}') == {
        "a": [1, 2.5, "x", None, True]
    }
    assert _decode_checkpoint_result('{"t": "zz", "v": 1}') == {"t": "zz", "v": 1}
    assert calls == []
    assert _decode_checkpoint_result("[NaN]") == [None]
    assert len(calls) == 1


def test_600_nested_arrays():
    depth = 600
    raw = "[" * depth + "]" * depth
    decoded = _decode_checkpoint_result(raw)
    level = 1
    while decoded:
        decoded = decoded[0]
        level += 1
    assert level == depth

    # Converted without recursion, also for tuples.
    value: Any = ()
    for _ in range(depth - 1):
        value = (value, decimal.Decimal("1.5"))
    converted = _to_json_compatible(value)
    level = 1
    while converted:
        assert converted[1] == 1.5
        converted = converted[0]
        level += 1
    assert level == depth


def test_deep_envelope_the_core_cannot_decode_does_not_raise():
    depth = 600
    raw = '{"t":"l","v":[' * depth + "]}" * depth
    decoded = _decode_checkpoint_result(raw)
    # json.loads or the core decoder gives up; the raw/parsed value is kept.
    assert decoded is not None


def test_non_envelope_results_pass_through():
    assert _decode_checkpoint_result("42") == 42
    assert _decode_checkpoint_result('{"a": 1}') == {"a": 1}
    assert _decode_checkpoint_result("not json") == "not json"
    # Unknown tag: decoding fails, parsed value kept.
    assert _decode_checkpoint_result('{"t":"zz","v":1}') == {"t": "zz", "v": 1}
    # Extra key: not an envelope.
    assert _decode_checkpoint_result('{"t":"s","v":"x","w":1}') == {
        "t": "s",
        "v": "x",
        "w": 1,
    }
    # Shape collision: a JsonSerDes value that looks like an envelope is decoded.
    assert _decode_checkpoint_result('{"t":"s","v":"x"}') == "x"
    assert _decode_checkpoint_result('{"t":"i","v":5}') == 5
    assert _decode_checkpoint_result('{"t":"m","v":{"a":1}}') == {"a": 1}
    # The core coerces ``v`` for a known tag.
    assert _decode_checkpoint_result('{"t":"b","v":"yes"}') is True
    assert _decode_checkpoint_result('{"t":"temperature","v":21.5}') == {
        "t": "temperature",
        "v": 21.5,
    }


def test_one_failing_result_keeps_the_rest_of_the_record():
    results = _results(
        _op("good", result=_envelope({"a": decimal.Decimal("1.5")})),
        _op("bad", result='{"t":"d","v":"not-a-decimal"}'),
        _op("text", result="plain text"),
    )
    assert results == {
        "good": {"a": 1.5},
        "bad": {"t": "d", "v": "not-a-decimal"},
        "text": "plain text",
    }


def test_unsupported_decoded_type_falls_back_to_raw(monkeypatch):
    class Odd:
        pass

    monkeypatch.setattr(
        EXTENDED_TYPES_SERDES, "deserialize", lambda data, ctx=None: {"x": Odd()}
    )
    raw = '{"t":"m","v":{}}'
    # Decoded value cannot be converted; the parsed payload is kept.
    assert _decode_checkpoint_result(raw) == {"t": "m", "v": {}}


def test_unsupported_value_after_fallback_returns_raw_string(monkeypatch):
    from aws_durable_execution_sdk_python_telemetry_exporter import plugin

    def boom(value):
        raise TypeError

    monkeypatch.setattr(plugin, "_to_json_compatible", boom)
    assert _decode_checkpoint_result('{"a":NaN}') == '{"a":NaN}'
