# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Public plugin path into real exporters: decoded numbers, line size, caps."""

from __future__ import annotations

import datetime
import decimal
import io
import json
import logging
import sys
from typing import Any

import pytest
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
    DynamoDBExporter,
    LambdaLogExporter,
    OperationOverride,
    durable_execution_telemetry,
)

ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1"
T0 = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
T1 = datetime.datetime(2026, 1, 1, 0, 0, 1, tzinfo=datetime.UTC)
BIG_39_DIGITS = "123456789012345678901234567890123456789"


class StubDynamoDBClient:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def put_item(self, **kwargs: Any) -> None:
        self.items.append(kwargs["Item"])


def _op(name: str, index: int, *, result: str | None = None) -> OperationEndInfo:
    start = T0 + datetime.timedelta(milliseconds=index)
    return OperationEndInfo(
        operation_id=f"{name}-id",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=name,
        parent_id=None,
        start_time=start,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=T1,
        result=result,
        error=None,
        attempt=1,
    )


def _run(
    exporter: Any,
    ops: list[OperationEndInfo],
    *,
    execution_input: str | None = None,
    error: ErrorObject | None = None,
) -> None:
    overrides = [OperationOverride(op.name or "", result=lambda r: r) for op in ops]
    factory = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[exporter],
            content=ContentConfig(operations=ContentOperations(overrides=overrides)),
        )
    )
    operations: dict[str, OperationInfo] = {op.operation_id: op for op in ops}
    start = InvocationStartInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=T0,
        execution_input=execution_input,
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


def _envelope(value: Any) -> str:
    return EXTENDED_TYPES_SERDES.serialize(value, None)  # type: ignore[arg-type]


# -- B1: decoded numbers reach DynamoDB ----------------------------------------


def _dynamodb_results(*results: str, caplog: pytest.LogCaptureFixture) -> Any:
    client = StubDynamoDBClient()
    exporter = DynamoDBExporter(table_name="t", client=client)
    ops = [_op(f"op{i}", i, result=raw) for i, raw in enumerate(results)]
    with caplog.at_level(logging.WARNING):
        _run(exporter, ops)
    assert "failed" not in caplog.text
    assert len(client.items) == 1
    by_name = client.items[0]["operationsByName"]["M"]
    return [by_name[f"op{i}"]["M"]["result"] for i in range(len(results))]


def test_out_of_range_decoded_decimals_keep_the_dynamodb_item(caplog):
    results = _dynamodb_results(
        _envelope(decimal.Decimal("1E+299")),
        _envelope(decimal.Decimal(BIG_39_DIGITS)),
        _envelope(decimal.Decimal("9007199254740993")),
        _envelope({"amount": decimal.Decimal("12.50"), "n": decimal.Decimal("7")}),
        caplog=caplog,
    )
    assert results[:3] == [
        {"S": "1E+299"},
        {"S": BIG_39_DIGITS},
        # Exact for JavaScript readers, which would round the number.
        {"S": "9007199254740993"},
    ]
    assert results[3] == {"M": {"amount": {"N": "12.5"}, "n": {"N": "7"}}}


def test_out_of_range_plain_json_numbers_keep_the_dynamodb_item(caplog):
    results = _dynamodb_results(
        "1e300",
        BIG_39_DIGITS,
        "[1e-200, 1.5, 3]",
        caplog=caplog,
    )
    assert results == [
        {"S": "1e+300"},
        {"S": BIG_39_DIGITS},
        {"L": [{"S": "1e-200"}, {"N": "1.5"}, {"N": "3"}]},
    ]


def test_dynamodb_marshal_turns_non_finite_numbers_into_strings():
    client = StubDynamoDBClient()
    exporter = DynamoDBExporter(table_name="t", client=client)
    marshalled = exporter._marshal({"a": float("nan"), "b": float("inf"), "c": 1})
    assert marshalled == {"a": {"S": "NaN"}, "b": {"S": "Infinity"}, "c": {"N": "1"}}


def test_dynamodb_marshal_applies_the_service_magnitude_range():
    exporter = DynamoDBExporter(table_name="t", client=StubDynamoDBClient())
    marshalled = exporter._marshal({"a": [1e125, 1e126, 1e-130, 1e-131, 0, -0.0]})
    assert marshalled["a"]["L"] == [
        {"N": "1E+125"},
        {"S": "1e+126"},
        {"N": "1E-130"},
        {"S": "1e-131"},
        {"N": "0"},
        {"N": "-0.0"},
    ]


# -- B2: LambdaLogExporter bounds the printed line ------------------------------


def _printed_line(capsys: pytest.CaptureFixture[str]) -> str:
    out = capsys.readouterr().out
    assert out.endswith("\n") and out.count("\n") == 1
    return out[:-1]


def test_quote_heavy_record_line_fits_the_limit(capsys):
    limit = 262_144
    exporter = LambdaLogExporter(max_record_size_bytes=limit)
    # Each '"' is 2 bytes in the record JSON and 4 bytes in the printed line.
    ops = [_op(f"q{i}", i, result=json.dumps('"' * 4000)) for i in range(40)]
    _run(exporter, ops, execution_input='"\\' * 10_000)
    line = _printed_line(capsys)
    assert len(line.encode("utf-8")) <= limit
    envelope = json.loads(line)
    record = json.loads(envelope["message"])
    assert record["truncated"] is True
    # The single pass measured the line, so it still keeps what fits.
    kept = [summary for summary in record["operationsByName"].values()]
    assert kept
    assert any("result" in summary for summary in kept)


def test_capped_execution_error_fits_a_64_kb_line(capsys):
    limit = 65_536
    exporter = LambdaLogExporter(max_record_size_bytes=limit)
    # \x01 is the worst case: 6 bytes in the record, 7 in the printed line.
    message = "\x01" * 100_000
    ops = [_op(f"q{i}", i, result=json.dumps('"' * 2000)) for i in range(40)]
    _run(
        exporter,
        ops,
        execution_input='"' * 50_000,
        error=ErrorObject(message=message, type="E", data=None, stack_trace=None),
    )
    line = _printed_line(capsys)
    assert len(line.encode("utf-8")) <= limit
    record = json.loads(json.loads(line)["message"])
    assert record["error"]["message"] == "\x01" * 8192 + "\u2026"
    assert record["droppedInput"] is True


def test_line_separators_are_escaped_and_text_round_trips(capsys):
    exporter = LambdaLogExporter()
    text = 'a\u2028b\u2029c é "q" \\ \n \x01 \U0001f600'
    _run(exporter, [_op("s", 0, result=json.dumps(text))])
    out = capsys.readouterr().out
    assert "\u2028" not in out and "\u2029" not in out
    assert len(out.splitlines()) == 1
    # Non-ASCII stays raw; control characters and quotes are escaped.
    assert "é" in out and "\U0001f600" in out and "\x01" not in out
    record = json.loads(json.loads(out)["message"])
    assert record["operationsByName"]["s"]["result"] == text


def test_line_separator_escaping_is_counted_in_the_limit(capsys):
    limit = 4096
    exporter = LambdaLogExporter(max_record_size_bytes=limit)
    ops = [_op(f"s{i}", i, result=json.dumps("\u2028" * 300)) for i in range(10)]
    _run(exporter, ops)
    line = _printed_line(capsys)
    assert len(line.encode("utf-8")) <= limit
    assert json.loads(json.loads(line)["message"])["truncated"] is True


# -- rule 7: message cap --------------------------------------------------------


def test_error_messages_are_capped_at_8192_characters(capsys):
    exporter = LambdaLogExporter(max_record_size_bytes=0)
    op = OperationEndInfo(
        operation_id="f-id",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name="f",
        parent_id=None,
        start_time=T0,
        is_replayed=False,
        status=OperationStatus.FAILED,
        end_time=T1,
        result=None,
        error=ErrorObject(message="m" * 8193, type="E", data=None, stack_trace=None),
        attempt=1,
    )
    _run(
        exporter,
        [op],
        error=ErrorObject(message="x" * 8192, type="E", data=None, stack_trace=None),
    )
    record = json.loads(json.loads(_printed_line(capsys))["message"])
    # Exactly 8,192 characters is kept as is.
    assert record["error"]["message"] == "x" * 8192
    assert record["operationsByName"]["f"]["error"]["message"] == "m" * 8192 + "…"


def test_error_names_are_capped_at_8192_code_points(capsys):
    exporter = LambdaLogExporter(max_record_size_bytes=0)
    _run(
        exporter,
        [],
        error=ErrorObject(
            message="\U0001f600" * 8193, type="N" * 70_000, data=None, stack_trace=None
        ),
    )
    record = json.loads(json.loads(_printed_line(capsys))["message"])
    assert record["error"]["name"] == "N" * 8192 + "\u2026"
    # Code points, not UTF-16 units: 8,192 whole emoji are kept.
    assert record["error"]["message"] == "\U0001f600" * 8192 + "\u2026"


def test_long_error_name_fits_a_64_kb_line(capsys):
    limit = 65_536
    exporter = LambdaLogExporter(max_record_size_bytes=limit)
    ops = [_op(f"q{i}", i, result=json.dumps('"' * 2000)) for i in range(20)]
    _run(
        exporter,
        ops,
        error=ErrorObject(
            message="m" * 100, type="N" * 70_000, data=None, stack_trace=None
        ),
    )
    line = _printed_line(capsys)
    assert len(line.encode("utf-8")) <= limit
    record = json.loads(json.loads(line)["message"])
    assert record["error"]["name"] == "N" * 8192 + "\u2026"


def test_worst_case_capped_name_and_message_fit_a_128_kb_line(capsys):
    limit = 131_072
    exporter = LambdaLogExporter(max_record_size_bytes=limit)
    # \x01 is the worst case for both: 7 bytes each in the printed line.
    ops = [_op(f"q{i}", i, result=json.dumps('"' * 2000)) for i in range(40)]
    _run(
        exporter,
        ops,
        execution_input='"' * 50_000,
        error=ErrorObject(
            message="\x01" * 70_000, type="\x01" * 70_000, data=None, stack_trace=None
        ),
    )
    line = _printed_line(capsys)
    assert len(line.encode("utf-8")) <= limit
    record = json.loads(json.loads(line)["message"])
    assert record["error"] == {
        "name": "\x01" * 8192 + "\u2026",
        "message": "\x01" * 8192 + "\u2026",
    }
    assert record["droppedInput"] is True


# -- N-B1: a lone surrogate never drops the record -----------------------------


def test_lone_surrogate_in_error_message_keeps_the_dynamodb_item(caplog):
    client = StubDynamoDBClient()
    exporter = DynamoDBExporter(table_name="t", client=client)
    with caplog.at_level(logging.WARNING):
        _run(
            exporter,
            [],
            error=ErrorObject(
                message="a\ud800b", type="E", data=None, stack_trace=None
            ),
        )
    assert "failed" not in caplog.text
    assert len(client.items) == 1
    assert client.items[0]["error"]["M"]["message"] == {"S": "a\ud800b"}


def test_lone_surrogate_is_escaped_in_the_printed_line(monkeypatch, caplog):
    # A strict UTF-8 stdout, as in a real process; printing a raw lone
    # surrogate to it raises UnicodeEncodeError.
    raw = io.BytesIO()
    stdout = io.TextIOWrapper(raw, encoding="utf-8", errors="strict")
    monkeypatch.setattr(sys, "stdout", stdout)
    with caplog.at_level(logging.WARNING):
        _run(
            LambdaLogExporter(),
            [],
            error=ErrorObject(
                message="a\ud800b", type="E", data=None, stack_trace=None
            ),
        )
    stdout.flush()
    assert "failed" not in caplog.text
    line = raw.getvalue().decode("utf-8")
    assert line.count("\n") == 1
    assert "a\\ud800b" in line
    record = json.loads(json.loads(line)["message"])
    assert record["error"]["message"] == "a\ud800b"


def test_lone_surrogates_are_counted_in_the_line_limit(capsys):
    limit = 4096
    exporter = LambdaLogExporter(max_record_size_bytes=limit)
    ops = [_op(f"s{i}", i, result=json.dumps("\ud800" * 300)) for i in range(10)]
    _run(exporter, ops)
    line = _printed_line(capsys)
    assert len(line.encode("utf-8")) <= limit
    assert json.loads(json.loads(line)["message"])["truncated"] is True


# -- non-blocking 1: sizes are exact per exporter -------------------------------


def test_line_separators_are_counted_raw_for_other_exporters(caplog):
    client = StubDynamoDBClient()
    # 60,000 U+2028 are 180,000 bytes raw and 360,000 bytes escaped.
    exporter = DynamoDBExporter(
        table_name="t", client=client, max_record_size_bytes=200_000
    )
    with caplog.at_level(logging.WARNING):
        _run(exporter, [_op("s", 0, result=json.dumps("\u2028" * 60_000))])
    assert len(client.items) == 1
    item = client.items[0]
    assert "truncated" not in item
    assert item["operationsByName"]["M"]["s"]["M"]["result"] == {"S": "\u2028" * 60_000}


def test_lambda_log_measure_is_the_printed_line_size(capsys):
    exporter = LambdaLogExporter()
    record: dict[str, Any] = {
        "executionArn": ARN,
        "operations": [],
        "error": {"name": "E", "message": '\u2028\u2029\ud800é\U0001f600"\x01'},
    }
    size = exporter.measure(exporter.render(record))
    exporter.export(record)
    assert size == len(_printed_line(capsys).encode("utf-8"))
    assert exporter.measure({"a": object()}) is None


# -- non-blocking 2: integers beyond 2**53 - 1 ----------------------------------


def _results(capsys: pytest.CaptureFixture[str], *results: str) -> list[Any]:
    ops = [_op(f"op{i}", i, result=raw) for i, raw in enumerate(results)]
    _run(LambdaLogExporter(), ops)
    by_name = json.loads(json.loads(_printed_line(capsys))["message"])[
        "operationsByName"
    ]
    return [by_name[f"op{i}"]["result"] for i in range(len(results))]


def test_large_ints_become_strings(capsys):
    big = 12345678901234567
    safe = 2**53 - 1
    results = _results(
        capsys,
        _envelope({"x": big, "y": safe, "z": -big}),
        _envelope((1, -safe)),
        str(big),
        json.dumps([safe, -(2**60), {"k": 2**53}]),
        # Long digit runs in strings and floats are left alone.
        json.dumps({"s": "12345678901234567", "f": 0.12345678901234568}),
        str(safe),
    )
    assert results == [
        {"x": "12345678901234567", "y": safe, "z": "-12345678901234567"},
        [1, -safe],
        "12345678901234567",
        [safe, str(-(2**60)), {"k": str(2**53)}],
        {"s": "12345678901234567", "f": 0.12345678901234568},
        safe,
    ]


# -- non-blocking 7: integers with trailing zeros stay DynamoDB numbers ---------


def test_dynamodb_marshal_keeps_integers_with_few_significant_digits():
    exporter = DynamoDBExporter(table_name="t", client=StubDynamoDBClient())
    marshalled = exporter._marshal(
        {"a": [10**38, -(10**38), 10**125, 10**126, 10**38 + 1, 1.0e38]}
    )
    assert marshalled["a"]["L"] == [
        {"N": "1E+38"},
        {"N": "-1E+38"},
        {"N": "1E+125"},
        {"S": str(10**126)},
        {"S": str(10**38 + 1)},
        {"N": "1E+38"},
    ]
