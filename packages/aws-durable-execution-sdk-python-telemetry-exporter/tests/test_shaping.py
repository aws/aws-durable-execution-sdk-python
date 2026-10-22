# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the pure record-shaping helpers (no AWS)."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python_telemetry_exporter.operations_index import (
    OperationsFormat,
    apply_operations_format,
    build_operations_by_name,
    with_operations_by_name,
)
from aws_durable_execution_sdk_python_telemetry_exporter.truncation import (
    truncate_record,
)


def _op(name, **kw):
    base = {
        "id": kw.get("id", name),
        "name": name,
        "type": "STEP",
        "subType": "Step",
        "status": "SUCCEEDED",
    }
    base.update(kw)
    return base


def test_by_name_single_occurrence_keeps_result_and_error():
    ops = [_op("greet", result="hi", durationMs=5, attempt=1)]
    summary = build_operations_by_name(ops)["greet"]
    assert summary["count"] == 1
    assert summary["failedCount"] == 0
    assert summary["result"] == "hi"
    assert summary["maxAttempt"] == 1


def test_by_name_repeated_name_drops_result_and_error_and_aggregates():
    ops = [
        _op("task", id="a", result=1, durationMs=2, attempt=1),
        _op("task", id="b", result=2, durationMs=4, attempt=2),
        _op("task", id="c", result=3, durationMs=6, attempt=1),
    ]
    summary = build_operations_by_name(ops)["task"]
    assert summary["count"] == 3
    assert "result" not in summary
    assert "error" not in summary
    assert summary["maxAttempt"] == 2
    assert summary["minDurationMs"] == 2
    assert summary["maxDurationMs"] == 6
    assert summary["totalDurationMs"] == 12


def test_by_name_failed_count():
    ops = [
        _op("task", id="a", status="FAILED"),
        _op("task", id="b", status="SUCCEEDED"),
    ]
    summary = build_operations_by_name(ops)["task"]
    assert summary["failedCount"] == 1
    assert summary["count"] == 2


def test_unnamed_operations_are_skipped_in_index():
    ops = [_op("named"), {"id": "x", "type": "STEP", "status": "SUCCEEDED"}]
    result = build_operations_by_name(ops)
    assert set(result.keys()) == {"named"}


def test_with_operations_by_name_replaces_array():
    record = {"recordType": "DurableExecutionTelemetry", "operations": [_op("greet")]}
    shaped = with_operations_by_name(record)
    assert "operations" not in shaped
    assert "operationsByName" in shaped
    assert shaped["operationsByName"]["greet"]["count"] == 1


def _record_with_results(sizes):
    ops = []
    for i, size in enumerate(sizes):
        ops.append(
            {
                "id": f"{i:016x}",
                "name": f"bulk-{i + 1}",
                "type": "STEP",
                "subType": "Step",
                "status": "SUCCEEDED",
                "startTime": f"2026-01-01T00:00:0{i}.000Z",
                "attempt": 1,
                "result": "x" * size,
            }
        )
    return {
        "recordType": "DurableExecutionTelemetry",
        "schemaVersion": "1.0",
        "executionArn": "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/exec/inv",
        "status": "SUCCEEDED",
        "startTime": "2026-01-01T00:00:00.000Z",
        "input": "World",
        "output": "done",
        "operations": ops,
    }


def test_truncation_phase1_drops_results_oldest_first_keeps_all_ops():
    record = _record_with_results([2000, 2000, 2000])
    out = truncate_record(record, 4096, render=lambda r: r)
    assert out["truncated"] is True
    assert "droppedOperations" not in out
    ops = {op["name"]: op for op in out["operations"]}
    assert len(ops) == 3
    assert "result" not in ops["bulk-1"] and ops["bulk-1"]["truncated"] is True
    assert "result" not in ops["bulk-2"] and ops["bulk-2"]["truncated"] is True
    assert "result" in ops["bulk-3"]  # newest keeps its result


def test_truncation_phase2_drops_whole_ops_oldest_first():
    # bulk-1 and bulk-2 carry oversized results; bulk-3 has none. After Phase 1
    # drops both results the record is still over the limit, forcing Phase 2 to
    # drop whole operations oldest-first (mirrors telemetry-16).
    record = _record_with_results([2000, 2000, 0])
    record["operations"][2].pop("result", None)
    out = truncate_record(record, 480, render=lambda r: r)
    assert out["truncated"] is True
    assert out.get("droppedOperations", 0) >= 1
    names = {op["name"] for op in out["operations"]}
    assert "bulk-1" not in names  # oldest dropped
    assert "bulk-3" in names  # newest retained


def test_truncation_noop_when_within_limit():
    record = _record_with_results([5])
    out = truncate_record(record, 5_000_000, render=lambda r: r)
    assert out is record


def test_apply_operations_format_array_returns_record_unchanged():
    record = {"executionArn": "arn", "operations": [_op("greet")]}
    assert apply_operations_format(record, "array") is record
    assert apply_operations_format(record, OperationsFormat.ARRAY) is record


def test_apply_operations_format_by_name_replaces_array():
    record = {"executionArn": "arn", "operations": [_op("greet")]}
    out = apply_operations_format(record, "by-name")
    assert "operations" not in out
    assert out["operationsByName"]["greet"]["count"] == 1
    assert "operations" in record  # input never mutated


def test_apply_operations_format_both_keeps_array_and_adds_map():
    record = {"executionArn": "arn", "operations": [_op("greet")]}
    out = apply_operations_format(record, OperationsFormat.BOTH)
    assert out["operations"] == record["operations"]
    assert out["operationsByName"]["greet"]["count"] == 1
    assert "operationsByName" not in record


def test_operations_format_enum_values():
    assert {f.value for f in OperationsFormat} == {"array", "by-name", "both"}


def test_truncation_drops_every_cause_first():
    cause = {
        "name": "Inner",
        "message": "c" * 400,
        "cause": {"name": "E", "message": "x"},
    }
    op_error = {"name": "E", "message": "m", "cause": cause}
    record = {
        "executionArn": "arn",
        "error": {"name": "E", "message": "m", "cause": cause},
        "operations": [_op("a", error=op_error, result="r")],
    }
    out = truncate_record(record, 300, render=lambda r: r)
    assert out["droppedCauses"] is True
    assert out["error"] == {"name": "E", "message": "m"}
    assert out["operations"][0]["error"] == {"name": "E", "message": "m"}
    # Dropping causes was enough: later phases did not run.
    assert out["operations"][0]["result"] == "r"
    # The input record is never mutated.
    assert record["error"]["cause"] is cause
    assert record["operations"][0]["error"]["cause"] is cause


def test_truncation_without_causes_sets_no_dropped_causes():
    record = _record_with_results([2000])
    out = truncate_record(record, 400, render=lambda r: r)
    assert out["truncated"] is True
    assert "droppedCauses" not in out


def test_json_byte_size_is_exact_for_line_separators():
    from aws_durable_execution_sdk_python_telemetry_exporter.truncation import (
        json_byte_size,
    )

    # Written raw by every exporter except LambdaLogExporter: 3 bytes each.
    assert json_byte_size({"a": "\u2028" * 10}) == 38
    assert json_byte_size("\u2028\u2029") == len('"\u2028\u2029"'.encode())


def test_json_byte_size_returns_none_for_a_lone_surrogate():
    from aws_durable_execution_sdk_python_telemetry_exporter.truncation import (
        json_byte_size,
    )

    assert json_byte_size({"a": "a\ud800b"}) is None


def test_truncate_record_uses_the_given_measure():
    record = _record_with_results([100])
    sizes: list[Any] = []

    def measure(value: Any) -> int:
        sizes.append(value)
        return 10**6

    out = truncate_record(record, 1000, render=lambda r: r, measure=measure)
    assert sizes and out["truncated"] is True
    # The default measure would keep this small record unchanged.
    assert truncate_record(record, 1000, render=lambda r: r) is record
