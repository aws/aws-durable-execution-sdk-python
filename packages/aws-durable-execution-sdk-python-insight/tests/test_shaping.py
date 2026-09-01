# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the pure record-shaping helpers (no AWS)."""

from __future__ import annotations

from aws_durable_execution_sdk_python_insight.operations_index import (
    build_operations_by_name,
    with_operations_by_name,
)
from aws_durable_execution_sdk_python_insight.truncation import truncate_record


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
    record = {"recordType": "WorkflowInsight", "operations": [_op("greet")]}
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
        "recordType": "WorkflowInsight",
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
    # drop whole operations oldest-first (mirrors insight-16).
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
