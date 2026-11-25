# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the pure record-shaping helpers (no AWS)."""

from __future__ import annotations

import random
from typing import Any

from aws_durable_execution_sdk_python_telemetry_exporter.operations_index import (
    OperationsFormat,
    apply_operations_format,
    build_operations_by_name,
    with_operations_by_name,
)
from aws_durable_execution_sdk_python_telemetry_exporter.truncation import (
    json_byte_size,
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


def _named_ops_record(count: int) -> dict[str, Any]:
    ops = [
        {
            "id": f"{i:016x}",
            "name": f"step-{i:05d}",
            "type": "STEP",
            "subType": "Step",
            "status": "SUCCEEDED",
            "startTime": (
                f"2026-01-01T{i // 3600:02d}:{i // 60 % 60:02d}:{i % 60:02d}.000Z"
            ),
            "durationMs": 12,
            "attempt": 1,
        }
        for i in range(count)
    ]
    return {
        "recordType": "DurableExecutionTelemetry",
        "schemaVersion": "1.0",
        "executionArn": "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/exec/inv",
        "status": "SUCCEEDED",
        "startTime": "2026-01-01T00:00:00.000Z",
        "operations": ops,
    }


def test_truncation_renders_a_logarithmic_number_of_times():
    from aws_durable_execution_sdk_python_telemetry_exporter.exporters.lambda_log_exporter import (
        LambdaLogExporter,
    )

    exporter = LambdaLogExporter()
    renders = 0

    def render(record: dict[str, Any]) -> Any:
        nonlocal renders
        renders += 1
        return exporter.render(record)

    record = _named_ops_record(2000)
    out = truncate_record(record, 256_000, render, exporter.measure)
    size = exporter.measure(exporter.render(out))
    assert size is not None and size <= 256_000
    assert out["droppedOperations"] > 0
    # Two binary searches over 2,001 drop counts need about 2 * 11 renders.
    assert renders < 40


def test_truncation_renders_a_logarithmic_number_of_times_for_a_custom_render():
    renders = 0

    def render(record: dict[str, Any]) -> Any:
        nonlocal renders
        renders += 1
        return {"wrapped": [dict(op, extra="e") for op in record["operations"]]}

    out = truncate_record(_named_ops_record(2000), 100_000, render)
    size = json_byte_size(render(out))
    assert size is not None and size <= 100_000
    assert out["droppedOperations"] > 0
    assert renders < 40


_NAMES = ["a", "a", "b", "", 'q"uote', "back\\slash", "ünï", "😀", "\u2028", None]


def _random_record(rng: random.Random) -> dict[str, Any]:
    ops = []
    for i in range(rng.randint(0, 30)):
        op: dict[str, Any] = {
            "id": f"{i:016x}",
            "type": "STEP",
            "status": rng.choice(["SUCCEEDED", "FAILED"]),
            "durationMs": rng.randint(0, 100),
            "attempt": rng.randint(1, 3),
        }
        name = rng.choice(_NAMES)
        if name is not None:
            op["name"] = name
        if rng.random() < 0.9:
            op["startTime"] = f"2026-01-01T00:00:{rng.randint(0, 59):02d}.000Z"
        if rng.random() < 0.5:
            op["result"] = rng.choice(['"x"', "ü" * rng.randint(0, 80), "\\" * 9])
        if rng.random() < 0.3:
            op["error"] = {"name": "E", "message": "m" * rng.randint(0, 60)}
        ops.append(op)
    record: dict[str, Any] = {"executionArn": "arn", "operations": ops}
    if rng.random() < 0.7:
        record["input"] = "i" * rng.randint(0, 200)
    if rng.random() < 0.7:
        record["output"] = '"o"' * rng.randint(0, 50)
    return record


def _oldest_first(record: dict[str, Any]) -> list[str]:
    ops = record["operations"]
    order = sorted(
        range(len(ops)), key=lambda i: (ops[i].get("startTime") or "\uffff", i)
    )
    return [ops[i]["id"] for i in order]


def test_truncation_never_exceeds_the_limit_and_drops_oldest_first():
    rng = random.Random(908)
    for _ in range(300):
        record = _random_record(rng)
        fmt = rng.choice(list(OperationsFormat))

        def render(r: dict[str, Any], fmt: OperationsFormat = fmt) -> Any:
            return apply_operations_format(r, fmt)

        full = json_byte_size(render(record))
        assert full is not None
        limit = rng.randint(1, full + 10)
        out = truncate_record(record, limit, render)
        size = json_byte_size(render(out))
        assert size is not None
        # Over the limit only when nothing is left to drop.
        assert size <= limit or (
            not out["operations"] and "input" not in out and "output" not in out
        )
        # The kept operations are the newest ones.
        ids = _oldest_first(record)
        kept = {op["id"]: op for op in out["operations"]}
        assert [i for i in ids if i in kept] == ids[len(ids) - len(kept) :]
        # Results are cleared oldest first.
        had_result = {op["id"] for op in record["operations"] if "result" in op}
        cleared = [
            "result" not in kept[i] for i in ids if i in kept and i in had_result
        ]
        assert cleared == sorted(cleared, reverse=True)


def _one_at_a_time(record: dict[str, Any], limit: int) -> dict[str, Any]:
    # The truncation before the binary search, for the identity render.
    ids = _oldest_first(record)
    ops = {op["id"]: dict(op) for op in record["operations"]}
    out: dict[str, Any] = {}
    dropped = 0

    def fits() -> bool:
        out.clear()
        out.update(record, truncated=True)
        out["operations"] = [
            ops[op["id"]] for op in record["operations"] if op["id"] in ops
        ]
        if dropped:
            out["droppedOperations"] = dropped
        size = json_byte_size(out)
        return size is not None and size <= limit

    for i in ids:
        if fits():
            return out
        if ops[i].pop("result", None) is not None:
            ops[i]["truncated"] = True
    for i in ids:
        if fits():
            return out
        del ops[i]
        dropped += 1
    fits()
    return out


def test_truncation_matches_one_at_a_time_drops_for_the_array_format():
    rng = random.Random(1174)
    for _ in range(300):
        record = _random_record(rng)
        record.pop("input", None)
        record.pop("output", None)
        full = json_byte_size(record)
        assert full is not None
        limit = rng.randint(1, full - 1) if full > 1 else 1
        expected = _one_at_a_time(record, limit)
        out = truncate_record(record, limit)
        if out is record:
            continue
        assert out == expected


def test_by_name_truncation_can_drop_more_than_the_fewest_but_fits():
    # Dropping the older "x" makes the error of the newer "x" appear in the
    # by-name map. So the size does not shrink with every drop. Dropping one
    # operation fits, but the binary search measures 5 drops first, which do
    # not fit, and then settles on 6.
    ops = [_op(f"p{i}", id=f"{i:02d}", startTime=f"t{i:02d}") for i in range(4)]
    ops.append(_op("x", id="04", startTime="t04"))
    error = {"name": "E", "message": "m" * 2000}
    ops.append(_op("x", id="05", startTime="t05", status="FAILED", error=error))
    ops += [
        _op(f"q{i}", id=f"{6 + i:02d}", startTime=f"t{6 + i:02d}") for i in range(4)
    ]
    record = {"executionArn": "arn", "operations": ops}
    full = json_byte_size(with_operations_by_name(record))
    assert full is not None
    out = truncate_record(record, full - 10, with_operations_by_name)
    assert out["droppedOperations"] == 6
    size = json_byte_size(with_operations_by_name(out))
    assert size is not None and size <= full - 10
