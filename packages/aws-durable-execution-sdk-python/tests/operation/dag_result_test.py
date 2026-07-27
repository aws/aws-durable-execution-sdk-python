"""T6: DagResult accessors + serialization round-trip."""

from __future__ import annotations

from aws_durable_execution_sdk_python.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    SkipReason,
    TaskExecution,
    TaskHandle,
    TaskStatus,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.dag_result import (
    DagResultImpl,
    create_dag_result_serdes,
    dag_reason_from_core,
)


def _sample_results():
    return {
        "a": TaskExecution("a", TaskStatus.SUCCEEDED, result={"v": 1}),
        "b": TaskExecution(
            "b", TaskStatus.FAILED, error=ErrorObject.from_message("boom")
        ),
        "c": TaskExecution("c", TaskStatus.SKIPPED, skip_reason=SkipReason.TRIGGER_RULE),
    }


def test_accessors():
    r = DagResultImpl(
        _sample_results(),
        DagCompletionReason.COMPLETED_WITH_FAILURES,
        {"a": "step", "b": "step", "c": "step"},
    )
    assert r.success_count == 1
    assert r.failure_count == 1
    assert r.skipped_count == 1
    assert r.total_count == 3
    assert r.get_status("a") is TaskStatus.SUCCEEDED
    assert r.get_result("a") == {"v": 1}
    # C3: a TaskHandle arg resolves by its name (typed path), same as string.
    handle: TaskHandle = TaskHandle(_name="a", _dag=None)
    assert r.get_result(handle) == {"v": 1}
    assert r.get_status(handle) is TaskStatus.SUCCEEDED
    assert r.get_status("missing") is None
    assert [t.name for t in r.succeeded()] == ["a"]
    assert [t.name for t in r.failed()] == ["b"]
    assert [t.name for t in r.skipped()] == ["c"]


def test_dag_reason_from_core():
    assert (
        dag_reason_from_core(CompletionReason.ALL_COMPLETED)
        is DagCompletionReason.ALL_COMPLETED
    )
    assert (
        dag_reason_from_core(CompletionReason.MIN_SUCCESSFUL_REACHED)
        is DagCompletionReason.MIN_SUCCESSFUL_REACHED
    )


def test_roundtrip_plain_and_error():
    r = DagResultImpl(
        _sample_results(),
        DagCompletionReason.COMPLETED_WITH_FAILURES,
        {"a": "step", "b": "step", "c": "step"},
    )
    serdes = create_dag_result_serdes()
    data = serdes.serialize(r, None)
    restored = serdes.deserialize(data, None)
    assert restored.completion_reason is DagCompletionReason.COMPLETED_WITH_FAILURES
    assert restored.get_result("a") == {"v": 1}
    assert restored.get_status("c") is TaskStatus.SKIPPED
    assert restored.results["c"].skip_reason is SkipReason.TRIGGER_RULE
    assert restored.results["b"].error.message == "boom"


def test_roundtrip_batch_result_kind():
    batch = BatchResult(
        [BatchItem(0, BatchItemStatus.SUCCEEDED, "x")], CompletionReason.ALL_COMPLETED
    )
    results = {"m": TaskExecution("m", TaskStatus.SUCCEEDED, result=batch)}
    r = DagResultImpl(results, DagCompletionReason.ALL_COMPLETED, {"m": "map"})
    restored = DagResultImpl.from_dict(r.to_dict())
    inner = restored.get_result("m")
    assert isinstance(inner, BatchResult)
    assert inner.get_results() == ["x"]


def test_roundtrip_nested_dag_result_kind():
    inner = DagResultImpl(
        {"x": TaskExecution("x", TaskStatus.SUCCEEDED, result=42)},
        DagCompletionReason.ALL_COMPLETED,
        {"x": "step"},
    )
    outer = DagResultImpl(
        {"nested": TaskExecution("nested", TaskStatus.SUCCEEDED, result=inner)},
        DagCompletionReason.ALL_COMPLETED,
        {"nested": "dag"},
    )
    restored = DagResultImpl.from_dict(outer.to_dict())
    nested = restored.get_result("nested")
    assert isinstance(nested, DagResultImpl)
    assert nested.get_result("x") == 42


def test_envelope_shape_with_tasks():
    """The converged envelope: type, always-present aggregates + null-explicit
    per-task fields (contract rules 1 and 3)."""
    import datetime

    started = datetime.datetime(2026, 7, 26, 3, 19, 1, 884000, tzinfo=datetime.UTC)
    completed = datetime.datetime(2026, 7, 26, 3, 19, 1, 885000, tzinfo=datetime.UTC)
    results = {
        "load": TaskExecution(
            "load",
            TaskStatus.SUCCEEDED,
            result="ok",
            started_at=started,
            completed_at=completed,
        ),
        "charge": TaskExecution(
            "charge", TaskStatus.FAILED, error=ErrorObject.from_message("boom")
        ),
        "ship": TaskExecution(
            "ship", TaskStatus.SKIPPED, skip_reason=SkipReason.TRIGGER_RULE
        ),
    }
    env = DagResultImpl(
        results,
        DagCompletionReason.COMPLETED_WITH_FAILURES,
        {"load": "step", "charge": "step", "ship": "step"},
    ).to_dict()

    assert env["type"] == "DagResult"
    assert env["totalCount"] == 3
    assert env["successCount"] == 1
    assert env["failureCount"] == 1
    assert env["skippedCount"] == 1
    assert env["completionReason"] == "COMPLETED_WITH_FAILURES"
    assert env["startedTaskNames"] == []
    assert env["failedTaskNames"] == ["charge"]

    by_name = {t["name"]: t for t in env["tasks"]}
    # Every canonical per-task field is present, null when unset.
    for t in env["tasks"]:
        assert set(t) == {
            "name",
            "status",
            "skipReason",
            "resultKind",
            "result",
            "error",
            "startedAt",
            "completedAt",
        }
    assert by_name["load"]["startedAt"] == "2026-07-26T03:19:01.884Z"
    assert by_name["load"]["completedAt"] == "2026-07-26T03:19:01.885Z"
    assert by_name["load"]["skipReason"] is None
    assert by_name["load"]["resultKind"] == "plain"
    assert by_name["ship"]["skipReason"] == "TRIGGER_RULE"
    assert by_name["ship"]["startedAt"] is None
    # Canonical PascalCase error object with explicit nulls.
    err = by_name["charge"]["error"]
    assert err == {"ErrorMessage": "boom", "ErrorType": None, "StackTrace": None}


def test_envelope_tasks_dropped_is_valid():
    """The offloaded case is the same envelope minus ``tasks``; from_dict yields
    an empty results map and preserves the aggregates."""
    data = {
        "type": "DagResult",
        "totalCount": 8,
        "successCount": 6,
        "failureCount": 1,
        "skippedCount": 1,
        "completionReason": "COMPLETED_WITH_FAILURES",
        "startedTaskNames": ["reserve"],
        "failedTaskNames": ["charge"],
        # no "tasks"
    }
    restored = DagResultImpl.from_dict(data)
    assert restored.completion_reason is DagCompletionReason.COMPLETED_WITH_FAILURES
    assert restored.total_count == 8
    assert dict(restored.results) == {}


def test_from_dict_ignores_unknown_fields():
    """Contract rule 4: readers MUST ignore unknown fields and treat a missing
    field as absent rather than failing (additive-only evolution)."""
    env = DagResultImpl(
        {"a": TaskExecution("a", TaskStatus.SUCCEEDED, result=1)},
        DagCompletionReason.ALL_COMPLETED,
        {"a": "step"},
    ).to_dict()
    env["schemaVersion"] = "v99"  # unknown top-level field
    env["tasks"][0]["futureField"] = {"any": "thing"}  # unknown per-task field
    restored = DagResultImpl.from_dict(env)
    assert restored.get_result("a") == 1
    assert restored.completion_reason is DagCompletionReason.ALL_COMPLETED
