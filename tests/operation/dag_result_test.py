"""T6: DagResult accessors + serialization round-trip."""

from __future__ import annotations

from aws_durable_execution_sdk_python.concurrency import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    SkipReason,
    TaskExecution,
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
