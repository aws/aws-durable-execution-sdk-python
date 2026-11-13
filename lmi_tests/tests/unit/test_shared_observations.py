"""Public hook records preserve actual IDs and never fill missing observations."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from dataclasses import asdict
from itertools import count
import io
import json
import threading
import time
from typing import Any
from types import SimpleNamespace

import pytest

from aws_durable_execution_sdk_python.lambda_service import ErrorObject, OperationStatus
from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from lmi_tests import fixture
from aws_durable_execution_sdk_python_testing import invoker as local_invoker
from lmi_tests.shared_observations import (
    LifecycleObservationFactory,
    LifecycleObservations,
    OperationObservation,
)
from lmi_tests.tests.unit.support import invocation, lambda_context


class MemoryLedger:
    """Replace only evidence/control storage; the SDK local service stays real."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.lock = threading.Lock()

    def put_object(self, *, Bucket, Key, Body, **_kwargs):
        with self.lock:
            self.objects[Key] = Body
        return {}

    def get_object(self, *, Bucket, Key):
        with self.lock:
            return {"Body": io.BytesIO(self.objects[Key])}

    def records(self) -> list[dict[str, Any]]:
        with self.lock:
            records = [
                json.loads(body)
                for key, body in self.objects.items()
                if "/events/" in key
            ]
        return sorted(
            records, key=lambda record: (record["request"], record["sequence"])
        )

    def observations(self) -> list[dict[str, Any]]:
        return [record for record in self.records() if record["phase"] == "CONFORMANCE"]

    def wait_for(self, kind: str, timeout: float = 10) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = [
                record
                for record in self.observations()
                if record["observation"]["kind"] == kind
            ]
            if found:
                return found[-1]
            time.sleep(0.01)
        raise AssertionError(f"No actual {kind} observation")


class MemorySink:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def observe(self, kind: str, **fields: Any) -> dict[str, Any]:
        value = {"kind": kind, **fields}
        with self.lock:
            self.records.append(value)
        return value


def shared_payload(scenario: str = "success") -> dict[str, Any]:
    return {
        "runId": "unit",
        "caseId": "case",
        "cohortId": "cohort-1",
        "role": "victim",
        "coverageProfile": "standard",
        "sharedScenario": scenario,
        "scenario": scenario,
        "marker": "marker",
        "controlPrefix": "runs/unit/cases/owner/control/",
        "behaviorBudgets": {"observationGraceMs": 1},
    }


def install_ledger(monkeypatch) -> MemoryLedger:
    ledger = MemoryLedger()
    monkeypatch.setenv("LMI_RUN_ID", "unit")
    monkeypatch.setenv("LMI_COMMIT", "tested-commit")
    monkeypatch.setenv("LMI_BUCKET", "private-evidence")
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.setattr(fixture.boto3, "client", lambda *_args, **_kwargs: ledger)
    monkeypatch.setattr(fixture, "environment_id", lambda: "local-environment")
    original_context = local_invoker.create_test_lambda_context
    requests = count(1)

    def distinct_local_context(**kwargs):
        # The stock local service reuses one mock request ID. Supply its runtime
        # context before invoking the real fixture, never alter captured evidence.
        context = original_context(**kwargs)
        return type(context)(
            **{**asdict(context), "aws_request_id": f"local-request-{next(requests)}"}
        )

    monkeypatch.setattr(
        local_invoker, "create_test_lambda_context", distinct_local_context
    )
    return ledger


def user_info(
    operation_id: str,
    *,
    parent_id: str | None = None,
    name: str = "work",
    kind: OperationType = OperationType.STEP,
) -> UserFunctionStartInfo:
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=kind,
        sub_type=None,
        name=name,
        parent_id=parent_id,
        start_time=datetime.now(UTC),
        is_replayed=False,
        status=OperationStatus.STARTED,
        attempt=1,
    )


def test_missing_body_identity_stays_missing_instead_of_being_guessed() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    observer.body("work", "marker", 1)
    assert [item["kind"] for item in sink.records] == ["observation_error"]
    assert "operationId" not in sink.records[0]


@pytest.mark.parametrize("wrong", ["name", "attempt"])
def test_body_requires_matching_public_step_hook(wrong: str) -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    observer.on_user_function_start(user_info("actual-id"))
    observer.body(
        "other" if wrong == "name" else "work", "marker", 2 if wrong == "attempt" else 1
    )
    assert sink.records[-1]["kind"] == "observation_error"
    assert not any(item["kind"] == "body" for item in sink.records)


def test_concurrent_public_user_scopes_keep_their_actual_operation_ids() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    barrier = threading.Barrier(2)

    def body(identity: str) -> None:
        info = user_info(identity, parent_id="actual-parent")
        observer.on_user_function_start(info)
        barrier.wait(timeout=3)
        observer.body("work", identity, 1)
        observer.on_user_function_end(UserFunctionEndInfo.from_start_info(info, None))
        assert observer.current() is None

    with ThreadPoolExecutor(2) as executor:
        list(executor.map(body, ["first-id", "second-id"]))
    bodies = [item for item in sink.records if item["kind"] == "body"]
    assert {(item["operationId"], item["value"]) for item in bodies} == {
        ("first-id", "first-id"),
        ("second-id", "second-id"),
    }
    assert {
        item["taskId"] for item in sink.records if item["kind"] == "task_start"
    } == {item["taskId"] for item in sink.records if item["kind"] == "task_exit"}


def test_closed_ancestor_comes_from_the_real_parent_end_hook() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    parent = OperationEndInfo(
        operation_id="parent-id",
        operation_type=OperationType.CONTEXT,
        sub_type="Parallel",
        name="race",
        parent_id=None,
        start_time=None,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
    )
    observer.on_operation_end(parent)
    observer.on_user_function_start(
        user_info("child-id", parent_id="parent-id", kind=OperationType.CONTEXT)
    )
    assert observer.closed_ancestor() == "parent-id"
    assert sink.records[1] == {
        "kind": "scope_closed",
        "scopeId": "parent-id",
        "parentId": None,
    }


def test_missing_parent_completion_and_unmatched_task_end_are_visible() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    assert observer.closed_ancestor() is None
    info = user_info("child-id", parent_id="not-observed", kind=OperationType.CONTEXT)
    observer.on_user_function_start(info)
    assert observer.closed_ancestor() is None
    observer.on_user_function_end(
        UserFunctionEndInfo.from_start_info(user_info("wrong-id"), None)
    )
    assert sum(item["kind"] == "observation_error" for item in sink.records) == 3


def test_configuration_uses_hook_identity_and_reports_a_missing_hook() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    observer.on_user_function_start(user_info("root", kind=OperationType.EXECUTION))
    with observer.configuration("map", 1):
        observer.on_operation_start(
            OperationStartInfo(
                operation_id="actual-map",
                operation_type=OperationType.CONTEXT,
                sub_type="Map",
                name="map",
                parent_id=None,
                start_time=None,
                is_replayed=False,
                status=OperationStatus.STARTED,
            )
        )
    assert (
        next(item for item in sink.records if item["kind"] == "operation_config")[
            "operationId"
        ]
        == "actual-map"
    )
    with observer.configuration("missing-map", 1):
        pass
    assert sink.records[-1]["kind"] == "observation_error"


def test_checkpoint_snapshot_does_not_claim_reexecution_or_drop_stored_errors() -> None:
    original = OperationInfo(
        operation_id="stored-id",
        operation_type=OperationType.STEP,
        sub_type="Step",
        name="failure",
        parent_id=None,
        start_time=None,
        is_replayed=False,
        status=OperationStatus.FAILED,
        error=ErrorObject(
            type="ValueError", message="stored failure", data=None, stack_trace=None
        ),
    )
    record = OperationObservation.from_info(original).to_record()
    assert record["error"] == {"type": "ValueError", "message": "stored failure"}
    assert record["startedAtMs"] is None and record["isReplayed"] is False
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    info = InvocationStartInfo(
        request_id="actual-request",
        execution_arn="actual-execution",
        is_first_invocation=False,
        operations={"stored-id": original},
    )
    assert LifecycleObservationFactory(observer).create_plugin(info) is observer
    observer.on_invocation_start(info)
    assert sink.records[0]["operations"] == [record]
    assert not any(item["kind"] == "body" for item in sink.records)


def test_trace_records_immutable_case_identity_and_actual_source_sequence(
    monkeypatch,
) -> None:
    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    trace = fixture.Trace(payload, lambda_context(30_000), "actual-execution")
    observed_at = time.time() * 1000 - 10
    record = trace.observe("boundary", atMs=observed_at)
    assert record["observation"] == {
        "kind": "boundary",
        "atMs": observed_at,
        "sourceSequence": record["sequence"],
        "captureSequence": 1,
    }
    assert record["caseId"] == "case"
    assert trace.payload["run"] == "unit"
    assert ledger.observations() == [record]


@pytest.mark.parametrize("field", ["caseId", "role", "controlPrefix"])
def test_shared_case_cannot_silently_drop_ownership_metadata(
    monkeypatch, field: str
) -> None:
    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    del payload[field]
    with pytest.raises(ValueError, match=field):
        fixture.Trace(payload, lambda_context(30_000), "execution")
    assert ledger.records() == []


def test_shared_case_cannot_fall_back_to_global_controls(monkeypatch) -> None:
    install_ledger(monkeypatch)
    payload = shared_payload()
    payload["controlPrefix"] = "runs/unit/control/"
    with pytest.raises(ValueError, match="case-owned"):
        fixture.Trace(payload, lambda_context(30_000), "execution")


@pytest.mark.parametrize("mode", ["normal", "emergency"])
def test_gate_release_reports_observed_provenance_and_keeps_controls_case_owned(
    monkeypatch, mode: str
) -> None:
    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    trace = fixture.Trace(payload, lambda_context(30_000), "execution")
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + "gate"] = b"release" if mode == "normal" else b"hold"
    ledger.objects[prefix + "release-all"] = b"hold" if mode == "normal" else b"release"
    ledger.objects["runs/unit/cases/other/control/gate"] = b"hold"
    released = trace.gate_release("gate")
    assert released.mode == mode
    assert ledger.objects["runs/unit/cases/other/control/gate"] == b"hold"
    observations = [item["observation"] for item in ledger.observations()]
    assert [item["kind"] for item in observations] == [
        "io_start",
        "fault_released",
        "io_exit",
    ]
    assert observations[0]["taskId"] == observations[-1]["taskId"]
    assert observations[1]["atMs"] == released.observed_at_ms


def test_root_scope_requires_the_public_invocation_operation() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    with observer.root_scope():
        pass
    assert [item["kind"] for item in sink.records] == ["observation_error"]
    assert observer.completed_context("unknown") is None
    assert sink.records[-1]["kind"] == "observation_error"


def test_gate_preserves_the_existing_checkpointed_none_result(monkeypatch) -> None:
    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    ledger.objects[payload["controlPrefix"] + "gate"] = b"release"
    ledger.objects[payload["controlPrefix"] + "release-all"] = b"hold"
    trace = fixture.Trace(payload, lambda_context(30_000), "execution")
    assert trace.gate("gate") is None


@pytest.mark.parametrize("method", ["checkpoint", "get_execution_state"])
def test_failed_real_client_call_keeps_its_dispatch_and_exit_identity(
    monkeypatch, method: str
) -> None:
    from unittest.mock import Mock

    ledger = install_ledger(monkeypatch)
    trace = fixture.Trace(shared_payload(), lambda_context(30_000), "execution")
    backend = Mock()
    getattr(backend, method).side_effect = OSError("service unavailable")
    client = fixture.ObservedClient(backend, trace)
    with pytest.raises(OSError, match="service unavailable"):
        getattr(client, method)(**({"updates": []} if method == "checkpoint" else {}))
    observations = [record["observation"] for record in ledger.observations()]
    prefix = "checkpoint" if method == "checkpoint" else "poll"
    entered = next(item for item in observations if item["kind"] == prefix + "_call")
    exited = next(item for item in observations if item["kind"] == prefix + "_exit")
    assert entered["callId"] == exited["callId"] and entered["atMs"] <= exited["atMs"]
    assert not any(item["kind"] == "checkpoint_ack" for item in observations)


@pytest.mark.parametrize("completed_ns", [1794607984811568141, 1794607984811569459])
def test_pending_and_wrapper_return_share_one_captured_timestamp(
    monkeypatch, completed_ns
):
    ledger = install_ledger(monkeypatch)
    original = invocation()
    payload = original.to_json_dict()
    payload["InitialExecutionState"]["Operations"][0]["ExecutionDetails"][
        "InputPayload"
    ] = json.dumps(shared_payload())
    injected = fixture.DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        fixture.DurableExecutionInvocationInput.from_json_dict(payload),
        original.service_client,
    )
    monkeypatch.setattr(
        fixture, "time", SimpleNamespace(time=time.time, time_ns=lambda: completed_ns)
    )
    monkeypatch.setattr(
        fixture,
        "durable_execution",
        lambda *args, **kwargs: lambda *args: {"Status": "PENDING"},
    )
    assert fixture.handler(injected, lambda_context(30000)) == {"Status": "PENDING"}
    observations = [record["observation"] for record in ledger.observations()]
    wrapper = next(item for item in observations if item["kind"] == "wrapper_return")
    pending = next(item for item in observations if item["kind"] == "pending")
    assert pending["atMs"] == wrapper["atMs"] == completed_ns / 1_000_000


@pytest.mark.parametrize("kind", ["checkpoint", "background", "suspend"])
@pytest.mark.parametrize("effect", ["none", "body", "rpc"])
def test_stored_sdk_errors_are_rejections_only_before_new_work(
    monkeypatch, kind, effect
):
    ledger = install_ledger(monkeypatch)
    trace = fixture.Trace(shared_payload(), lambda_context(30000), "execution")
    errors = {
        "checkpoint": lambda: fixture.CheckpointError("Invalid checkpoint token"),
        "background": lambda: fixture.BackgroundThreadError(
            "failed", ValueError("source")
        ),
        "suspend": lambda: fixture.SuspendExecution("revoked"),
    }

    def checkpoint(**options):
        if effect == "rpc":
            trace.next_call_id("checkpoint")
        raise errors[kind]()

    def step(body, **options):
        if effect == "body":
            body(None)
        if effect == "rpc":
            trace.next_call_id("checkpoint")
        raise errors[kind]()

    trace.durable_context = SimpleNamespace(
        state=SimpleNamespace(create_checkpoint=checkpoint), step=step
    )
    fixture.admission_probes(trace)
    probes = [
        r["observation"]
        for r in ledger.observations()
        if r["observation"]["kind"] == "admission_probe"
    ]
    assert probes[0]["outcome"] == ("ERROR" if effect == "rpc" else "REJECTED")
    assert probes[1]["outcome"] == ("REJECTED" if effect == "none" else "ERROR")
