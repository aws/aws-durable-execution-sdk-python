"""Public hook records preserve actual IDs and never fill missing observations."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from dataclasses import asdict
from inspect import signature
from itertools import count
import io
import json
import threading
import time
from typing import Any
from unittest.mock import Mock
from types import SimpleNamespace

from botocore.exceptions import ClientError, ReadTimeoutError
import pytest

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    StateOutput,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    OperationInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from lmi_tests import fixture
from aws_durable_execution_sdk_python_testing import invoker as local_invoker
from lmi_tests.shared_observations import (
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
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key

    return {
        "runId": "unit",
        "caseId": "case",
        "role": "victim",
        "coverageProfile": "standard",
        "sharedScenario": scenario,
        "scenario": scenario,
        "marker": "marker",
        "controlPrefix": f"runs/unit/cases/{case_key('case')}/control/",
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
    install_distinct_local_request_ids(monkeypatch)
    return ledger


def install_distinct_local_request_ids(monkeypatch) -> None:
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


@pytest.fixture
def producer(monkeypatch):
    ledger = install_ledger(monkeypatch)
    trace = fixture.Trace(shared_payload(), lambda_context(30_000), "execution")
    return trace, ledger


def test_seal_counts_successful_observations_in_publication_order(producer):
    trace, ledger = producer
    trace.observe("first", atMs=20)
    trace.observe("second", atMs=10)
    trace.finish_wrapper()
    rows = ledger.records()
    assert [row["phase"] for row in rows] == ["CONFORMANCE", "CONFORMANCE", "SEALED"]
    assert [row["observation"]["captureSequence"] for row in rows[:-1]] == [1, 2]
    assert [row["observation"]["atMs"] for row in rows[:-1]] == [20, 10]
    assert rows[-1]["throughCapture"] == 2


def check_seal_with_installed_consumer(rows, *, valid):
    from aws_durable_execution_conformance_tests.lmi import source_evidence

    validator = getattr(source_evidence, "ledger_seal_status", None)
    if validator is None:
        # The published workflow still uses a consumer without seal support.
        return
    launch = {
        "marker": rows[0]["marker"],
        "arn": rows[0]["execution"],
        "native_requests": [rows[0]["request"]],
    }
    if "launches" in signature(validator).parameters:
        known, missing, errors = validator(rows, {}, [launch], set())
    else:
        known, missing, errors = validator(rows, {})
    assert known == {rows[0]["request"]}
    assert not missing
    assert bool(errors) is not valid


@pytest.mark.parametrize("closed_stdout", [False, True])
def test_post_seal_poll_reaches_backend_and_invalidates_last_record(
    producer, monkeypatch, closed_stdout
):
    trace, ledger = producer
    trace.emit("WRAPPER_ENTER")
    trace.finish_wrapper()
    if closed_stdout:
        monkeypatch.setattr(
            fixture,
            "print",
            Mock(side_effect=ValueError("closed stdout")),
            raising=False,
        )
    output = StateOutput()
    backend = Mock()

    def poll(**kwargs):
        assert [row["phase"] for row in ledger.records()][-2:] == [
            "SEALED",
            "POLL_CALL",
        ]
        return output

    backend.get_execution_state.side_effect = poll
    assert (
        fixture.ObservedClient(backend, trace).get_execution_state(next_marker="late")
        is output
    )
    rows = ledger.records()
    assert [row["phase"] for row in rows].count("SEALED") == 1
    assert [row["phase"] for row in rows].count("POLL_CALL") == 1
    assert rows[-1]["observation"]["kind"] == "poll_exit"
    check_seal_with_installed_consumer(rows, valid=False)
    assert rows[-1]["sequence"] > next(
        row["sequence"] for row in rows if row["phase"] == "SEALED"
    )


def test_post_seal_checkpoint_reaches_backend_and_is_written_once(producer):
    trace, ledger = producer
    trace.emit("WRAPPER_ENTER")
    trace.finish_wrapper()
    backend = Mock()

    def checkpoint(**kwargs):
        assert ledger.records()[-1]["phase"] == "CHECKPOINT_CALL"
        return "original result"

    backend.checkpoint.side_effect = checkpoint
    assert (
        fixture.ObservedClient(backend, trace).checkpoint(updates=[])
        == "original result"
    )
    rows = ledger.records()
    assert [row["phase"] for row in rows].count("CHECKPOINT_CALL") == 1
    assert rows[-1]["observation"]["kind"] == "checkpoint_exit"
    check_seal_with_installed_consumer(rows, valid=False)


@pytest.mark.parametrize("method", ["poll", "checkpoint"])
def test_running_sdk_call_defers_seal_until_exit_record(producer, method):
    trace, ledger = producer
    trace.emit("WRAPPER_ENTER")
    entered = threading.Event()
    release = threading.Event()
    output = object()

    def backend_call(**kwargs):
        entered.set()
        assert release.wait(timeout=2)
        return output

    client = fixture.ObservedClient(
        SimpleNamespace(get_execution_state=backend_call, checkpoint=backend_call),
        trace,
    )
    call = (
        client.get_execution_state
        if method == "poll"
        else lambda: client.checkpoint(updates=[])
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(call)
        try:
            assert entered.wait(timeout=2)
            trace.finish_wrapper()
            assert [row["phase"] for row in ledger.records()] == ["WRAPPER_ENTER"]
        finally:
            release.set()
        assert pending.result(timeout=2) is output
    rows = ledger.records()
    assert [row["phase"] for row in rows].count("SEALED") == 1
    assert rows[-2]["observation"]["kind"] == method + "_exit"
    assert rows[-1]["phase"] == "SEALED"
    check_seal_with_installed_consumer(rows, valid=True)


def test_dispatch_timestamp_does_not_include_prior_publication(producer, monkeypatch):
    trace, ledger = producer
    calls = []
    clock = iter([2_000_000, 3_000_000])
    now = fixture.time.time_ns

    def timestamp():
        return next(clock, now())

    def poll(**kwargs):
        calls.append(len(ledger.records()))
        return StateOutput()

    backend = Mock()
    backend.get_execution_state.side_effect = poll
    monkeypatch.setattr(fixture.time, "time_ns", timestamp)
    # Record wall time deterministically at the actual transport boundaries.
    client = fixture.ObservedClient(backend, trace)
    client.get_execution_state()
    assert calls == [0]
    observations = [row["observation"] for row in ledger.observations()]
    assert observations[0]["atMs"] == 2
    assert observations[1]["atMs"] >= observations[0]["atMs"]


def test_late_poll_during_total_publication_loss_remains_unproven(
    producer, monkeypatch
):
    trace, ledger = producer
    trace.emit("WRAPPER_ENTER")
    trace.finish_wrapper()
    backend = Mock()
    monkeypatch.setattr(
        trace, "_commit_record", Mock(side_effect=OSError("storage outage"))
    )
    assert (
        fixture.ObservedClient(backend, trace).get_execution_state()
        is backend.get_execution_state.return_value
    )
    backend.get_execution_state.assert_called_once()
    # The real consumer still sees the previous seal if every write is lost.
    rows = ledger.records()
    assert [row["phase"] for row in rows] == ["WRAPPER_ENTER", "SEALED"]
    check_seal_with_installed_consumer(rows, valid=True)


def test_seal_rechecks_failed_publication_after_waiting_for_writer(
    producer, monkeypatch
):
    trace, ledger = producer
    writing = threading.Event()
    ready_to_seal = threading.Event()
    release_write = threading.Event()
    seal_if_ready = trace._seal_if_ready

    def write(key, body):
        writing.set()
        assert release_write.wait(timeout=2)
        raise OSError("in-flight write failed")

    def finish_write():
        assert trace.emit("LOST_RAW_EVENT") == {}

    def seal():
        ready_to_seal.set()
        seal_if_ready()

    monkeypatch.setattr(trace, "_commit_record", write)
    monkeypatch.setattr(trace, "_seal_if_ready", seal)
    writer = threading.Thread(target=finish_write)
    finisher = threading.Thread(target=trace.finish_wrapper)
    writer.start()
    try:
        assert writing.wait(timeout=2)
        finisher.start()
        assert ready_to_seal.wait(timeout=2)
    finally:
        release_write.set()
        writer.join(timeout=2)
    finisher.join(timeout=2)
    assert not finisher.is_alive()
    assert trace.sequence == 1
    assert not ledger.records()


def test_seal_publication_failure_does_not_escape_wrapper(producer, monkeypatch):
    trace, ledger = producer
    monkeypatch.setattr(
        trace, "_commit_record", Mock(side_effect=OSError("write failed"))
    )
    trace.finish_wrapper()
    assert not ledger.records()
    trace._commit_record.assert_called_once()


def test_worker_failure_does_not_seal_incomplete_work(producer, monkeypatch):
    trace, ledger = producer
    monkeypatch.setattr(threading, "excepthook", lambda _: None)

    def fail():
        raise RuntimeError("worker interrupted")

    worker = trace.start_worker(fail, "test-worker")
    worker.join(timeout=2)
    trace.finish_wrapper()
    assert not worker.is_alive()
    assert not ledger.records()


def test_worker_start_failure_does_not_seal_unstarted_work(producer, monkeypatch):
    trace, ledger = producer
    monkeypatch.setattr(
        threading.Thread, "start", Mock(side_effect=RuntimeError("thread unavailable"))
    )
    with pytest.raises(RuntimeError, match="thread unavailable"):
        trace.start_worker(lambda: None, "test-worker")
    trace.finish_wrapper()
    assert trace.active_workers == 0
    assert not ledger.records()


def test_release_racing_cleanup_cannot_establish_normal_preparation(
    producer, monkeypatch
):
    trace, ledger = producer
    released = Mock(side_effect=[False, True, True])
    monkeypatch.setattr(trace, "released", released)
    assert trace.gate("prepare", preparation=True) is None
    assert (
        next(
            row["observation"]["mode"]
            for row in ledger.observations()
            if row["observation"]["kind"] == "fault_released"
        )
        == "emergency"
    )
    assert [call.args[0] for call in released.call_args_list] == [
        "release-all",
        "prepare",
        "release-all",
    ]
    assert not any(
        row["observation"]["kind"] == "barrier_arrived" for row in ledger.observations()
    )


def test_control_transport_timeout_is_observed_and_reraised(producer, monkeypatch):
    trace, ledger = producer
    timeout = ReadTimeoutError(endpoint_url="https://s3.example.test")
    monkeypatch.setattr(ledger, "get_object", Mock(side_effect=timeout))
    with pytest.raises(ReadTimeoutError) as error:
        trace.released("release-all")
    assert error.value is timeout
    assert [event["phase"] for event in ledger.records()] == [
        "CONTROL_ERROR",
        "CONFORMANCE",
    ]
    assert ledger.observations()[0]["observation"]["kind"] == "control_error"


def test_control_publication_error_does_not_replace_timeout(producer, monkeypatch):
    trace, ledger = producer
    timeout = ReadTimeoutError(endpoint_url="https://s3.example.test")
    monkeypatch.setattr(ledger, "get_object", Mock(side_effect=timeout))
    monkeypatch.setattr(
        trace, "_commit_record", Mock(side_effect=OSError("source unavailable"))
    )
    with pytest.raises(ReadTimeoutError) as error:
        trace.released("release-all")
    assert error.value is timeout
    trace.finish_wrapper()
    assert not ledger.records()


def test_warm_worker_start_failure_clears_registration(producer, monkeypatch):
    trace, _ = producer
    monkeypatch.setattr(
        trace, "start_worker", Mock(side_effect=OSError("unable to start"))
    )
    with pytest.raises(OSError, match="unable to start"):
        fixture.start_warm_work(trace)
    assert (trace.payload["runId"], trace.payload["caseId"]) not in fixture.WARM_WORK


def test_warm_worker_exit_publication_failure_still_clears_registration(
    producer, monkeypatch
):
    trace, ledger = producer
    put = ledger.put_object

    def store(**kwargs):
        record = json.loads(kwargs["Body"])
        if record.get("observation", {}).get("kind") == "task_exit":
            raise OSError("storage outage")
        return put(**kwargs)

    monkeypatch.setattr(ledger, "put_object", store)
    monkeypatch.setattr(fixture, "admission_probes", Mock())
    fixture.start_warm_work(trace)
    fixture.release_warm_work(
        SimpleNamespace(payload=trace.payload, identity={"request": "warm"})
    )
    assert (trace.payload["runId"], trace.payload["caseId"]) not in fixture.WARM_WORK
    trace.finish_wrapper()
    assert trace.source_failed
    assert not any(row["phase"] == "SEALED" for row in ledger.records())


def test_warm_worker_reports_expired_reuse_wait(producer, monkeypatch):
    trace, ledger = producer
    ledger.objects[trace.control_prefix + "release-all"] = b"hold"
    monkeypatch.setattr(fixture.time, "monotonic", Mock(side_effect=[0.0, 121.0]))
    fixture.start_warm_work(trace)
    worker = fixture.WARM_WORK[(trace.payload["runId"], trace.payload["caseId"])].worker
    assert worker is not None
    worker.join(timeout=2)
    assert not worker.is_alive()
    errors = [
        row["observation"]
        for row in ledger.observations()
        if row["observation"]["kind"] == "observation_error"
    ]
    assert [error["message"] for error in errors] == [
        "Original late work never observed warm reuse"
    ]


@pytest.mark.parametrize("aborted", [False, True])
def test_warm_owner_seals_after_completed_callback_or_abort(
    producer, monkeypatch, aborted
):
    trace, ledger = producer
    prefix = trace.control_prefix
    ledger.objects[prefix + "release-all"] = b"hold"
    probes = Mock()
    monkeypatch.setattr(fixture, "admission_probes", probes)
    fixture.start_warm_work(trace)
    trace.finish_wrapper()
    key = (trace.payload["runId"], trace.payload["caseId"])
    worker = fixture.WARM_WORK[key].worker
    assert worker is not None
    assert not any(row["phase"] == "SEALED" for row in ledger.records())
    if aborted:
        ledger.objects[prefix + "release-all"] = b"release"
    else:
        fixture.release_warm_work(
            SimpleNamespace(payload=trace.payload, identity={"request": "warm"})
        )
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert key not in fixture.WARM_WORK
    assert ledger.records()[-1]["phase"] == "SEALED"
    observations = [row["observation"] for row in ledger.observations()]
    assert {row["kind"] for row in observations} == (
        {"task_start", "task_exit"}
        if aborted
        else {"task_start", "task_exit", "late_activity"}
    )
    assert probes.call_count == (0 if aborted else 1)


def test_seal_waits_for_registered_work_after_wrapper_finish(producer):
    trace, ledger = producer
    resume = threading.Event()

    def work():
        assert resume.wait(timeout=2)
        trace.observe("late_activity")

    worker = trace.start_worker(work, "test-worker")
    try:
        trace.finish_wrapper()
        assert not ledger.records()
    finally:
        resume.set()
        worker.join(timeout=2)
    assert not worker.is_alive()
    assert [event["phase"] for event in ledger.records()] == ["CONFORMANCE", "SEALED"]


@pytest.mark.parametrize("first_write_committed", [False, True])
def test_lost_write_reuses_the_same_key_and_bytes(
    producer, monkeypatch, first_write_committed
):
    trace, ledger = producer
    real_put, real_get = ledger.put_object, ledger.get_object
    puts: list[dict[str, Any]] = []

    def put(**kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        puts.append(kwargs)
        if len(puts) == 1:
            if first_write_committed:
                real_put(**kwargs)
            raise ReadTimeoutError(endpoint_url="https://s3.example.test")
        return real_put(**kwargs)

    def get(**kwargs):
        if kwargs["Key"] not in ledger.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return real_get(**kwargs)

    monkeypatch.setattr(ledger, "put_object", put)
    monkeypatch.setattr(ledger, "get_object", get)
    trace.observe("event")
    if first_write_committed:
        assert len(puts) == 1
    else:
        assert puts == [puts[0], puts[0]]
    trace.finish_wrapper()
    assert [event["phase"] for event in ledger.records()] == ["CONFORMANCE", "SEALED"]


def test_permanent_raw_publication_loss_spends_one_retry_budget(producer, monkeypatch):
    trace, ledger = producer

    def put(**kwargs):
        raise ReadTimeoutError(endpoint_url="https://s3.example.test")

    def get(**kwargs):
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    slow_put = Mock(side_effect=put)
    slow_get = Mock(side_effect=get)
    monkeypatch.setattr(ledger, "put_object", slow_put)
    monkeypatch.setattr(ledger, "get_object", slow_get)
    for _ in range(10):
        assert trace.emit("LOST_RAW_EVENT") == {}
    trace.finish_wrapper()
    assert slow_put.call_count == slow_get.call_count == 2
    assert ledger.records() == []


def test_recovered_storage_after_seal_still_invalidates_it(producer, monkeypatch):
    trace, ledger = producer
    trace.emit("WRAPPER_ENTER")
    trace.finish_wrapper()
    with monkeypatch.context() as failed:
        failed.setattr(
            ledger, "put_object", Mock(side_effect=OSError("temporary failure"))
        )
        assert trace.emit("POLL_CALL") == {}
    trace.emit("POLL_EXIT")
    assert [row["phase"] for row in ledger.records()] == [
        "WRAPPER_ENTER",
        "SEALED",
        "POLL_EXIT",
    ]
    check_seal_with_installed_consumer(ledger.records(), valid=False)


def test_existing_sequence_with_different_bytes_is_never_replaced(
    producer, monkeypatch
):
    trace, ledger = producer
    existing = b'{"phase": "OTHER_RECORD", "request": "unit", "sequence": 1}'

    def put(**kwargs):
        ledger.objects.setdefault(kwargs["Key"], existing)
        raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")

    monkeypatch.setattr(ledger, "put_object", put)
    assert trace.emit("WRAPPER_RETURN") == {}
    trace.finish_wrapper()
    assert list(ledger.objects.values()) == [existing]


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


def test_unmatched_task_end_remains_visible() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    observer.on_user_function_start(
        user_info("child-id", parent_id="not-observed", kind=OperationType.CONTEXT)
    )
    observer.on_user_function_end(
        UserFunctionEndInfo.from_start_info(user_info("wrong-id"), None)
    )
    assert sum(item["kind"] == "observation_error" for item in sink.records) == 1


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


@pytest.mark.parametrize(
    "field", ["caseId", "role", "controlPrefix", "coverageProfile"]
)
def test_shared_case_cannot_silently_drop_ownership_metadata(
    monkeypatch, field: str
) -> None:
    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    del payload[field]
    with pytest.raises(ValueError, match=field):
        fixture.Trace(payload, lambda_context(30_000), "execution")
    assert ledger.records() == []


@pytest.mark.parametrize(
    ("alias", "canonical"),
    [("unit", "previous"), ("previous", "unit"), ("previous", "previous")],
)
def test_shared_case_rejects_conflicting_run_alias_before_storage(
    monkeypatch, alias, canonical
):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    payload.update(run=alias, runId=canonical)
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key

    payload["controlPrefix"] = (
        f"runs/{canonical}/cases/{case_key(payload['caseId'])}/control/"
    )
    storage = Mock(return_value=ledger)
    monkeypatch.setattr(fixture.boto3, "client", storage)
    with pytest.raises(ValueError, match="different deployment"):
        fixture.Trace(payload, lambda_context(30_000), "execution")
    storage.assert_not_called()


@pytest.mark.parametrize(
    "prefix", ["runs/unit/control/", "runs/unit/cases/other/control/"]
)
def test_shared_case_rejects_controls_outside_its_exact_prefix(
    monkeypatch, prefix
) -> None:
    install_ledger(monkeypatch)
    payload = shared_payload()
    payload["controlPrefix"] = prefix
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
    assert trace.gate("gate") is None
    assert ledger.objects["runs/unit/cases/other/control/gate"] == b"hold"
    observations = [item["observation"] for item in ledger.observations()]
    assert [item["kind"] for item in observations] == [
        "io_start",
        "fault_released",
        "io_exit",
    ]
    assert observations[0]["taskId"] == observations[-1]["taskId"]
    assert observations[1]["mode"] == mode


def test_root_scope_requires_the_public_invocation_operation() -> None:
    sink = MemorySink()
    observer = LifecycleObservations(sink)
    with observer.root_scope():
        pass
    assert [item["kind"] for item in sink.records] == ["observation_error"]


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


def test_recovered_evidence_writes_share_an_invocation_budget(producer, monkeypatch):
    trace, ledger = producer
    ticks = iter([0.0, 3.0, 3.0, 6.0])
    monkeypatch.setattr(fixture, "publication_clock", lambda: next(ticks))
    real_put = ledger.put_object

    def committed_but_timed_out(**kwargs):
        real_put(**kwargs)
        raise ReadTimeoutError(endpoint_url="https://s3.example.test")

    put = Mock(side_effect=committed_but_timed_out)
    monkeypatch.setattr(ledger, "put_object", put)
    for index in range(10):
        trace.emit("DIAGNOSTIC", index=index)
    trace.finish_wrapper()
    assert trace.source_failed
    assert put.call_count == 2
    assert [event["phase"] for event in ledger.records()] == [
        "DIAGNOSTIC",
        "DIAGNOSTIC",
    ]


def test_healthy_heartbeat_uploads_do_not_spend_retry_budget(producer, monkeypatch):
    trace, ledger = producer
    ticks = count()
    monkeypatch.setattr(fixture, "publication_clock", lambda: next(ticks) * 0.02)
    for _ in range(500):
        trace.emit("ALIVE")
    trace.finish_wrapper()
    assert len(ledger.records()) == 501
    assert ledger.records()[-1]["phase"] == "SEALED"
