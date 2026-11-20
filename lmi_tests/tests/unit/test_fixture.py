from contextlib import nullcontext
from dataclasses import replace
import json
import subprocess
import sys
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import StepError, durable_execution
from aws_durable_execution_sdk_python.exceptions import GetExecutionStateError
from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests.fixture import ObservedClient, workflow
from lmi_tests import fixture
from lmi_tests.tests.unit.test_shared_observations import (
    install_ledger,
    shared_payload,
)
from lmi_tests.tests.unit.support import (
    MemoryClient,
    invocation,
    lambda_context,
)


class LocalTrace:
    def __init__(self):
        self.events = []
        self.observer = Mock()
        self.observer.root_scope.side_effect = nullcontext
        self.call_sequence = 0
        self.closed = False
        self.payload = {"scenario": "checkpoint", "marker": "local"}

    def publishing(self):
        return nullcontext(self.closed)

    def emit(self, phase, **fields):
        self.events.append({"phase": phase, **fields})

    def observe(self, kind, **fields):
        pass

    def next_call_id(self, kind):
        self.call_sequence += 1
        return f"{kind}:{self.call_sequence}"

    def body(self, name, value, step):
        self.emit("BODY", operation=name, value=value, attempt=step.attempt)
        return value

    def gate(self, name):
        self.emit("GATE", name=name)


def test_environment_marker_is_shared_across_processes(tmp_path):
    path = str(tmp_path / "marker")
    workers = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys; from lmi_tests.fixture import environment_id; print(environment_id(sys.argv[1]))",
                path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    try:
        values = []
        for worker in workers:
            stdout, stderr = worker.communicate(timeout=15)
            assert worker.returncode == 0, stderr
            values.append(stdout.strip())
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
            worker.wait(timeout=5)
    assert len(set(values)) == 1
    assert len(values[0]) == 32


def test_checkpoint_proxy_preserves_real_result_and_errors():
    trace, backend = LocalTrace(), Mock()
    observed = ObservedClient(backend, trace)
    update = OperationUpdate(
        "id",
        OperationType.STEP,
        OperationAction.SUCCEED,
        name="checkpoint-loser",
        payload="result",
    )
    result = observed.checkpoint(
        updates=[update], checkpoint_token="secret", durable_execution_arn="arn"
    )
    assert result is backend.checkpoint.return_value
    backend.checkpoint.assert_called_once_with(
        updates=[update], checkpoint_token="secret", durable_execution_arn="arn"
    )
    assert [e["phase"] for e in trace.events] == [
        "CHECKPOINT_CALL",
        "CHECKPOINT_ACK",
        "CHECKPOINT_EXIT",
    ]
    assert "secret" not in json.dumps(trace.events)
    trace.events.clear()
    backend.checkpoint.side_effect = RuntimeError("network failure")
    with pytest.raises(RuntimeError, match="network failure"):
        observed.checkpoint(updates=[update])
    assert [e["phase"] for e in trace.events] == [
        "CHECKPOINT_CALL",
        "CHECKPOINT_ERROR",
        "CHECKPOINT_EXIT",
    ]


@pytest.fixture
def producer(monkeypatch):
    ledger = install_ledger(monkeypatch)
    return fixture.Trace(shared_payload(), lambda_context(30_000), "execution"), ledger


def fail_transport_writes(ledger, monkeypatch, *failing):
    put = ledger.put_object

    def store(**kwargs):
        record = json.loads(kwargs["Body"])
        phase = record.get("observation", {}).get("kind", record["phase"])
        if phase in failing:
            raise OSError("evidence store unavailable")
        return put(**kwargs)

    monkeypatch.setattr(ledger, "put_object", store)


def checkpoint_update():
    return OperationUpdate(
        "id", OperationType.STEP, OperationAction.SUCCEED, name="step"
    )


@pytest.mark.parametrize(
    "failing",
    [
        ("CHECKPOINT_CALL", "checkpoint_call"),
        ("CHECKPOINT_ACK", "checkpoint_ack"),
        ("CHECKPOINT_EXIT", "checkpoint_exit"),
    ],
)
def test_checkpoint_evidence_failure_cannot_change_a_successful_checkpoint(
    producer, monkeypatch, failing
):
    # The SDK must consume the backend's checkpoint token. A failed publication
    # is a collection error, never an artificial checkpoint failure.
    trace, ledger = producer
    backend = Mock()
    fail_transport_writes(ledger, monkeypatch, *failing)
    result = ObservedClient(backend, trace).checkpoint(updates=[checkpoint_update()])
    assert result is backend.checkpoint.return_value
    backend.checkpoint.assert_called_once()
    trace.finish_wrapper()
    assert trace.source_failed
    assert not {"CHECKPOINT_ERROR", "SEALED"} & {
        row["phase"] for row in ledger.records()
    }


def test_checkpoint_evidence_failure_cannot_replace_the_backend_error(
    producer, monkeypatch
):
    trace, ledger = producer
    backend = Mock()
    fail_transport_writes(ledger, monkeypatch, "CHECKPOINT_ERROR", "CHECKPOINT_EXIT")
    backend.checkpoint.side_effect = RuntimeError("network failure")
    with pytest.raises(RuntimeError, match="network failure"):
        ObservedClient(backend, trace).checkpoint(updates=[checkpoint_update()])


@pytest.mark.parametrize("failing", ["POLL_CALL", "POLL_EXIT"])
def test_state_poll_evidence_failure_cannot_change_the_backend_outcome(
    producer, monkeypatch, failing
):
    trace, ledger = producer
    backend = Mock()
    fail_transport_writes(ledger, monkeypatch, failing)
    observed = ObservedClient(backend, trace)
    assert (
        observed.get_execution_state(arn="arn")
        is backend.get_execution_state.return_value
    )
    backend.get_execution_state.side_effect = RuntimeError("state unavailable")
    with pytest.raises(RuntimeError, match="state unavailable"):
        observed.get_execution_state(arn="arn")


def test_replay_fixture_uses_real_public_apis_and_preserves_outcomes():
    trace = LocalTrace()
    handler = durable_execution(lambda e, c: workflow(e, c, trace))
    with DurableFunctionTestRunner(handler, poll_interval=0.01) as runner:
        arn = runner.run_async(input={"marker": "local", "scenario": "replay"})
        callback = runner.wait_for_callback(arn, name="completion", timeout=10)
        runner.send_callback_success(callback, result=b"local")
        result = runner.wait_for_result(arn, timeout=10)
    assert result.status.value == "SUCCEEDED"
    assert json.loads(result.result) == "local"
    bodies = [e for e in trace.events if e["phase"] == "BODY"]
    assert sum(e["operation"] == "success" for e in bodies) == 1
    assert sum(e["operation"] == "failure" for e in bodies) == 1
    assert [e["attempt"] for e in bodies if e["operation"] == "retry"] == [1, 2]
    failures = [e for e in trace.events if e["phase"] == "STORED_FAILURE"]
    assert len(failures) >= 2
    assert all(e["message"] == "expected:local" for e in failures)


def test_cloud_entrypoint_unwraps_runtime_event_and_observes_real_wrapper(monkeypatch):
    ledger = install_ledger(monkeypatch)
    monkeypatch.setattr(
        fixture.LambdaClient, "initialize_client", lambda: MemoryClient()
    )
    event = invocation().to_json_dict()
    event["InitialExecutionState"]["Operations"][0]["ExecutionDetails"][
        "InputPayload"
    ] = json.dumps(shared_payload())
    result = fixture.handler(event, lambda_context(30_000))
    assert result["Status"] == "SUCCEEDED"
    assert json.loads(result["Result"]) == "marker"
    phases = [row["phase"] for row in ledger.records()]
    assert phases[0] == "WRAPPER_ENTER" and phases[-1] == "SEALED"
    assert phases.index("USER_EXIT") < phases.index("WRAPPER_RETURN")
    assert "CHECKPOINT_ACK" in phases


@pytest.mark.parametrize(
    ("step_result", "diagnostic"),
    [
        ("unexpected value", "returned as a value"),
        (StepError("wrong", "ValueError"), "unexpected details"),
        (StepError("expected:marker", "TypeError"), "unexpected details"),
    ],
)
def test_replay_rejects_wrong_delivery_of_stored_failure(step_result, diagnostic):
    context = Mock()
    context.step.side_effect = ["marker", step_result]
    trace = LocalTrace()
    with pytest.raises(AssertionError, match=diagnostic):
        fixture.run_workflow({"marker": "marker", "scenario": "replay"}, context, trace)


def test_fixture_preserves_sdk_pagination_and_early_poll_observations(monkeypatch):
    ledger = install_ledger(monkeypatch)
    event = invocation()
    root = event.initial_execution_state.operations[0]
    assert root.execution_details is not None
    root = replace(
        root,
        execution_details=replace(
            root.execution_details, input_payload=json.dumps(shared_payload())
        ),
    )
    client = event.service_client
    client.get_execution_state = Mock(return_value=StateOutput(operations=[root]))
    event = replace(
        event,
        initial_execution_state=replace(
            event.initial_execution_state, operations=[], next_marker="page-two"
        ),
    )
    result = fixture.handler(event, lambda_context(30_000))
    assert result["Status"] == "SUCCEEDED"
    assert json.loads(result["Result"]) == "marker"
    client.get_execution_state.assert_called_once_with(
        durable_execution_arn=event.durable_execution_arn,
        checkpoint_token=event.checkpoint_token,
        next_marker="page-two",
    )
    records = [row["observation"] for row in ledger.observations()]
    polls = [
        record for record in records if record["kind"] in {"poll_call", "poll_exit"}
    ]
    assert [record["kind"] for record in polls] == ["poll_call", "poll_exit"]
    assert polls[0]["callId"] == polls[1]["callId"]
    assert polls[0]["atMs"] <= polls[1]["atMs"]
    assert ledger.records()[-1]["phase"] == "SEALED"


@pytest.mark.parametrize("raises", [False, True])
@pytest.mark.parametrize("broken_stdout", [None, BrokenPipeError, ValueError])
def test_wrapper_publication_does_not_replace_sdk_result_or_error(
    monkeypatch, raises, broken_stdout
):
    ledger = install_ledger(monkeypatch)
    event = invocation()
    root = event.initial_execution_state.operations[0]
    assert root.execution_details is not None
    root = replace(
        root,
        execution_details=replace(
            root.execution_details, input_payload=json.dumps(shared_payload())
        ),
    )
    event = replace(
        event,
        initial_execution_state=replace(
            event.initial_execution_state, operations=[root]
        ),
    )
    original_error = ValueError("SDK raised")
    returned = {"Status": "SUCCEEDED", "Result": '"marker"'}
    sdk = Mock(side_effect=original_error) if raises else Mock(return_value=returned)
    monkeypatch.setattr(fixture, "durable_execution", Mock(return_value=sdk))
    put = ledger.put_object

    def write(**kwargs):
        record = json.loads(kwargs["Body"])
        if record["phase"] == ("WRAPPER_RAISE" if raises else "WRAPPER_RETURN"):
            raise OSError("unavailable completion sink")
        return put(**kwargs)

    if broken_stdout:

        def broken_print(*args, **kwargs):
            raise broken_stdout("logging stream closed")

        monkeypatch.setattr(fixture, "print", broken_print, raising=False)

    monkeypatch.setattr(ledger, "put_object", write)
    if raises:
        with pytest.raises(ValueError) as error:
            fixture.handler(event, lambda_context(30_000))
        assert error.value is original_error
    else:
        assert fixture.handler(event, lambda_context(30_000)) is returned
    assert all(
        row["phase"] not in {"WRAPPER_RAISE", "WRAPPER_RETURN", "SEALED"}
        for row in ledger.records()
    )


@pytest.mark.parametrize("known_payload", [False, True])
def test_fixture_keeps_sdk_classification_of_failed_initial_fetch(
    monkeypatch, known_payload
):
    ledger = install_ledger(monkeypatch)
    event = invocation()
    root = event.initial_execution_state.operations[0]
    assert root.execution_details is not None
    payload = shared_payload()
    payload["sharedScenario"] = "normal-return"
    root = replace(
        root,
        execution_details=replace(
            root.execution_details, input_payload=json.dumps(payload)
        ),
    )
    client = event.service_client
    service_error = GetExecutionStateError("page access denied")
    monkeypatch.setattr(service_error, "is_retryable", lambda: False)
    client.get_execution_state = Mock(side_effect=service_error)
    event = replace(
        event,
        initial_execution_state=replace(
            event.initial_execution_state,
            operations=[root] if known_payload else [],
            next_marker="page-two",
        ),
    )
    result = fixture.handler(event, lambda_context(30_000))
    assert result["Status"] == "FAILED"
    assert result["Error"]["ErrorMessage"] == "page access denied"
    client.get_execution_state.assert_called_once()
    kinds = [row["observation"]["kind"] for row in ledger.observations()]
    assert "admission_probe" not in kinds
    if known_payload:
        assert {"poll_call", "poll_exit", "wrapper_return", "observation_error"} <= set(
            kinds
        )
    else:
        assert not ledger.records()


def test_admission_worker_start_failure_does_not_replace_sdk_return(monkeypatch):
    ledger = install_ledger(monkeypatch)
    event = invocation()
    root = event.initial_execution_state.operations[0]
    assert root.execution_details is not None
    payload = shared_payload()
    payload["sharedScenario"] = "normal-return"
    root = replace(
        root,
        execution_details=replace(
            root.execution_details, input_payload=json.dumps(payload)
        ),
    )
    event = replace(
        event,
        initial_execution_state=replace(
            event.initial_execution_state, operations=[root]
        ),
    )
    monkeypatch.setattr(
        fixture.Trace,
        "start_worker",
        Mock(side_effect=OSError("worker startup unavailable")),
    )
    result = fixture.handler(event, lambda_context(30_000))
    assert result["Status"] == "SUCCEEDED"
    assert json.loads(result["Result"]) == "marker"
    assert not any(row["phase"] == "SEALED" for row in ledger.records())


@pytest.mark.parametrize(
    ("retry", "mapped", "parallel", "diagnostic"),
    [
        (None, [1, 2, 3], ["marker", "marker"], "Retried step"),
        ("marker", [1, 3], ["marker", "marker"], "Replayed map"),
        ("marker", [True, 2, 3], ["marker", "marker"], "Replayed map"),
        (
            "marker",
            [1, 2, 3],
            [["left", "marker"], ["left", "marker"]],
            "Replayed parallel",
        ),
    ],
)
def test_replay_rejects_incomplete_actual_values(retry, mapped, parallel, diagnostic):
    context = Mock()
    context.step.side_effect = [
        "marker",
        StepError("expected:marker", "ValueError"),
        retry,
    ]
    context.run_in_child_context.return_value.get_results.return_value = mapped
    context.parallel.return_value.get_results.return_value = parallel
    with pytest.raises(AssertionError, match=diagnostic):
        fixture.run_workflow(
            {"marker": "marker", "scenario": "replay"}, context, LocalTrace()
        )


def test_body_publication_failure_does_not_fail_real_sdk_step(monkeypatch):
    ledger = install_ledger(monkeypatch)
    event = invocation()
    root = event.initial_execution_state.operations[0]
    assert root.execution_details is not None
    root = replace(
        root,
        execution_details=replace(
            root.execution_details, input_payload=json.dumps(shared_payload())
        ),
    )
    event = replace(
        event,
        initial_execution_state=replace(
            event.initial_execution_state, operations=[root]
        ),
    )
    put = ledger.put_object

    def write(**kwargs):
        if json.loads(kwargs["Body"])["phase"] == "BODY":
            raise OSError("body ledger unavailable")
        return put(**kwargs)

    monkeypatch.setattr(ledger, "put_object", write)
    result = fixture.handler(event, lambda_context(30_000))
    assert result["Status"] == "SUCCEEDED"
    assert json.loads(result["Result"]) == "marker"
    assert not any(row["phase"] == "SEALED" for row in ledger.records())


def test_optimized_fixture_still_waits_on_callback():
    script = """
from unittest.mock import Mock
from aws_durable_execution_sdk_python import StepError
from lmi_tests import fixture
from lmi_tests.tests.unit.test_fixture import LocalTrace

context = Mock()
context.step.side_effect = ["marker", StepError("expected:marker", "ValueError"), "marker"]
context.run_in_child_context.return_value.get_results.return_value = [1, 2, 3]
context.parallel.return_value.get_results.return_value = [["left", "marker"], ["right", "marker"]]
context.create_callback.return_value.result.return_value = "marker"
fixture.run_workflow({"marker": "marker", "scenario": "replay"}, context, LocalTrace())
context.create_callback.return_value.result.assert_called_once_with()
context.step.side_effect = ["marker", StepError("expected:marker", "ValueError"), "marker"]
context.create_callback.return_value.result.return_value = "different"
try:
    fixture.run_workflow({"marker": "marker", "scenario": "replay"}, context, LocalTrace())
except AssertionError as error:
    if "callback returned the wrong value" not in str(error):
        raise
else:
    raise RuntimeError("wrong callback result was accepted")
"""
    completed = subprocess.run(
        [sys.executable, "-O", "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
