from concurrent.futures import ThreadPoolExecutor
import json
import subprocess
import sys
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests.fixture import ObservedClient, workflow
from lmi_tests import fixture
from lmi_tests.tests.regressions.test_issue741 import (
    MemoryClient,
    invocation,
    lambda_context,
)


class LocalTrace:
    def __init__(self):
        self.events = []
        self.payload = {"scenario": "checkpoint", "marker": "local"}

    def emit(self, phase, **fields):
        self.events.append({"phase": phase, **fields})

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
        "ACK_HELD",
        "GATE",
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


def test_parallel_checkpoints_finish_with_local_runner():
    trace = LocalTrace()
    handler = durable_execution(lambda e, c: workflow(e, c, trace))
    with DurableFunctionTestRunner(handler) as runner, ThreadPoolExecutor(1) as caller:
        result = caller.submit(
            runner.run, input={"marker": "local", "scenario": "checkpoint"}
        ).result(timeout=10)
    assert result.status.value == "SUCCEEDED"


def test_cloud_entrypoint_unwraps_runtime_event_and_observes_real_wrapper(monkeypatch):
    trace = LocalTrace()
    trace.payload["scenario"] = "success"
    monkeypatch.setenv("LMI_RUN_ID", "unit")
    monkeypatch.setattr(fixture, "Trace", lambda *_: trace)
    monkeypatch.setattr(
        fixture.LambdaClient, "initialize_client", lambda: MemoryClient()
    )
    event = invocation().to_json_dict()
    event["InitialExecutionState"]["Operations"][0]["ExecutionDetails"][
        "InputPayload"
    ] = json.dumps({"run": "unit", "marker": "local", "scenario": "success"})
    result = fixture.handler(event, lambda_context(30000))
    assert result["Status"] == "SUCCEEDED"
    assert json.loads(result["Result"]) == "local"
    phases = [e["phase"] for e in trace.events]
    assert phases[0] == "WRAPPER_ENTER" and phases[-1] == "WRAPPER_RETURN"
    assert phases.index("USER_EXIT") < phases.index("WRAPPER_RETURN")
    assert "CHECKPOINT_ACK" in phases
