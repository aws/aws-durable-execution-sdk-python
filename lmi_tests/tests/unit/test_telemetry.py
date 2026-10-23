"""Public-wrapper OTel controls and negative evidence fixtures; no AWS calls."""

import copy
import json
import os
import time

import pytest

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import DurableExecutionInvocationInput
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import fixture, telemetry, telemetry_evidence
from lmi_tests.tests.regressions.test_issue741 import invocation, lambda_context


HEADER = "Root=1-6955b900-123456789012345678901234;Parent=1234567890123456;Sampled=1"


class MemoryTrace:
    def __init__(self, payload, context, execution):
        self.payload = payload
        self.sequence = 0
        self.events = []
        self.identity = {
            "request": context.aws_request_id,
            "execution": execution,
            "environment": "local",
            "process": "local-worker",
            "pid": os.getpid(),
            "marker": payload["marker"],
        }

    def emit(self, phase, **fields):
        self.sequence += 1
        event = {
            **self.identity,
            "phase": phase,
            "sequence": self.sequence,
            "time": time.time(),
            **fields,
        }
        self.events.append(event)
        return event

    def body(self, name, value, step):
        self.emit("BODY", operation=name, value=value, attempt=step.attempt)
        return value


@pytest.fixture(autouse=True)
def isolated_runtimes(monkeypatch):
    monkeypatch.setenv("LMI_RUN_ID", "unit")
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.setattr(
        fixture, "resources", lambda: {"threads": [], "fds": 0, "rss": 0}
    )
    telemetry._RUNTIMES.clear()
    yield
    for runtime in telemetry._RUNTIMES.values():
        runtime.provider.shutdown()
    telemetry._RUNTIMES.clear()


def run(
    monkeypatch, view, marker, *, header=HEADER, scenario="success", mode="success"
):
    if header is None:
        monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    else:
        monkeypatch.setenv("_X_AMZN_TRACE_ID", header)
    original = os.environ.get("_X_AMZN_TRACE_ID")
    source = invocation()
    event = source.to_json_dict()
    event["DurableExecutionArn"] = (
        "arn:aws:lambda:us-west-2:123456789012:function:lmi:1/durable-execution/"
        + marker
        + "/local"
    )
    event["InitialExecutionState"]["Operations"][0]["StartTimestamp"] = int(
        time.time() * 1000
    )
    payload = {
        "run": "unit",
        "marker": marker,
        "scenario": scenario,
        "otel_view": view,
        "otel_export": mode,
    }
    event["InitialExecutionState"]["Operations"][0]["ExecutionDetails"][
        "InputPayload"
    ] = json.dumps(payload)
    context = lambda_context(30000)
    context.aws_request_id = "request-" + marker
    trace = MemoryTrace(payload, context, event["DurableExecutionArn"])
    monkeypatch.setattr(fixture, "Trace", lambda *_: trace)
    monkeypatch.setattr(
        fixture.LambdaClient, "initialize_client", lambda: source.service_client
    )
    result = fixture.handler(event, context)
    assert os.environ.get("_X_AMZN_TRACE_ID") == original
    assert result["Status"] == ("FAILED" if scenario == "failure" else "SUCCEEDED")
    telemetry_evidence.invocation(
        trace.events, native=header == HEADER, expected_outcome=mode
    )
    return trace.events


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("sampled", ["0", "1"])
@pytest.mark.parametrize("scenario", ["success", "failure"])
def test_real_public_wrapper_exports_and_respects_sampling(
    monkeypatch, view, sampled, scenario
):
    events = run(
        monkeypatch, view, "first", header=HEADER[:-1] + sampled, scenario=scenario
    )
    telemetry_evidence.terminal(events, failed=scenario == "failure")
    exports = [e for e in events if e["phase"] == "OTEL_EXPORT"]
    assert bool(exports) == (sampled == "1")


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("header", [None, "", "malformed", "Root=invalid;Sampled=0"])
def test_warm_reuse_does_not_borrow_previous_header(monkeypatch, view, header):
    first = run(monkeypatch, view, "first")
    second = run(monkeypatch, view, "second", header=header)
    telemetry_evidence.warm_reuse(first + second)
    first_root = next(
        e["span"]["trace_id"] for e in first if e["phase"] == "OTEL_EXPORT"
    )
    second_roots = {
        e["span"]["trace_id"] for e in second if e["phase"] == "OTEL_EXPORT"
    }
    assert first_root not in second_roots
    assert bool(second_roots) == (
        telemetry_evidence.explicit_sampling(header) is not False
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_failed_export_does_not_close_provider_or_corrupt_next_invocation(
    monkeypatch, view
):
    first = run(monkeypatch, view, "failed-export", mode="failure")
    second = run(monkeypatch, view, "healthy")
    telemetry_evidence.warm_reuse(first + second)
    assert all(e["outcome"] == "success" for e in second if e["phase"] == "OTEL_EXPORT")


@pytest.mark.parametrize(
    "mutation", ["execution", "trace", "parent", "cleanup", "missing", "duplicate"]
)
def test_evidence_rejects_corrupt_or_incomplete_telemetry(monkeypatch, mutation):
    events = copy.deepcopy(run(monkeypatch, "invocation", "control"))
    spans = [e for e in events if e["phase"] == "OTEL_EXPORT"]
    root = next(e for e in spans if e["span"]["name"] == "Invocation")
    if mutation == "execution":
        root["span"]["attributes"]["durable.execution.arn"] = "other"
    elif mutation == "trace":
        root["span"]["trace_id"] = "0" * 32
    elif mutation == "parent":
        root["span"]["parent_id"] = "0" * 16
    elif mutation == "cleanup":
        next(e for e in events if e["phase"] == "OTEL_EXIT")["context_restored"] = False
    elif mutation == "missing":
        events.remove(root)
    else:
        events.append(copy.deepcopy(root))
    with pytest.raises((AssertionError, telemetry_evidence.CollectionError)):
        telemetry_evidence.invocation(events)


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("failed", [False, True])
def test_public_runner_suspends_replays_and_finalizes_once(monkeypatch, view, failed):
    monkeypatch.setenv("_X_AMZN_TRACE_ID", HEADER)
    traces = []
    bodies = []

    def business(event, context):
        value = context.step(
            lambda _: bodies.append("saved") or event["marker"], name="saved"
        )
        context.wait(Duration.from_seconds(1), name="pause")
        if failed:
            raise ValueError("expected failure")
        return value

    def entry(event, context):
        invocation_input = (
            event
            if hasattr(event, "durable_execution_arn")
            else DurableExecutionInvocationInput.from_json_dict(event)
        )
        diagnostic = MemoryTrace(
            {"marker": "replay", "otel_view": view},
            context,
            invocation_input.durable_execution_arn,
        )
        # The local runner uses a fixed Lambda-context test double. Give each
        # controlled attempt a diagnostic ID; native tests use real request IDs.
        diagnostic.identity["request"] = "controlled-" + str(len(traces))
        traces.append(diagnostic)
        return telemetry.invoke(
            diagnostic,
            lambda plugins: durable_execution(business, plugins=plugins)(
                event, context
            ),
        )

    with DurableFunctionTestRunner(entry, poll_interval=0.01) as runner:
        result = runner.run(input={"marker": "replay"}, execution_timeout=15)
    assert result.status.value == ("FAILED" if failed else "SUCCEEDED")
    assert bodies == ["saved"]
    assert len(traces) >= 2
    events = [event for diagnostic in traces for event in diagnostic.events]
    telemetry_evidence.invocation(events)
    telemetry_evidence.terminal(events, failed=failed)


@pytest.mark.parametrize("raises", [False, True])
def test_instrumented_invoke_owns_header_client_and_closes_it(monkeypatch, raises):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from lmi_tests import cloud as module

    isolated = Mock()
    owner = SimpleNamespace(lam=Mock())
    monkeypatch.setattr(module, "client", lambda name: isolated)
    expected = (
        "Root=1-6955b900-123456789012345678901234;Parent=1234567890123456;Sampled=0"
    )

    def send(**kwargs):
        event, hook = isolated.meta.events.register.call_args.args
        assert event == "before-call.lambda.Invoke"
        params = {"headers": {}}
        hook(params=params)
        assert params["headers"]["X-Amzn-Trace-Id"] == expected
        assert kwargs["Payload"] == b"payload"
        if raises:
            raise RuntimeError("transport failed")
        return {"DurableExecutionArn": "execution"}

    isolated.invoke.side_effect = send
    caller = module.RunnerClient(owner, expected)
    if raises:
        with pytest.raises(RuntimeError, match="transport failed"):
            caller.invoke(Payload=b"payload")
    else:
        assert caller.invoke(Payload=b"payload")["DurableExecutionArn"] == "execution"
    isolated.close.assert_called_once()
    owner.lam.invoke.assert_not_called()


def test_post_call_evidence_does_not_move_the_sdk_boundary(monkeypatch):
    original = MemoryTrace.emit

    def delayed(self, phase, **fields):
        record = original(self, phase, **fields)
        if phase == "OTEL_EXIT":
            time.sleep(0.02)  # Controlled diagnostic I/O latency, not a durable wait.
        return record

    monkeypatch.setattr(MemoryTrace, "emit", delayed)
    events = run(monkeypatch, "invocation", "boundary")
    boundary = next(event for event in events if event["phase"] == "WRAPPER_RETURN")
    evidence_exit = next(event for event in events if event["phase"] == "OTEL_EXIT")
    assert boundary["time"] <= evidence_exit["time"]
