# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real public SDK/plugin calls with in-memory observation and gate transport."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from lmi_tests import telemetry
from lmi_tests.shared_exporter import CURRENT_OWNER, owned
from lmi_tests.shared_telemetry import collect_telemetry
from lmi_tests.tests.regressions.test_issue741 import invocation, lambda_context

HEADER = "Root=1-6955b900-123456789012345678901234;Parent=1234567890123456;Sampled=1"


class SharedTrace:
    def __init__(
        self,
        marker="victim",
        *,
        view="invocation",
        mode="sync",
        behavior="success",
        profile="export-lifecycle",
    ):
        self.payload = {
            "marker": marker,
            "scenario": "success",
            "run": "unit",
            "caseId": marker,
            "coverageProfile": profile,
            "otel_view": view,
            "exportMode": mode,
            "otel_export": behavior,
            "otel_export_gate": marker + "-export",
            "behaviorBudgets": {
                "flushWaitMs": 40,
                "exportCompletionMs": 5000,
                "exportMs": 5000,
                "observationGraceMs": 1,
            },
        }
        self.identity = {
            "request": "request-" + marker,
            "execution": "arn:aws:lambda:us-west-2:123456789012:function:lmi:1/durable-execution/"
            + marker
            + "/local",
            "environment": "memory-environment",
            "process": "memory-process",
            "pid": os.getpid(),
            "marker": marker,
        }
        self.lock = threading.Lock()
        self.capture_lock = threading.Lock()
        self.capture_sequence = 0
        self.events = []
        self.sequence = 0
        self.held = threading.Event()
        self.release = threading.Event()
        self.published = threading.Event()
        self.publication_entered = threading.Event()
        self.allow_publication = threading.Event()
        self.allow_publication.set()
        self.fail_publication = False
        self.sdk_completed_at = None

    def emit(self, phase, **fields):
        with self.lock:
            self.sequence += 1
            record = {
                **self.identity,
                "phase": phase,
                "time": time.time(),
                "sequence": self.sequence,
                **fields,
            }
            self.events.append(record)
            return record

    def capture_observation(self, kind, **fields):
        with self.capture_lock:
            self.capture_sequence += 1
            return {
                "kind": kind,
                "atMs": time.time_ns() / 1_000_000,
                "captureSequence": self.capture_sequence,
                **fields,
            }

    def publish_observation(self, observation):
        self.publication_entered.set()
        if not self.allow_publication.wait(5):
            raise TimeoutError("local observer fixture was not released")
        if self.fail_publication:
            raise RuntimeError("observed ledger write failure")
        record = self.emit("CONFORMANCE", observation=observation)
        if observation["kind"] == "export_quiescence":
            self.published.set()
        return record

    def observe(self, kind, **fields):
        return self.publish_observation(self.capture_observation(kind, **fields))

    def gate_release(self, name, *, on_hold=None, on_release=None):
        assert name == self.payload["otel_export_gate"]
        held_at_ms = None if self.release.is_set() else time.time_ns() / 1_000_000
        self.held.set()
        if held_at_ms is not None and on_hold is not None:
            on_hold(held_at_ms)
        if not self.release.wait(5):
            raise TimeoutError("local export fixture was not released")
        return SimpleNamespace(
            gate_id=name, observed_at_ms=time.time_ns() / 1_000_000, mode="normal"
        )

    def gate(self, name):
        self.gate_release(name)


def run_public(trace):
    source = invocation()
    event = source.to_json_dict()
    event["DurableExecutionArn"] = trace.identity["execution"]
    event["InitialExecutionState"]["Operations"][0]["ExecutionDetails"][
        "InputPayload"
    ] = json.dumps(trace.payload)
    event["InitialExecutionState"]["Operations"][0]["StartTimestamp"] = int(
        time.time() * 1000
    )
    typed = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        DurableExecutionInvocationInput.from_json_dict(event), source.service_client
    )
    context = lambda_context(30000)
    context.aws_request_id = trace.identity["request"]
    trace.emit("WRAPPER_ENTER")

    def call(plugins):
        handler = durable_execution(
            lambda event, ctx: ctx.step(lambda _: event["marker"], name="saved"),
            plugins=plugins,
        )
        try:
            return handler(typed, context)
        finally:
            trace.sdk_completed_at = time.time()

    result = telemetry.invoke(trace, call)
    trace.emit("WRAPPER_RETURN", status=result["Status"], time=trace.sdk_completed_at)
    return result


def observations(trace, kind):
    return [
        record["observation"]
        for record in trace.events
        if record["phase"] == "CONFORMANCE" and record["observation"]["kind"] == kind
    ]


def wait_quiescence(trace):
    assert trace.published.wait(5), "actual publication never reached quiescence"
    until = time.monotonic() + 2
    while time.monotonic() < until:
        active = telemetry.active_work(trace)
        if (
            not active["pendingExportIds"]
            and not active["pendingFlushIds"]
            and not active["publicationCount"]
        ):
            assert active["publicationErrors"] == []
            return
        threading.Event().wait(0.001)
    pytest.fail("pending runtime ownership survived publication")


def attempt(trace):
    return {
        "requestId": trace.identity["request"],
        "executionArn": trace.identity["execution"],
        "environmentId": trace.identity["environment"],
        "processId": trace.identity["process"],
        "workerId": trace.identity["process"],
    }


@pytest.fixture(autouse=True)
def runtimes(monkeypatch):
    monkeypatch.setenv("_X_AMZN_TRACE_ID", HEADER)
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    telemetry._RUNTIMES.clear()
    factory = telemetry.Runtime
    monkeypatch.setattr(
        telemetry,
        "Runtime",
        lambda view, export_mode="sync", **options: factory(
            view, export_mode, **{"flush_timeout_ms": 20, **options}
        ),
    )
    yield
    for runtime in telemetry._RUNTIMES.values():
        runtime.provider.shutdown()
    telemetry._RUNTIMES.clear()


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("mode", ["sync", "async-bounded-flush"])
@pytest.mark.parametrize("behavior", ["success", "failure"])
def test_public_sdk_modes_preserve_handler_and_actual_export_outcome(
    view, mode, behavior
):
    trace = SharedTrace(view=view, mode=mode, behavior=behavior)
    assert run_public(trace)["Status"] == "SUCCEEDED"
    wait_quiescence(trace)
    completed = observations(trace, "export_completed")
    assert completed and {item["outcome"] for item in completed} == {
        "failed" if behavior == "failure" else "succeeded"
    }
    if mode == "sync":
        assert max(item["atMs"] for item in completed) <= trace.sdk_completed_at * 1000
    else:
        wait = observations(trace, "flush_wait_completed")
        assert len(wait) == 1 and wait[0]["outcome"] == (
            "error" if behavior == "failure" else "completed"
        )
        assert observations(trace, "flush_completed")[0]["atMs"] <= wait[0]["atMs"]
    normalized = collect_telemetry(
        trace.events,
        [attempt(trace)],
        view=view,
        expected_sampled=True,
        caller_headers={trace.identity["request"]: HEADER},
        observed_until_ms=time.time_ns() / 1_000_000,
    )
    assert normalized["views"][view]["collection"]["status"] == "complete"
    names = [record["span"]["name"] for record in normalized["views"][view]["spans"]]
    assert names.count("Invocation") == names.count("Workflow") == 1
    assert normalized["views"][view]["carriers"][0]["header"] == HEADER


@pytest.mark.parametrize("mode", ["sync", "async-bounded-flush"])
def test_held_export_has_mode_specific_real_sdk_boundary(mode):
    trace = SharedTrace(mode=mode, behavior="held")
    with ThreadPoolExecutor(max_workers=1) as worker:
        result = worker.submit(run_public, trace)
        try:
            assert trace.held.wait(3)
            if mode == "sync":
                assert trace.sdk_completed_at is None
                assert not result.done()
            else:
                assert result.result(timeout=3)["Status"] == "SUCCEEDED"
                assert (
                    observations(trace, "flush_wait_completed")[0]["outcome"]
                    == "timeout"
                )
                assert telemetry.active_work(trace)["pendingExportIds"]
                assert not trace.published.is_set()
                assert not observations(trace, "flush_completed")
        finally:
            trace.release.set()
        assert result.result(timeout=3)["Status"] == "SUCCEEDED"
    wait_quiescence(trace)
    if mode == "async-bounded-flush":
        assert (
            min(item["atMs"] for item in observations(trace, "export_completed"))
            > trace.sdk_completed_at * 1000
        )
    assert observations(trace, "export_quiescence")[-1]["pendingFlushIds"] == []


def test_late_async_export_keeps_original_owner_during_warm_invocation():
    first = SharedTrace("first", mode="async-bounded-flush", behavior="held")
    warm = SharedTrace("warm", mode="async-bounded-flush")
    try:
        assert run_public(first)["Status"] == "SUCCEEDED"
        assert first.held.is_set()
        assert run_public(warm)["Status"] == "SUCCEEDED"
        assert telemetry.active_work(first)["pendingExportIds"]
        assert telemetry.active_work(warm)["pendingExportIds"]
    finally:
        first.release.set()
    wait_quiescence(first)
    wait_quiescence(warm)
    for trace in (first, warm):
        assert all(
            record["span"]["attributes"]["durable.execution.arn"]
            == trace.identity["execution"]
            for record in trace.events
            if record["phase"] == "OTEL_EXPORT"
        )
    assert (
        observations(first, "provider_observed")[0]["providerId"]
        == observations(warm, "provider_observed")[0]["providerId"]
    )


def test_delayed_evidence_publication_does_not_extend_sdk_flush_wait():
    trace = SharedTrace(mode="async-bounded-flush")
    trace.allow_publication.clear()
    try:
        assert run_public(trace)["Status"] == "SUCCEEDED"
        assert trace.publication_entered.is_set()
        assert trace.sdk_completed_at is not None
        assert telemetry.active_work(trace)["publicationCount"] > 0
        assert not trace.published.is_set()
    finally:
        trace.allow_publication.set()
    wait_quiescence(trace)
    waited = observations(trace, "flush_wait_completed")[0]
    started = observations(trace, "flush_started")[0]
    assert (
        waited["atMs"] - started["atMs"]
        < trace.payload["behaviorBudgets"]["flushWaitMs"]
    )
    assert waited["atMs"] <= trace.sdk_completed_at * 1000


def test_failed_publication_stays_owned_and_never_claims_quiescence():
    trace = SharedTrace(mode="async-bounded-flush")
    trace.fail_publication = True
    assert run_public(trace)["Status"] == "SUCCEEDED"
    assert trace.publication_entered.wait(2)
    active = telemetry.active_work(trace)
    assert active["publicationCount"] > 0
    assert active["publicationErrors"]
    assert not trace.published.is_set()


@pytest.mark.parametrize(
    "hook",
    [
        "on_invocation_start",
        "on_invocation_end",
        "on_operation_start",
        "on_operation_end",
        "on_operation_change",
        "on_user_function_start",
        "on_user_function_end",
        "provide_propagation_metadata",
    ],
)
def test_proxy_forwards_every_public_hook_and_restores_calling_owner(hook):
    runtime = telemetry.Runtime("invocation")
    first = runtime.tracker.owner(SharedTrace("first"))
    newer = runtime.tracker.owner(SharedTrace("newer"))
    delegate = Mock(spec=DurableInstrumentationPlugin)
    info, answer = object(), object()

    def called(value):
        assert value is info and CURRENT_OWNER.get() is first
        return answer

    getattr(delegate, hook).side_effect = called
    proxy = telemetry.ObservedPlugin(delegate, first)
    try:
        with owned(newer):
            result = getattr(proxy, hook)(info)
            assert CURRENT_OWNER.get() is newer
            if hook == "provide_propagation_metadata":
                assert result is answer
    finally:
        runtime.provider.shutdown()
    getattr(delegate, hook).assert_called_once_with(info)


def test_missing_public_owner_cannot_borrow_current_runtime_state():
    runtime = telemetry.Runtime("invocation")
    try:
        with pytest.raises(RuntimeError, match="original public-plugin"):
            runtime.tracker.accept(())
    finally:
        runtime.provider.shutdown()


@pytest.mark.parametrize("sampled", [False, True])
def test_collection_requires_quiescence_and_source_sequence_completeness(
    monkeypatch, sampled
):
    monkeypatch.setenv("_X_AMZN_TRACE_ID", HEADER[:-1] + str(int(sampled)))
    trace = SharedTrace()
    run_public(trace)
    wait_quiescence(trace)
    options = dict(
        view="invocation",
        expected_sampled=sampled,
        caller_headers={},
        observed_until_ms=time.time_ns() / 1_000_000,
    )
    assert (
        collect_telemetry(trace.events, [attempt(trace)], **options)["views"][
            "invocation"
        ]["collection"]["status"]
        == "complete"
    )
    missing = [
        record
        for record in trace.events
        if record["phase"] != "CONFORMANCE"
        or record["observation"]["kind"] != "export_quiescence"
    ]
    assert (
        collect_telemetry(missing, [attempt(trace)], **options)["views"]["invocation"][
            "collection"
        ]["status"]
        == "incomplete"
    )
    assert (
        collect_telemetry(trace.events[1:], [attempt(trace)], **options)["views"][
            "invocation"
        ]["collection"]["status"]
        == "incomplete"
    )


def test_real_gate_reports_first_hold_and_does_not_rehold_an_open_gate(monkeypatch):
    from lmi_tests import fixture
    from lmi_tests.tests.unit.test_shared_observations import (
        install_ledger,
        shared_payload,
    )

    ledger = install_ledger(monkeypatch)
    payload = shared_payload()
    trace = fixture.Trace(payload, lambda_context(30000), "execution")
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + "gate"] = b"hold"
    ledger.objects[prefix + "release-all"] = b"hold"
    held, released = [], []
    with ThreadPoolExecutor(max_workers=1) as caller:
        work = caller.submit(
            trace.gate_release, "gate", on_hold=held.append, on_release=released.append
        )
        try:
            observed = ledger.wait_for("blocked")
        finally:
            ledger.put_object(Bucket=trace.bucket, Key=prefix + "gate", Body=b"release")
        result = work.result(timeout=2)
    assert held == [result.held_at_ms] == [observed["observation"]["atMs"]]
    assert released == [result]
    assert result.held_at_ms < result.observed_at_ms and result.mode == "normal"
    held.clear()
    opened = trace.gate_release("gate", on_hold=held.append)
    assert opened.held_at_ms is None and held == []


def test_actual_capture_order_survives_reversed_publication(monkeypatch):
    from lmi_tests import fixture
    from lmi_tests.tests.unit.test_shared_observations import (
        install_ledger,
        shared_payload,
    )

    ledger = install_ledger(monkeypatch)
    trace = fixture.Trace(shared_payload(), lambda_context(30000), "execution")
    at_ms = time.time_ns() / 1_000_000
    exported = trace.capture_observation("export_completed", atMs=at_ms)
    wrapper = trace.observe("wrapper_return", atMs=at_ms)
    published = trace.publish_observation(exported)
    assert exported["captureSequence"] < wrapper["observation"]["captureSequence"]
    assert published["sequence"] > wrapper["sequence"]
    assert ledger.observations()[0]["observation"]["kind"] == "wrapper_return"


def test_capture_does_not_wait_for_s3_publication_lock(monkeypatch):
    from lmi_tests import fixture
    from lmi_tests.tests.unit.test_shared_observations import (
        install_ledger,
        shared_payload,
    )

    ledger = install_ledger(monkeypatch)
    trace = fixture.Trace(shared_payload(), lambda_context(30000), "execution")
    entered, release = threading.Event(), threading.Event()
    original = ledger.put_object

    def blocked_put(**fields):
        entered.set()
        assert release.wait(3)
        return original(**fields)

    monkeypatch.setattr(ledger, "put_object", blocked_put)
    with ThreadPoolExecutor(max_workers=2) as workers:
        publication = workers.submit(trace.observe, "first")
        try:
            assert entered.wait(2)
            captured = workers.submit(trace.capture_observation, "second").result(
                timeout=1
            )
            assert captured["captureSequence"] == 2
        finally:
            release.set()
        publication.result(timeout=2)


def test_proxy_restores_owner_when_real_delegate_hook_raises():
    runtime = telemetry.Runtime("invocation")
    first = runtime.tracker.owner(SharedTrace("first"))
    newer = runtime.tracker.owner(SharedTrace("newer"))
    delegate = Mock(spec=DurableInstrumentationPlugin)
    delegate.on_operation_end.side_effect = RuntimeError("hook failure")
    try:
        with owned(newer):
            with pytest.raises(RuntimeError, match="hook failure"):
                telemetry.ObservedPlugin(delegate, first).on_operation_end(object())
            assert CURRENT_OWNER.get() is newer
    finally:
        runtime.provider.shutdown()


def test_async_processor_respects_smaller_provider_wait_budget():
    runtime = telemetry.Runtime(
        "invocation", "async-bounded-flush", flush_timeout_ms=2000
    )
    trace = SharedTrace(mode="async-bounded-flush", behavior="held")
    owner = runtime.tracker.owner(trace)
    try:
        with owned(owner):
            span = runtime.provider.get_tracer("public-fixture").start_span("held")
            span.end()
            assert trace.held.wait(2)
            before = time.monotonic()
            assert runtime.provider.force_flush(timeout_millis=20) is False
            assert time.monotonic() - before < 0.5
    finally:
        trace.release.set()
        runtime.tracker.close_invocation(owner)
        runtime.provider.shutdown()
    assert observations(trace, "flush_wait_completed")[0]["outcome"] == "timeout"
    assert observations(trace, "flush_completed")[0]["outcome"] == "succeeded"


def test_converter_rejects_mode_view_or_unjournaled_owner_substitution():
    trace = SharedTrace()
    run_public(trace)
    wait_quiescence(trace)
    options = dict(
        expected_sampled=True,
        caller_headers={},
        observed_until_ms=time.time_ns() / 1_000_000,
    )
    with pytest.raises(ValueError, match="view differs"):
        collect_telemetry(trace.events, [attempt(trace)], view="execution", **options)
    with pytest.raises(ValueError, match="exporter mode differs"):
        collect_telemetry(
            trace.events,
            [attempt(trace)],
            view="invocation",
            export_mode="async-bounded-flush",
            **options,
        )
    with pytest.raises(ValueError, match="unjournaled"):
        collect_telemetry(trace.events, [], view="invocation", **options)


def test_held_fixture_records_only_calls_that_actually_waited():
    trace = SharedTrace(mode="sync", behavior="held")
    with ThreadPoolExecutor(max_workers=1) as caller:
        work = caller.submit(run_public, trace)
        try:
            assert trace.held.wait(2)
        finally:
            trace.release.set()
        assert work.result(timeout=3)["Status"] == "SUCCEEDED"
    wait_quiescence(trace)
    started = observations(trace, "export_started")
    held = observations(trace, "export_held")
    assert len(started) > len(held) == 1
    assert len(observations(trace, "export_completed")) == len(started)


@pytest.mark.parametrize("mode", ["sync", "async-bounded-flush"])
def test_process_provider_does_not_retain_completed_invocations(mode):
    import gc
    import weakref

    references = []
    for index in range(12):
        trace = SharedTrace("retired-" + str(index), mode=mode)
        assert run_public(trace)["Status"] == "SUCCEEDED"
        wait_quiescence(trace)
        references.append(weakref.ref(trace))
    del trace
    gc.collect()
    assert all(reference() is None for reference in references)
    assert len(telemetry._RUNTIMES) == 1
    runtime = next(iter(telemetry._RUNTIMES.values()))
    assert (
        runtime.tracker.owners
        == runtime.tracker.exports
        == runtime.tracker.flushes
        == {}
    )


def test_runtime_variants_keep_view_and_processor_configuration_immutable():
    for view, mode in (
        ("invocation", "sync"),
        ("execution", "sync"),
        ("invocation", "async-bounded-flush"),
    ):
        trace = SharedTrace(view + mode, view=view, mode=mode)
        assert run_public(trace)["Status"] == "SUCCEEDED"
        wait_quiescence(trace)
    assert set(telemetry._RUNTIMES) == {
        ("invocation", "sync"),
        ("execution", "sync"),
        ("invocation", "async-bounded-flush"),
    }
    assert len({runtime.identity for runtime in telemetry._RUNTIMES.values()}) == 3


@pytest.mark.parametrize("shared,expected", [(True, "emergency"), (False, "normal")])
def test_shared_gate_abort_has_priority_without_changing_legacy_release_order(
    monkeypatch, shared, expected
):
    from lmi_tests import fixture
    from lmi_tests.tests.unit.test_shared_observations import (
        install_ledger,
        shared_payload,
    )

    ledger = install_ledger(monkeypatch)
    payload = (
        shared_payload()
        if shared
        else {"run": "unit", "marker": "legacy", "scenario": "success"}
    )
    trace = fixture.Trace(payload, lambda_context(30000), "execution")
    ledger.objects[trace.control_prefix + "gate"] = b"release"
    ledger.objects[trace.control_prefix + "release-all"] = b"release"
    result = trace.gate_release("gate")
    assert result.mode == expected
    assert result.held_at_ms is None
