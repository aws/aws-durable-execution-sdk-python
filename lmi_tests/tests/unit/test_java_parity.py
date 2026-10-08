"""Public-API counterparts of Java's root-finally and executor-starvation guards."""

import io
import json
import subprocess
import sys
import threading
import time
import uuid

import pytest

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import evidence
from lmi_tests.fixture import Trace, nested_progress, resources, workflow


class Controls:
    def __init__(self):
        self.condition = threading.Condition()
        self.objects = {"runs/unit/control/release-all": b"hold"}
        self.events = []

    def put_object(self, *, Key, Body, **_kwargs):
        with self.condition:
            self.objects[Key] = Body
            if Key.startswith("runs/unit/events/"):
                self.events.append(json.loads(Body))
            self.condition.notify_all()

    def get_object(self, *, Key, **_kwargs):
        with self.condition:
            return {"Body": io.BytesIO(self.objects[Key])}

    def wait(self, phase):
        with self.condition:
            assert self.condition.wait_for(
                lambda: any(e["phase"] == phase for e in self.events), timeout=5
            ), phase
            return next(e for e in self.events if e["phase"] == phase)

    def snapshot(self):
        with self.condition:
            return list(self.events)

    def trace(self, marker, scenario):
        trace = Trace.__new__(Trace)
        trace.s3, trace.bucket = self, "unit"
        trace.payload = {"marker": marker, "scenario": scenario}
        trace.identity = {
            "run": "unit",
            "marker": marker,
            "request": uuid.uuid4().hex,
            "environment": "local",
            "process": "local",
            "pid": 1,
        }
        trace.lock, trace.sequence, trace.deadline = (
            threading.Lock(),
            0,
            time.time() + 30,
        )
        return trace


def test_pending_waits_for_finally_then_replays_without_repeating_body():
    controls = Controls()
    controls.put_object(Key="runs/unit/control/local-cleanup", Body=b"hold")

    def entry(event, context):
        trace = controls.trace("local", "suspend-cleanup")
        trace.emit("WRAPPER_ENTER", resources=resources())
        result = durable_execution(lambda e, c: workflow(e, c, trace))(event, context)
        trace.emit("WRAPPER_RETURN", status=result["Status"], resources=resources())
        return result

    with DurableFunctionTestRunner(entry, poll_interval=0.01) as runner:
        arn = runner.run_async(input={"marker": "local", "scenario": "suspend-cleanup"})
        try:
            controls.wait("BLOCKED")
            evidence.scope_exit(controls.snapshot(), "local-cleanup")
            assert not [
                e for e in controls.snapshot() if e["phase"] == "WRAPPER_RETURN"
            ], "PENDING returned before finally completed"
        finally:
            controls.put_object(Key="runs/unit/control/local-cleanup", Body=b"release")
        result = runner.wait_for_result(arn, timeout=10)
        history = runner.get_execution_history(arn).to_dict()["Events"]
    assert result.status.value == "SUCCEEDED"
    evidence.suspension_cleanup(controls.snapshot(), history)


@pytest.mark.parametrize(
    "scenario", ["suspend-cleanup", "return-inflight", "failure-inflight"]
)
def test_placement_miss_runs_no_fault_or_finally_gate(scenario):
    controls = Controls()
    trace = controls.trace("local", scenario)
    handler = durable_execution(lambda event, context: workflow(event, context, trace))
    with DurableFunctionTestRunner(handler, poll_interval=0.01) as runner:
        result = runner.run(
            input={
                "marker": "local",
                "scenario": scenario,
                "target_environment": "another-environment",
            },
            execution_timeout=10,
        )
    assert result.status.value == "SUCCEEDED"
    phases = {e["phase"] for e in controls.snapshot()}
    assert "PLACEMENT_MISS" in phases
    assert not phases & {"BODY", "BLOCKED", "WINNER_READY", "CLEANUP_ENTER"}


def test_only_missed_admission_retries_environment_pair(monkeypatch):
    from unittest.mock import Mock
    from lmi_tests.tests.e2e import test_lifecycle

    cloud = Mock()
    cloud.concurrency = 2
    first, missed, second, admitted = [
        dict(marker=name) for name in ("first", "missed", "second", "admitted")
    ]
    cloud.start.side_effect = [first, missed, second, admitted]
    cloud.phase.side_effect = [[{"environment": "old"}], [{"environment": "new"}]]
    cloud.for_item.side_effect = [
        [{"phase": "PLACEMENT_MISS"}],
        [{"phase": "PLACEMENT_ACCEPTED"}],
    ]
    cloud.poll.side_effect = lambda predicate, **_: predicate()
    finish = Mock()
    monkeypatch.setattr(test_lifecycle, "finish_anchor", finish)
    anchor, _gate, victim = test_lifecycle.place_with_healthy_anchor(
        cloud, "return-inflight"
    )
    assert anchor is second and victim is admitted
    cloud.finish.assert_called_once_with(missed)
    assert finish.call_count == 1
    assert cloud.start.call_args_list[1].kwargs["target_environment"] == "old"
    assert cloud.start.call_args_list[3].kwargs["target_environment"] == "new"


def run_inflight_case(scenario, status):
    controls = Controls()
    for suffix in ("loser", "loser-started"):
        controls.put_object(Key="runs/unit/control/local-" + suffix, Body=b"hold")

    def entry(event, context):
        trace = controls.trace("local", scenario)
        trace.emit("WRAPPER_ENTER", resources=resources())
        result = durable_execution(lambda e, c: workflow(e, c, trace))(event, context)
        trace.emit("WRAPPER_RETURN", status=result["Status"], resources=resources())
        return result

    with DurableFunctionTestRunner(entry, poll_interval=0.01) as runner:
        arn = runner.run_async(input={"marker": "local", "scenario": scenario})
        try:
            controls.wait("USER_EXIT")
            evidence.held_loser(controls.snapshot())
            evidence.scope_exit(controls.snapshot(), "local-loser")
        finally:
            controls.put_object(Key="runs/unit/control/local-loser", Body=b"release")
        result = runner.wait_for_result(arn, timeout=5)
        history = runner.get_execution_history(arn).to_dict()["Events"]
    assert result.status.value == status
    if status == "FAILED":
        assert (
            result.error.message == "expected:local"
            and result.error.type == "ValueError"
        )
    evidence.scope_exit(controls.snapshot(), "local-loser", status)
    return controls.snapshot(), history


@pytest.mark.parametrize(
    "scenario,status",
    [("return-inflight", "SUCCEEDED"), ("failure-inflight", "FAILED")],
)
def test_inflight_return_and_failure_use_real_sdk_scopes(scenario, status):
    run_inflight_case(scenario, status)


def run_shared_roots(nested):
    """Runs in a child process so a future executor deadlock cannot hang pytest."""
    rendezvous = threading.Barrier(2, timeout=3)

    class QuietTrace:
        def emit(self, *_args, **_kwargs):
            pass

        def body(self, _name, value, _step=None):
            return value

    @durable_execution
    def handler(event, context):
        context.step(lambda _: rendezvous.wait(), name="root-rendezvous")
        if nested:
            nested_progress(context, QuietTrace(), event["marker"])
        return context.step(lambda _: event["marker"], name="result")

    with DurableFunctionTestRunner(handler, poll_interval=0.01) as runner:
        arns = [
            runner.run_async(input={"marker": marker}, execution_name=marker)
            for marker in ("left", "right")
        ]
        for marker, arn in zip(("left", "right"), arns):
            result = runner.wait_for_result(arn, timeout=5)
            assert result.status.value == "SUCCEEDED"
            assert json.loads(result.result) == marker


@pytest.mark.parametrize("nested", [False, True])
def test_shared_decorator_roots_progress_without_starving_children(nested):
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from lmi_tests.tests.unit.test_java_parity import run_shared_roots; "
                f"run_shared_roots({nested!r})",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "Shared root/nested work exceeded its bounded executor progress test"
        )
    assert result.returncode == 0, result.stdout + result.stderr
