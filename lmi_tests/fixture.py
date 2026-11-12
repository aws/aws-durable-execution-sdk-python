# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Fault-injection handlers for real LMI. Never install these as SDK examples."""

from contextlib import nullcontext
import fcntl
import json
import os
from pathlib import Path
import threading
import time
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python import StepError, durable_execution
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    Duration,
    MapConfig,
    ParallelConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
)
from aws_durable_execution_sdk_python.lambda_service import LambdaClient
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.exceptions import OrphanedChildException
from lmi_tests.shared_observations import (
    GateRelease,
    LifecycleObservationFactory,
    LifecycleObservations,
)


PROCESS_ID = uuid.uuid4().hex
IO_CONFIG = Config(connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1})
NO_RETRY = StepConfig(retry_strategy=lambda _error, _attempt: RetryDecision.no_retry())


def environment_id(path="/tmp/python-lmi-environment"):
    """/tmp is shared by the LMI Python workers; lock initialization across processes."""
    with open(path, "a+") as marker:
        fcntl.flock(marker, fcntl.LOCK_EX)
        marker.seek(0)
        value = marker.read().strip()
        if not value:
            value = uuid.uuid4().hex
            marker.write(value)
            marker.flush()
            os.fsync(marker.fileno())
        return value


def resources():
    # Current RSS, not ru_maxrss (a lifetime high-water mark).
    rss_pages = int(Path("/proc/self/statm").read_text().split()[1])
    return {
        "threads": [{"id": t.ident, "name": t.name} for t in threading.enumerate()],
        "fds": len(list(Path("/proc/self/fd").iterdir())),
        "rss": rss_pages * os.sysconf("SC_PAGE_SIZE"),
    }


def observe(trace, kind, **fields):
    """Preserve the legacy ledger when this invocation is not a shared case."""
    if trace.payload.get("coverageProfile"):
        return trace.observe(kind, **fields)
    return None


def configuration(trace, name, max_concurrency):
    if trace.payload.get("coverageProfile"):
        return trace.observer.configuration(name, max_concurrency)
    return nullcontext()


class Trace:
    def __init__(self, payload, context, execution_arn):
        self.payload = dict(payload)
        if "runId" in self.payload:
            self.payload.setdefault("run", self.payload["runId"])
        if "otelView" in self.payload:
            self.payload.setdefault("otel_view", self.payload["otelView"])
        self.shared = bool(self.payload.get("coverageProfile"))
        if self.shared:
            for key in (
                "runId",
                "caseId",
                "cohortId",
                "role",
                "sharedScenario",
                "controlPrefix",
            ):
                if not isinstance(self.payload.get(key), str) or not self.payload[key]:
                    raise ValueError(f"Shared invocation requires {key}")
            if not self.payload["controlPrefix"].startswith(
                f"runs/{self.payload['run']}/cases/"
            ) or not self.payload["controlPrefix"].endswith("/"):
                raise ValueError("Shared controls must use a case-owned run prefix")
        self.context = context
        self.s3 = boto3.client("s3", config=IO_CONFIG)
        self.bucket = os.environ["LMI_BUCKET"]
        self.lock = threading.Lock()
        # Never make observation capture wait for the S3 publication lock.
        self.capture_lock = threading.Lock()
        self.capture_sequence = 0
        self.sequence = 0
        self.call_sequence = 0
        self.sdk_completed_at = None
        self.sdk_observation = None
        self.sdk_resources = None
        self.identity = {
            "run": os.environ["LMI_RUN_ID"],
            "commit": os.environ["LMI_COMMIT"],
            "marker": self.payload["marker"],
            "scenario": self.payload["scenario"],
            "execution": execution_arn,
            "request": context.aws_request_id,
            "environment": environment_id(),
            "process": PROCESS_ID,
            "pid": os.getpid(),
        }
        if self.shared:
            self.identity.update(
                {
                    key: self.payload[key]
                    for key in (
                        "runId",
                        "caseId",
                        "cohortId",
                        "role",
                        "coverageProfile",
                        "sharedScenario",
                        "otelView",
                        "exportMode",
                        "behaviorBudgets",
                    )
                    if key in self.payload
                }
            )
        self.control_prefix = self.payload.get(
            "controlPrefix", f"runs/{self.identity['run']}/control/"
        )
        self.deadline_observed_at_ms = time.time() * 1000
        self.remaining_ms = context.get_remaining_time_in_millis()
        self.deadline = (self.deadline_observed_at_ms + self.remaining_ms) / 1000
        self.observer = LifecycleObservations(self) if self.shared else None

    def emit(self, phase, **fields):
        # Keep original evidence order independently of retrospective boundaries.
        with self.lock:
            self.sequence += 1
            if phase == "CONFORMANCE":
                fields["observation"] = {
                    **fields["observation"],
                    "sourceSequence": self.sequence,
                }
            event = {
                **self.identity,
                "phase": phase,
                "sequence": self.sequence,
                "time": time.time(),
                "deadline": self.deadline,
                **fields,
            }
            print("LMI_TEST " + json.dumps(event), flush=True)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"runs/{self.identity['run']}/events/{self.identity['marker']}/{self.identity['request']}/{self.sequence:06d}.json",
                Body=json.dumps(event).encode(),
                ContentType="application/json",
            )
        return event

    def capture_observation(self, kind, **fields):
        with self.capture_lock:
            self.capture_sequence += 1
            at_ms = fields.pop("atMs", time.time_ns() / 1_000_000)
            return {
                "kind": kind,
                "atMs": at_ms,
                **fields,
                "captureSequence": self.capture_sequence,
            }

    def publish_observation(self, observation):
        return self.emit("CONFORMANCE", observation=observation)

    def observe(self, kind, **fields):
        return self.publish_observation(self.capture_observation(kind, **fields))

    def next_call_id(self, kind):
        with self.lock:
            self.call_sequence += 1
            return f"{self.identity['request']}:{kind}:{self.call_sequence}"

    def signal(self, name):
        self.s3.put_object(
            Bucket=self.bucket, Key=self.control_prefix + name, Body=b"release"
        )

    def released(self, name):
        try:
            response = self.s3.get_object(
                Bucket=self.bucket, Key=self.control_prefix + name
            )
            with response["Body"] as body:
                state = body.read()
            if state not in {b"hold", b"release"}:
                raise ValueError(f"Invalid control state for {name}")
            return state == b"release"
        except (ClientError, ValueError) as error:
            self.emit("CONTROL_ERROR", gate=name, error=str(error))
            observe(self, "control_error", gateId=name, error=str(error))
            raise

    def gate(self, name, *, effects=False, attempt=1, ready=None):
        # This return value is checkpointed by existing controlled-io steps.
        self.gate_release(name, effects=effects, attempt=attempt, ready=ready)

    def gate_release(
        self,
        name,
        *,
        effects=False,
        attempt=1,
        ready=None,
        on_hold=None,
        on_release=None,
    ):
        """Observe normal versus emergency release without changing step results."""
        end = time.monotonic() + 150
        entered = False
        held_at_ms = None
        task_id = self.next_call_id("io") if self.shared else None
        observe(self, "io_start", taskId=task_id, gateId=name)
        try:
            while True:
                # Shared cleanup releases the case abort control first. It must
                # not masquerade as a normal measurement release of the target.
                if self.shared and self.released("release-all"):
                    mode = "emergency"
                    release_observed_at_ms = time.time_ns() / 1_000_000
                else:
                    specific_release = self.released(name)
                    target_observed_at_ms = time.time_ns() / 1_000_000
                    mode = (
                        "normal"
                        if specific_release
                        else "emergency"
                        if not self.shared and self.released("release-all")
                        else None
                    )
                    release_observed_at_ms = (
                        target_observed_at_ms
                        if specific_release
                        else time.time_ns() / 1_000_000
                    )
                if mode is not None:
                    released = GateRelease(
                        name, release_observed_at_ms, mode, held_at_ms
                    )
                    if on_release is not None:
                        on_release(released)
                    observe(
                        self,
                        "fault_released",
                        gateId=name,
                        mode=mode,
                        atMs=released.observed_at_ms,
                        source="control-poll",
                    )
                    return released
                if not entered:
                    held_at_ms = target_observed_at_ms
                    if on_hold is not None:
                        on_hold(held_at_ms)
                    self.emit("BLOCKED", gate=name)
                    observe(self, "blocked", gateId=name, atMs=held_at_ms)
                if time.monotonic() >= end:
                    self.emit("ESCAPE", gate=name)
                    observe(self, "escape", gateId=name)
                    raise TimeoutError(
                        "Test emergency release; never a passing regression"
                    )
                if effects:
                    self.emit(
                        "EFFECT", operation="blocked-io", gate=name, attempt=attempt
                    )
                    observe(
                        self,
                        "effect",
                        operation="blocked-io",
                        gateId=name,
                        attempt=attempt,
                    )
                else:
                    self.emit("ALIVE", gate=name)
                    observe(self, "alive", gateId=name)
                if not entered and ready:
                    self.signal(ready)
                entered = True
                time.sleep(0.2)  # fault-injected blocking I/O, not a durable delay
        except BaseException as error:
            self.emit("IO_INTERRUPTED", gate=name, error=type(error).__name__)
            observe(
                self,
                "io_interrupted",
                taskId=task_id,
                gateId=name,
                error=type(error).__name__,
            )
            raise
        finally:
            exited_at = time.time() * 1000
            self.emit("IO_EXIT", gate=name)
            observe(self, "io_exit", taskId=task_id, gateId=name, atMs=exited_at)

    def body(self, name, value, step=None):
        attempt = getattr(step, "attempt", 1)
        self.emit("BODY", operation=name, value=value, attempt=attempt)
        if self.shared:
            self.observer.body(name, value, attempt)
        return value


class ObservedClient:
    """Forward real SDK calls; never fabricate checkpoint acknowledgments."""

    def __init__(self, client, trace):
        self.client, self.trace = client, trace

    def get_execution_state(self, **kwargs):
        call_id = (
            self.trace.next_call_id("poll")
            if self.trace.payload.get("coverageProfile")
            else None
        )
        self.trace.emit("POLL_CALL")
        observe(self.trace, "poll_call", callId=call_id)
        try:
            return self.client.get_execution_state(**kwargs)
        finally:
            exited_at = time.time() * 1000
            self.trace.emit("POLL_EXIT")
            observe(self.trace, "poll_exit", callId=call_id, atMs=exited_at)

    def checkpoint(self, **kwargs):
        updates = [
            {
                "id": u.operation_id,
                "name": u.name,
                "action": u.action.value,
                "type": u.operation_type.value,
            }
            for u in kwargs["updates"]
        ]
        call_id = (
            self.trace.next_call_id("checkpoint")
            if self.trace.payload.get("coverageProfile")
            else None
        )
        self.trace.emit("CHECKPOINT_CALL", updates=updates)
        observe(self.trace, "checkpoint_call", callId=call_id, updates=updates)
        try:
            response = self.client.checkpoint(**kwargs)
            acknowledged_at = time.time() * 1000
            self.trace.emit("CHECKPOINT_ACK", updates=updates)
            observe(
                self.trace,
                "checkpoint_ack",
                callId=call_id,
                operationIds=[item["id"] for item in updates],
                source="service",
                atMs=acknowledged_at,
            )
            if self.trace.payload["scenario"] == "checkpoint" and any(
                u["name"] == "checkpoint-loser" and u["action"] == "SUCCEED"
                for u in updates
            ):
                # Real service response, withheld while a synchronous waiter owns a
                # branch. This tests settlement/join ordering without a fake service.
                self.trace.emit("ACK_HELD")
                observed_operation = next(
                    item["id"]
                    for item in updates
                    if item["name"] == "checkpoint-loser"
                    and item["action"] == "SUCCEED"
                )
                observe(
                    self.trace,
                    "ack_held",
                    callId=call_id,
                    operationId=observed_operation,
                    gateId=self.trace.payload["marker"] + "-checkpoint",
                )
                self.trace.gate(self.trace.payload["marker"] + "-checkpoint")
            return response
        except BaseException as error:
            self.trace.emit("CHECKPOINT_ERROR", error=type(error).__name__)
            observe(
                self.trace,
                "checkpoint_error",
                callId=call_id,
                error=type(error).__name__,
            )
            raise
        finally:
            exited_at = time.time() * 1000
            self.trace.emit("CHECKPOINT_EXIT")
            observe(self.trace, "checkpoint_exit", callId=call_id, atMs=exited_at)


def nested_progress(context, trace, marker):
    """Record public one-lane configurations without changing nested work."""
    trace.emit("PROGRESS_BEGIN")
    observe(trace, "progress_begin")

    def children(child):
        def mapped(branch):
            config = MapConfig(max_concurrency=1)
            with configuration(trace, "inner-map", config.max_concurrency):
                return branch.map(
                    [0, 1],
                    lambda inner, item, _index, _all: inner.run_in_child_context(
                        lambda leaf: leaf.step(
                            lambda step: trace.body("leaf", f"{marker}:{item}", step),
                            name="leaf",
                        ),
                        name="grandchild",
                    ),
                    name="inner-map",
                    config=config,
                ).get_results()

        config = ParallelConfig(max_concurrency=1)
        with configuration(trace, "outer-parallel", config.max_concurrency):
            return child.parallel(
                [
                    mapped,
                    lambda branch: branch.run_in_child_context(
                        lambda leaf: leaf.step(
                            lambda step: trace.body("sibling", marker, step),
                            name="sibling",
                        ),
                        name="sibling-child",
                    ),
                ],
                name="outer-parallel",
                config=config,
            ).get_results()

    result = context.run_in_child_context(children, name="progress-child")
    assert result == [[marker + ":0", marker + ":1"], marker]
    trace.emit("PROGRESS", value=result)
    observe(trace, "progress", value=result)


def workflow(event, context, trace):
    scope = (
        trace.observer.root_scope() if event.get("coverageProfile") else nullcontext()
    )
    with scope:
        return run_workflow(event, context, trace)


def run_workflow(event, context, trace):
    marker, scenario = event["marker"], event["scenario"]
    admitted = False
    try:
        if target := event.get("target_environment"):
            # Record placement before any fault work. Replays consume the same
            # decision even if Lambda later resumes in another environment.
            accepted = context.step(
                lambda _: trace.identity["environment"] == target,
                name="placement-admission",
                config=NO_RETRY,
            )
            trace.emit(
                "PLACEMENT_ACCEPTED" if accepted else "PLACEMENT_MISS", target=target
            )
            if not accepted:
                return marker
        admitted = True
        value = context.step(lambda s: trace.body("success", marker, s), name="success")
        if scenario == "failure":
            raise ValueError("expected:" + marker)
        if scenario in {"barrier", "deadline", "nested-progress"}:
            context.step(
                lambda step: trace.gate(
                    event["gate"], effects=scenario == "deadline", attempt=step.attempt
                ),
                name="controlled-io",
                config=NO_RETRY,
            )
            context.step(lambda s: trace.body("after-io", marker, s), name="after-io")
            if scenario == "nested-progress":
                nested_progress(context, trace, marker)
        elif scenario == "suspend-cleanup":
            context.wait(Duration.from_seconds(5), name="pause")
        elif scenario in {
            "parallel",
            "map",
            "nested",
            "return-inflight",
            "failure-inflight",
            "late-operation",
        }:

            def losing(child):
                def io(step):
                    trace.gate(
                        marker + "-loser",
                        effects=True,
                        attempt=step.attempt,
                        ready=marker + "-loser-started",
                    )
                    return marker

                try:
                    return child.step(io, name="losing-io", config=NO_RETRY)
                finally:
                    if scenario == "late-operation":
                        # The preceding step's final checkpoint can itself be
                        # rejected after the parent finishes. Still test the next
                        # SDK boundary, without swallowing unrelated failures.
                        trace.emit("LATE_ATTEMPT")
                        scope_id = (
                            trace.observer.closed_ancestor()
                            if event.get("coverageProfile")
                            else None
                        )
                        observe(trace, "late_attempt", scopeId=scope_id)
                        try:
                            child.step(
                                lambda step: trace.body("late-work", marker, step),
                                name="late-work",
                                config=NO_RETRY,
                            )
                        except OrphanedChildException as error:
                            trace.emit("LATE_REJECTED", error=type(error).__name__)
                            observe(
                                trace,
                                "late_rejected",
                                scopeId=scope_id,
                                error={"type": type(error).__name__},
                            )
                        else:
                            trace.emit("LATE_ACCEPTED")
                            observe(trace, "late_accepted", scopeId=scope_id)

            def winning(child):
                def io(_):
                    trace.gate(marker + "-loser-started")
                    trace.emit("WINNER_READY")
                    observe(trace, "winner_ready")
                    return marker

                return child.step(io, name="winner", config=NO_RETRY)

            def race(child):
                if scenario == "map":
                    return child.map(
                        [0, 1],
                        lambda c, item, _index, _all: (
                            losing(c) if item == 0 else winning(c)
                        ),
                        name="race-map",
                        config=MapConfig(
                            max_concurrency=2,
                            completion_config=CompletionConfig(min_successful=1),
                        ),
                    )
                return child.parallel(
                    [losing, winning],
                    name="race-parallel",
                    config=ParallelConfig(
                        max_concurrency=2,
                        completion_config=CompletionConfig.first_successful(),
                    ),
                )

            if scenario == "nested":
                context.map(
                    [0],
                    lambda c, _item, _index, _all: c.run_in_child_context(
                        race, name="nested-child"
                    ),
                    name="outer-map",
                    config=MapConfig(max_concurrency=3),
                )
            else:
                race(context)
            trace.emit("WINNER_SELECTED")
            observe(trace, "winner_selected")
            if scenario == "failure-inflight":
                trace.emit("ROOT_FAILURE_READY")
                raise ValueError("expected:" + marker)
        elif scenario == "checkpoint":
            context.parallel(
                [
                    lambda c: c.step(lambda _: marker, name="checkpoint-loser"),
                    lambda c: c.step(lambda _: marker, name="checkpoint-peer"),
                ],
                name="checkpoint-pool",
                config=ParallelConfig(max_concurrency=2),
            )
            if event.get("coverageProfile"):
                scope_id = trace.observer.completed_context("checkpoint-pool")
                trace.observe("branch_join", scopeId=scope_id)
        elif scenario == "replay":

            def fail(step):
                trace.body("failure", marker, step)
                raise ValueError("expected:" + marker)

            try:
                context.step(fail, name="failure", config=NO_RETRY)
            except StepError as error:
                trace.emit(
                    "STORED_FAILURE", message=error.message, error_type=error.error_type
                )
                observe(
                    trace,
                    "stored_failure",
                    error={"type": error.error_type, "message": error.message},
                )

            def retry(step):
                trace.body("retry", marker, step)
                if step.attempt == 1:
                    raise ValueError("retry once")
                return marker

            context.step(
                retry,
                name="retry",
                config=StepConfig(
                    retry_strategy=lambda _e, n: (
                        RetryDecision.retry(Duration.from_seconds(1))
                        if n == 1
                        else RetryDecision.no_retry()
                    )
                ),
            )
            context.run_in_child_context(
                lambda c: c.map(
                    [1, 2, 3],
                    lambda inner, item, _index, _all: inner.step(
                        lambda _: item, name="item"
                    ),
                    name="items",
                    config=MapConfig(max_concurrency=3),
                ),
                name="nested",
            )
            context.parallel(
                [
                    lambda c: c.step(lambda _: value, name="left"),
                    lambda c: c.step(lambda _: value, name="right"),
                ],
                name="pair",
            )
            context.wait(Duration.from_seconds(2), name="pause")
            callback = context.create_callback(name="completion")
            assert callback.result() == marker
        trace.emit("USER_RESULT", value=value)
        return value
    finally:
        if scenario == "suspend-cleanup" and admitted:
            # Test-only root-finally diagnostics. This gate changes only exit
            # timing; it introduces no durable operations during suspension.
            trace.emit("CLEANUP_ENTER")
            observe(trace, "cleanup_enter")
            try:
                trace.gate(marker + "-cleanup")
            finally:
                trace.emit("CLEANUP_EXIT")
                observe(trace, "cleanup_exit")
        exited_at = time.time() * 1000
        trace.emit("USER_EXIT")
        observe(trace, "user_exit", atMs=exited_at)


def handler(event, context):
    invocation = (
        event
        if isinstance(event, DurableExecutionInvocationInputWithClient)
        else DurableExecutionInvocationInput.from_json_dict(event)
    )
    payload = next(
        json.loads(op.execution_details.input_payload)
        for op in invocation.initial_execution_state.operations
        if op.execution_details and op.execution_details.input_payload
    )
    payload = dict(payload)
    if "runId" in payload:
        payload.setdefault("run", payload["runId"])
    if "otelView" in payload:
        payload.setdefault("otel_view", payload["otelView"])
    if payload["run"] != os.environ["LMI_RUN_ID"]:
        raise ValueError("Input belongs to a different deployment")
    trace = Trace(payload, context, invocation.durable_execution_arn)
    source_client = (
        invocation.service_client
        if isinstance(invocation, DurableExecutionInvocationInputWithClient)
        else LambdaClient.initialize_client()
    )
    client = ObservedClient(source_client, trace)
    injected = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        invocation, client
    )

    def invoke(plugins=None):
        configured_plugins = (
            [LifecycleObservationFactory(trace.observer), *(plugins or [])]
            if payload.get("coverageProfile")
            else plugins
        )
        decorated = durable_execution(
            lambda e, c: workflow(e, c, trace), plugins=configured_plugins
        )
        try:
            return decorated(injected, context)
        finally:
            # Preserve the actual SDK boundary before post-call evidence I/O.
            completed_at_ns = time.time_ns()
            trace.sdk_completed_at = completed_at_ns / 1_000_000_000
            if payload.get("coverageProfile"):
                trace.sdk_observation = trace.capture_observation(
                    "sdk_completion", atMs=completed_at_ns / 1_000_000
                )
            trace.sdk_resources = resources()

    before = resources()
    entered_at = time.time() * 1000
    trace.emit("WRAPPER_ENTER", resources=before)
    observe(trace, "wrapper_enter", atMs=entered_at, resources=before)
    if payload.get("coverageProfile"):
        trace.observe(
            "runtime_deadline",
            atMs=trace.deadline_observed_at_ms,
            remainingTimeMs=trace.remaining_ms,
            deadlineAtMs=trace.deadline * 1000,
            source="runtime",
        )
    try:
        if payload.get("otel_view"):
            from lmi_tests.telemetry import invoke as invoke_with_telemetry

            result = invoke_with_telemetry(trace, invoke)
        else:
            result = invoke()
        trace.emit(
            "WRAPPER_RETURN",
            status=result["Status"],
            resources=trace.sdk_resources,
            time=trace.sdk_completed_at,
        )
        if payload.get("coverageProfile"):
            trace.publish_observation(
                {
                    **trace.sdk_observation,
                    "kind": "wrapper_return",
                    "outcome": result["Status"],
                    "resources": trace.sdk_resources,
                }
            )
        return result
    except BaseException as error:
        if payload.get("coverageProfile") and trace.sdk_observation is None:
            trace.sdk_observation = trace.capture_observation("sdk_completion")
        trace.emit(
            "WRAPPER_RAISE",
            error=type(error).__name__,
            resources=getattr(trace, "sdk_resources", None) or resources(),
            time=getattr(trace, "sdk_completed_at", None) or time.time(),
        )
        if payload.get("coverageProfile"):
            trace.publish_observation(
                {
                    **trace.sdk_observation,
                    "kind": "wrapper_raise",
                    "outcome": "THREW",
                    "error": {"type": type(error).__name__, "message": str(error)},
                    "resources": getattr(trace, "sdk_resources", None) or resources(),
                }
            )
        raise
