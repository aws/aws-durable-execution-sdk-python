# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Fault-injection handlers for real LMI. Never install these as SDK examples."""

from contextlib import nullcontext
from dataclasses import dataclass, field
import fcntl
import json
import os
import threading
import time
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python import StepError, durable_execution
from aws_durable_execution_sdk_python.config import (
    Duration,
    MapConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
)
from aws_durable_execution_sdk_python.lambda_service import LambdaClient
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    CheckpointError,
    OrphanedChildException,
    SuspendExecution,
)
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
    return {"threads": [{"id": t.ident, "name": t.name} for t in threading.enumerate()]}


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
        self.shared = bool(self.payload.get("coverageProfile"))
        if self.shared:
            for key in (
                "runId",
                "caseId",
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
        self.sdk_request_count = 0
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
                        "role",
                        "coverageProfile",
                        "sharedScenario",
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
            if kind in {"checkpoint", "poll"}:
                self.sdk_request_count += 1
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

    def gate(self, name, *, effects=False, attempt=1, ready=None, preparation=False):
        # This return value is checkpointed by existing controlled-io steps.
        self.gate_release(
            name, effects=effects, attempt=attempt, ready=ready, preparation=preparation
        )

    def gate_release(
        self,
        name,
        *,
        effects=False,
        attempt=1,
        ready=None,
        on_hold=None,
        on_release=None,
        preparation=False,
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
                    if preparation and mode == "normal":
                        # The driver releases preparation only after the peer
                        # joined. This observes the cohort barrier within the
                        # original invocations' overlap, not at early warm-up.
                        observe(
                            self,
                            "barrier_arrived",
                            gateId=name,
                            atMs=released.observed_at_ms,
                            source="cohort-admission",
                        )
                    observe(
                        self,
                        "fault_released",
                        gateId=name,
                        mode=mode,
                        atMs=released.observed_at_ms,
                        source="control-poll",
                    )
                    return released
                if (
                    self.payload["scenario"] == "deadline"
                    and not preparation
                    and time.time() >= self.deadline + 0.2
                ):
                    observe(self, "cooperative_io_cancelled", source="runtime-deadline")
                    raise TimeoutError(
                        "Cooperative fixture I/O crossed the runtime deadline"
                    )
                if not entered:
                    held_at_ms = target_observed_at_ms
                    if on_hold is not None:
                        on_hold(held_at_ms)
                    self.emit("BLOCKED", gate=name)
                    observe(self, "blocked", gateId=name, atMs=held_at_ms)
                    if not preparation and not self.payload.get("preparation_gate"):
                        observe(self, "barrier_arrived", gateId=name, atMs=held_at_ms)
                    if self.payload["scenario"] == "deadline" and not preparation:
                        observe(self, "work_in_flight", gateId=name, atMs=held_at_ms)
                    if self.payload.get("role") == "recovery":
                        observe(self, "worker_recovered", atMs=held_at_ms)
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
                    if self.payload.get("role") == "peer":
                        observe(self, "peer_progress", gateId=name)
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


def workflow(event, context, trace):
    trace.durable_context = context
    scope = trace.observer.root_scope() if trace.shared else nullcontext()
    with scope:
        try:
            result = run_workflow(event, context, trace)
        except Exception:
            observe(trace, "root_completed", outcome="FAILED")
            raise
        else:
            observe(trace, "root_completed", outcome="SUCCEEDED")
            return result


def run_workflow(event, context, trace):
    marker, scenario = event["marker"], event["scenario"]
    if scenario not in {
        "success",
        "failure",
        "barrier",
        "deadline",
        "replay",
        "warm-owner",
        "warm-probe",
    }:
        raise ValueError(f"Unsupported lifecycle fixture: {scenario}")
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
        value = context.step(lambda s: trace.body("success", marker, s), name="success")
        if scenario == "failure":
            raise ValueError("expected:" + marker)
        if scenario in {"barrier", "deadline"}:
            original_request = None
            if preparation_gate := event.get("preparation_gate"):

                def prepare(_):
                    trace.gate(preparation_gate, preparation=True)
                    return trace.identity["request"]

                original_request = context.step(
                    prepare, name="prepare-recovery", config=NO_RETRY
                )
                trace.recovery_original_request = original_request

            def controlled_io(step):
                # Fault injection belongs to the selected native invocation.
                # This decision is inside the step; replay still consumes the
                # same preparation checkpoint and durable operation sequence.
                if (
                    original_request is not None
                    and trace.identity["request"] != original_request
                ):
                    observe(
                        trace, "fault_not_reapplied", originalRequestId=original_request
                    )
                    return None
                return trace.gate(
                    event["gate"], effects=scenario == "deadline", attempt=step.attempt
                )

            context.step(controlled_io, name="controlled-io", config=NO_RETRY)
            context.step(lambda s: trace.body("after-io", marker, s), name="after-io")
        elif scenario == "warm-owner":
            context.step(
                lambda _: start_warm_work(trace),
                name="start-late-work",
                config=NO_RETRY,
            )
        elif scenario == "warm-probe":
            context.step(
                lambda _: release_warm_work(trace),
                name="release-late-work",
                config=NO_RETRY,
            )
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
        exited_at = time.time() * 1000
        trace.emit("USER_EXIT")
        observe(trace, "user_exit", atMs=exited_at)


def admission_probes(trace):
    trace.observe("task_start", taskId="admission-probes", source="fixture")
    closed = False
    body_executed = False

    def body(_):
        nonlocal body_executed
        body_executed = True
        trace.observe("probe_body_executed")

    try:
        for probe, operation in (
            (
                "checkpoint",
                lambda: trace.durable_context.state.create_checkpoint(is_sync=False),
            ),
            (
                "operation",
                lambda: trace.durable_context.step(
                    body, name="operation-admission-probe", config=NO_RETRY
                ),
            ),
        ):
            trace.observe("probe_enter", probe=probe)
            calls_before = trace.sdk_request_count
            error_record = None
            try:
                operation()
            except (
                OrphanedChildException,
                BackgroundThreadError,
                CheckpointError,
                SuspendExecution,
            ) as error:
                # Failed/revoked SDK state can reject by re-raising its stored
                # error. It is a rejection only before user code or a new RPC.
                outcome = (
                    "REJECTED"
                    if not body_executed and trace.sdk_request_count == calls_before
                    else "ERROR"
                )
                error_record = {"type": type(error).__name__, "message": str(error)}
            except BaseException as error:
                outcome = "ERROR"
                error_record = {"type": type(error).__name__, "message": str(error)}
            else:
                outcome = "ACCEPTED"
            observed_at = time.time_ns() / 1_000_000
            if outcome == "REJECTED" and not closed:
                trace.observe(
                    "admission_closed", atMs=observed_at, source="sdk-rejection"
                )
                closed = True
            trace.observe(
                "admission_probe",
                probe=probe,
                outcome=outcome,
                error=error_record,
                atMs=observed_at,
                bodyExecuted=body_executed,
                newSdkRequests=trace.sdk_request_count - calls_before,
            )
    finally:
        trace.observe("task_exit", taskId="admission-probes", source="fixture")


def start_admission_probes(trace):
    # A broken SDK can leave a late operation waiting forever. Keep that real
    # task visible to the retirement audit instead of declaring it settled.
    thread = threading.Thread(
        target=admission_probes, args=(trace,), daemon=True, name="lmi-admission-probes"
    )
    thread.start()
    thread.join(timeout=0.5)


@dataclass
class WarmWork:
    trace: object
    ready: threading.Event = field(default_factory=threading.Event)
    observed_during: str | None = None


WARM_WORK = {}
WARM_LOCK = threading.Lock()


def start_warm_work(trace):
    key = (trace.payload["runId"], trace.payload["caseId"])
    work = WarmWork(trace)
    with WARM_LOCK:
        if key in WARM_WORK:
            raise ValueError("A warm case cannot replace its original work")
        WARM_WORK[key] = work

    def late():
        trace.observe("task_start", taskId="late-callback", source="fixture")
        try:
            if not work.ready.wait(timeout=120):
                trace.observe(
                    "observation_error",
                    message="Original late work never observed warm reuse",
                )
                return
            trace.observe("late_activity", observedDuringRequestId=work.observed_during)
            admission_probes(trace)
        finally:
            trace.observe("task_exit", taskId="late-callback", source="fixture")
            with WARM_LOCK:
                WARM_WORK.pop(key, None)

    threading.Thread(target=late, daemon=True, name="lmi-late-callback").start()
    return None


def release_warm_work(trace):
    key = (trace.payload["runId"], trace.payload["caseId"])
    with WARM_LOCK:
        work = WARM_WORK.get(key)
    if work is None:
        raise ValueError("Warm request did not reuse the original process")
    work.observed_during = trace.identity["request"]
    work.ready.set()
    # Keep the warm invocation alive long enough to observe the old callback.
    time.sleep(0.5)
    return None


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
            # Both semantic records describe this same boundary. Converting
            # seconds back to milliseconds can round to a different float.
            trace.sdk_completed_at_ms = completed_at_ns / 1_000_000
            if payload.get("coverageProfile"):
                trace.sdk_observation = trace.capture_observation(
                    "sdk_completion", atMs=trace.sdk_completed_at_ms
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
        if result["Status"] == "PENDING":
            observe(
                trace,
                "pending",
                atMs=trace.sdk_completed_at_ms,
                source="sdk-wrapper",
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
    finally:
        if (
            payload.get("sharedScenario")
            in {
                "normal-return",
                "normal-failure",
                "deadline-during-step",
                "peer-recovery",
            }
            and payload.get("role") == "victim"
            and (
                payload.get("sharedScenario") != "peer-recovery"
                or trace.identity["request"]
                == getattr(trace, "recovery_original_request", None)
            )
        ):
            start_admission_probes(trace)
