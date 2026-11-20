# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Fault-injection handlers for real LMI. Never install these as SDK examples."""

from collections.abc import Callable
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
import fcntl
import hashlib
import json
import os
import threading
import time
from typing import Any
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

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
from aws_durable_execution_sdk_python.lambda_service import (
    LambdaClient,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    CheckpointError,
    OrphanedChildException,
    SuspendExecution,
)
from lmi_tests.shared_observations import (
    LifecycleObservations,
)


PROCESS_ID = uuid.uuid4().hex
PUBLICATION_BUDGET_SECONDS = 5.0
publication_clock = time.monotonic
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


class Trace:
    def __init__(self, payload, context, execution_arn):
        self.payload = dict(payload)
        for key in (
            "runId",
            "caseId",
            "role",
            "coverageProfile",
            "sharedScenario",
            "controlPrefix",
        ):
            if not isinstance(self.payload.get(key), str) or not self.payload[key]:
                raise ValueError(f"Shared invocation requires {key}")
        run = self.payload["runId"]
        if self.payload.get("run", run) != run or run != os.environ["LMI_RUN_ID"]:
            raise ValueError("Input belongs to a different deployment")
        self.payload["run"] = run
        # The deployed fixture bundle omits the conformance package. Match
        # its case_key derivation when checking the supplied control namespace.
        owner = hashlib.sha256(self.payload["caseId"].encode()).hexdigest()[:20]
        if self.payload["controlPrefix"] != f"runs/{run}/cases/{owner}/control/":
            raise ValueError("Shared controls must use the case-owned run prefix")
        self.context = context
        self.s3 = boto3.client("s3", config=IO_CONFIG)
        self.bucket = os.environ["LMI_BUCKET"]
        # Publication sequences are assigned under the ledger lock.
        self.lock = threading.Lock()
        # Work registration and seal eligibility share this separate state lock.
        self.state_lock = threading.Lock()
        self.capture_sequence = 0
        self.publication_seconds = 0.0
        self.active_workers = 0
        self.wrapper_finished = False
        self.source_failed = False
        self.closed = False
        self.sequence = 0
        self.call_sequence = 0
        self.sdk_request_count = 0
        self.durable_context = None
        self.identity = {
            "run": run,
            "commit": os.environ["LMI_COMMIT"],
            "marker": self.payload["marker"],
            "scenario": self.payload["scenario"],
            "execution": execution_arn,
            "request": context.aws_request_id,
            "environment": environment_id(),
            "process": PROCESS_ID,
            "pid": os.getpid(),
        }
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
        self.control_prefix = self.payload["controlPrefix"]
        self.deadline_observed_at_ms = time.time() * 1000
        self.remaining_ms = context.get_remaining_time_in_millis()
        self.deadline = (self.deadline_observed_at_ms + self.remaining_ms) / 1000
        self.observer = LifecycleObservations(self)

    def emit(self, phase, **fields):
        with self.lock:
            return self._publish_record(phase, **fields)

    def _publish_record(self, phase: str, **fields: Any) -> dict[str, Any]:
        with self.state_lock:
            # Once a pre-seal record is lost, further evidence uploads cannot
            # certify this request. Avoid repeating a slow failed retry budget.
            if self.source_failed and not self.closed:
                return {}
        try:
            event = self._write_record(phase, **fields)
        except Exception as error:
            self.fail_source(f"Could not publish {phase}", error)
            return {}
        if not self.closed and self.publication_seconds >= PUBLICATION_BUDGET_SECONDS:
            self.fail_source(
                "Publication budget exhausted", TimeoutError("Slow evidence storage")
            )
        return event

    def fail_source(self, message: str, error: Exception) -> None:
        with self.state_lock:
            self.source_failed = True
        with suppress(OSError, ValueError):
            print(f"LMI_TEST_PUBLICATION_ERROR {message}: {error}", flush=True)

    def _write_record(self, phase: str, **fields: Any) -> dict[str, Any]:
        self.sequence += 1
        if phase == "CONFORMANCE":
            self.capture_sequence += 1
            fields["observation"] = {
                **fields["observation"],
                "sourceSequence": self.sequence,
                "captureSequence": self.capture_sequence,
            }
        event = {
            **self.identity,
            "phase": phase,
            "sequence": self.sequence,
            "time": time.time(),
            "deadline": self.deadline,
            **fields,
        }
        content = json.dumps(event)
        with suppress(OSError, ValueError):
            print("LMI_TEST " + content, flush=True)
        key = (
            f"runs/{self.identity['run']}/events/{self.identity['marker']}/"
            f"{self.identity['request']}/{self.sequence:06d}.json"
        )
        self._commit_record(key, content.encode())
        return event

    def _commit_record(self, key: str, content: bytes) -> None:
        started = publication_clock()
        failed = False
        try:
            for attempt in range(2):
                try:
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=content,
                        ContentType="application/json",
                        IfNoneMatch="*",
                    )
                    return
                except (BotoCoreError, ClientError) as error:
                    failed = True
                    try:
                        response = self.s3.get_object(Bucket=self.bucket, Key=key)
                        with response["Body"] as body:
                            existing = body.read()
                    except (BotoCoreError, ClientError):
                        if attempt:
                            raise error from None
                    else:
                        if existing == content:
                            return
                        raise ValueError(
                            f"Evidence record already exists with different content: {key}"
                        ) from error
        finally:
            if failed:
                self.publication_seconds += publication_clock() - started

    def _seal_if_ready(self) -> None:
        with self.state_lock:
            if not self.wrapper_finished or self.active_workers:
                return
        with self.lock:
            with self.state_lock:
                if self.active_workers or self.source_failed or self.closed:
                    return
                self.closed = True
            # This shares the publication boundary with every other record.
            self._publish_record(
                "SEALED", sourceProtocol=2, throughCapture=self.capture_sequence
            )

    def finish_wrapper(self) -> None:
        with self.state_lock:
            self.wrapper_finished = True
        self._seal_if_ready()

    def _begin_work(self) -> bool:
        with self.state_lock:
            self.active_workers += 1
            return self.closed

    def _end_work(self) -> None:
        with self.state_lock:
            self.active_workers -= 1
        self._seal_if_ready()

    @contextmanager
    def publishing(self):
        # Registration and checking the seal are atomic with seal eligibility.
        already_sealed = self._begin_work()
        try:
            yield already_sealed
        finally:
            self._end_work()

    def start_worker(
        self, operation: Callable[[], None], name: str
    ) -> threading.Thread:
        def run() -> None:
            try:
                operation()
            except BaseException:
                with self.state_lock:
                    self.source_failed = True
                raise
            finally:
                self._end_work()

        thread = threading.Thread(target=run, daemon=True, name=name)
        self._begin_work()
        try:
            thread.start()
        except BaseException:
            with self.state_lock:
                self.active_workers -= 1
                self.source_failed = True
            raise
        return thread

    def observe(self, kind, **fields):
        if "atMs" not in fields:
            fields["atMs"] = time.time_ns() / 1_000_000
        return self.emit("CONFORMANCE", observation={"kind": kind, **fields})

    def next_call_id(self, kind):
        with self.lock:
            self.call_sequence += 1
            if kind in {"checkpoint", "poll"}:
                self.sdk_request_count += 1
            return f"{self.identity['request']}:{kind}:{self.call_sequence}"

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
        except (BotoCoreError, ClientError, ValueError) as error:
            self.emit("CONTROL_ERROR", gate=name, error=str(error))
            self.observe("control_error", gateId=name, error=str(error))
            raise

    def gate(
        self,
        name,
        *,
        effects=False,
        attempt=1,
        preparation=False,
    ):
        """Observe normal versus emergency release without changing step results."""
        end = time.monotonic() + 150
        entered = False
        held_at_ms = None
        task_id = self.next_call_id("io")
        native_overlap = self.payload.get("sharedScenario") == "native-overlap"
        self.observe("io_start", taskId=task_id, gateId=name)
        try:
            while True:
                # Shared cleanup releases the case abort control first. It must
                # not masquerade as a normal measurement release of the target.
                if self.released("release-all"):
                    mode = "emergency"
                    release_observed_at_ms = time.time_ns() / 1_000_000
                else:
                    specific_release = self.released(name)
                    target_observed_at_ms = time.time_ns() / 1_000_000
                    # Shared cleanup releases the abort first. If it raced
                    # the target read, observe it before crediting a normal release.
                    abort = specific_release and self.released("release-all")
                    mode = (
                        "emergency" if abort else "normal" if specific_release else None
                    )
                    release_observed_at_ms = (
                        time.time_ns() / 1_000_000 if abort else target_observed_at_ms
                    )
                if mode is not None:
                    if (preparation or native_overlap) and mode == "normal":
                        # The driver releases these controls only after observing
                        # live overlap. BLOCKED retains the earlier held-read time.
                        self.observe(
                            "barrier_arrived",
                            gateId=name,
                            atMs=release_observed_at_ms,
                            source="cohort-admission",
                        )
                    self.observe(
                        "fault_released",
                        gateId=name,
                        mode=mode,
                        atMs=release_observed_at_ms,
                        source="control-poll",
                    )
                    return
                if (
                    self.payload["scenario"] == "deadline"
                    and not preparation
                    and time.time() >= self.deadline + 0.2
                ):
                    self.observe("cooperative_io_cancelled", source="runtime-deadline")
                    raise TimeoutError(
                        "Cooperative fixture I/O crossed the runtime deadline"
                    )
                if not entered:
                    held_at_ms = target_observed_at_ms
                    self.emit("BLOCKED", gate=name)
                    self.observe("blocked", gateId=name, atMs=held_at_ms)
                    if not (native_overlap or self.payload.get("preparation_gate")):
                        self.observe("barrier_arrived", gateId=name, atMs=held_at_ms)
                    if self.payload["scenario"] == "deadline" and not preparation:
                        self.observe("work_in_flight", gateId=name, atMs=held_at_ms)
                    if self.payload.get("role") == "recovery":
                        self.observe("worker_recovered", atMs=held_at_ms)
                if time.monotonic() >= end:
                    self.emit("ESCAPE", gate=name)
                    self.observe("escape", gateId=name)
                    raise TimeoutError(
                        "Test emergency release; never a passing regression"
                    )
                if effects:
                    self.emit(
                        "EFFECT", operation="blocked-io", gate=name, attempt=attempt
                    )
                    self.observe(
                        "effect",
                        operation="blocked-io",
                        gateId=name,
                        attempt=attempt,
                    )
                else:
                    self.emit("ALIVE", gate=name)
                    self.observe("alive", gateId=name)
                    if self.payload.get("role") == "peer":
                        self.observe("peer_progress", gateId=name)
                entered = True
                time.sleep(0.2)  # fault-injected blocking I/O, not a durable delay
        except BaseException as error:
            self.emit("IO_INTERRUPTED", gate=name, error=type(error).__name__)
            self.observe(
                "io_interrupted",
                taskId=task_id,
                gateId=name,
                error=type(error).__name__,
            )
            raise
        finally:
            exited_at = time.time() * 1000
            self.emit("IO_EXIT", gate=name)
            self.observe("io_exit", taskId=task_id, gateId=name, atMs=exited_at)

    def body(self, name, value, step=None):
        attempt = getattr(step, "attempt", 1)
        self.emit("BODY", operation=name, value=value, attempt=attempt)
        self.observer.body(name, value, attempt)
        return value


@dataclass(frozen=True)
class FixtureObserverFactory:
    bind: Callable[[dict[str, Any]], Trace]

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        trace = self.bind(info.execution_input)
        return trace.observer


class ObservedClient:
    """Forward real SDK calls; never fabricate checkpoint acknowledgments.

    Evidence publication is isolated from the backend call: a failed record
    can neither discard a successful response nor replace the backend's own
    error. A failed record leaves a publication gap and prevents a new seal.
    """

    def __init__(self, client, trace=None):
        self.client, self.trace = client, trace
        self.initial_polls: list[tuple[float, float]] = []

    def _record(self, phase, kind, emitted=None, *, prepublished=False, **observed):
        if not prepublished:
            self.trace.emit(phase, **(emitted or {}), time=observed["atMs"] / 1000)
        self.trace.observe(kind, **observed)

    def bind_trace(self, trace) -> None:
        self.trace = trace
        for start, end in self.initial_polls:
            call_id = self.trace.next_call_id("poll")
            self._record("POLL_CALL", "poll_call", callId=call_id, atMs=start)
            self._record("POLL_EXIT", "poll_exit", callId=call_id, atMs=end)
        self.initial_polls.clear()

    def get_execution_state(self, **kwargs):
        if self.trace is None:
            # The SDK owns pagination. Until its start hook exposes the input,
            # retain only the bootstrap poll times, never transport responses.
            start = time.time_ns() / 1_000_000
            try:
                return self.client.get_execution_state(**kwargs)
            finally:
                self.initial_polls.append((start, time.time_ns() / 1_000_000))

        with self.trace.publishing() as sealed:
            call_id = self.trace.next_call_id("poll")
            if sealed:
                self.trace.emit("POLL_CALL")
            start = time.time_ns() / 1_000_000
            try:
                return self.client.get_execution_state(**kwargs)
            finally:
                end = time.time_ns() / 1_000_000
                self._record(
                    "POLL_CALL",
                    "poll_call",
                    prepublished=sealed,
                    callId=call_id,
                    atMs=start,
                )
                self._record("POLL_EXIT", "poll_exit", callId=call_id, atMs=end)

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
        with self.trace.publishing() as sealed:
            call_id = self.trace.next_call_id("checkpoint")
            if sealed:
                self.trace.emit("CHECKPOINT_CALL", updates=updates)
            start = time.time_ns() / 1_000_000
            try:
                try:
                    response = self.client.checkpoint(**kwargs)
                finally:
                    end = time.time_ns() / 1_000_000
                    self._record(
                        "CHECKPOINT_CALL",
                        "checkpoint_call",
                        {"updates": updates},
                        prepublished=sealed,
                        callId=call_id,
                        updates=updates,
                        atMs=start,
                    )
            except BaseException as error:
                error_type = type(error).__name__
                self._record(
                    "CHECKPOINT_ERROR",
                    "checkpoint_error",
                    {"error": error_type},
                    callId=call_id,
                    error=error_type,
                    atMs=end,
                )
                raise
            else:
                self._record(
                    "CHECKPOINT_ACK",
                    "checkpoint_ack",
                    {"updates": updates},
                    callId=call_id,
                    operationIds=[item["id"] for item in updates],
                    source="service",
                    atMs=end,
                )
                return response
            finally:
                self._record(
                    "CHECKPOINT_EXIT", "checkpoint_exit", callId=call_id, atMs=end
                )


def workflow(event, context, trace):
    trace.durable_context = context
    with trace.observer.root_scope():
        try:
            result = run_workflow(event, context, trace)
        except Exception:
            trace.observe("root_completed", outcome="FAILED")
            raise
        else:
            trace.observe("root_completed", outcome="SUCCEEDED")
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
                    trace.observe(
                        "fault_not_reapplied", originalRequestId=original_request
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
                if (
                    error.error_type != "ValueError"
                    or error.message != "expected:" + marker
                ):
                    raise AssertionError(
                        "Stored step failure has unexpected details"
                    ) from error
                trace.emit(
                    "STORED_FAILURE", message=error.message, error_type=error.error_type
                )
                trace.observe(
                    "stored_failure",
                    error={"type": error.error_type, "message": error.message},
                )
            else:
                raise AssertionError("Stored step failure returned as a value")

            def retry(step):
                trace.body("retry", marker, step)
                if step.attempt == 1:
                    raise ValueError("retry once")
                return marker

            retried = context.step(
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
            if retried != marker:
                raise AssertionError("Retried step returned the wrong value")
            mapped = context.run_in_child_context(
                lambda c: c.map(
                    [1, 2, 3],
                    lambda inner, item, _index, _all: inner.step(
                        lambda step: trace.body("item", item, step), name="item"
                    ),
                    name="items",
                    config=MapConfig(max_concurrency=3),
                ),
                name="nested",
            )
            items = mapped.get_results()
            if items != [1, 2, 3] or any(type(item) is not int for item in items):
                raise AssertionError("Replayed map returned the wrong items")
            parallel = context.parallel(
                [
                    lambda c: c.step(
                        lambda step: trace.body("left", ["left", value], step),
                        name="left",
                    ),
                    lambda c: c.step(
                        lambda step: trace.body("right", ["right", value], step),
                        name="right",
                    ),
                ],
                name="pair",
            )
            if parallel.get_results() != [["left", value], ["right", value]]:
                raise AssertionError("Replayed parallel returned the wrong values")
            context.wait(Duration.from_seconds(2), name="pause")
            callback = context.create_callback(name="completion")
            if callback.result() != marker:
                raise AssertionError("Replayed callback returned the wrong value")
        trace.emit("USER_RESULT", value=value)
        return value
    finally:
        exited_at = time.time() * 1000
        trace.emit("USER_EXIT")
        trace.observe("user_exit", atMs=exited_at)


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
    thread = trace.start_worker(lambda: admission_probes(trace), "lmi-admission-probes")
    # Every probe observation is a separate S3 publication, and the runtime
    # freezes this environment as soon as the handler returns. Wait for the
    # declared admission budget -- the same window the shared validator allows
    # these records to land in -- so a slow put cannot silently truncate the
    # ledger or leak this request's sequence into the next invocation.
    # A probe that is still pending after that window is a real SDK finding the
    # shared validator already reports as a missing post-closure probe, so do
    # not add a competing signal here.
    budget = trace.payload.get("behaviorBudgets", {}).get("admissionMs", 5000)
    thread.join(timeout=max(0.5, budget / 1000))


@dataclass
class WarmWork:
    ready: threading.Event = field(default_factory=threading.Event)
    observed_during: str | None = None
    worker: threading.Thread | None = None


WARM_WORK: dict[tuple[str, str], WarmWork] = {}
WARM_LOCK = threading.Lock()


def start_warm_work(trace):
    key = (trace.payload["runId"], trace.payload["caseId"])
    work = WarmWork()
    with WARM_LOCK:
        if key in WARM_WORK:
            raise ValueError("A warm case cannot replace its original work")
        WARM_WORK[key] = work

    def late():
        try:
            trace.observe("task_start", taskId="late-callback", source="fixture")
            end = time.monotonic() + 120
            while not work.ready.wait(timeout=0.2):
                if trace.released("release-all"):
                    return
                if time.monotonic() >= end:
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

    try:
        work.worker = trace.start_worker(late, "lmi-late-callback")
    except BaseException:
        with WARM_LOCK:
            WARM_WORK.pop(key, None)
        raise
    return None


def release_warm_work(trace):
    key = (trace.payload["runId"], trace.payload["caseId"])
    with WARM_LOCK:
        work = WARM_WORK.get(key)
    if work is None:
        raise ValueError("Warm request did not reuse the original process")
    work.observed_during = trace.identity["request"]
    work.ready.set()
    # The old invocation owns its callback and seal. Let that work complete
    # while this warm invocation is still active and the process can run it.
    assert work.worker is not None
    budget = trace.payload.get("behaviorBudgets", {}).get("admissionMs", 5000)
    work.worker.join(timeout=max(0.5, budget / 1000))
    return None


def handler(event, context):
    invocation = (
        event
        if isinstance(event, DurableExecutionInvocationInputWithClient)
        else DurableExecutionInvocationInput.from_json_dict(event)
    )
    started_at = time.time_ns() / 1_000_000
    source_client = (
        invocation.service_client
        if isinstance(invocation, DurableExecutionInvocationInputWithClient)
        else LambdaClient.initialize_client()
    )
    client = ObservedClient(source_client)
    before = resources()

    def bind_trace(event_input: dict[str, Any]) -> Trace:
        if client.trace is not None:
            return client.trace
        trace = Trace(event_input, context, invocation.durable_execution_arn)
        trace.emit("WRAPPER_ENTER", time=started_at / 1000, resources=before)
        trace.observe("wrapper_enter", atMs=started_at, resources=before)
        trace.observe(
            "runtime_deadline",
            atMs=trace.deadline_observed_at_ms,
            remainingTimeMs=trace.remaining_ms,
            deadlineAtMs=trace.deadline * 1000,
            source="runtime",
        )
        client.bind_trace(trace)
        return trace

    # The initial page often has the input. Bind it immediately so failed
    # initialization still has a request ledger. Otherwise the SDK will
    # provide the input after fetching pages and before running user code.
    for operation in invocation.initial_execution_state.operations:
        if operation.execution_details and operation.execution_details.input_payload:
            bind_trace(json.loads(operation.execution_details.input_payload))
            break

    injected = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        invocation, client
    )
    decorated = durable_execution(
        lambda e, c: workflow(e, c, bind_trace(e)),
        plugins=[FixtureObserverFactory(bind_trace)],
    )

    def complete(*, result=None, error=None):
        trace = client.trace
        if trace is None:
            return
        completed_at_ns = time.time_ns()
        if error is None:
            phase, kind, outcome = (
                "WRAPPER_RETURN",
                "wrapper_return",
                result["Status"],
            )
            raw = {"status": outcome}
            details = {}
        else:
            phase, kind, outcome = "WRAPPER_RAISE", "wrapper_raise", "THREW"
            raw = {"error": type(error).__name__}
            details = {"error": {"type": type(error).__name__, "message": str(error)}}
        completed_resources = resources()
        trace.emit(
            phase,
            resources=completed_resources,
            time=completed_at_ns / 1_000_000_000,
            **raw,
        )
        trace.observe(
            kind,
            outcome=outcome,
            resources=completed_resources,
            atMs=completed_at_ns / 1_000_000,
            **details,
        )
        if outcome == "PENDING":
            trace.observe(
                "pending",
                atMs=completed_at_ns / 1_000_000,
                source="sdk-wrapper",
            )
        if needs_admission_probes(trace):
            if trace.durable_context is None:
                trace.observe(
                    "observation_error",
                    message="SDK did not initialize a context for admission probes",
                )
            else:
                try:
                    start_admission_probes(trace)
                except Exception as probe_error:
                    trace.fail_source("Could not start admission probes", probe_error)
        trace.finish_wrapper()

    try:
        result = decorated(injected, context)
    except BaseException as error:
        complete(error=error)
        raise
    else:
        complete(result=result)
        return result


def needs_admission_probes(trace: Trace) -> bool:
    scenario = trace.payload.get("sharedScenario")
    return (
        scenario
        in {"normal-return", "normal-failure", "deadline-during-step", "peer-recovery"}
        and trace.payload.get("role") == "victim"
        and (
            scenario != "peer-recovery"
            or trace.identity["request"]
            == getattr(trace, "recovery_original_request", None)
        )
    )
