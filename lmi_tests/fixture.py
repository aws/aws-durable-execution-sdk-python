# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Fault-injection handlers for real LMI. Never install these as SDK examples."""

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


class Trace:
    def __init__(self, payload, context, execution_arn):
        self.payload = payload
        self.context = context
        self.s3 = boto3.client("s3", config=IO_CONFIG)
        self.bucket = os.environ["LMI_BUCKET"]
        self.lock = threading.Lock()
        self.sequence = 0
        self.identity = {
            "run": os.environ["LMI_RUN_ID"],
            "commit": os.environ["LMI_COMMIT"],
            "marker": payload["marker"],
            "scenario": payload["scenario"],
            "execution": execution_arn,
            "request": context.aws_request_id,
            "environment": environment_id(),
            "process": PROCESS_ID,
            "pid": os.getpid(),
        }
        self.deadline = time.time() + context.get_remaining_time_in_millis() / 1000

    def emit(self, phase, **fields):
        # Diagnostic I/O is outside orchestration decisions. The ledger survives a
        # stuck wrapper and does not rely on the invocation response or log delivery.
        with self.lock:
            self.sequence += 1
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
                Key=f"events/{self.identity['marker']}/{self.identity['request']}/{self.sequence:06d}.json",
                Body=json.dumps(event).encode(),
                ContentType="application/json",
            )
        return event

    def signal(self, name):
        self.s3.put_object(Bucket=self.bucket, Key="control/" + name, Body=b"release")

    def released(self, name):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key="control/" + name)
            with response["Body"] as body:
                state = body.read()
            if state not in {b"hold", b"release"}:
                raise ValueError(f"Invalid control state for {name}")
            return state == b"release"
        except (ClientError, ValueError) as error:
            # Every key must exist before Invoke. Missing/forbidden objects are
            # fixture errors, never an implicit hold or release signal.
            self.emit("CONTROL_ERROR", gate=name, error=str(error))
            raise

    def gate(self, name, *, effects=False, attempt=1, ready=None):
        end = time.monotonic() + 75
        entered = False
        try:
            while not self.released(name) and not self.released("release-all"):
                if not entered:
                    self.emit("BLOCKED", gate=name)
                if time.monotonic() >= end:
                    self.emit("ESCAPE", gate=name)
                    raise TimeoutError(
                        "Test emergency release; never a passing regression"
                    )
                if effects:
                    # Each record is an independently identifiable external side effect.
                    # Do not stop it using the test's own Lambda deadline: that would
                    # mask the missing SDK cancellation/worker-retirement behavior.
                    self.emit(
                        "EFFECT", operation="blocked-io", gate=name, attempt=attempt
                    )
                else:
                    self.emit("ALIVE", gate=name)
                if not entered and ready:
                    self.signal(ready)
                entered = True
                time.sleep(0.2)  # fault-injected blocking I/O, not a durable delay
        except BaseException as error:
            self.emit("IO_INTERRUPTED", gate=name, error=type(error).__name__)
            raise
        finally:
            self.emit("IO_EXIT", gate=name)

    def body(self, name, value, step=None):
        self.emit(
            "BODY", operation=name, value=value, attempt=getattr(step, "attempt", 1)
        )
        return value


class ObservedClient:
    """Forward real SDK calls; never fabricate checkpoint acknowledgments."""

    def __init__(self, client, trace):
        self.client, self.trace = client, trace

    def get_execution_state(self, **kwargs):
        self.trace.emit("POLL_CALL")
        try:
            return self.client.get_execution_state(**kwargs)
        finally:
            self.trace.emit("POLL_EXIT")

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
        self.trace.emit("CHECKPOINT_CALL", updates=updates)
        try:
            response = self.client.checkpoint(**kwargs)
            self.trace.emit("CHECKPOINT_ACK", updates=updates)
            if self.trace.payload["scenario"] == "checkpoint" and any(
                u["name"] == "checkpoint-loser" and u["action"] == "SUCCEED"
                for u in updates
            ):
                # Real service response, withheld while a synchronous waiter owns a
                # branch. This tests settlement/join ordering without a fake service.
                self.trace.emit("ACK_HELD")
                self.trace.gate(self.trace.payload["marker"] + "-checkpoint")
            return response
        except BaseException as error:
            self.trace.emit("CHECKPOINT_ERROR", error=type(error).__name__)
            raise
        finally:
            self.trace.emit("CHECKPOINT_EXIT")


def nested_progress(context, trace, marker):
    """Nested public operations must progress even when each branch pool has one lane."""
    trace.emit("PROGRESS_BEGIN")

    def children(child):
        def mapped(branch):
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
                config=MapConfig(max_concurrency=1),
            ).get_results()

        return child.parallel(
            [
                mapped,
                lambda branch: branch.run_in_child_context(
                    lambda leaf: leaf.step(
                        lambda step: trace.body("sibling", marker, step), name="sibling"
                    ),
                    name="sibling-child",
                ),
            ],
            name="outer-parallel",
            config=ParallelConfig(max_concurrency=1),
        ).get_results()

    result = context.run_in_child_context(children, name="progress-child")
    assert result == [[marker + ":0", marker + ":1"], marker]
    trace.emit("PROGRESS", value=result)


def workflow(event, context, trace):
    marker, scenario = event["marker"], event["scenario"]
    try:
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
                        try:
                            child.step(
                                lambda step: trace.body("late-work", marker, step),
                                name="late-work",
                                config=NO_RETRY,
                            )
                        except OrphanedChildException as error:
                            trace.emit("LATE_REJECTED", error=type(error).__name__)
                        else:
                            trace.emit("LATE_ACCEPTED")

            def winning(child):
                def io(_):
                    trace.gate(marker + "-loser-started")
                    trace.emit("WINNER_READY")
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
        if scenario == "suspend-cleanup":
            # Test-only root-finally diagnostics. This gate changes only exit
            # timing; it introduces no durable operations during suspension.
            trace.emit("CLEANUP_ENTER")
            try:
                trace.gate(marker + "-cleanup")
            finally:
                trace.emit("CLEANUP_EXIT")
        trace.emit("USER_EXIT")


def handler(event, context):
    invocation = DurableExecutionInvocationInput.from_json_dict(event)
    payload = next(
        json.loads(op.execution_details.input_payload)
        for op in invocation.initial_execution_state.operations
        if op.execution_details and op.execution_details.input_payload
    )
    if payload["run"] != os.environ["LMI_RUN_ID"]:
        raise ValueError("Input belongs to a different deployment")
    trace = Trace(payload, context, invocation.durable_execution_arn)
    client = ObservedClient(LambdaClient.initialize_client(), trace)
    injected = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
        invocation, client
    )
    decorated = durable_execution(lambda e, c: workflow(e, c, trace))
    before = resources()
    trace.emit("WRAPPER_ENTER", resources=before)
    try:
        result = decorated(injected, context)
        trace.emit("WRAPPER_RETURN", status=result["Status"], resources=resources())
        return result
    except BaseException as error:
        trace.emit("WRAPPER_RAISE", error=type(error).__name__, resources=resources())
        raise
