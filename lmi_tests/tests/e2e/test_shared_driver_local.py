# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Exercise shared phase routing against the real SDK's local durable runner.

This checks public handler/callback/replay behavior, not real-LMI placement or
cloud conformance acceptance.
"""

import json
from threading import Lock
import time

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests.fixture import workflow
from lmi_tests.shared_scenarios import ScenarioDriver


class LocalTrace:
    def __init__(self):
        self.events = []
        self.shared = False
        self.payload = {}
        self.lock = Lock()

    def emit(self, phase, **fields):
        with self.lock:
            self.events.append({"phase": phase, "time": time.time(), **fields})

    def body(self, name, value, step):
        self.emit("BODY", operation=name, value=value, attempt=step.attempt)
        return value

    def snapshot(self):
        with self.lock:
            return list(self.events)


class LocalService:
    """Replace only AWS transport, keeping the actual fixture and SDK runner."""

    def __init__(self):
        self.manifest = {"driverTimeout": 15}
        self.trace = LocalTrace()
        self.invocations = []
        self.callback_records = []
        self.result = None
        decorated = durable_execution(
            lambda event, context: workflow(event, context, self.trace)
        )

        def handler(event, context):
            self.trace.emit("WRAPPER_ENTER", request=context.aws_request_id)
            result = decorated(event, context)
            self.trace.emit(
                "WRAPPER_RETURN",
                request=context.aws_request_id,
                status=result["Status"],
            )
            return result

        self.runner = DurableFunctionTestRunner(handler, poll_interval=0.01)

    def start(self, fixture, **options):
        payload = {"marker": options["marker"], "scenario": fixture}
        arn = self.runner.run_async(input=payload)
        item = {"arn": arn, "marker": payload["marker"], "runner": self.runner}
        self.invocations.append(item)
        return item

    def poll(self, predicate, seconds=15, **options):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            observed = predicate()
            if observed:
                return observed
            time.sleep(0.01)
        raise AssertionError(options.get("message", "local observation timeout"))

    def phase(self, item, phase, **options):
        return self.poll(
            lambda: [
                event for event in self.trace.snapshot() if event["phase"] == phase
            ],
            **options,
        )

    def for_item(self, item):
        return self.trace.snapshot()

    def check_active(self):
        pass

    def finish(self, item, status="SUCCEEDED"):
        self.result = self.runner.wait_for_result(item["arn"], timeout=15)
        assert self.result.status.value == status


def test_shared_callback_driver_resumes_the_original_public_sdk_execution():
    service = LocalService()
    profile = Profile("native-1", "local-function", 1, 1, "local-sdk", "local-config")
    context = ScenarioContext(
        "local-run",
        Scenario(
            "callback-case",
            "lmi-lifecycle-5",
            profile.profile_id,
            "suspension-retry",
            roles=("victim", "resume"),
        ),
        profile,
    )
    driver = ScenarioDriver(context, service)
    with service.runner:
        driver.start()
        driver.place()
        driver.execute()
    assert len(service.invocations) == 1
    assert len(service.callback_records) == 1
    assert service.callback_records[0]["executionArn"] == service.invocations[0]["arn"]
    assert json.loads(service.result.result) == service.invocations[0]["marker"]
    events = service.trace.snapshot()
    assert any(
        event["phase"] == "WRAPPER_RETURN" and event["status"] == "PENDING"
        for event in events
    )
    # The local runner deliberately reuses its fake AWS request ID. Actual
    # wrapper calls, PENDING results and skipped bodies establish local replay;
    # this is not a claim about native LMI request identity or placement.
    assert sum(event["phase"] == "WRAPPER_ENTER" for event in events) > 1
    bodies = [event for event in events if event["phase"] == "BODY"]
    assert sum(event["operation"] == "success" for event in bodies) == 1
    assert sum(event["operation"] == "failure" for event in bodies) == 1
    assert [event["attempt"] for event in bodies if event["operation"] == "retry"] == [
        1,
        2,
    ]
