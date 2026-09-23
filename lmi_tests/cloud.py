# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Bounded real-service driver using the repository's cloud runner."""

import json
from concurrent.futures import ThreadPoolExecutor
import time
import uuid

from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_testing import DurableFunctionCloudTestRunner
from lmi_tests.deploy import ARTIFACTS, QUALIFIER, client, save, verify
from lmi_tests.evidence import CollectionError, check_controls, select


class RunnerClient:
    """Share paced history reads with the public runner; other APIs are unchanged."""

    def __init__(self, cloud):
        self.cloud = cloud

    def __getattr__(self, name):
        return getattr(self.cloud.lam, name)

    def get_durable_execution_history(self, **kwargs):
        return self.cloud.history_page(**kwargs)


class Cloud:
    def __init__(self, manifest):
        self.manifest = manifest
        self.lam, self.s3, self.logs = client("lambda"), client("s3"), client("logs")
        self.events = {}
        self.invocations = []
        self.gates = set()
        self._next_history_read = 0.0

    def verify(self):
        for key, arn in self.manifest["functions"].items():
            config = self.lam.get_function_configuration(FunctionName=arn)
            scaling = self.lam.get_function_scaling_config(
                FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER
            )
            verify(config, scaling, self.manifest, key)

    def start(self, scenario, fixture="normal", gate=None):
        marker = scenario + "-" + uuid.uuid4().hex[:12]
        payload = {"scenario": scenario, "marker": marker, "run": self.manifest["run"]}
        if gate:
            payload["gate"] = gate
            self.hold(gate)
        if scenario in {
            "parallel",
            "map",
            "nested",
            "return-inflight",
            "failure-inflight",
            "late-operation",
        }:
            self.hold(marker + "-loser")
            self.hold(marker + "-loser-started")
        elif scenario == "checkpoint":
            self.hold(marker + "-checkpoint")
        elif scenario == "suspend-cleanup":
            self.hold(marker + "-cleanup")
        runner = DurableFunctionCloudTestRunner(
            self.manifest["functions"][fixture], region=self.manifest["region"]
        )
        runner.lambda_client = RunnerClient(self)
        try:
            arn = runner.run_async(input=payload)
            if not arn:
                raise CollectionError("Invoke did not return a durable execution ARN")
        except Exception as error:
            save(
                ARTIFACTS / f"invocations/{marker}.json",
                {"payload": payload, "error": str(error)},
            )
            raise CollectionError(f"{marker}: invocation API failed") from error
        item = {
            "marker": marker,
            "arn": arn,
            "fixture": fixture,
            "runner": runner,
            "started": time.time(),
        }
        self.invocations.append(item)
        print(f"LMI {scenario}: marker={marker}, execution={arn}", flush=True)
        save(
            ARTIFACTS / f"invocations/{marker}.json",
            {
                "payload": payload,
                "arn": arn,
                "function": runner.function_name,
                "started": item["started"],
            },
        )
        return item

    def hold(self, gate):
        if gate in self.gates:
            return  # another invocation in this case uses the same barrier
        try:
            self.s3.put_object(
                Bucket=self.manifest["bucket"],
                Key="control/" + gate,
                Body=b"hold",
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "PreconditionFailed":
                raise CollectionError(
                    f"Cannot initialize control {gate}: {error}"
                ) from error
        self.gates.add(gate)

    def release(self, gate):
        self.s3.put_object(
            Bucket=self.manifest["bucket"], Key="control/" + gate, Body=b"release"
        )

    def refresh(self, markers=None, *, validate_controls=True):
        # A new pytest fixture must not scan every earlier case before it can
        # observe its first event. Explicit [] requests the complete run for collection.
        if markers is None:
            markers = [item["marker"] for item in self.invocations]
        prefixes = (
            [f"events/{marker}/" for marker in markers] if markers else ["events/"]
        )
        missing = []
        try:
            for prefix in prefixes:
                for page in self.s3.get_paginator("list_objects_v2").paginate(
                    Bucket=self.manifest["bucket"], Prefix=prefix
                ):
                    missing.extend(
                        obj["Key"]
                        for obj in page.get("Contents", [])
                        if obj["Key"] not in self.events
                    )

            def read(key):
                response = self.s3.get_object(Bucket=self.manifest["bucket"], Key=key)
                with response["Body"] as stream:
                    event = json.loads(stream.read())
                if (
                    event["run"] != self.manifest["run"]
                    or event["commit"] != self.manifest["commit"]
                ):
                    raise CollectionError("Event came from another build/run")
                return key, event

            if missing:
                with ThreadPoolExecutor(max_workers=8) as readers:
                    self.events.update(readers.map(read, missing))
        except ClientError as error:
            raise CollectionError(
                "Could not retrieve external lifecycle/side-effect ledger"
            ) from error
        events = sorted(
            self.events.values(), key=lambda e: (e["time"], e["request"], e["sequence"])
        )
        save(ARTIFACTS / "events.json", events)
        selected = [e for e in events if not markers or e["marker"] in markers]
        if markers and validate_controls:
            check_controls(selected)
        return selected

    def for_item(self, item):
        return self.refresh(markers=[item["marker"]])

    def poll(
        self,
        predicate,
        seconds=30,
        category=CollectionError,
        message="Evidence deadline exceeded",
    ):
        end = time.monotonic() + seconds
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() >= end:
                raise category(message)
            time.sleep(0.5)

    def phase(self, item, phase, **kwargs):
        return self.poll(lambda: select(self.for_item(item), phase), **kwargs)

    def finish(self, item, status="SUCCEEDED"):
        try:
            item["runner"].wait_for_result(
                item["arn"], timeout=self.manifest["driverTimeout"]
            )
        except TimeoutError as error:
            raise CollectionError(
                "Driver timeout, not Lambda invocation timeout evidence"
            ) from error
        result = self.lam.get_durable_execution(DurableExecutionArn=item["arn"])
        save(ARTIFACTS / f"executions/{item['marker']}.json", result)
        assert result["Status"] == status, (
            f"{item['marker']}: {result['Status']} (expected {status})"
        )
        if status == "SUCCEEDED":
            assert json.loads(result["Result"]) == item["marker"], (
                "Result crossed execution boundaries"
            )
        self.phase(item, "WRAPPER_RETURN")
        return self.history(item)

    def history_page(self, **request):
        """Bound read-only throttling retries without replaying invocation writes."""
        deadline = time.monotonic() + 20
        last_error = None
        for attempt in range(5):
            delay = max(0, self._next_history_read - time.monotonic())
            if time.monotonic() + delay >= deadline:
                break
            if delay:
                time.sleep(delay)
            try:
                return self.lam.get_durable_execution_history(**request)
            except ClientError as error:
                if error.response["Error"]["Code"] not in {
                    "TooManyRequestsException",
                    "ThrottlingException",
                }:
                    raise
                last_error = error
            finally:
                self._next_history_read = time.monotonic() + 1
            self._next_history_read = time.monotonic() + min(2**attempt, 4)
        raise CollectionError(
            "Execution-history API throttle budget exhausted"
        ) from last_error

    def history(self, item):
        events, marker = [], None
        while True:
            request = {"DurableExecutionArn": item["arn"], "IncludeExecutionData": True}
            if marker:
                request["Marker"] = marker
            result = self.history_page(**request)
            events.extend(result.get("Events", []))
            marker = result.get("NextMarker")
            if not marker:
                break
        save(ARTIFACTS / f"histories/{item['marker']}.json", events)
        return events

    def platform_timeout(self, item, request):
        # A TIMED_OUT logical execution or a client poll limit is insufficient.
        # Require a real invocation-completed service event for this request.
        def observed():
            for event in self.history(item):
                details = event.get("InvocationCompletedDetails", {})
                error = json.dumps(details.get("Error", {})).lower().replace(" ", "")
                if details.get("RequestId") == request and any(
                    label in error for label in ("timeout", "timedout")
                ):
                    return event
            return None

        return self.poll(
            observed,
            seconds=40,
            message="No service invocation timeout evidence for " + request,
        )

    def collect(self, *, full_run=True):
        errors = []
        if not full_run and not self.invocations:
            return
        try:
            events = self.refresh(
                markers=[] if full_run else [i["marker"] for i in self.invocations],
                validate_controls=False,
            )
            # Independent collect steps can recover execution ARNs without the
            # pytest process or successful invoke response.
            items = {
                e["execution"]: {"arn": e["execution"], "marker": e["marker"]}
                for e in events
            }
        except Exception as error:
            errors.append(str(error))
            items = {}
        if not full_run:
            items.update({i["arn"]: i for i in self.invocations})
        for item in items.values():
            try:
                self.history(item)
                save(
                    ARTIFACTS / f"executions/{item['marker']}.json",
                    self.lam.get_durable_execution(DurableExecutionArn=item["arn"]),
                )
            except Exception as error:
                errors.append(str(error))
        for key, arn in self.manifest["functions"].items() if full_run else []:
            try:
                config = self.lam.get_function_configuration(FunctionName=arn)
                logs = []
                for page in self.logs.get_paginator("filter_log_events").paginate(
                    logGroupName=config["LoggingConfig"]["LogGroup"],
                    startTime=int(self.manifest["created"] * 1000),
                ):
                    logs.extend(page.get("events", []))
                save(ARTIFACTS / f"logs/{key}.json", logs)
            except Exception as error:
                errors.append(str(error))
        if errors:
            save(ARTIFACTS / "collection-errors.json", errors)
            raise CollectionError("; ".join(errors))

    def release_all(self):
        for gate in self.gates:
            self.release(gate)
        self.gates.clear()
