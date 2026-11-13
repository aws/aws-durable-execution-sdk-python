# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Bounded real-service driver using the repository's cloud runner."""

import json
import hashlib
from pathlib import Path
from threading import Event, Lock
from typing import Any
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import time
import uuid

from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_testing import DurableFunctionCloudTestRunner
from lmi_tests.deploy import ARTIFACTS, CONCURRENCIES, QUALIFIER, client, save, verify
from lmi_tests.evidence import (
    CollectionError,
    ProvisioningError,
    check_controls,
    select,
)


class RunnerClient:
    """Share paced history reads with the public runner; other APIs are unchanged."""

    def __init__(self, cloud, *, before_call=None):
        self.cloud = cloud
        self.before_call = before_call

    def invoke(self, **kwargs):
        if self.before_call is not None:
            self.before_call()
        return self.cloud.lam.invoke(**kwargs)

    def __getattr__(self, name):
        operation = getattr(self.cloud.lam, name)
        if not callable(operation):
            return operation

        def guarded(**kwargs):
            if self.before_call is not None:
                self.before_call()
            return operation(**kwargs)

        return guarded

    def get_durable_execution_history(self, **kwargs):
        return self.cloud.history_page(**kwargs)


class Cloud:
    def __init__(
        self, manifest, configuration=None, *, artifacts_dir=None, case_context=None
    ):
        self.manifest = manifest
        self.configuration = configuration
        self.lam, self.s3, self.logs = client("lambda"), client("s3"), client("logs")
        self.events = {}
        self.invocations = []
        self.gates = set()
        self.released_gates: set[str] = set()
        self._next_history_read = 0.0
        self.artifacts_dir = (
            Path(artifacts_dir) if artifacts_dir is not None else ARTIFACTS
        )
        self.case_context = case_context
        self.cancel_event = Event()
        self._history_lock = Lock()
        self._retiring = False
        self.request_roles: dict[str, str] = {}
        self.request_launches: dict[str, str] = {}
        self.raw_histories: dict[str, list[dict[str, Any]]] = {}
        self.execution_results: dict[str, dict[str, Any]] = {}
        self.control_records: list[dict[str, Any]] = []
        self.source_uris: set[str] = set()
        self.observation_ended_at: float | None = None
        self.deadline_measurement: dict[str, Any] | None = None
        self.retired_at: float | None = None
        self.callback_records: list[dict[str, Any]] = []
        self.control_prefix = f"runs/{manifest['run']}/control/"
        if case_context is not None:
            owner = hashlib.sha256(
                f"{case_context.scenario.case_id}:{case_context.scenario.cohort_id}".encode()
            ).hexdigest()[:20]
            self.control_prefix = f"runs/{manifest['run']}/cases/{owner}/control/"

    def check_active(self) -> None:
        if self.cancel_event.is_set():
            raise CollectionError("Case driver was cancelled")

    def cancel(self) -> None:
        """Request cooperative stop before joining this case's blocking worker."""
        self.cancel_event.set()

    def begin_retirement(self) -> None:
        """Called only after the cancelled phase worker has actually exited."""
        self._retiring = True
        self.cancel_event.clear()

    def wait(self, seconds: float) -> None:
        self.check_active()
        if self.case_context is None:
            time.sleep(max(0, seconds))
        elif self.cancel_event.wait(max(0, seconds)):
            self.check_active()

    def _save(self, relative: str, data: Any) -> None:
        save(self.artifacts_dir / relative, data)
        self.source_uris.add((self.artifacts_dir / relative).resolve().as_uri())

    def _require_execution_target(self, arn: str) -> None:
        if self.case_context is not None and not arn.startswith(
            self.manifest["functions"][self.configuration] + "/durable-execution/"
        ):
            raise CollectionError("Execution is outside the case-owned function")

    def _bind_requests(self, events: list[dict[str, Any]]) -> None:
        if self.case_context is None:
            return
        by_marker = {item["marker"]: item for item in self.invocations}
        for event in sorted(
            events, key=lambda item: (item["time"], item["request"], item["sequence"])
        ):
            if (
                event["phase"] != "WRAPPER_ENTER"
                or event["request"] in self.request_roles
            ):
                continue
            item = by_marker.get(event["marker"])
            if item is None:
                raise CollectionError(
                    "Observed an invocation outside this case's launch journal"
                )
            if item.get("arn") is not None and item["arn"] != event["execution"]:
                raise CollectionError(
                    "An invocation changed its original durable execution"
                )
            self._require_execution_target(event["execution"])
            item["arn"] = event["execution"]
            ordinal = len(item["native_requests"])
            role = item["role"]
            if ordinal and item["resume_roles"]:
                role = item["resume_roles"][
                    min(ordinal - 1, len(item["resume_roles"]) - 1)
                ]
            launch_id = (
                item["launch_id"]
                if ordinal == 0
                else f"{item['launch_id']}:native:{event['request']}"
            )
            if ordinal == 0:
                self.case_context.update_attempt(
                    launch_id,
                    requestId=event["request"],
                    executionArn=event["execution"],
                )
            else:
                self.case_context.record_attempt(
                    launch_id,
                    role=role,
                    requestId=event["request"],
                    executionArn=event["execution"],
                    source="service-observed",
                )
            item["native_requests"].append(event["request"])
            self.request_roles[event["request"]] = role
            self.request_launches[event["request"]] = launch_id

    def verify(self):
        if self.manifest.get("concurrencies") != CONCURRENCIES or set(
            self.manifest["functions"]
        ) != set(CONCURRENCIES):
            raise ProvisioningError(
                "One deployment must contain both c1 and c2 functions"
            )
        for key, arn in self.manifest["functions"].items():
            config = self.lam.get_function_configuration(FunctionName=arn)
            scaling = self.lam.get_function_scaling_config(
                FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER
            )
            verify(config, scaling, self.manifest, key)

    @property
    def concurrency(self):
        return self.manifest["concurrencies"][self.configuration]

    def start(
        self,
        scenario,
        gate=None,
        target_environment=None,
        *,
        role=None,
        resume_roles=(),
        dispatcher_payload=None,
        marker=None,
    ):
        self.check_active()
        if self._retiring:
            raise CollectionError("A retiring case cannot launch new executions")
        if self.configuration not in self.manifest["functions"]:
            raise ProvisioningError(
                "Select a deployed concurrency configuration before Invoke"
            )
        marker = marker or scenario + "-" + uuid.uuid4().hex[:12]
        payload = dict(dispatcher_payload or {})
        if dispatcher_payload:
            payload["sharedScenario"] = dispatcher_payload["scenario"]
        payload.update(scenario=scenario, marker=marker, run=self.manifest["run"])
        if self.case_context is not None:
            payload["coverageProfile"] = "standard"
            payload["behaviorBudgets"] = dict(
                self.case_context.scenario.behavior_budgets
            )
            payload["controlPrefix"] = self.control_prefix
            if "release-all" not in self.gates:
                self.hold("release-all")
            if role not in self.case_context.scenario.roles:
                raise ValueError("Launch role is not part of the admitted scenario")
            if any(
                resume not in self.case_context.scenario.roles
                for resume in resume_roles
            ):
                raise ValueError("Resume role is not part of the admitted scenario")
        if target_environment is not None:
            payload["target_environment"] = target_environment
        if gate:
            payload["gate"] = gate
            self.hold(gate)
        runner = DurableFunctionCloudTestRunner(
            self.manifest["functions"][self.configuration],
            region=self.manifest["region"],
        )
        runner.lambda_client = RunnerClient(self, before_call=self.check_active)
        item = {
            "marker": marker,
            "arn": None,
            "configuration": self.configuration,
            "runner": runner,
            "started": time.time(),
            "payload": payload,
            "role": role,
            "resume_roles": tuple(resume_roles),
            "native_requests": [],
            "launch_id": marker,
        }
        if self.case_context is not None:
            # Retain attempted writes before the transport, even if its response
            # is lost. Later ledger observations can recover their native IDs.
            self.invocations.append(item)
            self.case_context.record_attempt(
                marker, role=role, marker=marker, source="driver-invoke"
            )
            self._save(
                f"invocations/{marker}.json",
                {"payload": payload, "launched": item["started"]},
            )
        self.check_active()
        try:
            arn = runner.run_async(input=payload)
            if not arn:
                raise CollectionError("Invoke did not return a durable execution ARN")
            self._require_execution_target(arn)
        except Exception as error:
            self._save(
                f"invocations/{marker}.json", {"payload": payload, "error": str(error)}
            )
            raise CollectionError(f"{marker}: invocation API failed") from error
        item["arn"] = arn
        if self.case_context is None:
            self.invocations.append(item)
        print(f"LMI {scenario}: marker={marker}, execution={arn}", flush=True)
        self._save(
            f"invocations/{marker}.json",
            {
                "payload": payload,
                "arn": arn,
                "function": runner.function_name,
                "started": item["started"],
            },
        )
        return item

    def hold(self, gate):
        self.check_active()
        if gate in self.gates:
            return  # another invocation in this case uses the same barrier
        try:
            self.s3.put_object(
                Bucket=self.manifest["bucket"],
                Key=f"{self.control_prefix}{gate}",
                Body=b"hold",
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "PreconditionFailed":
                raise CollectionError(
                    f"Cannot initialize control {gate}: {error}"
                ) from error
        self.gates.add(gate)
        self.control_records.append(
            {"gate": gate, "action": "hold", "time": time.time()}
        )

    def release(self, gate):
        self.check_active()
        if gate in self.released_gates:
            return
        if self.case_context is not None and gate not in self.gates:
            raise CollectionError(
                "Refusing to release a control not owned by this case"
            )
        intent = {
            "gate": gate,
            "action": "release",
            "started": time.time(),
            "state": "PENDING",
        }
        self.control_records.append(intent)
        self._save("control-records.json", self.control_records)
        self.s3.put_object(
            Bucket=self.manifest["bucket"],
            Key=f"{self.control_prefix}{gate}",
            Body=b"release",
        )
        intent.update(time=time.time(), state="ACKNOWLEDGED")
        self.released_gates.add(gate)
        self._save("control-records.json", self.control_records)

    def refresh(self, markers=None, *, validate_controls=True):
        self.check_active()
        if self.case_context is not None and markers == []:
            raise CollectionError(
                "A shared case cannot collect another case's run prefix"
            )
        # A new pytest fixture must not scan every earlier case before it can
        # observe its first event. Explicit [] requests the complete run for collection.
        if markers is None:
            markers = [item["marker"] for item in self.invocations]
        if self.case_context is not None:
            if not markers:
                return []
            if not set(markers) <= {item["marker"] for item in self.invocations}:
                raise CollectionError("A shared case cannot read another case's marker")
        prefix = f"runs/{self.manifest['run']}/events/"
        prefixes = [f"{prefix}{marker}/" for marker in markers] if markers else [prefix]
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
                self.check_active()
                response = self.s3.get_object(Bucket=self.manifest["bucket"], Key=key)
                with response["Body"] as stream:
                    event = json.loads(stream.read())
                if (
                    event["run"] != self.manifest["run"]
                    or event["commit"] != self.manifest["commit"]
                ):
                    raise CollectionError("Event came from another build/run")
                self.check_active()
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
        self.check_active()
        self._bind_requests(events)
        self.source_uris.update(
            f"s3://{self.manifest['bucket']}/{key}" for key in self.events
        )
        self._save("events.json", events)
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
            self.check_active()
            value = predicate()
            if value:
                return value
            if time.monotonic() >= end:
                raise category(message)
            self.wait(min(0.5, max(0, end - time.monotonic())))

    def phase(self, item, phase, **kwargs):
        return self.poll(lambda: select(self.for_item(item), phase), **kwargs)

    def record_deadline_measurement(self, snapshot: dict[str, Any]) -> None:
        """Persist the completed measurement before any victim-fault release."""
        self.check_active()
        recorded = deepcopy(snapshot)
        self._save("measurements/deadline.json", recorded)
        self.deadline_measurement = recorded

    def wait_terminal(self, item: dict[str, Any]) -> dict[str, Any]:
        """Observe actual terminal state and wrapper exits without asserting success.

        Deadline fixtures can legitimately finish with several service outcomes.
        Their declared conformance expectations decide acceptance; this method
        only observes completion and does not stop or retry an execution.
        """
        self._require_execution_target(item["arn"])

        def terminal() -> dict[str, Any] | None:
            result = self.lam.get_durable_execution(DurableExecutionArn=item["arn"])
            if result["Status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}:
                return result
            return None

        result = self.poll(
            terminal,
            seconds=self.manifest["driverTimeout"],
            message="Released victim execution did not reach an actual terminal state",
        )
        self.execution_results[item["arn"]] = result
        self._save(f"executions/{item['marker']}.json", result)

        def wrappers_closed() -> bool:
            events = self.for_item(item)
            entered = {event["request"] for event in select(events, "WRAPPER_ENTER")}
            exited = {
                event["request"]
                for event in events
                if event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
            }
            return bool(entered) and entered <= exited

        self.poll(
            wrappers_closed,
            seconds=self.manifest["driverTimeout"],
            message="Released victim has native invocations without actual wrapper exits",
        )
        self.history(item)
        return result

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
        self.execution_results[item["arn"]] = result
        self._save(f"executions/{item['marker']}.json", result)
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
        """Serialize paced reads within one case, without shared-case throttling state."""
        with self._history_lock:
            return self._history_page(**request)

    def _history_page(self, **request):
        """Bound read-only throttling retries without replaying invocation writes."""
        deadline = time.monotonic() + 20
        last_error = None
        for attempt in range(5):
            self.check_active()
            delay = max(0, self._next_history_read - time.monotonic())
            if time.monotonic() + delay >= deadline:
                break
            if delay:
                self.wait(delay)
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
        self.raw_histories[item["arn"]] = events
        self._save(f"histories/{item['marker']}.json", events)
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
            seconds=self.manifest["invocationTimeout"] + 40,
            message="No service invocation timeout evidence for " + request,
        )

    def collect(self, *, full_run=True):
        self.check_active()
        if self.case_context is not None and full_run:
            raise CollectionError("A shared case may collect only its own invocations")
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
            items.update({i["arn"]: i for i in self.invocations if i.get("arn")})
        for item in items.values():
            try:
                self.history(item)
                result = self.lam.get_durable_execution(DurableExecutionArn=item["arn"])
                self.execution_results[item["arn"]] = result
                self._save(f"executions/{item['marker']}.json", result)
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
                self._save(f"logs/{key}.json", logs)
            except Exception as error:
                errors.append(str(error))
        if errors:
            self._save("collection-errors.json", errors)
            raise CollectionError("; ".join(errors))

    def release_all(self):
        for gate in sorted(
            self.gates - self.released_gates, key=lambda value: value != "release-all"
        ):
            self.release(gate)
        self.gates.clear()

    def run_invocations(self):
        """Recover only this run's invocations, including failed Invoke responses."""
        events = self.refresh(markers=[], validate_controls=False)
        items = {
            e["execution"]: {"arn": e["execution"], "marker": e["marker"]}
            for e in events
        }
        for path in (self.artifacts_dir / "invocations").glob("*.json"):
            record = json.loads(path.read_text())
            if (
                record.get("arn")
                and record.get("payload", {}).get("run") == self.manifest["run"]
            ):
                items[record["arn"]] = {
                    "arn": record["arn"],
                    "marker": record["payload"]["marker"],
                }
        targets = tuple(
            arn + "/durable-execution/" for arn in self.manifest["functions"].values()
        )
        if any(not item["arn"].startswith(targets) for item in items.values()):
            raise CollectionError(
                "Refusing to stop an execution outside this deployment"
            )
        return list(items.values())

    def settle_case(self):
        """Release fault work and drain this case before reusing its function."""
        self.release_all()
        pending = []
        for item in self.invocations:
            result = self.lam.get_durable_execution(DurableExecutionArn=item["arn"])
            if result["Status"] == "RUNNING":
                try:
                    self.lam.stop_durable_execution(DurableExecutionArn=item["arn"])
                    pending.append(item)
                except ClientError as error:
                    if (
                        error.response["Error"]["Code"]
                        != "InvalidParameterValueException"
                    ):
                        raise
                    # Completion can race the stop request. A still-running
                    # execution must not be ignored or leak into the next case.
                    if (
                        self.lam.get_durable_execution(DurableExecutionArn=item["arn"])[
                            "Status"
                        ]
                        == "RUNNING"
                    ):
                        raise

        def settled():
            pending[:] = [
                item
                for item in pending
                if self.lam.get_durable_execution(DurableExecutionArn=item["arn"])[
                    "Status"
                ]
                == "RUNNING"
            ]
            if pending:
                return False
            events = self.refresh(validate_controls=False)
            entered = {e["request"] for e in select(events, "WRAPPER_ENTER")}
            exited = {
                e["request"]
                for e in events
                if e["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
            }
            return entered <= exited

        if self.invocations:
            self.poll(
                settled,
                seconds=30,
                message="Released case still owns runtime workers; shared function is not ready for reuse",
            )
