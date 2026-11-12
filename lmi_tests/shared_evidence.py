# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Normalize observed SDK/service evidence for the pinned shared validator.

This module runs only in the driver. It never fills in a missing SDK boundary or
uses a fixture's expected answer as an observed result. Raw source objects remain
in the case artifact directory and S3 ledger.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any

from aws_durable_execution_conformance_tests.lmi.coordinator import ScenarioContext
from lmi_tests.cloud import Cloud


TERMINAL = {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}
_ACTIVE_FAULTS = {"deadline-recovery", "timeout-retry"}
_OPERATION_TYPES = {
    "Step": "STEP",
    "Wait": "WAIT",
    "Callback": "CALLBACK",
    "Context": "CONTEXT",
    "ChainedInvoke": "CHAINED_INVOKE",
}


def _milliseconds(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp() * 1000
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
    if type(value) in (int, float):
        return float(value) * 1000
    raise ValueError("Service evidence is missing its actual timestamp")


def _decoded(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _error(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    if "Payload" in value:
        value = value["Payload"]
    if not isinstance(value, Mapping):
        return None
    return {
        "type": value.get("ErrorType", value.get("type")),
        "message": value.get("ErrorMessage", value.get("message")),
    }


def _resources(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if "threads" in value:
        result["threads"] = [
            {**thread, "id": str(thread["id"])} for thread in value["threads"]
        ]
    if "fds" in value:
        result["fds"] = value["fds"]
    if "rssBytes" in value or "rss" in value:
        result["rssBytes"] = value.get("rssBytes", value.get("rss"))
    return result


@dataclass(frozen=True)
class CaseSources:
    context: ScenarioContext
    cloud: Cloud
    raw: tuple[Mapping[str, Any], ...]

    @classmethod
    def capture(cls, context: ScenarioContext, cloud: Cloud) -> CaseSources:
        return cls(context, cloud, tuple(cloud.events.values()))

    def observations(self) -> tuple[list[dict[str, Any]], list[str]]:
        events: list[dict[str, Any]] = []
        errors: list[str] = []
        by_request: dict[str, list[int]] = defaultdict(list)
        for raw in self.raw:
            request = raw.get("request")
            if (
                raw.get("run") != self.context.run_id
                or raw.get("commit") != self.cloud.manifest["commit"]
                or raw.get("caseId") != self.context.scenario.case_id
                or raw.get("cohortId") != self.context.scenario.cohort_id
            ):
                errors.append(
                    "Raw evidence does not belong to the admitted run/artifact/case"
                )
                continue
            if not isinstance(request, str) or type(raw.get("sequence")) is not int:
                errors.append(
                    "Raw evidence has no native request identity or source sequence"
                )
                continue
            by_request[request].append(raw["sequence"])
            observation = raw.get("observation")
            if raw.get("phase") != "CONFORMANCE" or not isinstance(
                observation, Mapping
            ):
                continue
            event = {
                "requestId": request,
                "ownerRequestId": request,
                "executionArn": raw["execution"],
                **observation,
                "sourceSequence": raw["sequence"],
                "observedAtMs": raw["time"] * 1000,
            }
            if "resources" in event:
                event["resources"] = _resources(event["resources"])
            if event.get("kind") == "wrapper_raise" and event.get("outcome") == "THREW":
                event["sourceOutcome"] = "THREW"
                event["outcome"] = (
                    "FAILED"  # Actual exception, not an inferred platform timeout.
                )
            if event.get("kind") in {
                "observation_error",
                "control_error",
                "export_publication_error",
            }:
                errors.append(
                    str(event.get("message", event.get("error", event["kind"])))
                )
            events.append(event)
        for request, sequences in by_request.items():
            if len(set(sequences)) != len(sequences) or sorted(sequences) != list(
                range(1, max(sequences) + 1)
            ):
                errors.append(f"Raw source publication is incomplete for {request}")
        return events, errors

    def operation_snapshots(
        self, events: list[dict[str, Any]]
    ) -> dict[str, dict[str, Mapping[str, Any]]]:
        snapshots: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
        for event in events:
            records = list(event.get("operations", []))
            if event.get("operationId") and event.get("operationType"):
                records.append(event)
            for record in records:
                snapshots[event["executionArn"]][record["operationId"]] = record
        return snapshots

    def histories(
        self, events: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
        snapshots = self.operation_snapshots(events)
        histories: list[dict[str, Any]] = []
        expected: dict[str, Any] = {}
        errors: list[str] = []
        for item in self.cloud.invocations:
            arn = item.get("arn")
            if (
                not arn
                or arn not in self.cloud.raw_histories
                or arn not in self.cloud.execution_results
            ):
                errors.append(
                    "An admitted execution is missing actual service history or state"
                )
                continue
            operations: dict[str, dict[str, Any]] = {}
            for raw in self.cloud.raw_histories[arn]:
                event_type = raw.get("EventType", "")
                kind = next(
                    (
                        value
                        for prefix, value in _OPERATION_TYPES.items()
                        if event_type.startswith(prefix)
                    ),
                    None,
                )
                identity = raw.get("Id")
                if kind is None or not isinstance(identity, str):
                    continue
                info = snapshots.get(arn, {}).get(identity, {})
                operation = operations.setdefault(
                    identity,
                    {
                        "id": identity,
                        "type": kind,
                        "name": raw.get("Name", info.get("name")),
                        "parentId": raw.get("ParentId", info.get("parentId")),
                        "status": "STARTED",
                    },
                )
                if "Name" in raw:
                    operation["name"] = raw["Name"]
                if "ParentId" in raw:
                    operation["parentId"] = raw["ParentId"]
                details = raw.get(event_type + "Details", {})
                if event_type.endswith("Succeeded"):
                    operation["status"] = "SUCCEEDED"
                    result = details.get("Result", info.get("serializedResult"))
                    if result is not None:
                        operation["result"] = _decoded(result)
                elif event_type.endswith("Failed"):
                    operation["status"] = "FAILED"
                    operation["error"] = _error(details.get("Error")) or info.get(
                        "error"
                    )
                elif event_type.endswith("TimedOut"):
                    operation["status"] = "TIMED_OUT"
            result = self.cloud.execution_results[arn]
            started = result.get("StartTimestamp")
            if started is None:
                started = next(
                    (
                        event.get("EventTimestamp")
                        for event in self.cloud.raw_histories[arn]
                        if event.get("EventType") == "ExecutionStarted"
                    ),
                    None,
                )
            history: dict[str, Any] = {
                "executionArn": arn,
                "ownerCaseId": self.context.scenario.case_id,
                "status": result.get("Status"),
                "operations": list(operations.values()),
            }
            if started is not None:
                history["startedAtMs"] = _milliseconds(started)
            if "Result" in result:
                history["result"] = _decoded(result["Result"])
            if "Error" in result:
                history["error"] = _error(result["Error"])
            histories.append(history)
            fixture = item.get("payload", {}).get("scenario")
            if fixture in {"failure", "failure-inflight"}:
                expected[arn] = {
                    "status": "FAILED",
                    "error": {
                        "type": "ValueError",
                        "message": "expected:" + item["marker"],
                    },
                }
            elif (
                fixture == "deadline"
                and self.context.scenario.scenario in _ACTIVE_FAULTS
            ):
                # This fixture asserts native deadline/retry behavior. Its logical
                # execution may still be active or may finish after normal release.
                expected[arn] = {
                    "allowedStatuses": [
                        "RUNNING",
                        "SUCCEEDED",
                        "FAILED",
                        "TIMED_OUT",
                        "STOPPED",
                    ]
                }
            else:
                expected[arn] = {"status": "SUCCEEDED", "result": item["marker"]}
        return histories, expected, errors

    def attempts(
        self, events: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[str, str], list[str]]:
        attempts: list[dict[str, Any]] = []
        selected: dict[str, str] = {}
        errors: list[str] = []
        journal = {item.get("requestId"): item for item in self.context.attempts}
        raw_by_request: dict[str, Mapping[str, Any]] = {}
        for raw in self.raw:
            if raw.get("phase") == "WRAPPER_ENTER":
                raw_by_request[raw["request"]] = raw
        for request, raw in raw_by_request.items():
            item = next(
                (
                    item
                    for item in self.cloud.invocations
                    if item["marker"] == raw["marker"]
                ),
                None,
            )
            if (
                item is None
                or request not in journal
                or request not in self.cloud.request_roles
            ):
                errors.append(
                    f"Native request {request} was not bound to an original launch"
                )
                continue
            owned = [event for event in events if event.get("requestId") == request]
            boundaries = [
                event
                for event in owned
                if event.get("kind") in {"wrapper_return", "wrapper_raise"}
            ]
            deadline = [
                event for event in owned if event.get("kind") == "runtime_deadline"
            ]
            timeout = None
            for service_event in self.cloud.raw_histories.get(raw["execution"], []):
                details = service_event.get("InvocationCompletedDetails", {})
                message = json.dumps(details.get("Error", {})).lower().replace(" ", "")
                if details.get("RequestId") == request and any(
                    word in message for word in ("timeout", "timedout")
                ):
                    timeout = {
                        "kind": "platform_timeout",
                        "requestId": request,
                        "ownerRequestId": request,
                        "executionArn": raw["execution"],
                        "atMs": _milliseconds(service_event.get("EventTimestamp")),
                        "source": "platform",
                        "sourceEventId": service_event.get("EventId"),
                    }
            if timeout is not None:
                events.append(timeout)
            role = self.cloud.request_roles[request]
            attempt: dict[str, Any] = {
                "requestId": request,
                "executionArn": raw["execution"],
                "environmentId": raw["environment"],
                "processId": raw["process"],
                "workerId": raw["process"],
                "pid": raw.get("pid"),
                "role": role,
                "attempt": journal[request]["attempt"],
                "marker": raw["marker"],
                "fixture": item.get("payload", {}).get("scenario"),
                "startedAtMs": min(
                    [raw["time"] * 1000, *(event["atMs"] for event in owned)]
                ),
            }
            if boundaries:
                attempt["outcome"] = boundaries[-1]["outcome"]
                attempt["endedAtMs"] = boundaries[-1]["atMs"]
            if timeout is not None:
                attempt.update(outcome="TIMED_OUT", endedAtMs=timeout["atMs"])
            if deadline:
                attempt["deadlineAtMs"] = deadline[0]["deadlineAtMs"]
            if "outcome" not in attempt:
                errors.append(
                    f"Native request {request} has no observed completion boundary"
                )
            if journal[request]["attempt"] == 1:
                selected[role] = request
            attempts.append(attempt)
        if (
            self.context.scenario.scenario == "timeout-retry"
            and "victim" in selected
            and "retry" in selected
        ):
            original = next(
                item for item in attempts if item["requestId"] == selected["victim"]
            )
            retry = next(
                item for item in attempts if item["requestId"] == selected["retry"]
            )
            if (
                original.get("outcome") == "TIMED_OUT"
                and retry["executionArn"] == original["executionArn"]
            ):
                events.append(
                    {
                        "kind": "service_retry",
                        "requestId": retry["requestId"],
                        "ownerRequestId": retry["requestId"],
                        "executionArn": retry["executionArn"],
                        "atMs": retry["startedAtMs"],
                        "source": "platform",
                        "previousRequestId": original["requestId"],
                        "sourceEvidence": "single-original-Invoke-and-native-service-continuation",
                    }
                )
        return attempts, selected, errors


def _causal_order(events: list[dict[str, Any]]) -> None:
    requests = {event["requestId"] for event in events}
    for request in requests:
        owned = sorted(
            (event for event in events if event["requestId"] == request),
            key=lambda event: (
                event["atMs"],
                event.get("captureSequence", event.get("sourceSequence", -1)),
            ),
        )
        for sequence, event in enumerate(owned, 1):
            event["sequence"] = sequence


def _quiescent(request: str, events: list[dict[str, Any]], *, telemetry: bool) -> bool:
    local = [event for event in events if event.get("requestId") == request]
    if not any(
        event.get("kind") in {"wrapper_return", "wrapper_raise"} for event in local
    ):
        return False
    if not any(
        event.get("kind") == "task_exit" and event.get("taskId") == "root"
        for event in local
    ):
        return False
    for start, end, key in (
        ("task_start", "task_exit", "taskId"),
        ("io_start", "io_exit", "taskId"),
        ("checkpoint_call", "checkpoint_exit", "callId"),
        ("poll_call", "poll_exit", "callId"),
        ("export_started", "export_completed", "exportId"),
        ("flush_started", "flush_completed", "flushId"),
    ):
        begun = Counter(event.get(key) for event in local if event.get("kind") == start)
        ended = Counter(event.get(key) for event in local if event.get("kind") == end)
        if begun - ended:
            return False
    if telemetry and not any(
        event.get("kind") == "export_quiescence"
        and event.get("pendingExportIds") == []
        and event.get("pendingFlushIds") == []
        and event.get("publicationCount") == 0
        for event in local
    ):
        return False
    return not any(
        event.get("kind") in {"observation_error", "control_error"} for event in local
    )


def build_evidence(
    context: ScenarioContext, cloud: Cloud, *, phase: str = "observations"
) -> dict[str, Any]:
    sources = CaseSources.capture(context, cloud)
    events, errors = sources.observations()
    histories, expected, history_errors = sources.histories(events)
    attempts, selected, attempt_errors = sources.attempts(events)
    errors.extend(history_errors + attempt_errors)
    _causal_order(events)
    ended = (cloud.observation_ended_at or 0) * 1000
    complete = bool(attempts) and all(
        _quiescent(
            item["requestId"], events, telemetry=context.scenario.otel_view is not None
        )
        for item in attempts
    )
    if not complete:
        errors.append(
            "Actual wrapper/task/export publication has not established a complete observation window"
        )
    latest = max((event["atMs"] for event in events), default=0)
    if ended < latest:
        errors.append("Collection predates a retained observation")
    watermarks = (
        {name: ended for name in ("lifecycle", "sdkRequests", "history")}
        if not errors
        else {}
    )
    if context.scenario.export_mode and not errors:
        watermarks["exportLifecycle"] = ended
    artifact = {
        "sdk": context.profile.artifact_id,
        "conformance": cloud.manifest.get("conformanceRevision", "unverified"),
    }
    if context.scenario.otel_view:
        artifact["plugin"] = next(
            (
                f"sha256:{digest}"
                for name, digest in cloud.manifest.get("wheels", {}).items()
                if "_otel-" in name
            ),
            "",
        )
    profile = {
        "functionArn": context.profile.resource_id,
        "configurationDigest": context.profile.configuration_id,
        "nativeConcurrency": context.profile.native_concurrency,
        "timeoutMs": cloud.manifest["invocationTimeout"] * 1000,
    }
    if context.profile.recovery_policy:
        profile["recoveryPolicy"] = context.profile.recovery_policy
    evidence: dict[str, Any] = {
        "schemaVersion": 1,
        "runId": context.run_id,
        "caseId": context.scenario.case_id,
        "cohortId": context.scenario.cohort_id,
        "coverageProfile": context.scenario.coverage_profile,
        "scenario": context.scenario.scenario,
        "requirementId": context.scenario.requirement_id,
        "runtime": "python",
        "artifact": artifact,
        "profile": profile,
        "attempts": attempts,
        "selectedRequests": selected,
        "events": events,
        "histories": histories,
        "expectedHistories": expected,
        "observationEndedAtMs": ended,
        "sdkRequests": [],
        "collection": {
            "watermarks": watermarks,
            "evidenceUris": sorted(cloud.source_uris),
        },
        "collectionErrors": errors,
    }
    if context.scenario.otel_view:
        evidence["otelView"] = context.scenario.otel_view
    if context.scenario.export_mode:
        evidence["exportMode"] = context.scenario.export_mode
        evidence["exportLifecycle"] = {
            "budgets": dict(context.scenario.behavior_budgets)
        }
    else:
        evidence["regression"] = {"budgets": dict(context.scenario.behavior_budgets)}
        victim = next(
            (item for item in cloud.invocations if item.get("role") == "victim"), None
        )
        if victim:
            fixture = victim.get("payload", {}).get("scenario")
            suffix = (
                "-checkpoint"
                if fixture == "checkpoint"
                else "-cleanup"
                if fixture == "suspend-cleanup"
                else "-loser"
            )
            if fixture in {
                "checkpoint",
                "suspend-cleanup",
                "parallel",
                "map",
                "nested",
                "return-inflight",
                "failure-inflight",
                "late-operation",
            }:
                evidence["regression"]["gateId"] = victim["marker"] + suffix
        if context.scenario.scenario in {"warm-lifecycle", "otel-warm-lifecycle"}:
            evidence["regression"]["warmSequence"] = [
                {"executionArn": item["arn"], "fixture": item["payload"]["scenario"]}
                for item in cloud.invocations
            ]
    for event in events:
        if event.get("kind") in {"checkpoint_call", "poll_call"}:
            evidence["sdkRequests"].append(
                {
                    **event,
                    "method": "CheckpointDurableExecution"
                    if event["kind"] == "checkpoint_call"
                    else "GetDurableExecutionState",
                    "outcome": "DISPATCHED",
                }
            )
    created = [
        {
            "kind": "invocation",
            "id": item["requestId"],
            "ownerCaseId": context.scenario.case_id,
        }
        for item in attempts
    ]
    created.extend(
        {"kind": "execution", "id": arn, "ownerCaseId": context.scenario.case_id}
        for arn in dict.fromkeys(item["executionArn"] for item in attempts)
    )
    for kind in ("export", "flush"):
        created.extend(
            {
                "kind": kind,
                "id": event[f"{kind}Id"],
                "ownerCaseId": context.scenario.case_id,
            }
            for event in events
            if event.get("kind") == f"{kind}_started"
        )
    evidence["resourcesCreated"] = created
    faults = []
    for gate in dict.fromkeys(
        record["gate"] for record in cloud.control_records if record["action"] == "hold"
    ):
        fault: dict[str, Any] = {"id": gate, "ownerCaseId": context.scenario.case_id}
        receipts = [
            event["atMs"]
            for event in events
            if event.get("gateId") == gate
            and event.get("kind") in {"fault_released", "export_released"}
            and event.get("mode") == "normal"
        ]
        releases = [
            record
            for record in cloud.control_records
            if record["gate"] == gate
            and record["action"] == "release"
            and record.get("state", "ACKNOWLEDGED") == "ACKNOWLEDGED"
        ]
        if receipts:
            fault["releasedAtMs"] = min(receipts)
        if releases:
            release = releases[0]
            requested = release.get("started", release["time"]) * 1000
            if not receipts or requested <= min(receipts):
                fault.update(
                    releaseRequestedAtMs=requested,
                    releaseAcknowledgedAtMs=release["time"] * 1000,
                )
            fault.setdefault("releasedAtMs", release["time"] * 1000)
        faults.append(fault)
    evidence["faultsCreated"] = faults
    if context.scenario.otel_view:
        from lmi_tests.shared_telemetry import collect_telemetry

        headers = {
            request: item["trace_header"]
            for item in cloud.invocations
            for request in item.get("native_requests", [])
            if item.get("trace_header")
        }
        evidence["telemetry"] = collect_telemetry(
            list(sources.raw),
            attempts,
            view=context.scenario.otel_view,
            expected_sampled=context.scenario.scenario != "runtime-carrier-unsampled",
            caller_headers=headers,
            observed_until_ms=ended,
            export_mode=context.scenario.export_mode or "sync",
        )
    if phase == "final":
        retired = (cloud.retired_at or 0) * 1000
        settled = []
        for resource in created:
            item = {
                **resource,
                "settledAtMs": retired,
                "evidenceUri": (cloud.artifacts_dir / "events.json").resolve().as_uri(),
            }
            if resource["kind"] == "invocation":
                item["outcome"] = (
                    "QUIESCENT"
                    if _quiescent(
                        resource["id"],
                        events,
                        telemetry=context.scenario.otel_view is not None,
                    )
                    else "UNKNOWN"
                )
            elif resource["kind"] == "execution":
                item["outcome"] = cloud.execution_results.get(resource["id"], {}).get(
                    "Status", "UNKNOWN"
                )
            else:
                completion = [
                    event
                    for event in events
                    if event.get("kind") == resource["kind"] + "_completed"
                    and event.get(resource["kind"] + "Id") == resource["id"]
                ]
                item["outcome"] = (
                    completion[0].get("outcome") if len(completion) == 1 else "UNKNOWN"
                )
            settled.append(item)
        evidence["retirement"] = {
            "retiredAtMs": retired,
            "resources": settled,
            "faults": [
                {
                    "id": fault["id"],
                    "ownerCaseId": context.scenario.case_id,
                    "releasedAtMs": fault.get(
                        "releaseAcknowledgedAtMs", fault.get("releasedAtMs", 0)
                    ),
                    "mode": "emergency"
                    if any(
                        event.get("gateId") == fault["id"]
                        and event.get("kind") in {"fault_released", "export_released"}
                        and event.get("mode") == "emergency"
                        for event in events
                    )
                    else "normal",
                }
                for fault in faults
            ],
        }
    cloud._save(f"shared-{phase}-evidence.json", evidence)
    return evidence
