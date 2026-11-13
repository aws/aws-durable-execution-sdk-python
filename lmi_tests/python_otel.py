# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Mandatory OTel observations for the Python LMI regression matrix.

The SDK adapter records actual public-plugin boundaries, exporter attempts and
runtime carriers. Canonical spans still use the ordinary backend normalization
and assertion engine. This profile describes Python's supported fixtures; it
does not require other runtimes to allocate identical plugin objects.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.chained_invoke import (
    execution_trace_context,
)
from aws_durable_execution_conformance_tests_otel.lmi import (
    _identifier,
    _number,
    _records,
    _service_execution_starts,
    _span,
    _view_errors,
)
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    TelemetryQuery,
    Trace,
)
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

SCENARIOS = (
    "runtime-carrier-sampled",
    "runtime-carrier-unsampled",
    "otel-warm-lifecycle",
    "otel-concurrent",
    "export-error-recovery",
    "held-export",
    "deadline-recovery",
    "timeout-retry",
    "pending-cleanup",
    "late-operation",
    "return-inflight",
    "failure-inflight",
)
_TERMINAL_SCENARIOS = frozenset(SCENARIOS[:6])


class _Telemetry:
    def __init__(self, evidence: Mapping[str, Any]) -> None:
        self.evidence = evidence
        self.errors: list[str] = []
        self.scenario = evidence.get("scenario")
        self.view_name = str(evidence.get("otelView", ""))
        self.attempts: dict[str, Mapping[str, Any]] = {}
        self.events: list[Mapping[str, Any]] = []
        self.enters: dict[str, Mapping[str, Any]] = {}
        self.exits: dict[str, Mapping[str, Any]] = {}
        self.boundaries: dict[str, Mapping[str, Any]] = {}
        self.carriers: dict[str, Mapping[str, Any]] = {}
        self.view: Mapping[str, Any] = {}

    def fail(self, detail: str, kind: str = "ASSERTION") -> None:
        self.errors.append(f"{kind}: {detail}")

    def records(self, request_id: str, kind: str) -> list[Mapping[str, Any]]:
        return [
            event
            for event in self.events
            if event.get("requestId") == request_id and event.get("kind") == kind
        ]

    def one(self, request_id: str, kind: str) -> Mapping[str, Any]:
        records = self.records(request_id, kind)
        if len(records) != 1:
            self.fail(f"{request_id} needs exactly one observed {kind}", "INCOMPLETE")
            return {}
        return records[0]

    def selected(self, role: str) -> str:
        request_id = self.evidence.get("selectedRequests", {}).get(role)
        if not isinstance(request_id, str) or request_id not in self.attempts:
            raise ValueError(f"missing original selected {role} request")
        return request_id

    def common(self) -> bool:
        if (
            self.evidence.get("coverageProfile") != "python-lifecycle"
            or self.evidence.get("runtime") != "python"
        ):
            self.fail(
                "Python OTel regressions require coverageProfile=python-lifecycle and runtime=python",
                "INCOMPLETE",
            )
            return False
        if self.scenario not in SCENARIOS or self.view_name not in (
            "invocation",
            "execution",
        ):
            self.fail("unknown OTel regression scenario or view", "INCOMPLETE")
            return False
        if not self.evidence.get("artifact", {}).get("plugin"):
            self.fail("exact plugin artifact identity is required", "INCOMPLETE")
        for attempt in _records(self.evidence.get("attempts"), "attempts"):
            request_id = attempt.get("requestId")
            if (
                not isinstance(request_id, str)
                or not request_id
                or request_id in self.attempts
            ):
                raise ValueError("attempts need unique request identities")
            self.attempts[request_id] = attempt
        if not self.attempts:
            raise ValueError("no observed native requests")
        self.events = _records(self.evidence.get("events"), "events")
        sequences: set[tuple[str, int]] = set()
        for event in self.events:
            request_id = event.get("requestId")
            if (
                request_id not in self.attempts
                or event.get("ownerRequestId") != request_id
            ):
                self.fail("event does not retain original request ownership")
                continue
            if event.get("executionArn") != self.attempts[request_id]["executionArn"]:
                self.fail("event does not retain original execution ownership")
            sequence = event.get("sequence")
            if (
                type(sequence) is not int
                or sequence < 1
                or (request_id, sequence) in sequences
            ):
                self.fail(
                    "events require unique positive per-request sequence numbers",
                    "INCOMPLETE",
                )
            else:
                sequences.add((request_id, sequence))
            if (
                not _number(self.attempts[request_id]["startedAtMs"])
                <= _number(event.get("atMs"))
                <= _number(self.evidence.get("observationEndedAtMs"))
            ):
                self.fail(
                    "event lies outside its original observation window", "INCOMPLETE"
                )
        telemetry = self.evidence.get("telemetry", {})
        views = telemetry.get("views", {})
        if (
            not isinstance(views, Mapping)
            or set(views) != {self.view_name}
            or not isinstance(views[self.view_name], Mapping)
        ):
            raise ValueError(
                "the matrix cell must collect exactly its declared otelView"
            )
        self.view = views[self.view_name]
        for carrier in _records(self.view.get("carriers"), "carriers"):
            request_id = carrier.get("requestId")
            if request_id not in self.attempts or request_id in self.carriers:
                raise ValueError("carriers need one record per observed request")
            self.carriers[request_id] = carrier
        if set(self.carriers) != set(self.attempts):
            raise ValueError("carriers must include every original and resumed request")
        return not self.errors

    def lifecycle(self) -> None:
        budget = _number(
            self.evidence.get("regression", {}).get("budgets", {}).get("exportMs")
        )
        if budget <= 0:
            self.fail(
                "regression.budgets.exportMs must be declared before measurement",
                "INCOMPLETE",
            )
        plugin_instances: set[str] = set()
        providers: dict[tuple[str, str], tuple[str, str]] = {}
        for request_id, attempt in self.attempts.items():
            enter = self.one(request_id, "otel_enter")
            end = self.one(request_id, "otel_exit")
            factory = self.one(request_id, "otel_factory")
            self.enters[request_id], self.exits[request_id] = enter, end
            boundary = self.records(request_id, "wrapper_return") + self.records(
                request_id, "wrapper_raise"
            )
            if len(boundary) != 1:
                self.fail(
                    f"{request_id} is missing the actual wrapper completion boundary",
                    "INCOMPLETE",
                )
            else:
                self.boundaries[request_id] = boundary[0]
                if (
                    not _number(enter.get("atMs"))
                    <= _number(factory.get("atMs"))
                    <= _number(boundary[0].get("atMs"))
                    <= _number(end.get("atMs"))
                ):
                    self.fail(
                        f"{request_id} plugin diagnostics contradict the actual SDK wrapper boundary"
                    )
            before, after = enter.get("contextBefore"), end.get("contextAfter")
            for snapshot in (before, after):
                if not isinstance(snapshot, Mapping) or set(snapshot) != {
                    "traceId",
                    "spanId",
                }:
                    self.fail(
                        "context restoration needs actual before/after trace/span snapshots",
                        "INCOMPLETE",
                    )
                elif snapshot["traceId"] is None and snapshot["spanId"] is None:
                    pass
                else:
                    _identifier(snapshot["traceId"], 32)
                    _identifier(snapshot["spanId"], 16)
            if before != after:
                self.fail(f"{request_id} did not restore its prior OTel context")
            if (
                type(end.get("exporterShutdowns")) is not int
                or end["exporterShutdowns"] != 0
            ):
                self.fail(
                    f"{request_id} shut down or failed to observe the shared exporter"
                )
            instance = factory.get("pluginInstanceId")
            if (
                not isinstance(instance, str)
                or not instance
                or instance in plugin_instances
            ):
                self.fail(
                    "Python plugin instances must be distinct per native invocation"
                )
            else:
                plugin_instances.add(instance)
            if factory.get("pluginExecutionArn") != attempt["executionArn"]:
                self.fail(
                    "plugin factory did not receive the original durable execution identity"
                )
            provider, exporter = enter.get("providerId"), enter.get("exporterId")
            if (
                not isinstance(provider, str)
                or not provider
                or not isinstance(exporter, str)
                or not exporter
            ):
                self.fail(
                    "provider/exporter identities must be observed at entry",
                    "INCOMPLETE",
                )
                continue
            process = (attempt["environmentId"], attempt["processId"])
            if providers.setdefault(process, (provider, exporter)) != (
                provider,
                exporter,
            ):
                self.fail("warm process replaced its shared provider/exporter")

    def spans(self) -> None:
        expected_sampled = self.scenario != "runtime-carrier-unsampled"
        if self.scenario in {
            "runtime-carrier-sampled",
            "runtime-carrier-unsampled",
            "otel-concurrent",
            "held-export",
        } and any(
            attempt.get("outcome") != "SUCCEEDED" for attempt in self.attempts.values()
        ):
            self.fail(
                f"{self.scenario} requires the original successful handler outcome"
            )
        for request_id, carrier in self.carriers.items():
            if carrier.get("expectedSampled") is not expected_sampled:
                self.fail(
                    f"{self.scenario} requires explicit Sampled={int(expected_sampled)} for every request"
                )
            if self.scenario in (
                "runtime-carrier-sampled",
                "runtime-carrier-unsampled",
            ):
                actual = execution_trace_context(
                    {"TraceHeader": {"XAmznTraceId": carrier.get("header")}}
                )
                caller = execution_trace_context(
                    {"TraceHeader": {"XAmznTraceId": carrier.get("callerHeader")}}
                )
                if (
                    actual.trace_id != caller.trace_id
                    or actual.sampled != caller.sampled
                ):
                    self.fail(
                        f"{request_id} runtime carrier did not preserve the caller Root and explicit sampling"
                    )
        records = _records(self.view.get("spans"), "spans")
        first_observation: dict[tuple[str, str], tuple[Any, Span]] = {}
        occurrences: list[tuple[Any, Span]] = []
        observed: list[Mapping[str, Any]] = []
        for record in records:
            span = _span(record.get("span"))
            key = (span.trace_id, span.span_id)
            occurrence = (record.get("requestId"), span)
            if first_observation.setdefault(key, occurrence) != occurrence:
                self.fail(
                    "duplicate span identity has inconsistent export observations"
                )
            # Collapse identical transport receipts only. Preserve contradictory
            # data and every native owner for attribution checks and diagnostics.
            if occurrence not in occurrences:
                occurrences.append(occurrence)
                observed.append(record)
            if record.get("sampled") is not True:
                self.fail("recording exporter span has no sampled SpanContext")
        # Preserve raw duplicates, but assert logical span identity/counts rather
        # than promising exactly-once transport delivery.
        assertions: dict[str, dict[str, Any]] = {}
        histories = {
            item["executionArn"]: item
            for item in _records(self.evidence.get("histories"), "histories")
        }
        for arn in {attempt["executionArn"] for attempt in self.attempts.values()}:
            requests = [
                request_id
                for request_id, attempt in self.attempts.items()
                if attempt["executionArn"] == arn
            ]
            expected: dict[str, Any] = {
                "minimum_spans": 1,
                "require_execution_correlation": True,
                "allowed_execution_arns": [arn],
                "span_assertions": [],
            }
            if expected_sampled:
                expected["span_assertions"].append(
                    {
                        "select": {"name": "Invocation"},
                        "count": len(requests),
                        "expect": {},
                    }
                )
            for request_id in requests:
                invocations = [
                    record
                    for record in observed
                    if record.get("requestId") == request_id
                    and record["span"]["name"] == "Invocation"
                ]
                if len(invocations) != int(expected_sampled):
                    self.fail(
                        f"{request_id} must emit exactly {int(expected_sampled)} logical Invocation span"
                    )
            if expected_sampled and self.scenario in _TERMINAL_SCENARIOS:
                terminal = max(
                    requests,
                    key=lambda request_id: self.attempts[request_id]["endedAtMs"],
                )
                status = histories.get(arn, {}).get("status")
                if status not in ("SUCCEEDED", "FAILED"):
                    self.fail(
                        f"{arn} lacks a successful or failed terminal service history",
                        "INCOMPLETE",
                    )
                expected["span_assertions"].append(
                    {
                        "select": {"name": "Workflow"},
                        "expect": {
                            "status": "OK" if status == "SUCCEEDED" else "ERROR"
                        },
                    }
                )
                workflows = [
                    record
                    for record in observed
                    if record["span"]["name"] == "Workflow"
                    and self.attempts.get(record.get("requestId", ""), {}).get(
                        "executionArn"
                    )
                    == arn
                ]
                if len(workflows) != 1 or workflows[0].get("requestId") != terminal:
                    self.fail(
                        f"{arn} must emit its Workflow once at the terminal invocation, not during replay"
                    )
            assertions[arn] = expected
            selected = [
                _span(record["span"])
                for record in observed
                if record.get("requestId") in requests
            ]
            if selected and expected_sampled:
                query = TelemetryQuery(
                    arn,
                    selected[0].service_name or "",
                    min(span.start_time for span in selected),
                    max(span.end_time for span in selected),
                )
                self.errors.extend(
                    f"ASSERTION: {error}"
                    for error in validate_trace(
                        Trace(selected[0].trace_id, tuple(selected)), expected, query
                    )
                )
        strict_view = {**self.view, "spans": observed, "assertions": assertions}
        self.errors.extend(
            _view_errors(
                self.view_name,
                strict_view,
                self.attempts,
                "python",
                _number(self.evidence.get("observationEndedAtMs")),
                execution_starts=_service_execution_starts(
                    self.evidence.get("histories", [])
                ),
                operation_histories=_records(
                    self.evidence.get("histories", []), "histories"
                ),
            )
        )
        outcomes = {
            item["requestId"]: item.get("status")
            for item in _records(self.view.get("exportOutcomes"), "exportOutcomes")
        }
        victim = self.evidence.get("selectedRequests", {}).get("victim")
        for request_id in self.attempts:
            expected_outcome = (
                "unsampled"
                if not expected_sampled
                else "export-error"
                if self.scenario == "export-error-recovery" and request_id == victim
                else "delivered"
            )
            if outcomes.get(request_id) != expected_outcome:
                self.fail(
                    f"{self.scenario}/{request_id} requires export outcome {expected_outcome}"
                )
            exports = self.records(request_id, "otel_export")
            starts = self.records(request_id, "otel_export_started")
            if {(event.get("traceId"), event.get("spanId")) for event in starts} != {
                (event.get("traceId"), event.get("spanId")) for event in exports
            }:
                self.fail(
                    f"{request_id} export start/completion observations disagree",
                    "INCOMPLETE",
                )
            emitted = {
                (record["span"]["trace_id"], record["span"]["span_id"])
                for record in records
                if record.get("requestId") == request_id
            }
            if {
                (event.get("traceId"), event.get("spanId")) for event in exports
            } != emitted:
                self.fail(
                    f"{request_id} export observations omit or add canonical spans",
                    "INCOMPLETE",
                )
            expected_event = (
                "failure"
                if expected_outcome == "export-error"
                else "held"
                if self.scenario == "held-export"
                else "success"
            )
            for event in exports:
                begun = [
                    start
                    for start in starts
                    if (start.get("traceId"), start.get("spanId"))
                    == (event.get("traceId"), event.get("spanId"))
                ]
                if len(begun) != 1:
                    self.fail(
                        f"{request_id} needs a unique observed exporter start",
                        "INCOMPLETE",
                    )
                elif (
                    not 0
                    <= _number(event.get("atMs")) - _number(begun[0].get("atMs"))
                    <= _number(self.evidence["regression"]["budgets"]["exportMs"])
                ):
                    self.fail(
                        f"{request_id} exporter exceeded its immutable exportMs budget"
                    )
                if event.get("outcome") != expected_event:
                    self.fail(
                        f"{request_id} exporter outcome differs from the selected fixture"
                    )
                if (
                    not _number(self.enters[request_id].get("atMs"))
                    <= _number(event.get("atMs"))
                    <= _number(self.boundaries[request_id].get("atMs"))
                ):
                    self.fail(
                        f"{request_id} exporter activity escaped its OTel lifetime"
                    )

    def warm(self) -> None:
        sequence = _records(
            self.evidence.get("regression", {}).get("warmSequence"),
            "regression.warmSequence",
        )
        if [item.get("fixture") for item in sequence] != [
            "success",
            "failure",
            "replay",
        ] * 2:
            self.fail(
                "OTel warm coverage requires success/failure/replay repeated twice",
                "INCOMPLETE",
            )
            return
        arns = [item.get("executionArn") for item in sequence]
        if len(set(arns)) != 6 or set(arns) != {
            attempt["executionArn"] for attempt in self.attempts.values()
        }:
            self.fail(
                "warm sequence must identify all six distinct actual executions",
                "INCOMPLETE",
            )
            return
        previous_end = -1.0
        reused: set[tuple[str, str]] = set()
        saw_reuse = False
        for item in sequence:
            attempts = sorted(
                (
                    attempt
                    for attempt in self.attempts.values()
                    if attempt["executionArn"] == item["executionArn"]
                ),
                key=lambda attempt: attempt["startedAtMs"],
            )
            if not attempts or any(
                attempt.get("fixture") != item["fixture"] for attempt in attempts
            ):
                self.fail(
                    "warm fixtures must match observed invocation routing", "INCOMPLETE"
                )
                continue
            if _number(attempts[0]["startedAtMs"]) <= previous_end:
                self.fail("warm executions were not sequential")
            previous_end = _number(attempts[-1]["endedAtMs"])
            key = (attempts[0]["environmentId"], attempts[0]["processId"])
            saw_reuse |= key in reused
            reused.add(key)
            expected = "FAILED" if item["fixture"] == "failure" else "SUCCEEDED"
            history: Mapping[str, Any] = next(
                (
                    history
                    for history in self.evidence["histories"]
                    if history["executionArn"] == item["executionArn"]
                ),
                {},
            )
            if attempts[-1]["outcome"] != expected or history.get("status") != expected:
                self.fail(
                    f"warm {item['fixture']} has the wrong terminal invocation outcome"
                )
            if item["fixture"] == "replay":
                from lmi_tests.python_lifecycle import validate_replay_evidence

                self.errors.extend(
                    validate_replay_evidence(self.evidence, item["executionArn"])
                )
        if not saw_reuse:
            self.fail("warm fixtures never reused an identified process", "PLACEMENT")

    def concurrency(self) -> None:
        capacity = self.evidence["profile"]["nativeConcurrency"]
        originals = [
            attempt for attempt in self.attempts.values() if attempt.get("attempt") == 1
        ]
        if len(originals) != capacity or len(self.attempts) != capacity:
            self.fail(
                "concurrent telemetry must occupy exactly the configured native cohort",
                "PLACEMENT",
            )
            return
        if (
            len({attempt["environmentId"] for attempt in originals}) != 1
            or len({attempt["processId"] for attempt in originals}) != capacity
        ):
            self.fail(
                "concurrent Python requests need distinct processes in one original environment",
                "PLACEMENT",
            )
        ordered = sorted(
            originals,
            key=lambda attempt: self.exits[attempt["requestId"]].get("atMs", 0),
        )
        for index, attempt in enumerate(ordered):
            request_id = attempt["requestId"]
            blocked = self.one(request_id, "blocked")
            release = self.one(request_id, "control_released")
            if blocked.get("gateId") != release.get("gateId") or not blocked.get(
                "gateId"
            ):
                self.fail("cohort control release must identify its held gate")
            if (
                not _number(blocked.get("atMs"))
                < _number(release.get("atMs"))
                <= _number(self.exits[request_id].get("atMs"))
            ):
                self.fail("invocation ended before its own barrier was released")
            for peer in ordered[index + 1 :]:
                peer_id = peer["requestId"]
                boundary = _number(self.exits[request_id].get("atMs"))
                peer_release = self.one(peer_id, "control_released")
                if (
                    not _number(self.enters[peer_id].get("atMs"))
                    < boundary
                    < _number(peer_release.get("atMs"))
                ):
                    self.fail(
                        "ending one invocation did not leave the established peer held"
                    )
                if not any(
                    boundary
                    < _number(event.get("atMs"))
                    < _number(peer_release.get("atMs"))
                    for event in self.records(peer_id, "alive")
                ):
                    self.fail(
                        "held peer made no observed progress after the other invocation ended"
                    )

    def export_recovery(self) -> None:
        victim, healthy = self.selected("victim"), self.selected("warm")
        if any(
            self.attempts[victim][key] != self.attempts[healthy][key]
            for key in ("environmentId", "processId", "workerId")
        ):
            self.fail(
                "export recovery must reuse the original worker/process", "PLACEMENT"
            )
        if _number(self.attempts[healthy]["startedAtMs"]) <= _number(
            self.exits[victim].get("atMs")
        ):
            self.fail("healthy export did not occur after the failed invocation ended")
        if (
            self.attempts[victim].get("outcome") != "SUCCEEDED"
            or self.attempts[healthy].get("outcome") != "SUCCEEDED"
        ):
            self.fail(
                "export fixture failure must not change either successful handler outcome"
            )

    def held(self) -> None:
        victim = self.selected("victim")
        held = self.one(victim, "export_held")
        release = self.one(victim, "export_released")
        if not held.get("gateId") or held.get("gateId") != release.get("gateId"):
            self.fail("held export and release must identify the same owned gate")
        faults = _records(self.evidence.get("faultsCreated"), "faultsCreated")
        owned = [
            fault
            for fault in faults
            if fault.get("id") == held.get("gateId")
            and fault.get("ownerCaseId") == self.evidence.get("caseId")
        ]
        if (
            len(owned) != 1
            or owned[0].get("releasedAtMs") != release.get("atMs")
            or release.get("mode") != "normal"
        ):
            self.fail(
                "held-export release lacks case-owned normal measurement evidence",
                "INCOMPLETE",
            )
        if (
            not _number(held.get("atMs"))
            < _number(release.get("atMs"))
            <= _number(self.boundaries[victim].get("atMs"))
        ):
            self.fail(
                "held export did not prevent SDK wrapper completion until normal release"
            )
        markers = {
            (event.get("traceId"), event.get("spanId"))
            for event in self.records(victim, "otel_export")
        }
        if (held.get("traceId"), held.get("spanId")) not in markers:
            self.fail(
                "held export was not bound to an observed exporter span", "INCOMPLETE"
            )
        else:
            key = (held.get("traceId"), held.get("spanId"))
            started = [
                event
                for event in self.records(victim, "otel_export_started")
                if (event.get("traceId"), event.get("spanId")) == key
            ]
            completed = [
                event
                for event in self.records(victim, "otel_export")
                if (event.get("traceId"), event.get("spanId")) == key
            ]
            if (
                len(started) != 1
                or not completed
                or not _number(started[0].get("atMs"))
                <= _number(held.get("atMs"))
                < _number(release.get("atMs"))
                <= min(_number(event.get("atMs")) for event in completed)
            ):
                self.fail("held-export gate was not inside the actual exporter call")


def validate_lifecycle_telemetry(
    evidence: Mapping[str, Any], *, phase: str = "final"
) -> list[str]:
    """Validate one mandatory view/configuration cell before releasing faults.

    Call the core lifecycle regression validator as well. Both phases preserve
    observation checks; final resource retirement remains the core's concern.
    """
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown validation phase"]
    validator = _Telemetry(evidence)
    try:
        if not validator.common():
            return validator.errors
        validator.lifecycle()
        if validator.errors:
            return validator.errors
        validator.spans()
        if validator.scenario == "otel-warm-lifecycle":
            validator.warm()
        elif validator.scenario == "otel-concurrent":
            validator.concurrency()
        elif validator.scenario == "export-error-recovery":
            validator.export_recovery()
        elif validator.scenario == "held-export":
            validator.held()
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        validator.fail(f"malformed OTel regression evidence: {error}", "INCOMPLETE")
    return validator.errors
