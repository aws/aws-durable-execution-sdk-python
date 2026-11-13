# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Python SDK LMI lifecycle, replay, deadline and settled-scope regressions."""

from __future__ import annotations

import math
from collections.abc import Mapping
from itertools import pairwise
from typing import Any

SCENARIOS = {
    "native-concurrency": 1,
    "deadline-recovery": 4,
    "replay-callback-retry": 5,
    "warm-lifecycle": 6,
    "early-completion-parallel": 9,
    "early-completion-map": 9,
    "early-completion-nested": 9,
    "checkpoint-settlement": 10,
    "timeout-retry": 11,
    "late-operation": 12,
    "return-inflight": 13,
    "failure-inflight": 13,
    "pending-cleanup": 14,
    "nested-progress": 15,
}
OTEL_SCENARIOS = {
    "runtime-carrier-sampled": 7,
    "runtime-carrier-unsampled": 7,
    "otel-warm-lifecycle": 7,
    "otel-concurrent": 7,
    "export-error-recovery": 8,
    "held-export": 8,
}
ACTIVITY = {
    "body",
    "effect",
    "checkpoint_call",
    "checkpoint_ack",
    "checkpoint_exit",
    "poll_call",
    "poll_exit",
}
ENDINGS = {"wrapper_return", "wrapper_raise"}


class _Observations:
    def __init__(self, evidence: Mapping[str, Any]) -> None:
        self.evidence = evidence
        self.errors: list[str] = []
        self.config = self.mapping(evidence.get("regression"), "regression")
        self.profile = self.mapping(evidence.get("profile"), "profile")
        self.attempts = {
            self.string(item.get("requestId"), "attempt.requestId"): item
            for item in self.records(evidence.get("attempts"), "attempts")
        }
        self.events = self.records(evidence.get("events"), "events")
        self.histories = self.records(evidence.get("histories"), "histories")
        self.selected = self.mapping(
            evidence.get("selectedRequests"), "selectedRequests"
        )
        self.end = self.number(
            evidence.get("observationEndedAtMs"), "observationEndedAtMs"
        )

    def fail(self, message: str, category: str = "ASSERTION") -> None:
        self.errors.append(f"{category}: {message}")

    def mapping(self, value: Any, path: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            self.fail(f"{path} must be an object", "INCOMPLETE")
            return {}
        return value

    def records(self, value: Any, path: str) -> list[Mapping[str, Any]]:
        if not isinstance(value, list):
            self.fail(f"{path} must be an array", "INCOMPLETE")
            return []
        return [self.mapping(item, path) for item in value]

    def string(self, value: Any, path: str) -> str:
        if not isinstance(value, str) or not value:
            self.fail(f"{path} must be a non-empty string", "INCOMPLETE")
            return ""
        return value

    def number(self, value: Any, path: str) -> float:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            self.fail(f"{path} must be a finite non-negative number", "INCOMPLETE")
            return 0
        return float(value)

    def budget(self, name: str) -> float:
        budgets = self.mapping(self.config.get("budgets"), "regression.budgets")
        value = self.number(budgets.get(name), f"regression.budgets.{name}")
        if value <= 0:
            self.fail(f"regression.{name} must be positive", "INCOMPLETE")
        return value

    def role(self, role: str) -> Mapping[str, Any]:
        request_id = self.selected.get(role)
        if not isinstance(request_id, str) or request_id not in self.attempts:
            self.fail(f"selected original {role} request is missing", "INCOMPLETE")
            return {}
        return self.attempts[request_id]

    def select(
        self,
        kind: str | set[str],
        request: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> list[Mapping[str, Any]]:
        kinds = {kind} if isinstance(kind, str) else kind
        return [
            item
            for item in self.events
            if item.get("kind") in kinds
            and (request is None or item.get("requestId") == request.get("requestId"))
            and all(item.get(key) == value for key, value in fields.items())
        ]

    def one(
        self, kind: str | set[str], request: Mapping[str, Any], **fields: Any
    ) -> Mapping[str, Any]:
        matches = self.select(kind, request, **fields)
        if len(matches) != 1:
            self.fail(
                f"request {request.get('requestId')} requires exactly one {kind} observation {fields}",
                "INCOMPLETE",
            )
            return {}
        return matches[0]

    def ordered(self, *events: Mapping[str, Any]) -> bool:
        return all(
            first.get("requestId") == second.get("requestId")
            and first.get("sequence", -1) < second.get("sequence", -1)
            and first.get("atMs", -1) <= second.get("atMs", -1)
            for first, second in pairwise(events)
        )

    def history(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        matches = [
            item
            for item in self.histories
            if item.get("executionArn") == request.get("executionArn")
        ]
        if len(matches) != 1:
            self.fail(
                "one collected history is required for each selected execution",
                "INCOMPLETE",
            )
            return {}
        return matches[0]

    def operation(
        self, request: Mapping[str, Any], name: str, status: str | None = None
    ) -> Mapping[str, Any]:
        matches = [
            item
            for item in self.records(
                self.history(request).get("operations"), "history.operations"
            )
            if item.get("name") == name
        ]
        if len(matches) != 1 or (
            status is not None and matches[0].get("status") != status
        ):
            self.fail(
                f"collected history needs one {status or ''} {name} operation",
                "INCOMPLETE",
            )
            return {}
        return matches[0]

    def resources(self, event: Mapping[str, Any]) -> Mapping[str, Any]:
        resources = self.mapping(event.get("resources"), "wrapper.resources")
        threads = self.records(resources.get("threads"), "wrapper.resources.threads")
        ids = []
        for thread in threads:
            ids.append(self.string(thread.get("id"), "thread.id"))
            self.string(thread.get("name"), "thread.name")
        if len(ids) != len(set(ids)):
            self.fail("duplicate thread identities in resource snapshot", "INCOMPLETE")
        for counter in ("fds", "rssBytes"):
            self.number(resources.get(counter), f"wrapper.resources.{counter}")
        return resources

    def lifecycle(self) -> None:
        """Validate every request, including unselected/resumed attempts."""
        if self.evidence.get("runtime") != "python":
            self.fail("Python lifecycle coverage requires runtime python", "INCOMPLETE")
        grace = self.budget("observationGraceMs")
        if not self.attempts or not self.events:
            self.fail("lifecycle attempts and events must not be empty", "INCOMPLETE")
        per_request: dict[str, list[Mapping[str, Any]]] = {}
        for event in self.events:
            request_id = event.get("requestId")
            if not isinstance(request_id, str) or request_id not in self.attempts:
                self.fail("event has no retained invocation owner", "INCOMPLETE")
                continue
            if event.get("ownerRequestId") != request_id or event.get(
                "executionArn"
            ) != self.attempts[request_id].get("executionArn"):
                self.fail("lifecycle event belongs to another request or execution")
            if type(event.get("sequence")) is not int or event["sequence"] < 0:
                self.fail("event.sequence must be a non-negative integer", "INCOMPLETE")
            when = self.number(event.get("atMs"), "event.atMs")
            if when > self.end or when < self.attempts[request_id].get(
                "startedAtMs", 0
            ):
                self.fail(
                    "event lies outside retained observation window", "INCOMPLETE"
                )
            per_request.setdefault(request_id, []).append(event)
            if event.get("kind") in ("escape", "control_error"):
                self.fail(
                    "emergency escape or failed fault control cannot establish SDK cleanup"
                )
        for request_id, request in self.attempts.items():
            self.string(request.get("marker"), "attempt.marker")
            events = sorted(
                per_request.get(request_id, []),
                key=lambda item: item.get("sequence", -1),
            )
            if len({item.get("sequence") for item in events}) != len(events) or any(
                a.get("atMs", 0) > b.get("atMs", 0) for a, b in pairwise(events)
            ):
                self.fail(
                    "lifecycle sequence is duplicated or contradicts the normalized clock",
                    "INCOMPLETE",
                )
            entry = self.one("wrapper_enter", request)
            baseline = self.resources(entry)
            if request.get("outcome") == "TIMED_OUT":
                self.one("platform_timeout", request, source="platform")
            exits = self.select(ENDINGS, request)
            if not exits:
                if request.get("outcome") != "TIMED_OUT":
                    self.fail(
                        "non-timeout invocation has no actual wrapper exit",
                        "INCOMPLETE",
                    )
                retirement = self.select("worker_retired", request, source="platform")
                task_exit = self.select("task_exit", request, taskId="root")
                if not retirement and not task_exit:
                    self.fail(
                        "timed-out request has neither actual root exit nor platform worker retirement",
                        "INCOMPLETE",
                    )
                if (
                    retirement
                    and self.profile.get("recoveryPolicy") != "replace-worker"
                ):
                    self.fail(
                        "worker retirement requires the declared runtime replacement policy"
                    )
                continue
            end = self.one(ENDINGS, request)
            wrapper_outcome = end.get("outcome")
            if wrapper_outcome not in ("SUCCEEDED", "FAILED", "PENDING", "TIMED_OUT"):
                self.fail(
                    "actual wrapper outcome is missing or unrecognized", "INCOMPLETE"
                )
            elif request.get(
                "outcome"
            ) != "TIMED_OUT" and wrapper_outcome != request.get("outcome"):
                self.fail("actual wrapper outcome differs from its original request")
            # A platform timeout does not stop the process. A later SDK return
            # or exception is a separate boundary, still subject to settlement
            # and stale-activity checks below.
            user_exit = self.one("user_exit", request)
            if not self.ordered(entry, user_exit, end):
                self.fail("wrapper exited before actual user-scope exit")
            if self.end < end.get("atMs", 0) + grace:
                self.fail(
                    "collection ended before the declared post-wrapper observation window",
                    "INCOMPLETE",
                )
            resources = self.resources(end)
            baseline_ids = {thread.get("id") for thread in baseline.get("threads", [])}
            for thread in resources.get("threads", []):
                if thread.get("id") not in baseline_ids and (
                    str(thread.get("name", "")).startswith(
                        ("dex-", "ThreadPoolExecutor")
                    )
                    or thread.get("ownerRequestId") == request_id
                ):
                    self.fail(
                        "invocation-owned worker survived the actual wrapper exit"
                    )
            if any(
                item.get("kind") in ACTIVITY
                and item.get("sequence", -1) > end.get("sequence", -1)
                for item in events
            ):
                self.fail(
                    "user activity or request settlement continued after wrapper exit"
                )
            for start_kind, end_kind, identity in (
                ("task_start", "task_exit", "taskId"),
                ("io_start", "io_exit", "taskId"),
                ("checkpoint_call", "checkpoint_exit", "callId"),
                ("poll_call", "poll_exit", "callId"),
            ):
                for start in self.select(start_kind, request):
                    token = self.string(start.get(identity), f"{start_kind}.{identity}")
                    settled = self.one(end_kind, request, **{identity: token})
                    if not self.ordered(start, settled, end):
                        self.fail(f"{start_kind} remained unsettled at wrapper exit")
            for effect in self.select("body", request):
                name = self.string(effect.get("operation"), "body.operation")
                # Static names may repeat in distinct child contexts. Bind to
                # service identity rather than requiring globally unique names.
                operations = self.records(
                    self.history(request).get("operations"), "history.operations"
                )
                matches = [
                    item
                    for item in operations
                    if item.get("id") == effect.get("operationId")
                    and item.get("name") == name
                ]
                if len(matches) != 1:
                    self.fail("body observation changed its service operation identity")

    def overlap(
        self,
        roles: list[str],
        *,
        environment: str | None = None,
        gate_ids: Mapping[str, str] | None = None,
    ) -> list[Mapping[str, Any]]:
        requests = [self.role(role) for role in roles]
        if not requests or any(not request for request in requests):
            return requests
        n = len(requests)
        for field in ("requestId", "executionArn", "processId", "pid"):
            if len({request.get(field) for request in requests}) != n:
                self.fail(
                    f"native cohort lacks distinct {field} identities", "PLACEMENT"
                )
        if any(
            type(request.get("pid")) is not int or request["pid"] <= 0
            for request in requests
        ):
            self.fail("actual runtime PID evidence is required", "INCOMPLETE")
        environments = {request.get("environmentId") for request in requests}
        if len(environments) != 1 or (
            environment is not None and environments != {environment}
        ):
            self.fail("native cohort reached an unrelated environment", "PLACEMENT")
        # A fixture may also hold a handshake gate. Only its explicitly
        # selected work gate establishes this role's cohort participation.
        # Roles without a declared selector still require an unambiguous gate.
        blocked = [
            self.one(
                "blocked",
                request,
                **({"gateId": gate_ids[role]} if gate_ids and role in gate_ids else {}),
            )
            for role, request in zip(roles, requests, strict=True)
        ]
        pivot = max(item.get("atMs", 0) for item in blocked)
        for request, start in zip(requests, blocked, strict=True):
            alive = self.select(
                {"alive", "effect"}, request, gateId=start.get("gateId")
            )
            if (
                not any(item.get("atMs", -1) >= pivot for item in alive)
                or request.get("endedAtMs", 0) <= pivot
            ):
                self.fail(
                    "native overlap lacks positive continued liveness", "PLACEMENT"
                )
            if any(
                item.get("atMs", 0) <= pivot
                for item in self.select("io_exit", request, gateId=start.get("gateId"))
            ):
                self.fail(
                    "native capacity stopped before overlap was established",
                    "PLACEMENT",
                )
        return requests

    def gate(self, request: Mapping[str, Any]) -> str:
        gate_id = self.string(self.config.get("gateId"), "regression.gateId")
        faults = self.records(self.evidence.get("faultsCreated"), "faultsCreated")
        if not any(
            item.get("id") == gate_id
            and item.get("ownerCaseId") == self.evidence.get("caseId")
            for item in faults
        ):
            self.fail("fault gate lacks retained case ownership", "INCOMPLETE")
        self.one("blocked", request, gateId=gate_id)
        return gate_id

    def released(self, gate_id: str) -> float | None:
        releases = self.select("fault_released", gateId=gate_id)
        retirement = self.mapping(self.evidence.get("retirement", {}), "retirement")
        releases += [
            item
            for item in self.records(retirement.get("faults", []), "retirement.faults")
            if item.get("id") == gate_id
        ]
        times = []
        for release in releases:
            if release.get("mode") != "normal":
                self.fail(
                    "emergency or unclassified fault release cannot establish the lifecycle assertion"
                )
            times.append(
                self.number(
                    release.get("atMs", release.get("releasedAtMs")),
                    "fault release time",
                )
            )
        return min(times) if times else None

    def held_loser(self, request: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
        gate_id = self.gate(request)
        ready = self.one("winner_ready", request)
        selected = self.one("winner_selected", request)
        if not self.ordered(ready, selected):
            self.fail("parent winner selection preceded readiness")
        if not any(
            self.ordered(item, ready)
            for item in self.select("effect", request, gateId=gate_id)
        ) or any(
            self.ordered(item, ready)
            for item in self.select("io_exit", request, gateId=gate_id)
        ):
            self.fail(
                "losing branch was not actually held when its winner became ready",
                "INCOMPLETE",
            )
        return gate_id, selected

    def early(self) -> None:
        victim = self.role("victim")
        gate_id, winner = self.held_loser(victim)
        cutoff = winner.get("atMs", 0) + self.budget("cleanupMs")
        end = self.one(ENDINGS, victim)
        if end.get("atMs", self.end) > cutoff:
            self.fail(
                "early completion left the wrapper pinned beyond the cleanup budget"
            )
        if self.end < cutoff + self.budget("observationGraceMs"):
            self.fail(
                "early-completion collection ended before the cleanup assertion window",
                "INCOMPLETE",
            )
        if any(item.get("atMs", 0) > cutoff for item in self.select(ACTIVITY, victim)):
            self.fail(
                "abandoned scope continued effects or request activity beyond its cleanup budget"
            )
        released = self.released(gate_id)
        if released is not None and released <= cutoff:
            self.fail(
                "driver release supplied the losing-branch cleanup being asserted"
            )
        names = {
            "early-completion-parallel": ("race-parallel",),
            "early-completion-map": ("race-map",),
            "early-completion-nested": ("outer-map", "nested-child", "race-parallel"),
        }
        for name in names[self.evidence["scenario"]]:
            self.operation(victim, name, "SUCCEEDED")

    def checkpoint(self) -> None:
        victim = self.role("victim")
        gate_id = self.gate(victim)
        held = self.one("ack_held", victim, gateId=gate_id)
        blocked = self.one("blocked", victim, gateId=gate_id)
        call_id = self.string(held.get("callId"), "ack_held.callId")
        call = self.one("checkpoint_call", victim, callId=call_id)
        ack = self.one("checkpoint_ack", victim, callId=call_id, source="service")
        exited = self.one("checkpoint_exit", victim, callId=call_id)
        end = self.one(ENDINGS, victim)
        operation = self.operation(victim, "checkpoint-loser", "SUCCEEDED")
        pool = self.operation(victim, "checkpoint-pool", "SUCCEEDED")
        joined = self.one("branch_join", victim, scopeId=pool.get("id"))
        if held.get("operationId") != operation.get("id") or operation.get(
            "id"
        ) not in ack.get("operationIds", []):
            self.fail(
                "held acknowledgment does not belong to the selected real checkpoint"
            )
        release = self.released(gate_id)
        if not self.ordered(call, ack, held, blocked, exited, joined, end):
            self.fail(
                "real checkpoint acknowledgment/settlement did not precede branch join and wrapper return"
            )
        if release is None or not blocked.get("atMs", 0) < release <= exited.get(
            "atMs", 0
        ):
            self.fail(
                "checkpoint did not stay held until the scenario's normal release"
            )

    def deadline_measurement_end(
        self, gate_id: str, deadline: float, cutoff: float, grace: float
    ) -> float:
        if "deadlineMeasurement" not in self.config:
            return self.end
        prior_errors = len(self.errors)
        measurement = self.mapping(
            self.config["deadlineMeasurement"], "deadline measurement"
        )
        if measurement.get("selectedRequests") != self.selected:
            self.fail(
                "deadline measurement changed the selected original requests",
                "INCOMPLETE",
            )
        for field, expected in (
            ("deadlineAtMs", deadline),
            ("cleanupCutoffAtMs", cutoff),
            ("observationCutoffAtMs", cutoff + grace),
        ):
            actual = self.number(
                measurement.get(field), f"deadline measurement.{field}"
            )
            if not math.isclose(actual, expected, rel_tol=0, abs_tol=1):
                self.fail(
                    "deadline measurement differs from the runtime deadline or declared budgets",
                    "INCOMPLETE",
                )
        observed = self.number(
            measurement.get("observedAtMs"), "deadline measurement.observedAtMs"
        )
        if not cutoff + grace <= observed <= self.end:
            self.fail(
                "deadline measurement does not cover its complete original observation window",
                "INCOMPLETE",
            )
        uri = measurement.get("sourceEvidenceUri")
        collection = self.mapping(self.evidence.get("collection"), "collection")
        sources = collection.get("evidenceUris")
        if (
            not isinstance(uri, str)
            or not uri
            or not isinstance(sources, list)
            or uri not in sources
        ):
            self.fail(
                "deadline measurement has no retained source evidence", "INCOMPLETE"
            )
        faults = [
            fault
            for fault in self.records(
                self.evidence.get("faultsCreated"), "faultsCreated"
            )
            if fault.get("id") == gate_id
            and fault.get("ownerCaseId") == self.evidence.get("caseId")
        ]
        requested = self.end
        if len(faults) != 1:
            self.fail(
                "deadline measurement must retain its original fault gate", "INCOMPLETE"
            )
        else:
            requested = self.number(
                faults[0].get("releaseRequestedAtMs"),
                "deadline measurement.releaseRequestedAtMs",
            )
            acknowledged = self.number(
                faults[0].get("releaseAcknowledgedAtMs"),
                "deadline measurement.releaseAcknowledgedAtMs",
            )
            receipt = self.number(
                faults[0].get("releasedAtMs"), "deadline measurement.releasedAtMs"
            )
            earliest_release = self.released(gate_id)
            if (
                not observed <= requested <= acknowledged <= self.end
                or receipt < requested
                or (earliest_release is not None and earliest_release < requested)
            ):
                self.fail(
                    "deadline measurement contradicts the controller request or release evidence",
                    "INCOMPLETE",
                )
        # Observe the capture-to-release gap too; invalid snapshots never
        # shorten validation of the full observation tail.
        return requested if len(self.errors) == prior_errors else self.end

    def late_deadline_activity(
        self, victim: Mapping[str, Any], cutoff: float, released_after: float
    ) -> bool:
        for event in self.select(ACTIVITY, victim):
            when = event.get("atMs", 0)
            if when <= cutoff:
                continue
            # Release cannot authorize new user bodies or SDK dispatch. Only
            # completion of previously dispatched I/O is cleanup activity.
            if when <= released_after or event.get("kind") in {
                "body",
                "effect",
                "checkpoint_call",
                "poll_call",
            }:
                return True
            start_kind = {
                "checkpoint_ack": "checkpoint_call",
                "checkpoint_exit": "checkpoint_call",
                "poll_exit": "poll_call",
            }.get(event["kind"])
            if start_kind is not None:
                calls = self.select(start_kind, victim, callId=event.get("callId"))
                if len(calls) != 1 or calls[0].get("atMs", when) > when:
                    return True
        return False

    def timeout(self, victim: Mapping[str, Any]) -> float:
        timeout = self.one("platform_timeout", victim, source="platform")
        blocked = self.one("blocked", victim)
        observed = self.one("runtime_deadline", victim, source="runtime")
        remaining = self.number(
            observed.get("remainingTimeMs"), "runtime_deadline.remainingTimeMs"
        )
        configured = self.number(self.profile.get("timeoutMs"), "profile.timeoutMs")
        deadline = self.number(victim.get("deadlineAtMs"), "attempt.deadlineAtMs")
        observed_at = self.number(observed.get("atMs"), "runtime_deadline.atMs")
        if not 0 < remaining <= configured:
            self.fail(
                "runtime remaining time must be positive and within the configured timeout",
                "INCOMPLETE",
            )
        # A runtime deadline can precede wrapper-entry + configured timeout:
        # native dispatch/setup has already consumed some invocation lifetime.
        # One millisecond allows canonical timestamp rounding, not extra cleanup.
        if deadline <= observed_at or not math.isclose(
            deadline, observed_at + remaining, rel_tol=0, abs_tol=1
        ):
            self.fail(
                "attempt deadline does not match its observed runtime remaining time",
                "INCOMPLETE",
            )
        if (
            victim.get("outcome") != "TIMED_OUT"
            or timeout.get("atMs", 0) < deadline
            or blocked.get("atMs", deadline) >= deadline
        ):
            self.fail(
                "fixture did not cross the selected request's actual platform deadline with work held"
            )
        cutoff = deadline + self.budget("cleanupMs")
        grace = self.budget("observationGraceMs")
        if self.end < cutoff + grace:
            self.fail(
                "timeout collection ended before the stale-activity observation window",
                "INCOMPLETE",
            )
        gate_id = self.string(blocked.get("gateId"), "blocked.gateId")
        measured_until = self.deadline_measurement_end(gate_id, deadline, cutoff, grace)
        if self.late_deadline_activity(victim, cutoff, measured_until):
            self.fail(
                "original timed-out request continued side effects or checkpoint activity"
            )
        released = self.released(gate_id)
        if released is not None and released <= cutoff:
            self.fail("fault release supplied the timeout cleanup being asserted")
        return deadline

    def recovery(self) -> None:
        victim = self.role("victim")
        policy = self.profile.get("recoveryPolicy")
        if policy not in ("reuse-worker", "replace-worker"):
            self.fail(
                "deadline recovery requires an explicit supported recoveryPolicy",
                "INCOMPLETE",
            )
        deadline = self.timeout(victim)
        native = self.profile.get("nativeConcurrency")
        if native == 2:
            self.overlap(["victim", "peer"])
            peer = self.role("peer")
            if peer.get("outcome") != "SUCCEEDED":
                self.fail("healthy peer did not complete successfully")
        roles = ["recovery"] + (["recovery-peer"] if native == 2 else [])
        requests = self.overlap(roles, environment=victim.get("environmentId"))
        bound = deadline + self.budget("recoveryMs")
        release = self.released(self.one("blocked", victim).get("gateId", ""))
        recovered = max(
            (self.one("blocked", request).get("atMs", 0) for request in requests),
            default=self.end,
        )
        if release is not None and release <= recovered:
            self.fail("driver release supplied the capacity recovery being asserted")
        for request in requests:
            blocked = self.one("blocked", request)
            if (
                not deadline <= blocked.get("atMs", -1) <= bound
                or request.get("outcome") != "SUCCEEDED"
            ):
                self.fail(
                    "full original-environment capacity did not recover within its declared budget"
                )
        if self.profile.get("recoveryPolicy") == "reuse-worker" and not any(
            request.get("processId") == victim.get("processId")
            and request.get("workerId") == victim.get("workerId")
            for request in requests
        ):
            self.fail("full-capacity recovery omitted the original worker", "PLACEMENT")
        if self.profile.get("recoveryPolicy") == "replace-worker" and not any(
            request.get("replacesWorkerId") == victim.get("workerId")
            for request in requests
        ):
            self.fail(
                "replacement capacity did not identify the retired original worker",
                "PLACEMENT",
            )

    def replay(self, execution_arn: str) -> None:
        requests = [
            item
            for item in self.attempts.values()
            if item.get("executionArn") == execution_arn
        ]
        pending = [item for item in requests if item.get("outcome") == "PENDING"]
        completed = [item for item in requests if item.get("outcome") == "SUCCEEDED"]
        if (
            len(requests) < 2
            or not pending
            or not completed
            or not any(
                first.get("endedAtMs", 0) < last.get("startedAtMs", 0)
                for first in pending
                for last in completed
            )
        ):
            self.fail(
                "replay needs a real PENDING request followed by a service resume of the same execution"
            )
        if not requests:
            return
        original = min(requests, key=lambda item: item.get("startedAtMs", 0))
        marker = self.string(original.get("marker"), "attempt.marker")
        if any(item.get("marker") != marker for item in requests):
            self.fail("replay input marker changed between invocations")
        ids = {item["requestId"] for item in requests}
        for name, terminal in (("success", "SUCCEEDED"), ("failure", "FAILED")):
            operation = self.operation(original, name, terminal)
            bodies = [
                item
                for item in self.select("body", operation=name)
                if item.get("requestId") in ids
            ]
            if (
                len(bodies) != 1
                or bodies[0].get("value") != marker
                or bodies[0].get("operationId") != operation.get("id")
            ):
                self.fail(
                    f"completed {name} body was repeated, omitted or misattributed"
                )
        failures = [
            item
            for item in self.select("stored_failure")
            if item.get("requestId") in ids
        ]
        expected_error = {"type": "ValueError", "message": "expected:" + marker}
        if len({item.get("requestId") for item in failures}) < 2 or any(
            item.get("error") != expected_error for item in failures
        ):
            self.fail(
                "stored failure was not rethrown with the original meaning on replay"
            )
        retry = self.operation(original, "retry", "SUCCEEDED")
        bodies = sorted(
            [
                item
                for item in self.select("body", operation="retry")
                if item.get("requestId") in ids
            ],
            key=lambda item: item.get("atMs", 0),
        )
        if (
            any(type(item.get("attempt")) is not int for item in bodies)
            or [item.get("attempt") for item in bodies] != [1, 2]
            or any(
                item.get("value") != marker
                or item.get("operationId") != retry.get("id")
                for item in bodies
            )
        ):
            self.fail(
                "retry fixture did not execute exactly its two configured attempts"
            )
        operations = self.records(
            self.history(original).get("operations"), "history.operations"
        )
        for kind in ("WAIT", "CALLBACK", "CONTEXT"):
            if not any(
                item.get("type") == kind and item.get("status") == "SUCCEEDED"
                for item in operations
            ):
                self.fail(
                    f"real replay history lacks a successful {kind} operation",
                    "INCOMPLETE",
                )

    def service_retry(self) -> None:
        victim, retry = self.role("victim"), self.role("retry")
        deadline = self.timeout(victim)
        timeout = self.one("platform_timeout", victim, source="platform")
        if victim.get("executionArn") != retry.get("executionArn") or retry.get(
            "startedAtMs", 0
        ) <= timeout.get("atMs", deadline):
            self.fail(
                "timeout retry must be a later platform request of the same durable execution"
            )
        resumed = self.one("service_retry", retry)
        if (
            resumed.get("previousRequestId") != victim.get("requestId")
            or resumed.get("source") != "platform"
        ):
            self.fail(
                "driver-created invocation is not evidence of a service timeout retry"
            )
        if retry.get("startedAtMs", self.end) > deadline + self.budget("retryMs"):
            self.fail("service timeout retry exceeded its declared observation budget")
        success = self.operation(victim, "success", "SUCCEEDED")
        ids = {
            item["requestId"]
            for item in self.attempts.values()
            if item.get("executionArn") == victim.get("executionArn")
        }
        bodies = [
            item
            for item in self.select("body", operation="success")
            if item.get("requestId") in ids
        ]
        if (
            len(bodies) != 1
            or bodies[0].get("requestId") != victim.get("requestId")
            or bodies[0].get("operationId") != success.get("id")
            or bodies[0].get("value") != victim.get("marker")
        ):
            self.fail(
                "completed success was repeated or lost across the timeout service retry"
            )

    def peer_progress(
        self, victim: Mapping[str, Any], boundary: Mapping[str, Any], gate_id: str
    ) -> None:
        if self.profile.get("nativeConcurrency") != 2:
            return
        self.overlap(["victim", "peer"], gate_ids={"victim": gate_id})
        peer = self.role("peer")
        progress = self.select({"alive", "peer_progress"}, peer)
        if not any(
            boundary.get("atMs", 0) <= item.get("atMs", -1) < victim.get("endedAtMs", 0)
            for item in progress
        ):
            self.fail(
                "healthy peer did not progress while victim cleanup remained held"
            )

    def scope_settlement(self, victim: Mapping[str, Any], gate_id: str) -> None:
        blocked = self.one("blocked", victim, gateId=gate_id)
        exited = self.one("io_exit", victim, gateId=gate_id)
        end = self.one(ENDINGS, victim)
        release = self.released(gate_id)
        if (
            not self.ordered(blocked, exited, end)
            or release is None
            or not blocked.get("atMs", 0) < release <= exited.get("atMs", 0)
        ):
            self.fail(
                "wrapper abandoned held invocation-owned work before normal settlement"
            )

    def inflight(self) -> None:
        victim = self.role("victim")
        gate_id, selected = self.held_loser(victim)
        root = self.one("user_exit", victim)
        exited = self.one("io_exit", victim, gateId=gate_id)
        if not self.ordered(selected, root, exited):
            self.fail(
                "root completion was not observed with losing work still in flight"
            )
        self.peer_progress(victim, root, gate_id)
        self.scope_settlement(victim, gate_id)
        status = (
            "FAILED"
            if self.evidence.get("scenario") == "failure-inflight"
            else "SUCCEEDED"
        )
        if victim.get("outcome") != status:
            self.fail("inflight root outcome changed during scope settlement")
        if status == "FAILED" and self.history(victim).get("error") != {
            "type": "ValueError",
            "message": "expected:" + victim.get("marker", ""),
        }:
            self.fail("settling inflight work replaced the original root failure")

    def pending_cleanup(self) -> None:
        victim, resume = self.role("victim"), self.role("resume")
        gate_id = self.gate(victim)
        blocked = self.one("blocked", victim, gateId=gate_id)
        self.peer_progress(victim, blocked, gate_id)
        self.scope_settlement(victim, gate_id)
        enter, exited = (
            self.one("cleanup_enter", victim),
            self.one("cleanup_exit", victim),
        )
        end = self.one(ENDINGS, victim)
        io_exit = self.one("io_exit", victim, gateId=gate_id)
        user_exit = self.one("user_exit", victim)
        if (
            not self.ordered(enter, blocked, io_exit, exited, user_exit, end)
            or victim.get("outcome") != "PENDING"
        ):
            self.fail("PENDING preceded completion of the original root finally block")
        if (
            resume.get("executionArn") != victim.get("executionArn")
            or resume.get("startedAtMs", 0) <= end.get("atMs", 0)
            or resume.get("outcome") != "SUCCEEDED"
        ):
            self.fail(
                "cleanup fixture did not really resume the original suspended execution"
            )
        bodies = [
            item
            for item in self.select("body", operation="success")
            if item.get("executionArn") == victim.get("executionArn")
        ]
        if len(bodies) != 1 or bodies[0].get("value") != victim.get("marker"):
            self.fail(
                "completed side effect repeated after finally cleanup and suspension"
            )
        if not any(
            item.get("type") == "WAIT" and item.get("status") == "SUCCEEDED"
            for item in self.records(
                self.history(victim).get("operations"), "history.operations"
            )
        ):
            self.fail("cleanup replay did not use a real durable wait", "INCOMPLETE")

    def late_operation(self) -> None:
        victim = self.role("victim")
        gate_id, selected = self.held_loser(victim)
        self.scope_settlement(victim, gate_id)
        attempted, rejected = (
            self.one("late_attempt", victim),
            self.one("late_rejected", victim),
        )
        closed = self.one("scope_closed", victim, scopeId=attempted.get("scopeId"))
        operations = self.records(
            self.history(victim).get("operations"), "history.operations"
        )
        if not any(
            item.get("id") == attempted.get("scopeId") and item.get("type") == "CONTEXT"
            for item in operations
        ):
            self.fail(
                "late attempt is not bound to a real completed child scope",
                "INCOMPLETE",
            )
        if not self.ordered(self.one("io_exit", victim, gateId=gate_id), attempted):
            self.fail(
                "late operation fixture did not follow settlement of the held losing step"
            )
        if (
            not self.ordered(closed, attempted, rejected)
            or not self.ordered(selected, attempted)
            or rejected.get("scopeId") != attempted.get("scopeId")
            or rejected.get("error") != {"type": "OrphanedChildException"}
        ):
            self.fail(
                "completed child scope did not reject the attempted late durable operation"
            )
        if self.select("late_accepted", victim) or self.select(
            "body", victim, operation="late-work"
        ):
            self.fail("abandoned child executed or admitted late user work")
        if any(
            item.get("name") == "late-work"
            for item in self.records(
                self.history(victim).get("operations"), "history.operations"
            )
        ):
            self.fail("late child operation reached the durable service")
        for request in self.records(self.evidence.get("sdkRequests"), "sdkRequests"):
            if (
                request.get("requestId") == victim.get("requestId")
                and request.get("operation") == "late-work"
                and request.get("outcome") == "DISPATCHED"
            ):
                self.fail("late child operation dispatched a checkpoint")

    def nested(self) -> None:
        roles = ["victim"] + (
            ["peer"] if self.profile.get("nativeConcurrency") == 2 else []
        )
        requests = self.overlap(roles)
        budget = self.budget("progressMs")
        for request in requests:
            begin, end = (
                self.one("progress_begin", request),
                self.one("progress", request),
            )
            if (
                not self.ordered(begin, end)
                or end.get("atMs", self.end) - begin.get("atMs", 0) > budget
            ):
                self.fail(
                    "nested public operations starved beyond their progress budget"
                )
            marker = request.get("marker", "")
            if end.get("value") != [[marker + ":0", marker + ":1"], marker]:
                self.fail("nested results crossed invocation boundaries")
            for name in ("outer-parallel", "inner-map"):
                operation = self.operation(request, name, "SUCCEEDED")
                configuration = self.one(
                    "operation_config",
                    request,
                    operationId=operation.get("id"),
                    source="public-fixture",
                )
                if (
                    type(configuration.get("maxConcurrency")) is not int
                    or configuration.get("maxConcurrency") != 1
                ):
                    self.fail(
                        "nested fixture did not exercise configured single-lane branch pools",
                        "INCOMPLETE",
                    )
            self.operation(request, "progress-child", "SUCCEEDED")

    def warm(self) -> None:
        sequence = self.records(
            self.config.get("warmSequence"), "regression.warmSequence"
        )
        if [item.get("fixture") for item in sequence] != [
            "success",
            "failure",
            "replay",
        ] * 3:
            self.fail(
                "warm lifecycle requires the nine success/failure/replay fixture executions",
                "INCOMPLETE",
            )
        arns = [item.get("executionArn") for item in sequence]
        if len(set(arns)) != len(arns):
            self.fail(
                "warm sequence substituted an execution for a later fixture",
                "INCOMPLETE",
            )
        previous_end = -1.0
        samples: dict[tuple[Any, Any], list[Mapping[str, Any]]] = {}
        for item in sequence:
            requests = [
                request
                for request in self.attempts.values()
                if request.get("executionArn") == item.get("executionArn")
            ]
            if not requests:
                self.fail("warm fixture has no retained requests", "INCOMPLETE")
                continue
            if (
                any(
                    request.get("fixture") != item.get("fixture")
                    for request in requests
                )
                or min(request.get("startedAtMs", 0) for request in requests)
                <= previous_end
            ):
                self.fail(
                    "warm fixture routing changed or executions were not sequential"
                )
            previous_end = max(request.get("endedAtMs", 0) for request in requests)
            history = self.history(requests[0])
            expected = "FAILED" if item.get("fixture") == "failure" else "SUCCEEDED"
            if history.get("status") != expected:
                self.fail("warm fixture execution has the wrong terminal result")
            if item.get("fixture") == "replay":
                self.replay(item["executionArn"])
            for request in requests:
                for end in self.select(ENDINGS, request):
                    samples.setdefault(
                        (request.get("environmentId"), request.get("processId")), []
                    ).append(end)
        reused = [
            sorted(values, key=lambda item: item.get("atMs", 0))
            for values in samples.values()
            if len(values) >= 3
        ]
        if not reused:
            self.fail(
                "warm resources lack at least three observed uses of one original process",
                "PLACEMENT",
            )
        growth = self.mapping(self.config.get("budgets"), "regression.budgets")
        allowances = {
            counter: self.number(growth.get(key), f"regression.budgets.{key}")
            for counter, key in (
                ("threads", "maxThreadGrowth"),
                ("fds", "maxFdGrowth"),
                ("rssBytes", "maxRssGrowthBytes"),
            )
        }
        for values in reused:
            baseline = self.resources(values[1])
            for sample in values[2:]:
                current = self.resources(sample)
                for counter, allowance in allowances.items():
                    before = (
                        len(baseline.get("threads", []))
                        if counter == "threads"
                        else baseline.get(counter, 0)
                    )
                    after = (
                        len(current.get("threads", []))
                        if counter == "threads"
                        else current.get(counter, 0)
                    )
                    if after > before + allowance:
                        self.fail(
                            f"warm process {counter} grew beyond its declared post-cache allowance"
                        )


def _run(
    evidence: Mapping[str, Any], check: str, execution_arn: str | None = None
) -> list[str]:
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: lifecycle evidence must be an object"]
    observations: _Observations | None = None
    try:
        observations = _Observations(evidence)
        if check == "events":
            observations.lifecycle()
        elif check == "replay":
            observations.replay(execution_arn or "")
        else:
            observations.lifecycle()
            scenario = evidence.get("scenario")
            if evidence.get("coverageProfile") != "python-lifecycle":
                observations.fail(
                    "Python regression semantics require the explicit coverageProfile",
                    "INCOMPLETE",
                )
            elif scenario in OTEL_SCENARIOS:
                if (
                    evidence.get("requirementId")
                    != f"python-lmi-{OTEL_SCENARIOS[scenario]}"
                ):
                    observations.fail(
                        "OTel scenario does not match its lifecycle requirement",
                        "INCOMPLETE",
                    )
                # The OTel extension validates carriers/providers/exports. Keep
                # invocation-owned lifecycle checks for these six cases too.
            elif (
                scenario not in SCENARIOS
                or evidence.get("requirementId") != f"python-lmi-{SCENARIOS[scenario]}"
            ):
                observations.fail(
                    "scenario does not match a Python lifecycle requirement",
                    "INCOMPLETE",
                )
            elif not observations.errors:
                native = observations.profile.get("nativeConcurrency")
                if native not in (1, 2):
                    observations.fail(
                        "Python regression matrix requires native concurrency 1 or 2",
                        "INCOMPLETE",
                    )
                elif scenario == "native-concurrency":
                    observations.overlap(["victim"] + (["peer"] if native == 2 else []))
                elif scenario.startswith("early-completion-"):
                    observations.early()
                else:
                    checks = {
                        "deadline-recovery": observations.recovery,
                        "warm-lifecycle": observations.warm,
                        "checkpoint-settlement": observations.checkpoint,
                        "timeout-retry": observations.service_retry,
                        "late-operation": observations.late_operation,
                        "return-inflight": observations.inflight,
                        "failure-inflight": observations.inflight,
                        "pending-cleanup": observations.pending_cleanup,
                        "nested-progress": observations.nested,
                    }
                    if scenario == "replay-callback-retry":
                        observations.replay(
                            observations.role("victim").get("executionArn", "")
                        )
                    else:
                        checks[scenario]()
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        error = f"INCOMPLETE: malformed lifecycle regression evidence: {exc}"
        return [*(observations.errors if observations else []), error]
    return observations.errors


def validate_lifecycle_events(evidence: Mapping[str, Any]) -> list[str]:
    """Check settled wrapper/task/request/resource lifetimes, including OTel cases."""
    return _run(evidence, "events")


def validate_replay_evidence(
    evidence: Mapping[str, Any], execution_arn: str
) -> list[str]:
    """Check real wait/callback/child replay and configured retries for one execution."""
    return _run(evidence, "replay", execution_arn)


def validate_lifecycle_regressions(
    evidence: Mapping[str, Any], *, phase: str = "final"
) -> list[str]:
    """Validate one coverage cell; core common/retirement validation also applies.

    Both phases preserve behavioral observations. Final validation also inspects
    any subsequently recorded release, so emergency release never repairs an
    earlier failed assertion. The caller must retain observation-phase errors.
    """
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown lifecycle regression validation phase"]
    return _run(evidence, "scenario")
