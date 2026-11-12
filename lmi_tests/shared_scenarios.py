# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Drive existing public-API fixtures through coordinated service boundaries."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from copy import deepcopy
import hashlib
import time
from typing import Any
import uuid

from aws_durable_execution_conformance_tests.lmi.coordinator import ScenarioContext
from lmi_tests.cloud import Cloud
from lmi_tests.evidence import CollectionError, PlacementError, overlap, select


_SIMPLE = {
    "native-concurrency": "barrier",
    "replay-callback-retry": "replay",
    "warm-lifecycle": "success",
    "early-completion-parallel": "parallel",
    "early-completion-map": "map",
    "early-completion-nested": "nested",
    "checkpoint-settlement": "checkpoint",
    "deadline-recovery": "deadline",
    "timeout-retry": "deadline",
    "pending-cleanup": "suspend-cleanup",
    "nested-progress": "nested-progress",
    "return-inflight": "return-inflight",
    "failure-inflight": "failure-inflight",
    "late-operation": "late-operation",
    "runtime-carrier-sampled": "success",
    "runtime-carrier-unsampled": "success",
    "otel-warm-lifecycle": "success",
    "otel-concurrent": "barrier",
    "export-error-recovery": "success",
    "export-success": "success",
    "export-failure-recovery": "success",
    "held-export": "success",
}
_COHORT = {"native-concurrency", "nested-progress", "otel-concurrent"}
_ANCHORED = {"pending-cleanup", "return-inflight", "failure-inflight"}


def canonical(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Read actual observation records without replacing missing boundaries."""
    return [
        {
            "requestId": event.get("request"),
            "ownerRequestId": event.get("request"),
            "executionArn": event.get("execution"),
            **event["observation"],
        }
        for event in events
        if event.get("phase") == "CONFORMANCE"
        and isinstance(event.get("observation"), dict)
    ]


class ScenarioDriver:
    """Case-local orchestration; the shared package owns acceptance assertions."""

    def __init__(
        self,
        context: ScenarioContext,
        cloud: Cloud,
        *,
        release_capacity: Callable[[], None] | None = None,
        reserve_capacity: Callable[[], None] | None = None,
    ) -> None:
        self.context = context
        self.cloud = cloud
        self.items: dict[str, dict[str, Any]] = {}
        self.release_capacity = release_capacity
        self.reserve_capacity = reserve_capacity
        self.boundary: dict[str, Any] | None = None
        self.peer_gate: str | None = None
        self.cohort_gate: str | None = None
        self.cohort_gates: dict[str, str] = {}
        self.target_environment: str | None = None

    @property
    def scenario(self) -> str:
        return self.context.scenario.scenario

    def seconds(self, budget: str, fallback: float) -> float:
        value = self.context.scenario.behavior_budgets.get(budget)
        return value / 1000 if value is not None else fallback

    def gate(self, label: str) -> str:
        owner = hashlib.sha256(
            f"{self.context.run_id}:{self.context.scenario.case_id}:{self.context.scenario.cohort_id}".encode()
        ).hexdigest()[:20]
        return f"case-{owner}-{label}"

    def launch(
        self,
        role: str,
        fixture: str,
        *,
        gate: str | None = None,
        resume_roles: tuple[str, ...] = (),
        target_environment: str | None = None,
        exporter: str = "success",
    ) -> dict[str, Any]:
        if role in self.items:
            raise CollectionError("A later launch cannot replace a selected original")
        payload = self.context.dispatcher(role)
        sampled = "0" if self.scenario == "runtime-carrier-unsampled" else "1"
        # This header is sent on the real caller's HTTP Invoke request. It never
        # supplies the runtime carrier through function input.
        header = (
            f"Root=1-{int(time.time()):08x}-{uuid.uuid4().hex[:24]};"
            f"Parent={uuid.uuid4().hex[:16]};Sampled={sampled}"
            if self.context.scenario.otel_view
            else None
        )
        item = self.cloud.start(
            fixture,
            gate=gate,
            target_environment=target_environment,
            otel_view=self.context.scenario.otel_view,
            trace_header=header,
            otel_export=exporter,
            role=role,
            resume_roles=resume_roles,
            dispatcher_payload=payload,
            marker=self.gate(role) + "-" + uuid.uuid4().hex[:12],
        )
        self.items[role] = item
        return item

    def start(self) -> None:
        if self.scenario not in _SIMPLE:
            raise NotImplementedError(f"No Python fixture for {self.scenario}")
        if self.scenario in _COHORT:
            self.cohort_gate = self.gate("cohort")
            for role in ("victim", "peer")[: self.cloud.concurrency]:
                gate = (
                    self.gate(f"cohort-{role}")
                    if self.scenario == "otel-concurrent"
                    else self.cohort_gate
                )
                self.cohort_gates[role] = gate
                self.launch(role, _SIMPLE[self.scenario], gate=gate)
            return
        if self.scenario in _ANCHORED and self.cloud.concurrency == 2:
            self.peer_gate = self.gate("peer")
            peer = self.launch("peer", "barrier", gate=self.peer_gate)
            self.target_environment = self.cloud.phase(peer, "BLOCKED")[0][
                "environment"
            ]
        resume_roles = (
            ("resume",)
            if self.scenario in {"replay-callback-retry", "pending-cleanup"}
            else ("retry",)
            if self.scenario == "timeout-retry"
            else ()
        )
        exporter = (
            "held"
            if self.scenario == "held-export"
            else "failure"
            if self.scenario in {"export-error-recovery", "export-failure-recovery"}
            else "success"
        )
        self.launch(
            "victim",
            _SIMPLE[self.scenario],
            gate=self.gate("fault")
            if self.scenario in {"deadline-recovery", "timeout-retry"}
            else None,
            resume_roles=resume_roles,
            target_environment=self.target_environment,
            exporter=exporter,
        )

    def wait_overlap(
        self, roles: tuple[str, ...], environment: str | None = None
    ) -> list[dict[str, Any]]:
        markers = {self.items[role]["marker"] for role in roles}

        def established() -> list[dict[str, Any]] | None:
            events = [
                event for event in self.cloud.refresh() if event["marker"] in markers
            ]
            if environment is not None and any(
                event["environment"] != environment
                for event in select(events, "BLOCKED")
            ):
                raise PlacementError(
                    "Selected cohort reached a replacement environment"
                )
            try:
                overlap(events, markers, len(roles), environment)
            except PlacementError:
                return None
            return events

        return self.cloud.poll(
            established,
            seconds=self.cloud.manifest.get("driverTimeout", 120),
            category=PlacementError,
            message="Original cohort placement/overlap was not established",
        )

    def place(self) -> None:
        victim = self.items["victim"]
        if self.scenario in _COHORT:
            self.wait_overlap(tuple(self.items))
            return
        if self.scenario in _ANCHORED and self.cloud.concurrency == 2:
            admission = self.cloud.poll(
                lambda: [
                    event
                    for event in self.cloud.for_item(victim)
                    if event["phase"] in {"PLACEMENT_ACCEPTED", "PLACEMENT_MISS"}
                ],
                category=PlacementError,
                message="Victim placement admission was not observed",
            )[0]
            if admission["phase"] != "PLACEMENT_ACCEPTED":
                raise PlacementError("Original victim missed the selected environment")
        if self.scenario in {"deadline-recovery", "timeout-retry"}:
            self.boundary = self.cloud.phase(victim, "BLOCKED")[0]
            if self.scenario == "deadline-recovery" and self.cloud.concurrency == 2:
                self.peer_gate = self.gate("peer")
                self.launch("peer", "barrier", gate=self.peer_gate)
                self.wait_overlap(("victim", "peer"), self.boundary["environment"])
        else:
            self.cloud.phase(victim, "WRAPPER_ENTER")

    def _finish(self, item: dict[str, Any], status: str = "SUCCEEDED") -> None:
        self.cloud.finish(item, status)

    def _callback(self, item: dict[str, Any]) -> None:
        # Public callback waiting observes real service history. Waiting does not
        # create a replacement durable execution.
        self.cloud.poll(
            lambda: [
                event
                for event in self.cloud.for_item(item)
                if event["phase"] == "WRAPPER_RETURN"
                and event.get("status") == "PENDING"
            ],
            seconds=self.cloud.manifest.get("driverTimeout", 120),
            message="Callback fixture did not actually suspend",
        )
        if self.release_capacity is not None:
            self.release_capacity()
        try:
            callback = item["runner"].wait_for_callback(
                item["arn"],
                name="completion",
                timeout=self.cloud.manifest.get("driverTimeout", 120),
            )
        finally:
            if self.reserve_capacity is not None:
                self.reserve_capacity()
        self.cloud.check_active()
        # Retain every earlier retry/wait invocation before sending completion.
        self.cloud.for_item(item)
        item["runner"].send_callback_success(callback, result=item["marker"].encode())
        self.cloud.callback_records.append(
            {
                "executionArn": item["arn"],
                "callbackId": callback,
                "action": "success",
                "time": time.time(),
            }
        )

    def _peer_progress(self, boundary: Mapping[str, Any]) -> None:
        if "peer" not in self.items:
            return
        self.wait_overlap(("victim", "peer"), self.target_environment)
        self.cloud.poll(
            lambda: [
                event
                for event in self.cloud.for_item(self.items["peer"])
                if event["phase"] == "ALIVE" and event["time"] >= boundary["time"]
            ],
            message="Selected healthy peer did not progress during the held boundary",
        )

    def _finish_peer(self) -> None:
        if self.peer_gate is not None:
            self.cloud.release(self.peer_gate)
            self._finish(self.items["peer"])

    def _warm(self) -> None:
        count = 3 if self.scenario == "warm-lifecycle" else 2
        sequence = ["success", "failure", "replay"] * count
        for index, fixture in enumerate(sequence):
            if index == 0:
                item = self.items["victim"]
            else:
                if self.scenario == "warm-lifecycle":
                    role = "warm" if index == 1 else f"warm-{index}"
                else:
                    role = "warm" if index == 5 else f"warm-{index}"
                item = self.launch(
                    role,
                    fixture,
                    resume_roles=(f"{role}-resume",) if fixture == "replay" else (),
                )
            if fixture == "replay":
                self._callback(item)
            self._finish(item, "FAILED" if fixture == "failure" else "SUCCEEDED")

    def _deadline(self) -> None:
        victim = self.items["victim"]
        blocked = self.boundary
        if blocked is None:
            raise CollectionError("Deadline fixture lost its original placement record")
        if self.scenario == "deadline-recovery":
            self._finish_peer()
            self.cloud.poll(
                lambda: time.time() >= blocked["deadline"] - 1,
                seconds=self.cloud.manifest["invocationTimeout"] + 5,
            )
            gate = self.gate("recovery")
            roles = ("recovery", "recovery-peer")[: self.cloud.concurrency]
            for role in roles:
                self.launch(role, "barrier", gate=gate)
            # Demand the original environment before eventual service-history
            # delivery consumes the measurement window.
            self.wait_overlap(roles, blocked["environment"])
            self.cloud.release(gate)
            for role in roles:
                self._finish(self.items[role])
        self.cloud.platform_timeout(victim, blocked["request"])
        cutoff = blocked["deadline"] + self.seconds("cleanupMs", 5)
        self.cloud.poll(
            lambda: time.time() > cutoff + self.seconds("observationGraceMs", 1),
            seconds=self.cloud.manifest["invocationTimeout"] + 30,
        )
        if self.scenario == "timeout-retry":
            self.cloud.poll(
                lambda: (
                    len(
                        {
                            event["request"]
                            for event in self.cloud.for_item(victim)
                            if event["phase"] == "WRAPPER_ENTER"
                        }
                    )
                    >= 2
                ),
                seconds=self.seconds("retryMs", 40),
                message="No real service retry of the original execution was observed",
            )
        # Close the measurement before the normal release. The later execution
        # result cannot rewrite the original deadline, cohort or late-activity
        # evidence, and no outcome is invented for a still-running retry.
        self.cloud.history(victim)
        events = self.cloud.refresh(validate_controls=False)
        selected = {
            attempt["role"]: attempt["requestId"]
            for attempt in self.context.attempts
            if attempt["attempt"] == 1 and attempt.get("requestId")
        }
        if selected.get("victim") != blocked["request"] or not set(
            self.context.scenario.roles
        ) <= set(selected):
            raise CollectionError(
                "Deadline measurement could not bind every original selected request"
            )
        self.cloud.record_deadline_measurement(
            {
                "runId": self.context.run_id,
                "caseId": self.context.scenario.case_id,
                "cohortId": self.context.scenario.cohort_id,
                "scenario": self.scenario,
                "executionArn": victim["arn"],
                "selectedRequests": selected,
                "deadlineAtMs": blocked["deadline"] * 1000,
                "cleanupCutoffAtMs": cutoff * 1000,
                "observationCutoffAtMs": (
                    cutoff + self.seconds("observationGraceMs", 1)
                )
                * 1000,
                "observedAtMs": time.time() * 1000,
                "events": deepcopy(events),
                "histories": deepcopy(self.cloud.raw_histories),
                "sourceUris": sorted(self.cloud.source_uris),
            }
        )
        self.cloud.release(self.gate("fault"))
        self.cloud.wait_terminal(victim)

    def _held_export(self) -> None:
        victim = self.items["victim"]
        self.cloud.phase(victim, "OTEL_EXPORT_HELD")
        if self.context.scenario.export_mode == "async-bounded-flush":
            self.cloud.poll(
                lambda: [
                    event
                    for event in canonical(self.cloud.for_item(victim))
                    if event.get("kind") == "flush_wait_completed"
                    and event.get("outcome") == "timeout"
                ],
                message="Asynchronous held export did not reach the real bounded flush timeout",
            )
            self.cloud.phase(victim, "WRAPPER_RETURN")
        self.cloud.release(victim["marker"] + "-export")
        self._finish(victim)
        self.cloud.poll(
            lambda: [
                event
                for event in canonical(self.cloud.for_item(victim))
                if event.get("kind") == "export_completed"
            ],
            message="Held exporter did not actually finish after its case-owned release",
        )

    def execute(self) -> None:
        victim = self.items["victim"]
        if self.scenario in _COHORT:
            if self.cohort_gate is None:
                raise CollectionError("Missing cohort barrier")
            # Separate gates let telemetry observe one request ending while its
            # peer remains held. Ordinary cohorts share one case-owned barrier.
            if self.scenario != "otel-concurrent":
                self.cloud.release(self.cohort_gate)
            for role, item in self.items.items():
                if self.scenario == "otel-concurrent":
                    self.cloud.release(self.cohort_gates[role])
                if self.scenario == "nested-progress":
                    self.cloud.phase(
                        item, "PROGRESS", seconds=self.seconds("progressMs", 20)
                    )
                self._finish(item)
                if (
                    self.scenario == "otel-concurrent"
                    and role == "victim"
                    and "peer" in self.items
                ):
                    returned = self.cloud.phase(item, "WRAPPER_RETURN")[-1]
                    self.cloud.poll(
                        lambda: [
                            event
                            for event in self.cloud.for_item(self.items["peer"])
                            if event["phase"] == "ALIVE"
                            and event["time"] >= returned["time"]
                        ],
                        message="Ending the original OTel invocation did not preserve its held peer",
                    )
        elif self.scenario in {"warm-lifecycle", "otel-warm-lifecycle"}:
            self._warm()
        elif self.scenario == "replay-callback-retry":
            self._callback(victim)
            self._finish(victim)
        elif self.scenario.startswith("early-completion-"):
            winner = self.cloud.phase(victim, "WINNER_SELECTED")[0]
            cutoff = (
                winner["time"]
                + self.seconds("cleanupMs", 5)
                + self.seconds("observationGraceMs", 1)
            )
            self.cloud.poll(
                lambda: time.time() >= cutoff, seconds=self.seconds("cleanupMs", 5) + 30
            )
            # Do not release the losing branch to manufacture earlier cleanup.
            self.cloud.for_item(victim)
        elif self.scenario in {"deadline-recovery", "timeout-retry"}:
            self._deadline()
        elif self.scenario == "checkpoint-settlement":
            self.cloud.phase(victim, "ACK_HELD")
            self.cloud.phase(victim, "BLOCKED")
            self.cloud.release(victim["marker"] + "-checkpoint")
            self._finish(victim)
        elif self.scenario == "pending-cleanup":
            blocked = self.cloud.phase(victim, "BLOCKED")[0]
            self._peer_progress(blocked)
            self.cloud.release(victim["marker"] + "-cleanup")
            self._finish(victim)
            self._finish_peer()
        elif self.scenario in {"return-inflight", "failure-inflight"}:
            self.cloud.phase(victim, "WINNER_SELECTED")
            exit_boundary = self.cloud.phase(victim, "USER_EXIT")[0]
            self._peer_progress(exit_boundary)
            self.cloud.release(victim["marker"] + "-loser")
            self._finish(
                victim, "FAILED" if self.scenario == "failure-inflight" else "SUCCEEDED"
            )
            self._finish_peer()
        elif self.scenario == "late-operation":
            self.cloud.phase(victim, "WINNER_SELECTED")
            self.cloud.release(victim["marker"] + "-loser")
            self.cloud.poll(
                lambda: [
                    event
                    for event in self.cloud.for_item(victim)
                    if event["phase"] in {"LATE_REJECTED", "LATE_ACCEPTED"}
                ],
                message="Residual child never attempted the late public durable operation",
            )
            self._finish(victim)
        elif self.scenario == "held-export":
            self._held_export()
        else:
            self._finish(victim)
            if self.scenario in {"export-error-recovery", "export-failure-recovery"}:
                self._finish(self.launch("warm", "success"))
        self.cloud.observation_ended_at = time.time()

    def collect(self) -> None:
        if not self.scenario.startswith("early-completion-") and self.scenario not in {
            "deadline-recovery",
            "timeout-retry",
        }:
            self.cloud.poll(
                self.quiescent,
                seconds=self.cloud.manifest.get("driverTimeout", 120),
                message="Completed scenario evidence still has unsettled task/export publication",
            )
        self.cloud.wait(self.seconds("observationGraceMs", 1))
        self.cloud.collect(full_run=False)
        self.cloud.observation_ended_at = time.time()

    def quiescent(self) -> bool:
        events = self.cloud.refresh(validate_controls=False)
        observations = canonical(events)
        if any(item.get("kind") == "observation_error" for item in observations):
            return False
        if not self.cloud.invocations:
            return True
        if any(
            not item.get("arn") or not item["native_requests"]
            for item in self.cloud.invocations
        ):
            return False
        entered = {event["request"] for event in select(events, "WRAPPER_ENTER")}
        ended = {
            event["request"]
            for event in events
            if event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
        }
        if not entered or not entered <= ended:
            return False
        for start, end, identity in (
            ("task_start", "task_exit", "taskId"),
            ("io_start", "io_exit", "taskId"),
            ("checkpoint_call", "checkpoint_exit", "callId"),
            ("poll_call", "poll_exit", "callId"),
            ("export_started", "export_completed", "exportId"),
            ("flush_started", "flush_completed", "flushId"),
        ):
            starts = Counter(
                (item.get("requestId"), item.get(identity))
                for item in observations
                if item.get("kind") == start
            )
            ends = Counter(
                (item.get("requestId"), item.get(identity))
                for item in observations
                if item.get("kind") == end
            )
            if starts - ends:
                return False
        # The public root task hook is a positive witness that task instrumentation
        # ran; absent instrumentation must not look like an empty, settled pool.
        root_ends = {
            item.get("requestId")
            for item in observations
            if item.get("kind") == "task_exit" and item.get("taskId") == "root"
        }
        if not entered <= root_ends:
            return False
        if self.context.scenario.otel_view:
            published = {
                item.get("requestId")
                for item in observations
                if item.get("kind") == "export_quiescence"
                and item.get("pendingExportIds") == []
                and item.get("pendingFlushIds") == []
                and item.get("publicationCount") == 0
            }
            if not entered <= published:
                return False
        return True

    def retire(self) -> bool:
        # Recover native request IDs for responses lost during the cancelled
        # phase before deciding which owned service executions can be stopped.
        self.cloud.refresh(validate_controls=False)
        if any(not item.get("arn") for item in self.cloud.invocations):
            raise CollectionError(
                "A launched invocation has no proven execution identity"
            )
        self.cloud.settle_case()
        self.cloud.poll(
            self.quiescent,
            seconds=self.cloud.manifest.get("driverTimeout", 120),
            message="Case-owned task/request/export work is not quiescent",
        )
        self.cloud.collect(full_run=False)
        self.cloud.retired_at = time.time()
        return True
