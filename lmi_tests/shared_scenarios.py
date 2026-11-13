# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Drive the seven shared lifecycle scenarios through real SDK handlers."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
import hashlib
import time
from typing import Any
import uuid

from aws_durable_execution_conformance_tests.lmi.coordinator import ScenarioContext
from lmi_tests.cloud import Cloud
from lmi_tests.evidence import CollectionError, PlacementError, overlap, select

FIXTURES = {
    "native-overlap": "barrier",
    "normal-return": "success",
    "deadline-during-step": "deadline",
    "peer-recovery": "deadline",
    "suspension-retry": "replay",
    "warm-reuse": "warm-owner",
    "normal-failure": "failure",
}


def canonical(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    def __init__(
        self,
        context: ScenarioContext,
        cloud: Cloud,
        *,
        release_capacity: Callable[[], None] | None = None,
        reserve_capacity: Callable[[], None] | None = None,
    ) -> None:
        self.context, self.cloud = context, cloud
        self.items: dict[str, dict[str, Any]] = {}
        self.release_capacity, self.reserve_capacity = (
            release_capacity,
            reserve_capacity,
        )
        self.boundary: dict[str, Any] | None = None
        self.peer_boundary: dict[str, Any] | None = None

    @property
    def scenario(self) -> str:
        return self.context.scenario.scenario

    def seconds(self, budget: str, fallback: float) -> float:
        return (
            self.context.scenario.behavior_budgets.get(budget, fallback * 1000) / 1000
        )

    def gate(self, label: str) -> str:
        owner = hashlib.sha256(
            f"{self.context.run_id}:{self.context.scenario.case_id}".encode()
        ).hexdigest()[:20]
        return f"case-{owner}-{label}"

    def launch(
        self,
        role: str,
        fixture: str,
        *,
        gate: str | None = None,
        preparation_gate: str | None = None,
        resume_roles: tuple[str, ...] = (),
        target_environment: str | None = None,
    ) -> dict[str, Any]:
        if role in self.items:
            raise CollectionError("A later launch cannot replace a selected original")
        item = self.cloud.start(
            fixture,
            gate=gate,
            preparation_gate=preparation_gate,
            target_environment=target_environment,
            role=role,
            resume_roles=resume_roles,
            dispatcher_payload=self.context.dispatcher(role),
            marker=self.gate(role) + "-" + uuid.uuid4().hex[:12],
        )
        self.items[role] = item
        return item

    def start(self) -> None:
        if self.scenario not in FIXTURES:
            raise NotImplementedError(f"No Python fixture for {self.scenario}")
        if self.scenario == "native-overlap":
            for role in self.context.scenario.roles:
                self.launch(role, "barrier", gate=self.gate("cohort"))
            return
        self.launch(
            "victim",
            FIXTURES[self.scenario],
            gate=self.gate("fault")
            if self.scenario in {"deadline-during-step", "peer-recovery"}
            else None,
            preparation_gate=self.gate("prepare")
            if self.scenario == "peer-recovery"
            else None,
            resume_roles=("resume",) if self.scenario == "suspension-retry" else (),
        )

    def wait_overlap(
        self, roles: tuple[str, ...], environment: str | None = None
    ) -> list[dict[str, Any]]:
        markers = {self.items[role]["marker"] for role in roles}

        def established():
            observed = [
                event for event in self.cloud.refresh() if event["marker"] in markers
            ]
            originals = {}
            for role in roles:
                entries = [
                    event
                    for event in observed
                    if event["phase"] == "WRAPPER_ENTER"
                    and event["marker"] == self.items[role]["marker"]
                ]
                if not entries:
                    return None
                entry = min(
                    entries, key=lambda event: (event["time"], event["sequence"])
                )
                originals[role] = entry["request"]
                if time.time() >= entry["deadline"]:
                    raise PlacementError(
                        f"Original {role} invocation expired before overlap was established"
                    )
            events = [
                event for event in observed if event["request"] in originals.values()
            ]
            if any(
                event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
                for event in events
            ):
                raise PlacementError("An original cohort invocation already ended")
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
        if self.scenario == "native-overlap":
            self.wait_overlap(("victim", "peer"))
        elif self.scenario in {"deadline-during-step", "peer-recovery"}:
            self.boundary = self.cloud.phase(victim, "BLOCKED")[0]
            if self.scenario == "peer-recovery":
                self._prepare_recovery_peer()
        else:
            self.cloud.phase(victim, "WRAPPER_ENTER")

    def _prepare_recovery_peer(self) -> None:
        if self.boundary is None:
            raise CollectionError(
                "Recovery preparation has no original victim boundary"
            )
        deadline = self.boundary["deadline"]
        lead = self.cloud.manifest["invocationTimeout"] / 2
        self.cloud.poll(
            lambda: time.time() >= deadline - lead,
            seconds=self.cloud.manifest["invocationTimeout"],
            message="Original victim did not reach the peer-admission window",
        )
        if time.time() >= deadline:
            raise PlacementError("Original victim expired before peer admission")
        peer = self.launch(
            "peer",
            "barrier",
            gate=self.gate("peer"),
            target_environment=self.boundary["environment"],
        )
        self.peer_boundary = self.cloud.phase(peer, "BLOCKED")[0]
        required_window = self.seconds("recoveryMs", 15) + self.seconds(
            "observationGraceMs", 1
        )
        if self.peer_boundary["deadline"] <= deadline + required_window:
            raise PlacementError(
                "Original peer has insufficient time remaining for recovery"
            )
        self.wait_overlap(("victim", "peer"), self.boundary["environment"])
        # Establish the original healthy cohort before admitting the actual fault.
        self.cloud.release(self.gate("prepare"))
        request = self.boundary["request"]
        self.cloud.poll(
            lambda: [
                event
                for event in canonical(self.cloud.for_item(self.items["victim"]))
                if event.get("requestId") == request
                and event.get("kind") == "work_in_flight"
                and event.get("atMs", deadline * 1000) < deadline * 1000
            ],
            seconds=max(0.001, deadline - time.time()),
            message="Original victim never entered the fault before its deadline",
        )

    def _peer_still_active(self) -> None:
        if self.peer_boundary is None:
            raise CollectionError("Recovery lost the original peer boundary")
        if time.time() >= self.peer_boundary["deadline"]:
            raise PlacementError("Original healthy peer expired during recovery")
        if any(
            event["request"] == self.peer_boundary["request"]
            and event["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
            for event in self.cloud.for_item(self.items["peer"])
        ):
            raise PlacementError("Original healthy peer ended during recovery")

    def _callback(self, item: dict[str, Any]) -> None:
        self.cloud.poll(
            lambda: [
                e
                for e in self.cloud.for_item(item)
                if e["phase"] == "WRAPPER_RETURN" and e.get("status") == "PENDING"
            ],
            seconds=self.cloud.manifest.get("driverTimeout", 120),
            message="Callback fixture did not suspend",
        )
        if self.release_capacity:
            self.release_capacity()
        try:
            callback = item["runner"].wait_for_callback(
                item["arn"],
                name="completion",
                timeout=self.cloud.manifest.get("driverTimeout", 120),
            )
        finally:
            if self.reserve_capacity:
                self.reserve_capacity()
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

    def _deadline(self) -> None:
        if self.boundary is None:
            raise CollectionError("Deadline fixture lost its original placement record")
        victim = self.items["victim"]
        # The fixture cooperatively exits real held I/O after the runtime deadline;
        # no control release is allowed to establish the cleanup assertion.
        self.cloud.poll(
            lambda: time.time() > self.boundary["deadline"],
            seconds=self.cloud.manifest["invocationTimeout"] + 5,
        )
        if self.scenario == "peer-recovery":
            self._peer_still_active()
            recovery = self.launch(
                "recovery",
                "barrier",
                gate=self.gate("recovery"),
                target_environment=self.boundary["environment"],
            )
            self.wait_overlap(("peer", "recovery"), self.boundary["environment"])
            self.cloud.release(self.gate("recovery"))
            self.cloud.finish(recovery)
            self._peer_still_active()
            self.cloud.release(self.gate("peer"))
            self.cloud.finish(self.items["peer"])
        self.cloud.platform_timeout(victim, self.boundary["request"])
        self.cloud.poll(
            lambda: (
                time.time()
                > self.boundary["deadline"]
                + self.seconds("cleanupMs", 5)
                + self.seconds("observationGraceMs", 1)
            ),
            seconds=self.cloud.manifest["invocationTimeout"] + 30,
        )
        # RUNNING is a valid pre-cleanup execution state for this measurement.
        # Collection records it; retirement stops/reconciles the execution later.

    def execute(self) -> None:
        victim = self.items["victim"]
        if self.scenario == "native-overlap":
            self.cloud.release(self.gate("cohort"))
            for item in self.items.values():
                self.cloud.finish(item)
        elif self.scenario in {"deadline-during-step", "peer-recovery"}:
            self._deadline()
        elif self.scenario == "suspension-retry":
            self._callback(victim)
            self.cloud.finish(victim)
        elif self.scenario == "warm-reuse":
            self.cloud.finish(victim)
            self.cloud.finish(self.launch("warm", "warm-probe"))
        else:
            self.cloud.finish(
                victim, "FAILED" if self.scenario == "normal-failure" else "SUCCEEDED"
            )
        self.cloud.observation_ended_at = time.time()

    def collect(self) -> None:
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
            message="Case-owned task/request work is not quiescent",
        )
        self.cloud.collect(full_run=False)
        self.cloud.retired_at = time.time()
        return True
