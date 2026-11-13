# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Complete retained controls for every Python lifecycle coverage cell."""

from copy import deepcopy
from typing import Any

import pytest

from lmi_tests.python_lifecycle import (
    SCENARIOS,
    validate_lifecycle_events,
    validate_lifecycle_regressions,
    validate_replay_evidence,
)


def resources() -> dict[str, Any]:
    return {
        "threads": [{"id": "runtime", "name": "MainThread"}],
        "fds": 10,
        "rssBytes": 1_000_000,
    }


class _Builder:
    def __init__(self, scenario: str, native: int) -> None:
        self.data: dict[str, Any] = {
            "schemaVersion": 1,
            "coverageProfile": "python-lifecycle",
            "scenario": scenario,
            "runtime": "python",
            "runId": "run",
            "caseId": "case",
            "cohortId": "cohort-1",
            "requirementId": f"python-lmi-{SCENARIOS[scenario]}",
            "artifact": {
                "sdk": "sdk@sha256:tested",
                "conformance": "conformance@sha256:tested",
            },
            "profile": {
                "functionArn": f"arn:function:c{native}",
                "configurationDigest": "sha256:configuration",
                "nativeConcurrency": native,
                "timeoutMs": 500,
                "recoveryPolicy": "reuse-worker",
            },
            "attempts": [],
            "events": [],
            "selectedRequests": {},
            "histories": [],
            "expectedHistories": {},
            "sdkRequests": [],
            "faultsCreated": [],
            "resourcesCreated": [],
            "observationEndedAtMs": 20_000,
            "collection": {
                "watermarks": {
                    "lifecycle": 20_000,
                    "sdkRequests": 20_000,
                    "history": 20_000,
                },
                "evidenceUris": ["s3://evidence/run/case/raw.json"],
            },
            "retirement": {"retiredAtMs": 21_000, "resources": [], "faults": []},
            "regression": {"budgets": {"observationGraceMs": 5}},
        }
        self.requests: dict[str, dict[str, Any]] = {}
        self.histories: dict[str, dict[str, Any]] = {}

    def request(
        self,
        role: str,
        start: int = 100,
        end: int = 1_000,
        *,
        outcome: str = "SUCCEEDED",
        process: int = 1,
        execution: str | None = None,
        fixture: str = "success",
    ) -> dict[str, Any]:
        arn = execution or f"arn:execution:{role}"
        marker = f"marker:{arn}"
        request = {
            "requestId": f"request:{role}",
            "executionArn": arn,
            "environmentId": "environment-1",
            "workerId": f"worker-{process}",
            "processId": f"process-{process}",
            "pid": process + 100,
            "role": role,
            "attempt": 1,
            "startedAtMs": start,
            "endedAtMs": end,
            "outcome": outcome,
            "marker": marker,
            "fixture": fixture,
        }
        self.requests[role] = request
        self.data["attempts"].append(request)
        self.data["selectedRequests"][role] = request["requestId"]
        self.histories.setdefault(
            arn,
            {
                "executionArn": arn,
                "ownerCaseId": "case",
                "status": "SUCCEEDED",
                "result": marker,
                "operations": [],
            },
        )
        self.event(role, "wrapper_enter", start, resources=resources())
        if outcome != "TIMED_OUT":
            self.event(role, "user_exit", end - 2)
            self.event(
                role, "wrapper_return", end, outcome=outcome, resources=resources()
            )
        else:
            request["deadlineAtMs"] = start + 500
            self.event(
                role, "runtime_deadline", start, source="runtime", remainingTimeMs=500
            )
            self.event(role, "platform_timeout", start + 500, source="platform")
            self.event(role, "task_exit", start + 550, taskId="root")
        self.op(role, "success", "SUCCEEDED")
        if execution is None:
            self.body(role, "success", start + 5)
        if outcome == "FAILED":
            self.histories[arn].pop("result", None)
            self.histories[arn].update(
                status="FAILED",
                error={"type": "ValueError", "message": "expected:" + marker},
            )
        return request

    def event(self, role: str, kind: str, time: int, **extra: Any) -> dict[str, Any]:
        request = self.requests[role]
        item = {
            "kind": kind,
            "requestId": request["requestId"],
            "ownerRequestId": request["requestId"],
            "executionArn": request["executionArn"],
            "atMs": time,
            "sequence": 0,
            **extra,
        }
        self.data["events"].append(item)
        return item

    def op(
        self,
        role: str,
        name: str,
        status: str = "SUCCEEDED",
        kind: str = "STEP",
        **extra: Any,
    ) -> dict[str, Any]:
        history = self.histories[self.requests[role]["executionArn"]]
        existing = next(
            (item for item in history["operations"] if item["name"] == name), None
        )
        if existing is not None:
            return existing
        operation = {
            "id": f"{history['executionArn']}:{name}",
            "name": name,
            "type": kind,
            "parentId": None,
            "status": status,
            **extra,
        }
        history["operations"].append(operation)
        return operation

    def body(self, role: str, name: str, time: int, *, attempt: int = 1) -> None:
        operation = self.op(role, name, "FAILED" if name == "failure" else "SUCCEEDED")
        self.event(
            role,
            "body",
            time,
            operation=name,
            operationId=operation["id"],
            attempt=attempt,
            value=self.requests[role]["marker"],
        )

    def block(
        self,
        role: str,
        start: int,
        end: int,
        *,
        gate: str | None = None,
        release: int | None = None,
    ) -> str:
        gate = gate or f"gate:{role}"
        self.data["faultsCreated"].append({"id": gate, "ownerCaseId": "case"})
        self.data["retirement"]["faults"].append(
            {
                "id": gate,
                "ownerCaseId": "case",
                "releasedAtMs": release or 20_500,
                "mode": "normal",
            }
        )
        self.event(role, "io_start", start - 10, taskId=gate, gateId=gate)
        self.event(role, "blocked", start, gateId=gate)
        self.event(role, "effect", start + 20, gateId=gate)
        self.event(role, "alive", start + 40, gateId=gate)
        self.event(role, "io_exit", end, taskId=gate, gateId=gate)
        if release is not None:
            self.event(role, "fault_released", release, gateId=gate, mode="normal")
        return gate

    def replay(
        self,
        role: str,
        start: int,
        *,
        resume_role: str = "resume",
        fixture: str = "replay",
    ) -> None:
        original = self.request(
            role, start, start + 100, outcome="PENDING", fixture=fixture
        )
        self.request(
            resume_role,
            start + 150,
            start + 300,
            execution=original["executionArn"],
            fixture=fixture,
        )
        self.body(role, "failure", start + 10)
        self.body(role, "retry", start + 20, attempt=1)
        self.body(role, "retry", start + 30, attempt=2)
        error = {"type": "ValueError", "message": "expected:" + original["marker"]}
        self.event(role, "stored_failure", start + 15, error=error)
        self.event(resume_role, "stored_failure", start + 160, error=deepcopy(error))
        for name, kind in (
            ("pause", "WAIT"),
            ("completion", "CALLBACK"),
            ("nested", "CONTEXT"),
        ):
            self.op(role, name, kind=kind)

    def finish(self) -> dict[str, Any]:
        for request in self.data["attempts"]:
            local = sorted(
                [
                    item
                    for item in self.data["events"]
                    if item["requestId"] == request["requestId"]
                ],
                key=lambda item: item["atMs"],
            )
            for index, item in enumerate(local):
                item["sequence"] = index
            self.data["resourcesCreated"].append(
                {
                    "id": request["requestId"],
                    "kind": "invocation",
                    "ownerCaseId": "case",
                }
            )
        self.data["histories"] = list(self.histories.values())
        for arn, history in self.histories.items():
            self.data["expectedHistories"][arn] = {
                key: deepcopy(history[key])
                for key in ("status", "result", "error")
                if key in history
            }
            self.data["resourcesCreated"].append(
                {"id": arn, "kind": "execution", "ownerCaseId": "case"}
            )
        self.data["retirement"]["resources"] = [
            {
                **item,
                "settledAtMs": 20_900,
                "evidenceUri": "s3://evidence/run/case/retired.json",
                "outcome": "QUIESCENT" if item["kind"] == "invocation" else "SUCCEEDED",
            }
            for item in self.data["resourcesCreated"]
        ]
        self.data["sdkRequests"] = [
            {**item, "method": "CheckpointDurableExecution", "outcome": "DISPATCHED"}
            for item in self.data["events"]
            if item["kind"] == "checkpoint_call"
        ]
        return self.data


def build_evidence(scenario: str, native: int = 1) -> dict[str, Any]:
    """Synthetic positive controls, never claims of deployed SDK acceptance."""
    b = _Builder(scenario, native)
    budgets = b.data["regression"]["budgets"]
    if scenario == "replay-callback-retry":
        b.replay("victim", 100)
    elif scenario == "warm-lifecycle":
        budgets.update(
            maxThreadGrowth=1, maxFdGrowth=8, maxRssGrowthBytes=32 * 1024 * 1024
        )
        sequence = []
        for index, fixture in enumerate(["success", "failure", "replay"] * 3):
            role = "victim" if index == 0 else "warm" if index == 1 else f"warm-{index}"
            start = 100 + index * 400
            if fixture == "replay":
                b.replay(role, start, resume_role=f"{role}-resume")
            else:
                b.request(
                    role,
                    start,
                    start + 300,
                    outcome="FAILED" if fixture == "failure" else "SUCCEEDED",
                    fixture=fixture,
                )
            sequence.append(
                {"executionArn": b.requests[role]["executionArn"], "fixture": fixture}
            )
        b.data["regression"]["warmSequence"] = sequence
    elif scenario in ("deadline-recovery", "timeout-retry"):
        budgets.update(cleanupMs=100)
        victim = b.request("victim", end=600, outcome="TIMED_OUT")
        b.block("victim", 200, 640)
        if scenario == "timeout-retry":
            budgets["retryMs"] = 500
            b.request("retry", 750, 1_000, execution=victim["executionArn"])
            b.event(
                "retry",
                "service_retry",
                751,
                source="platform",
                previousRequestId=victim["requestId"],
            )
        else:
            budgets["recoveryMs"] = 100
            if native == 2:
                b.request("peer", 110, 500, process=2)
                b.block("peer", 210, 450, release=440)
            b.request("recovery", 650, 1_000)
            b.block("recovery", 670, 800, release=790)
            if native == 2:
                b.request("recovery-peer", 660, 1_010, process=2)
                b.block("recovery-peer", 680, 810, release=800)
    elif scenario == "pending-cleanup":
        original = b.request("victim", end=500, outcome="PENDING")
        b.request("resume", 600, 900, execution=original["executionArn"])
        gate = b.block("victim", 220, 400, release=390)
        b.data["regression"]["gateId"] = gate
        b.event("victim", "cleanup_enter", 180)
        b.event("victim", "cleanup_exit", 410)
        b.op("victim", "pause", kind="WAIT")
        if native == 2:
            b.request("peer", 110, 1_000, process=2)
            b.block("peer", 210, 800, release=790)
            b.event("peer", "peer_progress", 300)
    elif scenario == "checkpoint-settlement":
        b.request("victim")
        gate = b.block("victim", 180, 810, release=800)
        b.data["regression"]["gateId"] = gate
        b.event("victim", "checkpoint_call", 150, callId="checkpoint-1")
        operation = b.op("victim", "checkpoint-loser")
        pool = b.op("victim", "checkpoint-pool", kind="CONTEXT")
        b.event(
            "victim",
            "checkpoint_ack",
            160,
            callId="checkpoint-1",
            source="service",
            operationIds=[operation["id"]],
        )
        b.event(
            "victim",
            "ack_held",
            170,
            callId="checkpoint-1",
            gateId=gate,
            operationId=operation["id"],
        )
        b.event("victim", "checkpoint_exit", 820, callId="checkpoint-1")
        b.event("victim", "branch_join", 830, scopeId=pool["id"])
    elif scenario.startswith("early-completion-") or scenario in (
        "return-inflight",
        "failure-inflight",
        "late-operation",
    ):
        early = scenario.startswith("early-completion-")
        b.request(
            "victim",
            end=400 if early else 1_000,
            outcome="FAILED" if scenario == "failure-inflight" else "SUCCEEDED",
        )
        gate = b.block(
            "victim", 200, 350 if early else 810, release=None if early else 800
        )
        b.data["regression"]["gateId"] = gate
        b.event("victim", "winner_ready", 250)
        b.event("victim", "winner_selected", 260)
        if early:
            budgets["cleanupMs"] = 200
            names = (
                ("race-map",)
                if scenario.endswith("map")
                else ("outer-map", "nested-child", "race-parallel")
                if scenario.endswith("nested")
                else ("race-parallel",)
            )
            for name in names:
                b.op("victim", name, kind="CONTEXT")
        elif scenario == "late-operation":
            scope = b.op("victim", "losing-child", kind="CONTEXT")["id"]
            b.event("victim", "scope_closed", 300, scopeId=scope)
            b.event("victim", "late_attempt", 830, scopeId=scope)
            b.event(
                "victim",
                "late_rejected",
                840,
                scopeId=scope,
                error={"type": "OrphanedChildException"},
            )
        else:
            next(item for item in b.data["events"] if item["kind"] == "user_exit")[
                "atMs"
            ] = 600
            if native == 2:
                b.request("peer", 110, 1_100, process=2)
                b.block("peer", 210, 900, release=890)
                b.event("peer", "peer_progress", 700)
    else:
        b.request("victim")
        b.block(
            "victim",
            200,
            310 if scenario == "nested-progress" else 800,
            release=300 if scenario == "nested-progress" else 790,
        )
        if native == 2:
            b.request("peer", 110, 1_010, process=2)
            b.block(
                "peer",
                210,
                320 if scenario == "nested-progress" else 810,
                release=310 if scenario == "nested-progress" else 800,
            )
        if scenario == "nested-progress":
            budgets["progressMs"] = 500
            for role, request in b.requests.items():
                b.event(role, "progress_begin", 400)
                b.event(
                    role,
                    "progress",
                    700,
                    value=[
                        [request["marker"] + ":0", request["marker"] + ":1"],
                        request["marker"],
                    ],
                )
                for index, name in enumerate(("outer-parallel", "inner-map")):
                    operation = b.op(role, name, kind="CONTEXT")
                    b.event(
                        role,
                        "operation_config",
                        410 + index,
                        operationId=operation["id"],
                        source="public-fixture",
                        maxConcurrency=1,
                    )
                b.op(role, "progress-child", kind="CONTEXT")
    return b.finish()


def find(value: dict[str, Any], kind: str, role: str = "victim") -> dict[str, Any]:
    return next(
        item
        for item in value["events"]
        if item["kind"] == kind and item["requestId"] == value["selectedRequests"][role]
    )


@pytest.mark.parametrize("native", [1, 2])
@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_all_28_lifecycle_cells_have_positive_controls(
    scenario: str, native: int
) -> None:
    assert validate_lifecycle_regressions(build_evidence(scenario, native)) == []


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_each_case_rejects_missing_wrapper_evidence(scenario: str) -> None:
    value = build_evidence(scenario)
    value["events"] = [
        item for item in value["events"] if item["kind"] != "wrapper_enter"
    ]
    assert validate_lifecycle_regressions(value)


@pytest.mark.parametrize(
    "scenario",
    ["early-completion-parallel", "early-completion-map", "early-completion-nested"],
)
def test_early_completion_cannot_be_supplied_by_release(scenario: str) -> None:
    value = build_evidence(scenario)
    value["retirement"]["faults"][0]["releasedAtMs"] = 300
    assert any(
        "driver release" in item for item in validate_lifecycle_regressions(value)
    )


def test_loser_must_be_active_before_winner() -> None:
    value = build_evidence("early-completion-parallel")
    value["events"] = [item for item in value["events"] if item["kind"] != "effect"]
    assert any(
        "actually held" in item for item in validate_lifecycle_regressions(value)
    )


@pytest.mark.parametrize(
    "kind", ["body", "effect", "checkpoint_ack", "checkpoint_exit", "poll_exit"]
)
def test_late_activity_and_inflight_settlement_are_separate_assertions(
    kind: str,
) -> None:
    value = build_evidence("return-inflight")
    event = deepcopy(find(value, "user_exit"))
    event.update(kind=kind, sequence=100, atMs=1_001)
    value["events"].append(event)
    assert any(
        "after wrapper exit" in item for item in validate_lifecycle_regressions(value)
    )


def test_held_checkpoint_requires_real_ack_and_ordered_exit() -> None:
    value = build_evidence("checkpoint-settlement")
    find(value, "checkpoint_ack")["source"] = "fixture"
    assert validate_lifecycle_regressions(value)
    value = build_evidence("checkpoint-settlement")
    find(value, "checkpoint_exit")["atMs"] = 799
    find(value, "checkpoint_exit")["sequence"] = find(value, "io_exit")["sequence"] - 1
    assert any(
        "held until" in item or "clock" in item
        for item in validate_lifecycle_regressions(value)
    )


def test_real_timeout_is_not_step_retry_or_driver_replacement() -> None:
    value = build_evidence("timeout-retry")
    find(value, "service_retry", "retry")["source"] = "driver"
    assert any(
        "driver-created" in item for item in validate_lifecycle_regressions(value)
    )
    value = build_evidence("timeout-retry")
    success = deepcopy(find(value, "body"))
    retry = next(item for item in value["attempts"] if item["role"] == "retry")
    success.update(
        requestId=retry["requestId"],
        ownerRequestId=retry["requestId"],
        atMs=800,
        sequence=2,
    )
    value["events"].append(success)
    assert validate_lifecycle_regressions(value)


def test_recovery_needs_original_environment_all_slots_and_bound() -> None:
    value = build_evidence("deadline-recovery", 2)
    next(item for item in value["attempts"] if item["role"] == "recovery-peer")[
        "environmentId"
    ] = "replacement"
    assert any(
        "unrelated environment" in item
        for item in validate_lifecycle_regressions(value)
    )
    value = build_evidence("deadline-recovery")
    value["regression"]["budgets"]["recoveryMs"] = 10
    assert any(
        "recover within" in item for item in validate_lifecycle_regressions(value)
    )


def test_pending_cannot_precede_finally() -> None:
    value = build_evidence("pending-cleanup")
    find(value, "cleanup_exit")["sequence"] = 100
    find(value, "cleanup_exit")["atMs"] = 501
    assert any("finally" in item for item in validate_lifecycle_regressions(value))


def test_nested_progress_is_not_satisfied_by_unrelated_peer() -> None:
    value = build_evidence("nested-progress", 2)
    find(value, "progress", "peer")["value"] = find(value, "progress")["value"]
    assert any(
        "crossed invocation" in item for item in validate_lifecycle_regressions(value)
    )
    value = build_evidence("nested-progress")
    find(value, "operation_config")["maxConcurrency"] = 2
    assert any("single-lane" in item for item in validate_lifecycle_regressions(value))


@pytest.mark.parametrize("scenario", ["return-inflight", "failure-inflight"])
def test_inflight_work_must_settle_before_actual_wrapper_exit(scenario: str) -> None:
    value = build_evidence(scenario)
    find(value, "io_exit").update(sequence=100, atMs=1_001)
    assert any("unsettled" in item for item in validate_lifecycle_regressions(value))


def test_failure_survives_scope_settlement() -> None:
    value = build_evidence("failure-inflight")
    value["histories"][0]["error"]["message"] = "cleanup replaced root failure"
    assert any(
        "original root failure" in item
        for item in validate_lifecycle_regressions(value)
    )


def test_abandoned_child_is_checked_before_invocation_closure() -> None:
    value = build_evidence("late-operation")
    find(value, "late_rejected")["error"] = {"type": "UnrelatedError"}
    assert any(
        "completed child scope" in item
        for item in validate_lifecycle_regressions(value)
    )
    value = build_evidence("late-operation")
    value["histories"][0]["operations"].append(
        {
            "id": "late",
            "name": "late-work",
            "type": "STEP",
            "parentId": "child",
            "status": "SUCCEEDED",
        }
    )
    assert any(
        "reached the durable service" in item
        for item in validate_lifecycle_regressions(value)
    )


@pytest.mark.parametrize("counter", ["fds", "rssBytes", "threads"])
def test_warm_resource_growth_uses_post_cache_baseline(counter: str) -> None:
    value = build_evidence("warm-lifecycle")
    last = sorted(
        [item for item in value["events"] if item["kind"] == "wrapper_return"],
        key=lambda item: item["atMs"],
    )[-1]
    if counter == "threads":
        last["resources"][counter] += [
            {"id": str(index), "name": "retained-cache-thread"} for index in range(10)
        ]
    else:
        last["resources"][counter] += 100_000_000
    assert any(
        "post-cache allowance" in item for item in validate_lifecycle_regressions(value)
    )


def test_replay_requires_real_callback_wait_context_and_stored_failure() -> None:
    value = build_evidence("replay-callback-retry")
    value["histories"][0]["operations"] = [
        item
        for item in value["histories"][0]["operations"]
        if item["type"] != "CALLBACK"
    ]
    assert any("CALLBACK" in item for item in validate_lifecycle_regressions(value))
    value = build_evidence("replay-callback-retry")
    find(value, "stored_failure", "resume")["error"]["type"] = "ChangedError"
    assert any("rethrow" in item for item in validate_lifecycle_regressions(value))


def test_common_lifecycle_and_per_execution_replay_are_reusable_for_otel() -> None:
    value = build_evidence("replay-callback-retry")
    assert validate_lifecycle_events(value) == []
    assert validate_replay_evidence(value, value["attempts"][0]["executionArn"]) == []


@pytest.mark.parametrize("invalid", [None, [], False, 1, float("nan"), "bad"])
def test_malformed_regression_data_returns_errors(invalid: Any) -> None:
    for key in ("regression", "attempts", "events", "profile", "histories"):
        value = build_evidence("native-concurrency")
        value[key] = invalid
        assert validate_lifecycle_regressions(value)


@pytest.mark.parametrize(
    "scenario,requirement",
    [
        ("runtime-carrier-sampled", 7),
        ("runtime-carrier-unsampled", 7),
        ("otel-warm-lifecycle", 7),
        ("otel-concurrent", 7),
        ("export-error-recovery", 8),
        ("held-export", 8),
    ],
)
def test_otel_only_cells_keep_common_lifetimes_for_extension_validation(
    scenario: str, requirement: int
) -> None:
    value = build_evidence("native-concurrency")
    value.update(scenario=scenario, requirementId=f"python-lmi-{requirement}")
    assert validate_lifecycle_regressions(value) == []


def test_unrelated_ack_and_missing_branch_join_cannot_prove_checkpoint_settlement() -> (
    None
):
    value = build_evidence("checkpoint-settlement")
    find(value, "checkpoint_ack")["operationIds"] = ["unrelated-operation"]
    assert any(
        "selected real checkpoint" in item
        for item in validate_lifecycle_regressions(value)
    )
    value = build_evidence("checkpoint-settlement")
    value["events"] = [
        item for item in value["events"] if item["kind"] != "branch_join"
    ]
    assert any("branch_join" in item for item in validate_lifecycle_regressions(value))


def test_static_body_names_may_repeat_in_distinct_service_contexts() -> None:
    value = build_evidence("native-concurrency")
    body = deepcopy(find(value, "body"))
    value["histories"][0]["operations"].append(
        {
            "id": "child-success",
            "name": "success",
            "parentId": "child",
            "status": "SUCCEEDED",
            "type": "STEP",
        }
    )
    body.update(operationId="child-success", sequence=50, atMs=900)
    value["events"].append(body)
    for sequence, event in enumerate(
        sorted(value["events"], key=lambda item: item["atMs"])
    ):
        event["sequence"] = sequence
    assert validate_lifecycle_events(value) == []


@pytest.mark.parametrize("policy", [None, "undefined", False])
def test_recovery_cannot_bypass_original_capacity_with_an_unknown_policy(
    policy: Any,
) -> None:
    value = build_evidence("deadline-recovery")
    if policy is None:
        value["profile"].pop("recoveryPolicy")
    else:
        value["profile"]["recoveryPolicy"] = policy
    recovery = next(item for item in value["attempts"] if item["role"] == "recovery")
    recovery.update(processId="replacement", workerId="replacement", pid=999)
    assert any(
        "supported recoveryPolicy" in item
        for item in validate_lifecycle_regressions(value)
    )


def test_boolean_retry_attempt_and_lane_count_are_not_integers() -> None:
    value = build_evidence("replay-callback-retry")
    next(
        item
        for item in value["events"]
        if item["kind"] == "body" and item["operation"] == "retry"
    )["attempt"] = True
    assert any(
        "two configured attempts" in item
        for item in validate_lifecycle_regressions(value)
    )
    value = build_evidence("nested-progress")
    find(value, "operation_config")["maxConcurrency"] = True
    assert any("single-lane" in item for item in validate_lifecycle_regressions(value))


def test_runtime_remaining_time_accounts_for_dispatch_time_before_wrapper_entry() -> (
    None
):
    value = build_evidence("timeout-retry")
    value["profile"]["timeoutMs"] = 1_000
    # The configured timer began 500ms before the observed SDK wrapper entry.
    assert value["attempts"][0]["deadlineAtMs"] == 600
    assert find(value, "runtime_deadline")["remainingTimeMs"] == 500
    assert validate_lifecycle_regressions(value) == []


@pytest.mark.parametrize("missing", ["deadlineAtMs", "runtime_deadline"])
def test_actual_runtime_deadline_evidence_is_mandatory(missing: str) -> None:
    value = build_evidence("deadline-recovery")
    if missing == "deadlineAtMs":
        value["attempts"][0].pop(missing)
    else:
        value["events"] = [item for item in value["events"] if item["kind"] != missing]
    assert validate_lifecycle_regressions(value)


@pytest.mark.parametrize("change", ["source", "deadline", "remaining", "boolean"])
def test_counterfeit_or_inconsistent_runtime_deadline_is_rejected(change: str) -> None:
    value = build_evidence("timeout-retry")
    observation = find(value, "runtime_deadline")
    if change == "source":
        observation["source"] = "input"
    elif change == "deadline":
        value["attempts"][0]["deadlineAtMs"] += 5
    elif change == "remaining":
        observation["remainingTimeMs"] = 501
    else:
        observation["remainingTimeMs"] = True
    assert validate_lifecycle_regressions(value)


def _resequence(value: dict[str, Any]) -> None:
    for request in value["attempts"]:
        events = sorted(
            (
                item
                for item in value["events"]
                if item["requestId"] == request["requestId"]
            ),
            key=lambda item: item["atMs"],
        )
        for sequence, item in enumerate(events):
            item["sequence"] = sequence


def _timeout_wrapper(
    outcome: str = "FAILED", kind: str = "wrapper_raise"
) -> dict[str, Any]:
    value = build_evidence("timeout-retry")
    owner = {
        key: value["events"][0][key]
        for key in ("requestId", "ownerRequestId", "executionArn")
    }
    value["events"].extend(
        [
            {**owner, "kind": "task_start", "atMs": 101, "taskId": "root"},
            {**owner, "kind": "user_exit", "atMs": 645},
            {
                **owner,
                "kind": kind,
                "atMs": 660,
                "outcome": outcome,
                "resources": resources(),
            },
        ]
    )
    _resequence(value)
    return value


@pytest.mark.parametrize(
    "outcome,kind",
    [
        ("FAILED", "wrapper_raise"),
        ("SUCCEEDED", "wrapper_return"),
        ("PENDING", "wrapper_return"),
    ],
)
def test_actual_sdk_wrapper_outcome_remains_independent_after_platform_timeout(
    outcome: str, kind: str
) -> None:
    value = _timeout_wrapper(outcome, kind)
    assert value["attempts"][0]["outcome"] == "TIMED_OUT"
    assert validate_lifecycle_regressions(value) == []
    assert find(value, kind)["outcome"] == outcome


def test_timeout_wrapper_exception_does_not_erase_task_settlement_checks() -> None:
    value = _timeout_wrapper()
    find(value, "io_exit")["atMs"] = 670
    _resequence(value)
    assert any("unsettled" in error for error in validate_lifecycle_regressions(value))


def test_timeout_wrapper_does_not_erase_platform_or_late_activity_evidence() -> None:
    value = _timeout_wrapper()
    value["events"] = [
        item for item in value["events"] if item["kind"] != "platform_timeout"
    ]
    assert any(
        "platform_timeout" in error for error in validate_lifecycle_regressions(value)
    )
    value = _timeout_wrapper()
    find(value, "body")["atMs"] = 680
    _resequence(value)
    assert any(
        "after wrapper exit" in error for error in validate_lifecycle_regressions(value)
    )


def test_wrapper_outcomes_remain_required_and_exact_without_platform_timeout() -> None:
    value = _timeout_wrapper()
    find(value, "wrapper_raise")["outcome"] = "invented"
    assert any(
        "unrecognized" in error for error in validate_lifecycle_regressions(value)
    )
    value = build_evidence("native-concurrency")
    find(value, "wrapper_return")["outcome"] = "FAILED"
    assert any("differs" in error for error in validate_lifecycle_regressions(value))


def test_public_operation_end_can_close_parent_before_winner_result_returns() -> None:
    value = build_evidence("late-operation")
    find(value, "scope_closed")["atMs"] = 255
    assert find(value, "winner_selected")["atMs"] == 260
    _resequence(value)
    assert validate_lifecycle_regressions(value) == []


@pytest.mark.parametrize("boundary", ["scope_closed", "winner_selected"])
def test_both_parent_closure_and_winner_selection_must_precede_late_attempt(
    boundary: str,
) -> None:
    value = build_evidence("late-operation")
    find(value, boundary)["atMs"] = 835
    _resequence(value)
    assert any(
        "completed child scope" in error
        for error in validate_lifecycle_regressions(value)
    )


def _add_handshake_gate(
    value: dict[str, Any], role: str = "victim", *, before: bool = False
) -> None:
    """Real fixtures observe losing I/O and a separate winner-admission gate."""
    blocked = find(value, "blocked", role)
    gate = blocked["gateId"] + "-handshake"
    start = blocked["atMs"] + (-10 if before else 10)
    for offset, kind in (
        (-1, "io_start"),
        (0, "blocked"),
        (1, "alive"),
        (20, "fault_released"),
        (21, "io_exit"),
    ):
        value["events"].append(
            {
                **blocked,
                "kind": kind,
                "atMs": start + offset,
                "gateId": gate,
                "taskId": gate,
                "mode": "normal",
            }
        )
    value["faultsCreated"].append({"id": gate, "ownerCaseId": value["caseId"]})
    value["retirement"]["faults"].append(
        {
            "id": gate,
            "ownerCaseId": value["caseId"],
            "releasedAtMs": start + 20,
            "mode": "normal",
        }
    )
    _resequence(value)


@pytest.mark.parametrize("scenario", ["return-inflight", "failure-inflight"])
@pytest.mark.parametrize("handshake_first", [False, True])
def test_peer_overlap_selects_declared_losing_work_gate(
    scenario: str, handshake_first: bool
) -> None:
    value = build_evidence(scenario, 2)
    _add_handshake_gate(value, before=handshake_first)
    assert validate_lifecycle_regressions(value) == []


@pytest.mark.parametrize(
    "damage", ["missing", "duplicate", "wrong-gate-liveness", "early-exit"]
)
def test_handshake_cannot_replace_losing_work_overlap(damage: str) -> None:
    value = build_evidence("return-inflight", 2)
    gate = value["regression"]["gateId"]
    _add_handshake_gate(value)
    if damage == "missing":
        value["events"] = [
            event
            for event in value["events"]
            if not (event["kind"] == "blocked" and event.get("gateId") == gate)
        ]
        expected = "exactly one blocked"
    elif damage == "duplicate":
        value["events"].append(deepcopy(find(value, "blocked")))
        expected = "exactly one blocked"
    elif damage == "wrong-gate-liveness":
        pivot = find(value, "blocked", "peer")["atMs"]
        for event in value["events"]:
            if event.get("gateId") == gate and event["kind"] in {"alive", "effect"}:
                event["atMs"] = pivot - 1
        expected = "native overlap lacks positive continued liveness"
    else:
        find(value, "io_exit")["atMs"] = find(value, "blocked", "peer")["atMs"] - 1
        expected = "native capacity stopped before overlap"
    _resequence(value)
    assert any(expected in error for error in validate_lifecycle_regressions(value))


@pytest.mark.parametrize(
    "scenario,role",
    [
        ("return-inflight", "peer"),
        ("native-concurrency", "victim"),
        ("nested-progress", "peer"),
        ("deadline-recovery", "recovery"),
    ],
)
def test_undeclared_cohort_gate_ambiguity_stays_incomplete(
    scenario: str, role: str
) -> None:
    value = build_evidence(scenario, 2)
    _add_handshake_gate(value, role)
    assert any(
        "exactly one blocked" in error
        for error in validate_lifecycle_regressions(value)
    )


def _measured_timeout() -> dict[str, Any]:
    value = _timeout_wrapper()
    find(value, "wrapper_raise")["atMs"] = 900
    find(value, "user_exit")["atMs"] = 895
    find(value, "task_exit")["atMs"] = 898
    owner = {
        key: value["events"][0][key]
        for key in ("requestId", "ownerRequestId", "executionArn")
    }
    for offset, kind in enumerate(
        ("checkpoint_call", "checkpoint_ack", "checkpoint_exit")
    ):
        value["events"].append(
            {
                **owner,
                "kind": kind,
                "atMs": 690 if offset == 0 else 800 + offset,
                "callId": "cleanup-checkpoint",
            }
        )
    fault = value["faultsCreated"][0]
    fault.update(
        releaseRequestedAtMs=750, releaseAcknowledgedAtMs=751, releasedAtMs=752
    )
    value["retirement"]["faults"][0]["releasedAtMs"] = 752
    uri = "s3://evidence/run/case/measurements/deadline.json"
    value["collection"]["evidenceUris"].append(uri)
    value["regression"]["deadlineMeasurement"] = {
        "selectedRequests": deepcopy(value["selectedRequests"]),
        "deadlineAtMs": 600,
        "cleanupCutoffAtMs": 700,
        "observationCutoffAtMs": 705,
        "observedAtMs": 720,
        "sourceEvidenceUri": uri,
    }
    _resequence(value)
    return value


def test_normal_postmeasurement_cleanup_is_not_deadline_measurement_activity() -> None:
    value = _measured_timeout()
    assert validate_lifecycle_regressions(value) == []
    assert any(
        event["kind"] == "checkpoint_exit" and event["atMs"] == 802
        for event in value["events"]
    )


def test_frozen_measurement_keeps_real_prerelease_late_effect_failures() -> None:
    value = _measured_timeout()
    effect = deepcopy(find(value, "effect"))
    effect["atMs"] = 710
    value["events"].append(effect)
    _resequence(value)
    assert any(
        "continued side effects" in error
        for error in validate_lifecycle_regressions(value)
    )


@pytest.mark.parametrize(
    "damage",
    [
        "owner",
        "deadline",
        "cleanup",
        "grace",
        "short-window",
        "after-release",
        "unknown-source",
        "missing-release",
        "malformed",
    ],
)
def test_deadline_measurement_cannot_shorten_or_substitute_the_original_window(
    damage: str,
) -> None:
    value = _measured_timeout()
    measurement = value["regression"]["deadlineMeasurement"]
    if damage == "owner":
        measurement["selectedRequests"]["victim"] = value["selectedRequests"]["retry"]
    elif damage == "deadline":
        measurement["deadlineAtMs"] = 650
    elif damage == "cleanup":
        measurement["cleanupCutoffAtMs"] = 750
    elif damage == "grace":
        measurement["observationCutoffAtMs"] = 700
    elif damage == "short-window":
        measurement["observedAtMs"] = 704
    elif damage == "after-release":
        measurement["observedAtMs"] = 760
    elif damage == "unknown-source":
        measurement["sourceEvidenceUri"] = "s3://uncollected/measurement"
    elif damage == "missing-release":
        value["faultsCreated"][0].pop("releaseRequestedAtMs")
    else:
        value["regression"]["deadlineMeasurement"] = []
    assert any(
        "deadline measurement" in error
        for error in validate_lifecycle_regressions(value)
    )


def test_legacy_timeout_evidence_keeps_full_observation_activity_checks() -> None:
    value = _measured_timeout()
    value["regression"].pop("deadlineMeasurement")
    assert any(
        "continued side effects" in error
        for error in validate_lifecycle_regressions(value)
    )


def test_deadline_measurement_does_not_hide_work_after_wrapper_exit() -> None:
    value = _measured_timeout()
    find(value, "checkpoint_exit")["atMs"] = 901
    _resequence(value)
    assert any(
        "after wrapper exit" in error or "unsettled" in error
        for error in validate_lifecycle_regressions(value)
    )


@pytest.mark.parametrize("when", [710, 730, 750])
def test_effects_until_controller_release_remain_deadline_failures(when: int) -> None:
    value = _measured_timeout()
    effect = deepcopy(find(value, "effect"))
    effect["atMs"] = when
    value["events"].append(effect)
    _resequence(value)
    assert any(
        "continued side effects" in error
        for error in validate_lifecycle_regressions(value)
    )


@pytest.mark.parametrize("field", ["receipt", "acknowledgement"])
def test_deadline_measurement_rejects_release_evidence_before_request(
    field: str,
) -> None:
    value = _measured_timeout()
    if field == "receipt":
        value["faultsCreated"][0]["releasedAtMs"] = 710
        value["retirement"]["faults"][0]["releasedAtMs"] = 710
    else:
        value["faultsCreated"][0]["releaseAcknowledgedAtMs"] = 710
    assert any(
        "deadline measurement" in error
        for error in validate_lifecycle_regressions(value)
    )


@pytest.mark.parametrize(
    "kind", ["body", "effect", "checkpoint_call", "poll_call", "unpaired-response"]
)
def test_postrelease_cleanup_cannot_admit_new_user_work(kind: str) -> None:
    value = _measured_timeout()
    if kind in {"body", "effect"}:
        body = deepcopy(find(value, kind))
        body["atMs"] = 810
        value["events"].append(body)
    elif kind == "unpaired-response":
        value["events"] = [
            event for event in value["events"] if event["kind"] != "checkpoint_call"
        ]
    else:
        call = find(value, "checkpoint_call")
        call["atMs"] = 800
        if kind == "poll_call":
            call["kind"] = kind
            value["events"].remove(find(value, "checkpoint_ack"))
            find(value, "checkpoint_exit")["kind"] = "poll_exit"
    _resequence(value)
    assert any(
        "continued side effects" in error
        for error in validate_lifecycle_regressions(value)
    )
