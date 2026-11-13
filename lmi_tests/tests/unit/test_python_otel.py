# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Native-profile OTel cells and deliberate regressions in observed evidence."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest

from lmi_tests.python_otel import SCENARIOS, validate_lifecycle_telemetry
from aws_durable_execution_conformance_tests_otel.model import Span, span_to_dict


def build_evidence(
    scenario: str, native: int = 2, view: str = "invocation"
) -> dict[str, Any]:
    """Synthetic observations, never a claim of deployment or cloud acceptance."""
    if scenario in SCENARIOS[6:]:
        from lmi_tests.tests.unit.test_python_lifecycle import (
            build_evidence as lifecycle_evidence,
        )

        baseline = lifecycle_evidence(scenario, native)
        return add_telemetry(baseline, view)
    attempts: list[dict[str, Any]] = []
    sequence = []

    def add(
        role: str,
        start: int,
        end: int,
        *,
        fixture: str = "success",
        process: str = "process-1",
        arn: str | None = None,
        outcome: str = "SUCCEEDED",
        ordinal: int = 1,
    ) -> dict[str, Any]:
        execution = arn or f"arn:execution:{role}"
        attempt = {
            "requestId": f"request-{role}",
            "executionArn": execution,
            "role": role,
            "attempt": ordinal,
            "fixture": fixture,
            "marker": execution,
            "environmentId": "environment-1",
            "processId": process,
            "workerId": process,
            "startedAtMs": start,
            "endedAtMs": end,
            "outcome": outcome,
        }
        attempts.append(attempt)
        return attempt

    if scenario == "otel-warm-lifecycle":
        for index, fixture in enumerate(["success", "failure", "replay"] * 2):
            role = "victim" if index == 0 else "warm" if index == 5 else f"warm-{index}"
            start = 100 + index * 200
            original = add(
                role,
                start,
                start + (60 if fixture == "replay" else 100),
                fixture=fixture,
                outcome="FAILED"
                if fixture == "failure"
                else "PENDING"
                if fixture == "replay"
                else "SUCCEEDED",
            )
            sequence.append(
                {"executionArn": original["executionArn"], "fixture": fixture}
            )
            if fixture == "replay":
                add(
                    f"{role}-resume",
                    start + 80,
                    start + 140,
                    fixture=fixture,
                    arn=original["executionArn"],
                    ordinal=1,
                )
    else:
        add("victim", 100, 200)
        if scenario == "otel-concurrent" and native == 2:
            add("peer", 105, 230, process="process-2")
        if scenario == "export-error-recovery":
            add("warm", 300, 400)
    selected = {
        attempt["role"]: attempt["requestId"]
        for attempt in attempts
        if attempt["attempt"] == 1
    }
    end = max(attempt["endedAtMs"] for attempt in attempts) + 100
    events: list[dict[str, Any]] = []

    def emit(owner: dict[str, Any], kind: str, at: int, **details: Any) -> None:
        events.append(
            {
                "kind": kind,
                "atMs": at,
                "requestId": owner["requestId"],
                "ownerRequestId": owner["requestId"],
                "executionArn": owner["executionArn"],
                **details,
            }
        )

    sampled = scenario != "runtime-carrier-unsampled"
    carriers = []
    spans = []
    export_outcomes = []
    histories = []
    expected_histories = {}
    executions = list(dict.fromkeys(attempt["executionArn"] for attempt in attempts))
    for arn in executions:
        members = [attempt for attempt in attempts if attempt["executionArn"] == arn]
        terminal = members[-1]
        expectation: dict[str, Any] = {"status": terminal["outcome"]}
        if terminal["outcome"] == "FAILED":
            expectation["error"] = {"type": "ValueError", "message": f"expected:{arn}"}
        else:
            expectation["result"] = arn
        operations = []
        if members[0]["fixture"] == "replay":
            for name, op_type in (
                ("success", "STEP"),
                ("failure", "STEP"),
                ("retry", "STEP"),
                ("pause", "WAIT"),
                ("completion", "CALLBACK"),
                ("nested", "CONTEXT"),
            ):
                operation: dict[str, Any] = {
                    "id": name,
                    "name": name,
                    "type": op_type,
                    "parentId": None,
                    "status": "FAILED" if name == "failure" else "SUCCEEDED",
                }
                operation["error" if name == "failure" else "result"] = (
                    {"type": "ValueError", "message": f"expected:{arn}"}
                    if name == "failure"
                    else arn
                )
                operations.append(operation)
        expected_histories[arn] = expectation
        histories.append(
            {
                "executionArn": arn,
                "ownerCaseId": "case-1",
                "operations": operations,
                **expectation,
            }
        )
    faults = []
    for attempt in attempts:
        request_id, start, finish = (
            attempt["requestId"],
            attempt["startedAtMs"],
            attempt["endedAtMs"],
        )
        trace_id = f"{executions.index(attempt['executionArn']) + 1:032x}"
        parent_id = f"{attempts.index(attempt) + 100:016x}"
        carrier = f"Root=1-{trace_id[:8]}-{trace_id[8:]};Parent={parent_id};Sampled={int(sampled)}"
        carriers.append(
            {
                "requestId": request_id,
                "environmentId": attempt["environmentId"],
                "processId": attempt["processId"],
                "workerId": attempt["workerId"],
                "source": "process-environment",
                "header": carrier,
                "callerHeader": carrier,
                "expectedSampled": sampled,
            }
        )
        snapshot = {
            "threads": [{"id": "main", "name": "main"}],
            "fds": 3,
            "rssBytes": 1000,
        }
        emit(attempt, "wrapper_enter", start, resources=deepcopy(snapshot))
        emit(
            attempt,
            "otel_factory",
            start + 2,
            pluginInstanceId=f"plugin-{request_id}",
            pluginExecutionArn=attempt["executionArn"],
        )
        emit(
            attempt,
            "otel_enter",
            start + 1,
            providerId=f"provider-{attempt['processId']}",
            exporterId=f"exporter-{attempt['processId']}",
            contextBefore={"traceId": None, "spanId": None},
        )
        emit(attempt, "user_exit", finish - 30)
        if attempt["fixture"] == "replay":
            if attempt is next(
                item
                for item in attempts
                if item["executionArn"] == attempt["executionArn"]
            ):
                emit(
                    attempt,
                    "body",
                    start + 4,
                    operation="success",
                    operationId="success",
                    attempt=1,
                    value=attempt["marker"],
                )
                emit(
                    attempt,
                    "body",
                    start + 5,
                    operation="failure",
                    operationId="failure",
                    attempt=1,
                    value=attempt["marker"],
                )
                emit(
                    attempt,
                    "body",
                    start + 6,
                    operation="retry",
                    operationId="retry",
                    attempt=1,
                    value=attempt["marker"],
                )
            else:
                emit(
                    attempt,
                    "body",
                    start + 6,
                    operation="retry",
                    operationId="retry",
                    attempt=2,
                    value=attempt["marker"],
                )
            emit(
                attempt,
                "stored_failure",
                start + 7,
                error={
                    "type": "ValueError",
                    "message": f"expected:{attempt['marker']}",
                },
            )
        emit(
            attempt,
            "otel_exit",
            finish + 1,
            contextAfter={"traceId": None, "spanId": None},
            exporterShutdowns=0,
        )
        emit(
            attempt,
            "wrapper_raise" if attempt["outcome"] == "FAILED" else "wrapper_return",
            finish,
            outcome=attempt["outcome"],
            resources=deepcopy(snapshot),
        )
        failure = scenario == "export-error-recovery" and attempt["role"] == "victim"
        status = (
            "unsampled" if not sampled else "export-error" if failure else "delivered"
        )
        outcome: dict[str, Any] = {"requestId": request_id, "status": status}
        if failure:
            outcome.update(
                error="observed exporter failure", fixture="bounded-export-error"
            )
        export_outcomes.append(outcome)
        if sampled:
            members = [
                item
                for item in attempts
                if item["executionArn"] == attempt["executionArn"]
            ]
            names = ["Invocation"] + (["Workflow"] if attempt is members[-1] else [])
            for index, name in enumerate(names):
                span_id = f"{attempts.index(attempt) * 2 + index + 1:016x}"
                span_start = (
                    members[0]["startedAtMs"] + 2 if name == "Workflow" else start + 2
                )
                parent = (
                    next(
                        item["header"].split("Parent=")[1].split(";")[0]
                        for item in carriers
                        if item["requestId"] == members[0]["requestId"]
                    )
                    if name == "Workflow"
                    else parent_id
                )
                span = Span(
                    trace_id=trace_id,
                    span_id=span_id,
                    name=name,
                    start_time=datetime.fromtimestamp(span_start / 1000, UTC),
                    end_time=datetime.fromtimestamp((finish - 25) / 1000, UTC),
                    parent_span_id=parent,
                    status="ERROR" if attempt["outcome"] == "FAILED" else "OK",
                    attributes={
                        "durable.execution.arn": attempt["executionArn"],
                        "durable.invocation.first": attempt is members[0],
                    },
                )
                spans.append(
                    {
                        "requestId": request_id,
                        "sampled": True,
                        "span": span_to_dict(span),
                    }
                )
                emit(
                    attempt,
                    "otel_export_started",
                    finish - 24,
                    traceId=trace_id,
                    spanId=span_id,
                )
                emit(
                    attempt,
                    "otel_export",
                    finish - 9,
                    traceId=trace_id,
                    spanId=span_id,
                    outcome="failure"
                    if failure
                    else "held"
                    if scenario == "held-export"
                    else "success",
                )
                if scenario == "held-export" and index == 0:
                    emit(
                        attempt,
                        "export_held",
                        finish - 20,
                        gateId="export-gate",
                        traceId=trace_id,
                        spanId=span_id,
                    )
                    emit(
                        attempt,
                        "export_released",
                        finish - 10,
                        gateId="export-gate",
                        mode="normal",
                    )
                    faults.append(
                        {
                            "id": "export-gate",
                            "ownerCaseId": "case-1",
                            "releasedAtMs": finish - 10,
                        }
                    )
        if scenario == "otel-concurrent":
            emit(attempt, "blocked", 120, gateId=f"gate-{request_id}")
            emit(attempt, "control_released", finish - 10, gateId=f"gate-{request_id}")
            if attempt["role"] == "peer":
                emit(attempt, "alive", 205)
    for attempt in attempts:
        owned = sorted(
            (event for event in events if event["requestId"] == attempt["requestId"]),
            key=lambda event: event["atMs"],
        )
        for index, event in enumerate(owned, start=1):
            event["sequence"] = index
    resources = [
        {"id": attempt["requestId"], "kind": "invocation", "ownerCaseId": "case-1"}
        for attempt in attempts
    ]
    resources.extend(
        {"id": arn, "kind": "execution", "ownerCaseId": "case-1"} for arn in executions
    )
    return {
        "schemaVersion": 1,
        "coverageProfile": "python-lifecycle",
        "runtime": "python",
        "runId": "run-1",
        "caseId": "case-1",
        "cohortId": "cohort-1",
        "requirementId": "python-lmi-8"
        if scenario in {"export-error-recovery", "held-export"}
        else "python-lmi-7",
        "scenario": scenario,
        "otelView": view,
        "artifact": {
            "sdk": "sdk-commit",
            "conformance": "conformance-commit",
            "plugin": "plugin-commit",
        },
        "profile": {
            "functionArn": f"arn:function:c{native}",
            "configurationDigest": f"config-{native}",
            "nativeConcurrency": native,
            "recoveryPolicy": "reuse-worker",
            "timeoutMs": 100,
        },
        "observationEndedAtMs": end,
        "attempts": attempts,
        "selectedRequests": selected,
        "events": events,
        "sdkRequests": [],
        "histories": histories,
        "expectedHistories": expected_histories,
        "collection": {
            "watermarks": {"lifecycle": end, "sdkRequests": end, "history": end},
            "evidenceUris": ["s3://example/run-1/case-1/raw"],
        },
        "resourcesCreated": resources,
        "faultsCreated": faults,
        "regression": {
            "warmSequence": sequence,
            "budgets": {"observationGraceMs": 10, "exportMs": 30},
        },
        "retirement": {
            "retiredAtMs": end + 10,
            "resources": [
                {
                    **item,
                    "settledAtMs": end + 1,
                    "evidenceUri": "s3://example/run-1/case-1/retired",
                    "outcome": "QUIESCENT"
                    if item["kind"] == "invocation"
                    else "SUCCEEDED",
                }
                for item in resources
            ],
            "faults": [{**item, "mode": "normal"} for item in faults],
        },
        "telemetry": {
            "views": {
                view: {
                    "collection": {
                        "status": "complete",
                        "scope": "cohort",
                        "requestIds": [attempt["requestId"] for attempt in attempts],
                    },
                    "carriers": carriers,
                    "spans": spans,
                    "exportOutcomes": export_outcomes,
                }
            }
        },
    }


@pytest.mark.parametrize("scenario", SCENARIOS)
@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("native", [1, 2])
def test_all_48_otel_cells_require_real_observations(
    scenario: str, view: str, native: int
) -> None:
    assert validate_lifecycle_telemetry(build_evidence(scenario, native, view)) == []


@pytest.mark.parametrize(
    "scenario", ["runtime-carrier-sampled", "runtime-carrier-unsampled"]
)
def test_sampled_and_unsampled_are_distinct_mandatory_cases(scenario: str) -> None:
    value = build_evidence(scenario)
    carrier = value["telemetry"]["views"]["invocation"]["carriers"][0]
    carrier["expectedSampled"] = not carrier["expectedSampled"]
    assert any(
        "requires explicit Sampled" in error
        for error in validate_lifecycle_telemetry(value)
    )


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "malformed",
        "Root=1-11111111-111111111111111111111111;Parent=1111111111111111;Sampled=1;Sampled=0",
    ],
)
def test_injected_bad_carriers_do_not_satisfy_runtime_cases(header: Any) -> None:
    value = build_evidence("runtime-carrier-sampled")
    value["telemetry"]["views"]["invocation"]["carriers"][0]["header"] = header
    assert validate_lifecycle_telemetry(value)


def test_runtime_root_must_match_actual_caller_propagation() -> None:
    value = build_evidence("runtime-carrier-sampled")
    carrier = value["telemetry"]["views"]["invocation"]["carriers"][0]
    carrier["callerHeader"] = carrier["callerHeader"].replace(
        "000000000000000000000001", "000000000000000000000002"
    )
    assert any("caller Root" in error for error in validate_lifecycle_telemetry(value))


@pytest.mark.parametrize(
    "mutation", ["missing-exit", "context-leak", "exporter-shutdown", "shared-instance"]
)
def test_actual_plugin_lifetime_is_not_inferred_from_final_trace(mutation: str) -> None:
    value = build_evidence("export-error-recovery")
    exits = [event for event in value["events"] if event["kind"] == "otel_exit"]
    if mutation == "missing-exit":
        value["events"].remove(exits[0])
    elif mutation == "context-leak":
        exits[0]["contextAfter"] = {"traceId": "a" * 32, "spanId": "b" * 16}
    elif mutation == "exporter-shutdown":
        exits[0]["exporterShutdowns"] = 1
    else:
        factories = [
            event for event in value["events"] if event["kind"] == "otel_factory"
        ]
        factories[1]["pluginInstanceId"] = factories[0]["pluginInstanceId"]
    assert validate_lifecycle_telemetry(value)


def test_failure_fixture_cannot_use_all_successful_exports() -> None:
    value = build_evidence("export-error-recovery")
    value["telemetry"]["views"]["invocation"]["exportOutcomes"][0]["status"] = (
        "delivered"
    )
    assert any(
        "requires export outcome export-error" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_export_recovery_cannot_use_a_replacement_process() -> None:
    value = build_evidence("export-error-recovery")
    value["attempts"][1]["workerId"] = "replacement"
    assert any(
        "original worker/process" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_held_export_prevents_otel_and_wrapper_completion() -> None:
    value = build_evidence("held-export")
    next(event for event in value["events"] if event["kind"] == "export_released")[
        "atMs"
    ] = 200.5
    value["faultsCreated"][0]["releasedAtMs"] = 200.5
    assert any(
        "prevent SDK wrapper completion" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_emergency_release_cannot_establish_held_export_contract() -> None:
    value = build_evidence("held-export")
    next(event for event in value["events"] if event["kind"] == "export_released")[
        "mode"
    ] = "emergency"
    assert any(
        "normal measurement" in error for error in validate_lifecycle_telemetry(value)
    )


def test_ending_victim_must_leave_live_peer_observed() -> None:
    value = build_evidence("otel-concurrent", 2)
    value["events"] = [event for event in value["events"] if event["kind"] != "alive"]
    assert any(
        "no observed progress" in error for error in validate_lifecycle_telemetry(value)
    )


def test_weak_supplied_assertions_cannot_skip_workflow_status() -> None:
    value = build_evidence("runtime-carrier-sampled")
    view = value["telemetry"]["views"]["invocation"]
    view["assertions"] = {"arn:execution:victim": {"minimum_spans": 0}}
    next(record for record in view["spans"] if record["span"]["name"] == "Workflow")[
        "span"
    ]["status"] = "ERROR"
    assert any("status" in error for error in validate_lifecycle_telemetry(value))


def test_every_boundary_case_runs_plugin_validation() -> None:
    value = build_evidence("pending-cleanup")
    from lmi_tests.python_catalog import validate

    assert value["requirementId"] == "python-lmi-14"
    assert validate(value) == []
    value["events"] = [
        event for event in value["events"] if event["kind"] != "otel_exit"
    ]
    assert validate(value)


def add_telemetry(baseline: dict[str, Any], view: str) -> dict[str, Any]:
    """Attach real-shape plugin observations to a complete core positive control."""
    value = deepcopy(baseline)
    value["otelView"] = view
    value["artifact"]["plugin"] = "plugin-commit"
    value["regression"]["budgets"]["exportMs"] = 30
    attempts = value["attempts"]
    events = value["events"]
    arns = list(dict.fromkeys(attempt["executionArn"] for attempt in attempts))
    carriers, spans, exports = [], [], []

    def emit(owner: dict[str, Any], kind: str, when: float, **extra: Any) -> None:
        events.append(
            {
                "requestId": owner["requestId"],
                "ownerRequestId": owner["requestId"],
                "executionArn": owner["executionArn"],
                "kind": kind,
                "atMs": when,
                **extra,
            }
        )

    for index, attempt in enumerate(attempts):
        request_id, arn = attempt["requestId"], attempt["executionArn"]
        starts = [
            event
            for event in events
            if event["requestId"] == request_id and event["kind"] == "wrapper_enter"
        ]
        endings = [
            event
            for event in events
            if event["requestId"] == request_id
            and event["kind"] in {"wrapper_return", "wrapper_raise"}
        ]
        start = starts[0]["atMs"]
        if not endings:
            # Positive cooperative-timeout observation. The platform result and
            # later actual wrapper cleanup are separate retained boundaries.
            root_exit = next(
                event
                for event in events
                if event["requestId"] == request_id
                and event["kind"] == "task_exit"
                and event.get("taskId") == "root"
            )
            finish = root_exit["atMs"]
            emit(attempt, "user_exit", finish - 3)
            emit(
                attempt,
                "wrapper_raise",
                finish,
                outcome=attempt["outcome"],
                resources=deepcopy(starts[0]["resources"]),
            )
        else:
            finish = endings[0]["atMs"]
        trace_id, parent_id = f"{arns.index(arn) + 1:032x}", f"{index + 100:016x}"
        header = f"Root=1-{trace_id[:8]}-{trace_id[8:]};Parent={parent_id};Sampled=1"
        carriers.append(
            {
                "requestId": request_id,
                "environmentId": attempt["environmentId"],
                "workerId": attempt["workerId"],
                "processId": attempt["processId"],
                "source": "process-environment",
                "header": header,
                "expectedSampled": True,
            }
        )
        emit(
            attempt,
            "otel_factory",
            start + 0.2,
            pluginInstanceId=f"plugin-{request_id}",
            pluginExecutionArn=arn,
        )
        emit(
            attempt,
            "otel_enter",
            start + 0.1,
            providerId=f"provider-{attempt['processId']}",
            exporterId=f"exporter-{attempt['processId']}",
            contextBefore={"traceId": None, "spanId": None},
        )
        emit(
            attempt,
            "otel_exit",
            finish + 0.1,
            exporterShutdowns=0,
            contextAfter={"traceId": None, "spanId": None},
        )
        members = [member for member in attempts if member["executionArn"] == arn]
        for offset, name in enumerate(
            ["Invocation"]
            + (
                ["Workflow"]
                if attempt is members[-1]
                and attempt["outcome"] in {"SUCCEEDED", "FAILED"}
                else []
            )
        ):
            span_id = f"{2 * index + offset + 1:016x}"
            original = members[0]
            span_start = (
                original["startedAtMs"] + 0.2 if name == "Workflow" else start + 0.2
            )
            parent = (
                next(
                    carrier["header"].split("Parent=")[1].split(";")[0]
                    for carrier in carriers
                    if carrier["requestId"] == original["requestId"]
                )
                if name == "Workflow"
                else parent_id
            )
            span = Span(
                trace_id,
                span_id,
                name,
                datetime.fromtimestamp(span_start / 1000, UTC),
                datetime.fromtimestamp((finish - 0.9) / 1000, UTC),
                parent_span_id=parent,
                status="ERROR"
                if attempt["outcome"] in {"FAILED", "TIMED_OUT"}
                else "OK",
                attributes={
                    "durable.execution.arn": arn,
                    "durable.invocation.first": attempt is original,
                },
            )
            spans.append(
                {"requestId": request_id, "sampled": True, "span": span_to_dict(span)}
            )
            emit(
                attempt,
                "otel_export_started",
                finish - 0.8,
                traceId=trace_id,
                spanId=span_id,
            )
            emit(
                attempt,
                "otel_export",
                finish - 0.2,
                traceId=trace_id,
                spanId=span_id,
                outcome="success",
            )
        exports.append({"requestId": request_id, "status": "delivered"})
    for attempt in attempts:
        local = sorted(
            (event for event in events if event["requestId"] == attempt["requestId"]),
            key=lambda event: event["atMs"],
        )
        for index, event in enumerate(local, start=1):
            event["sequence"] = index
    value["telemetry"] = {
        "views": {
            view: {
                "collection": {
                    "status": "complete",
                    "scope": "cohort",
                    "requestIds": [attempt["requestId"] for attempt in attempts],
                },
                "carriers": carriers,
                "spans": spans,
                "exportOutcomes": exports,
            }
        }
    }
    return value


@pytest.mark.parametrize("scenario", SCENARIOS[6:])
@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("native", [1, 2])
def test_boundary_overlay_passes_both_actual_lifecycle_and_otel_validators(
    scenario: str, view: str, native: int
) -> None:
    from lmi_tests.python_lifecycle import validate_lifecycle_regressions

    value = build_evidence(scenario, native, view)
    assert validate_lifecycle_regressions(value) == []
    assert validate_lifecycle_telemetry(value) == []


@pytest.mark.parametrize("scenario", SCENARIOS[:6])
def test_otel_only_cells_preserve_actual_wrapper_lifecycle_checks(
    scenario: str,
) -> None:
    from lmi_tests.python_lifecycle import validate_lifecycle_events

    assert validate_lifecycle_events(build_evidence(scenario)) == []


def test_export_budget_cannot_be_chosen_by_the_export_observation() -> None:
    value = build_evidence("held-export")
    value["regression"]["budgets"]["exportMs"] = 5
    assert any(
        "immutable exportMs" in error for error in validate_lifecycle_telemetry(value)
    )


def test_export_start_is_required_even_when_final_trace_is_complete() -> None:
    value = build_evidence("runtime-carrier-sampled")
    value["events"] = [
        event for event in value["events"] if event["kind"] != "otel_export_started"
    ]
    assert any(
        "export start" in error or "exporter start" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_factory_runs_inside_sdk_call_after_observation_entry() -> None:
    value = build_evidence("runtime-carrier-sampled")
    next(event for event in value["events"] if event["kind"] == "otel_factory")[
        "atMs"
    ] = 100
    assert any(
        "actual SDK wrapper boundary" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_post_call_diagnostic_does_not_hide_export_after_sdk_return() -> None:
    value = build_evidence("runtime-carrier-sampled")
    event = next(event for event in value["events"] if event["kind"] == "otel_export")
    event["atMs"] = 200.5  # Actual SDK return is 200; post-call OTEL_EXIT is 201.
    assert any(
        "escaped its OTel lifetime" in error
        for error in validate_lifecycle_telemetry(value)
    )


@pytest.mark.parametrize("missing", ["failure", "replay"])
def test_warm_coverage_cannot_replace_failure_or_replay_with_success(
    missing: str,
) -> None:
    value = build_evidence("otel-warm-lifecycle")
    next(
        item
        for item in value["regression"]["warmSequence"]
        if item["fixture"] == missing
    )["fixture"] = "success"
    assert any(
        "success/failure/replay" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_workflow_is_not_exported_from_an_earlier_pending_invocation() -> None:
    value = build_evidence("otel-warm-lifecycle")
    spans = value["telemetry"]["views"]["invocation"]["spans"]
    workflow = next(
        record
        for record in spans
        if record["span"]["name"] == "Workflow"
        and record["requestId"] == "request-warm-2-resume"
    )
    workflow["requestId"] = "request-warm-2"
    assert any(
        "terminal invocation" in error for error in validate_lifecycle_telemetry(value)
    )


def test_same_logical_span_reexport_does_not_claim_exactly_once_transport() -> None:
    value = build_evidence("runtime-carrier-sampled")
    spans = value["telemetry"]["views"]["invocation"]["spans"]
    spans.append(deepcopy(spans[0]))
    assert validate_lifecycle_telemetry(value) == []


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_replayed_workflow_uses_its_original_service_execution_start(view: str) -> None:
    value = build_evidence("otel-warm-lifecycle", view=view)
    workflow = next(
        record
        for record in value["telemetry"]["views"][view]["spans"]
        if record["span"]["name"] == "Workflow"
        and record["requestId"] == "request-warm-2-resume"
    )
    original = next(
        attempt
        for attempt in value["attempts"]
        if attempt["requestId"] == "request-warm-2"
    )
    service_start = original["startedAtMs"] - 25
    next(
        history
        for history in value["histories"]
        if history["executionArn"] == original["executionArn"]
    )["startedAtMs"] = service_start
    workflow["span"]["start_time"] = datetime.fromtimestamp(
        service_start / 1000, UTC
    ).isoformat()
    assert validate_lifecycle_telemetry(value) == []
    workflow["span"]["start_time"] = datetime.fromtimestamp(
        (service_start - 1) / 1000, UTC
    ).isoformat()
    assert any(
        "outside its lifecycle observation window" in error
        for error in validate_lifecycle_telemetry(value)
    )


def resumed_operation_evidence(*, duplicate: bool = False) -> dict[str, Any]:
    """Minimal, sanitized versions of run 43331709913's execution-view spans.

    The wait finishes before resume but is exported on resume. The timeout case
    re-exports one logical step identity from two native requests, changing its
    start and invocation link. Neither case re-runs the completed step body.
    """
    value = build_evidence(
        "timeout-retry" if duplicate else "pending-cleanup", 2, "execution"
    )
    owner = value["selectedRequests"]["retry" if duplicate else "resume"]
    original = value["selectedRequests"]["victim"]
    attempt = next(item for item in value["attempts"] if item["requestId"] == owner)
    history = next(
        item
        for item in value["histories"]
        if item["executionArn"] == attempt["executionArn"]
    )
    operation = next(
        item
        for item in history["operations"]
        if item["name"] == ("success" if duplicate else "pause")
    )
    operation.update(
        startedAtMs=110 if duplicate else 250, endedAtMs=130 if duplicate else 550
    )
    view = value["telemetry"]["views"]["execution"]
    workflow = next(
        item
        for item in view["spans"]
        if item["requestId"] == owner and item["span"]["name"] == "Workflow"
    )

    def append(request_id: str, start: float, exported: float) -> None:
        record = deepcopy(workflow)
        record["requestId"] = request_id
        invocation = next(
            item["span"]
            for item in view["spans"]
            if item["requestId"] == request_id and item["span"]["name"] == "Invocation"
        )
        record["span"].update(
            name=operation["name"],
            span_id="f" * 16,
            parent_span_id=workflow["span"]["span_id"],
            start_time=datetime.fromtimestamp(start / 1000, UTC).isoformat(),
            end_time=datetime.fromtimestamp(
                operation["endedAtMs"] / 1000, UTC
            ).isoformat(),
            attributes={
                "durable.execution.arn": attempt["executionArn"],
                "durable.operation.id": operation["id"],
                "durable.operation.name": operation["name"],
                "durable.operation.type": operation["type"],
                "durable.operation.status": "SUCCEEDED",
            },
            links=[
                {"trace_id": invocation["trace_id"], "span_id": invocation["span_id"]}
            ],
        )
        if duplicate:
            record["span"]["attributes"]["durable.attempt.number"] = (
                1  # A logical STEP may carry this attribute.
            )
        view["spans"].append(record)
        for kind, when in (
            ("otel_export_started", exported),
            ("otel_export", exported + 1),
        ):
            value["events"].append(
                {
                    "requestId": request_id,
                    "ownerRequestId": request_id,
                    "executionArn": attempt["executionArn"],
                    "kind": kind,
                    "atMs": when,
                    "traceId": invocation["trace_id"],
                    "spanId": "f" * 16,
                    "outcome": "success",
                }
            )

    if duplicate:
        append(original, 105, 350)
    append(owner, operation["startedAtMs"], 800)
    for request in value["attempts"]:
        events = sorted(
            (
                item
                for item in value["events"]
                if item["requestId"] == request["requestId"]
            ),
            key=lambda item: item["atMs"],
        )
        for sequence, event in enumerate(events, 1):
            event["sequence"] = sequence
    return value


def test_resumed_logical_wait_uses_its_actual_service_operation_interval() -> None:
    assert validate_lifecycle_telemetry(resumed_operation_evidence()) == []


@pytest.mark.parametrize(
    "change",
    [
        "missing-start",
        "missing-end",
        "wrong-id",
        "wrong-type",
        "wrong-name",
        "wrong-status",
        "open",
        "reversed",
        "nonfinite",
        "duplicate",
    ],
)
def test_early_logical_span_requires_matching_complete_operation_history(
    change: str,
) -> None:
    value = resumed_operation_evidence()
    history = next(
        item
        for item in value["histories"]
        if any(op["name"] == "pause" for op in item["operations"])
    )
    operation = next(item for item in history["operations"] if item["name"] == "pause")
    if change == "missing-start":
        del operation["startedAtMs"]
    elif change == "missing-end":
        del operation["endedAtMs"]
    elif change == "wrong-id":
        operation["id"] = "different-operation"
    elif change == "wrong-type":
        operation["type"] = "STEP"
    elif change == "wrong-name":
        operation["name"] = "other-wait"
    elif change == "wrong-status":
        operation["status"] = "FAILED"
    elif change == "open":
        operation["status"] = "STARTED"
        value["telemetry"]["views"]["execution"]["spans"][-1]["span"]["attributes"][
            "durable.operation.status"
        ] = "STARTED"
    elif change == "reversed":
        operation["endedAtMs"] = 200
    elif change == "nonfinite":
        operation["startedAtMs"] = float("nan")
    else:
        history["operations"].append(deepcopy(operation))
    assert validate_lifecycle_telemetry(value)


@pytest.mark.parametrize(
    "change",
    [
        "invocation-view",
        "attempt",
        "user-span",
        "too-early",
        "too-late",
        "wrong-execution",
    ],
)
def test_operation_history_does_not_relax_unrelated_span_windows(change: str) -> None:
    value = resumed_operation_evidence()
    view = value["telemetry"]["views"]["execution"]
    span = view["spans"][-1]["span"]
    if change == "invocation-view":
        value["otelView"] = "invocation"
        value["telemetry"]["views"] = {"invocation": view}
    elif change == "attempt":
        span["attributes"]["durable.attempt.outcome"] = "SUCCEEDED"
    elif change == "user-span":
        del span["attributes"]["durable.operation.id"]
    elif change == "too-early":
        span["start_time"] = datetime.fromtimestamp(0.249, UTC).isoformat()
    elif change == "too-late":
        span["end_time"] = datetime.fromtimestamp(0.551, UTC).isoformat()
    else:
        history = next(
            item
            for item in value["histories"]
            if any(op["name"] == "pause" for op in item["operations"])
        )
        history["executionArn"] = "arn:execution:unrelated"
    assert any(
        "observation window" in error for error in validate_lifecycle_telemetry(value)
    )


def test_inconsistent_cross_request_span_does_not_corrupt_either_export_ledger() -> (
    None
):
    value = resumed_operation_evidence(duplicate=True)
    before = deepcopy(value)
    errors = validate_lifecycle_telemetry(value)
    assert any("duplicate span identity" in error for error in errors)
    assert not any("omit or add canonical spans" in error for error in errors)
    assert not any("observation window" in error for error in errors)
    assert value == before


def test_per_request_ledger_still_rejects_an_actually_missing_canonical_span() -> None:
    value = resumed_operation_evidence(duplicate=True)
    view = value["telemetry"]["views"]["execution"]
    original = value["selectedRequests"]["victim"]
    view["spans"] = [
        item
        for item in view["spans"]
        if not (item["requestId"] == original and item["span"]["span_id"] == "f" * 16)
    ]
    assert any(
        "omit or add canonical spans" in error
        for error in validate_lifecycle_telemetry(value)
    )


def test_identical_same_request_reexport_may_have_another_transport_receipt() -> None:
    value = build_evidence("runtime-carrier-sampled", 1, "execution")
    spans = value["telemetry"]["views"]["execution"]["spans"]
    repeated = {**deepcopy(spans[0]), "exportId": "another-observed-export-receipt"}
    spans.append(repeated)
    assert validate_lifecycle_telemetry(value) == []


def test_historical_span_proof_does_not_hide_sdk_exporter_shutdown_failure() -> None:
    value = resumed_operation_evidence()
    next(event for event in value["events"] if event["kind"] == "otel_exit")[
        "exporterShutdowns"
    ] = 1
    assert any("shut down" in error for error in validate_lifecycle_telemetry(value))


@pytest.mark.parametrize("name_missing", [False, True])
def test_historical_unnamed_operation_uses_its_actual_id_without_inventing_a_name(
    name_missing: bool,
) -> None:
    value = resumed_operation_evidence()
    history = next(
        item
        for item in value["histories"]
        if any(op["name"] == "pause" for op in item["operations"])
    )
    operation = next(item for item in history["operations"] if item["name"] == "pause")
    operation.update(name=None, type="CONTEXT")
    span = value["telemetry"]["views"]["execution"]["spans"][-1]["span"]
    span["name"] = operation["id"]
    span["attributes"].pop("durable.operation.name")
    span["attributes"]["durable.operation.type"] = "CONTEXT"
    if name_missing:
        del operation["name"]
        assert validate_lifecycle_telemetry(value)
    else:
        assert validate_lifecycle_telemetry(value) == []
