# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real SDK invocation/replay evidence reaches the shared validator unchanged."""

from copy import deepcopy
from inspect import signature
import json
import time

import pytest
from pathlib import Path

from aws_durable_execution_conformance_tests.lmi.app import validate
from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import fixture
from lmi_tests.cloud import Cloud
from lmi_tests.shared_evidence import (
    build_evidence,
    expected_outcome,
    shared_build_evidence,
)
from lmi_tests.tests.unit.test_shared_observations import install_ledger, shared_payload


def test_real_sdk_replay_and_service_history_normalize_for_shared_acceptance(
    monkeypatch, tmp_path
):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("replay")
    payload["sharedScenario"] = "suspension-retry"
    payload["behaviorBudgets"] = {"observationGraceMs": 1}
    # The shared evidence uses one observation clock, and callback retirement
    # is timed from the service history. The local runner's default virtual
    # clock skips this fixture's durable wait, which would date service events
    # ahead of the wall-clock retirement they are compared against.
    with DurableFunctionTestRunner(handler=fixture.handler, skip_time=False) as runner:
        arn = runner.run_async(input=json.dumps(payload), execution_timeout=30)
        callback = runner.wait_for_callback(arn, name="completion", timeout=20)
        runner.send_callback_success(callback, result=b"marker")
        result = runner.wait_for_result(arn, timeout=20)
        history = runner.get_execution_history(
            arn, include_execution_data=True
        ).to_dict()["Events"]
    evidence, cloud = assemble(
        ledger,
        result,
        history,
        payload,
        tmp_path,
        "lmi-lifecycle-5",
        "suspension-retry",
    )
    assert evidence["collectionErrors"] == []
    assert validate(evidence) == []
    bodies = [event for event in evidence["events"] if event["kind"] == "body"]
    assert len([event for event in bodies if event["operation"] == "success"]) == 1
    nested = {
        event["operationId"]
        for event in bodies
        if event["operation"] in {"item", "left", "right"}
    }
    assert len(nested) == 5
    counts = {
        item["operationId"]: (item["count"], item["expectedCount"])
        for item in evidence["replay"]["sideEffects"]
    }
    assert all(counts[operation] == (1, 1) for operation in nested)
    duplicated = deepcopy(evidence)
    repeated = next(
        item
        for item in duplicated["replay"]["sideEffects"]
        if item["operationId"] in nested
    )
    repeated["count"] += 1
    assert any("side-effect count" in error for error in validate(duplicated))
    # The installed builder must publish exactly the returned snapshot.
    saved = json.loads((cloud.artifacts_dir / "shared-final-evidence.json").read_text())
    assert saved == evidence

    if "replay_effect_policy" in signature(shared_build_evidence).parameters:
        from aws_durable_execution_conformance_tests.lmi.evidence import (
            validate_retirement_evidence,
        )

        item = next(
            event
            for event in bodies
            if event["operation"] == "item" and event["value"] == 1
        )
        stream = sorted(
            (
                raw
                for raw in cloud.events.values()
                if raw["request"] == item["requestId"]
            ),
            key=lambda raw: raw["sequence"],
        )
        raw_body = next(
            raw
            for raw in stream
            if raw.get("observation", {}).get("sourceSequence")
            == item["sourceSequence"]
        )
        seal = stream[-1]
        assert seal["phase"] == "SEALED"
        seal_key = next(key for key, raw in cloud.events.items() if raw is seal)
        sequence = seal["sequence"]
        capture = seal["throughCapture"] + 1
        # Inject one witnessed extra execution before the terminal seal,
        # preserving independent native completion and retirement records.
        cloud.events["extra-observed-item"] = {
            **raw_body,
            "sequence": sequence,
            "observation": {
                **raw_body["observation"],
                "operationId": "unexpected-repeat",
                "sourceSequence": sequence,
                "captureSequence": capture,
            },
        }
        raw_effect = next(
            raw
            for raw in stream
            if raw["phase"] == "BODY"
            and raw.get("operation") == "item"
            and raw.get("value") == 1
        )
        cloud.events["extra-raw-item"] = {**raw_effect, "sequence": sequence + 1}
        cloud.events[seal_key] = {
            **seal,
            "sequence": sequence + 2,
            "throughCapture": capture,
        }
        partial = build_evidence(cloud.case_context, cloud, phase="partial")
        assert {
            "operationId": "unexpected-repeat",
            "count": 1,
            "expectedCount": 0,
        } in partial["replay"]["sideEffects"]
        assert not partial["replay"].get("missingExpectedEffects")

        observed_duplicate = build_evidence(cloud.case_context, cloud, phase="final")
        assert any(
            "side-effect count" in error for error in validate(observed_duplicate)
        )
        assert validate_retirement_evidence(observed_duplicate) == []
        saved_duplicate = json.loads(
            (cloud.artifacts_dir / "shared-final-evidence.json").read_text()
        )
        assert saved_duplicate == observed_duplicate
        del cloud.events["extra-observed-item"]
        del cloud.events["extra-raw-item"]
        cloud.events[seal_key] = seal
    # Losing a real source object must not turn into an inferred complete stream.
    cloud.events.pop(next(iter(cloud.events)))
    incomplete = build_evidence(cloud.case_context, cloud, phase="observations")
    assert incomplete["collectionErrors"]
    assert incomplete["collection"]["watermarks"] == {}


def assemble(ledger, result, history, payload, tmp_path, requirement_id, scenario):
    raw = ledger.records()
    arn = next(e["execution"] for e in raw if e["phase"] == "WRAPPER_ENTER")
    profile = Profile(
        "c1", "local-function", 1, 1, "actual-local-sdk", "local-config", "reuse-worker"
    )
    context = ScenarioContext(
        "unit",
        Scenario(
            "case",
            requirement_id,
            "c1",
            scenario,
            roles=("victim", "resume")
            if scenario == "suspension-retry"
            else ("victim",),
            coverage_profile="standard",
            behavior_budgets={"observationGraceMs": 1},
        ),
        profile,
    )
    raw = ledger.records()
    entries = sorted(
        (event for event in raw if event["phase"] == "WRAPPER_ENTER"),
        key=lambda event: event["time"],
    )
    roles = {}
    for index, event in enumerate(entries):
        role = "resume" if index and scenario == "suspension-retry" else "victim"
        roles[event["request"]] = role
        context.record_attempt(event["request"], role=role, requestId=event["request"])
    cloud = object.__new__(Cloud)
    cloud.case_context = context
    cloud.manifest = {
        "commit": "tested-commit",
        "invocationTimeout": 30,
        "conformanceRevision": "local-pinned-conformance",
    }
    cloud.events = {str(index): event for index, event in enumerate(raw)}
    cloud.invocations = [
        {
            "arn": arn,
            "marker": "marker",
            "role": "victim",
            "payload": payload,
            "native_requests": list(roles),
            "trace_header": None,
        }
    ]
    cloud.request_roles = roles
    cloud.raw_histories = {arn: history}
    # These tests read history only after the real local service returns a
    # terminal execution result, the same boundary as Cloud.collect.
    cloud.terminal_histories = {arn}
    actual = {"Status": result.status.value}
    if result.result is not None:
        actual["Result"] = result.result
    if result.error is not None:
        actual["Error"] = result.error.to_dict()
    cloud.execution_results = {arn: actual}
    cloud.control_records = []
    cloud.source_uris = {"memory://actual-sdk-ledger"}
    time.sleep(0.002)
    cloud.observation_ended_at = time.time()
    cloud.retired_at = cloud.observation_ended_at
    cloud.artifacts_dir = Path(tmp_path)
    evidence = build_evidence(context, cloud, phase="final")
    return evidence, cloud


@pytest.mark.parametrize(
    "requirement_id,scenario,fixture_name,outcome",
    [
        ("lmi-lifecycle-2", "normal-return", "success", "SUCCEEDED"),
        ("lmi-lifecycle-7", "normal-failure", "failure", "FAILED"),
    ],
)
def test_actual_disposal_outcome_and_late_admission_findings_reach_shared_validation(
    monkeypatch, tmp_path, requirement_id, scenario, fixture_name, outcome
):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload(fixture_name)
    payload["sharedScenario"] = scenario
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        result = runner.run(input=json.dumps(payload), execution_timeout=15)
        arn = next(
            e["execution"] for e in ledger.records() if e["phase"] == "WRAPPER_ENTER"
        )
        history = runner.get_execution_history(
            arn, include_execution_data=True
        ).to_dict()["Events"]
    assert result.status.value == outcome
    evidence, _ = assemble(
        ledger, result, history, payload, tmp_path, requirement_id, scenario
    )
    victim = evidence["selectedRequests"]["victim"]
    original = next(
        item for item in evidence["attempts"] if item["requestId"] == victim
    )
    assert original["outcome"] == outcome
    if outcome == "FAILED":
        assert evidence["histories"][0]["error"] == {
            "type": "ValueError",
            "message": "expected:marker",
        }
    probes = [e for e in evidence["events"] if e["kind"] == "admission_probe"]
    checkpoint_probe = next(e for e in probes if e["probe"] == "checkpoint")
    errors = validate(evidence)
    if evidence["collectionErrors"]:
        assert errors
        assert not {event["kind"] for event in evidence["events"]} & {
            "observation_error",
            "control_error",
        }
        assert all(
            "complete observation window" in error or "Missing ledger seals" in error
            for error in evidence["collectionErrors"]
        ), errors
    elif checkpoint_probe["outcome"] != "REJECTED":
        assert any(
            "closed invocation admitted a checkpoint probe" in error for error in errors
        )
    else:
        assert not any(
            "closed invocation admitted a checkpoint probe" in error for error in errors
        )
        wrapper = next(
            event for event in evidence["events"] if event["kind"] == "wrapper_return"
        )
        late_dispatch = any(
            request["requestId"] == victim
            and request["atMs"] > wrapper["atMs"]
            and request["outcome"] == "DISPATCHED"
            for request in evidence["sdkRequests"]
        )
        if late_dispatch:
            assert any(
                "SDK work after" in error or "SDK request" in error for error in errors
            )
        else:
            operation = next(
                (probe for probe in probes if probe["probe"] == "operation"), None
            )
            assert errors == expected_disposal_diagnostics(operation)

    # Test late admission independently of any incomplete collection. The
    # published validator checks the collection envelope before the scenario.
    admitted = deepcopy(evidence)
    admitted["collectionErrors"] = []
    admitted["collection"]["watermarks"] = dict.fromkeys(
        ("lifecycle", "sdkRequests", "history"), admitted["observationEndedAtMs"]
    )
    checkpoint = next(
        event
        for event in admitted["events"]
        if event["kind"] == "admission_probe" and event["probe"] == "checkpoint"
    )
    safe = deepcopy(admitted)
    safe["events"] = [
        event for event in safe["events"] if event.get("probe") != "operation"
    ]
    wrapper_end = next(
        event["atMs"] for event in safe["events"] if event["kind"] == "wrapper_return"
    )
    safe["sdkRequests"] = [
        request
        for request in safe["sdkRequests"]
        if request.get("requestId") != victim or request["atMs"] < wrapper_end
    ]
    synthetic_checkpoint = next(
        event
        for event in safe["events"]
        if event["kind"] == "admission_probe" and event.get("probe") == "checkpoint"
    )
    synthetic_checkpoint["outcome"] = "REJECTED"
    operation = {
        **synthetic_checkpoint,
        "probe": "operation",
        "outcome": "ACCEPTED",
        "sequence": max(event["sequence"] for event in safe["events"]) + 1,
    }
    safe["events"].append(operation)
    assert validate(safe, phase="observations") == expected_disposal_diagnostics(
        operation
    )
    checkpoint["outcome"] = "ACCEPTED"
    assert any(
        "closed invocation admitted a checkpoint probe" in error
        for error in validate(admitted, phase="observations")
    )


def expected_disposal_diagnostics(operation):
    if "expected_replay_effects" in signature(shared_build_evidence).parameters:
        return []
    if operation is None:
        return ["INCOMPLETE: missing post-closure operation admission probe"]
    if operation["outcome"] == "REJECTED":
        return []
    return ["ASSERTION: closed invocation admitted a operation probe"]


@pytest.mark.parametrize("scenario", ["deadline-during-step", "peer-recovery"])
def test_deadline_fixtures_validate_physical_timeout_independent_of_logical_disposition(
    scenario,
):
    assert expected_outcome({"payload": {"scenario": "deadline"}}, scenario) == {
        "allowedStatuses": ["RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"]
    }
