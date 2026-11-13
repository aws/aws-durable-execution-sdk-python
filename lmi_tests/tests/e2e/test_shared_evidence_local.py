# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real SDK invocation/replay evidence reaches the shared validator unchanged."""

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
from lmi_tests.shared_evidence import build_evidence
from lmi_tests.tests.unit.test_shared_observations import install_ledger, shared_payload


def test_real_sdk_replay_and_service_history_normalize_for_shared_acceptance(
    monkeypatch, tmp_path
):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("replay")
    payload["sharedScenario"] = "suspension-retry"
    payload["behaviorBudgets"] = {"observationGraceMs": 1}
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
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
    cloud.execution_results = {
        arn: {"Status": result.status.value, "Result": result.result}
    }
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
    probes = [e for e in evidence["events"] if e["kind"] == "admission_probe"]
    assert any(e["probe"] == "checkpoint" for e in probes)
    errors = validate(evidence)
    if len(probes) == 2 and all(e["outcome"] == "REJECTED" for e in probes):
        assert errors == []
    else:
        # Test the harness, not an assumed SDK fix: actual acceptance or missing
        # completion must remain a failed shared result, never fabricated rejection.
        assert errors
        assert any(e["outcome"] != "REJECTED" for e in probes) or len(probes) < 2
