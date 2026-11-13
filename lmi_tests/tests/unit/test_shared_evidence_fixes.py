# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Service lifetimes and frozen measurements retain independently observed facts."""

from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from lmi_tests.shared_evidence import CaseSources, _deadline_measurement, build_evidence


def sources(raw):
    context = SimpleNamespace(
        scenario=SimpleNamespace(case_id="case", scenario="native-concurrency")
    )
    cloud = SimpleNamespace(
        invocations=[
            {"arn": "execution", "marker": "marker", "payload": {"scenario": "success"}}
        ],
        raw_histories={"execution": raw},
        execution_results={"execution": {"Status": "SUCCEEDED", "Result": '"marker"'}},
    )
    return CaseSources(context, cloud, ())


@pytest.mark.parametrize(
    "family", ["Step", "Wait", "Callback", "Context", "ChainedInvoke"]
)
def test_operation_lifetime_comes_from_its_service_events(family):
    raw = [
        {
            "EventType": family + "Started",
            "Id": "op",
            "Name": "controlled",
            "EventTimestamp": 1.25,
        },
        {"EventType": family + "Succeeded", "Id": "op", "EventTimestamp": 2.75},
    ]
    history, _, errors = sources(raw).histories([])
    assert errors == []
    assert history[0]["operations"][0]["startedAtMs"] == 1250
    assert history[0]["operations"][0]["endedAtMs"] == 2750


def test_retry_events_cannot_move_the_original_operation_start():
    raw = [
        {
            "EventType": "StepStarted",
            "Id": "op",
            "Name": "controlled",
            "EventTimestamp": 1,
        },
        {"EventType": "StepRetryStarted", "Id": "op", "EventTimestamp": 2},
        {"EventType": "StepFailed", "Id": "op", "EventTimestamp": 3},
        {"EventType": "StepStarted", "Id": "op", "EventTimestamp": 4},
        {"EventType": "StepSucceeded", "Id": "op", "EventTimestamp": 5},
    ]
    history, _, _ = sources(raw).histories([])
    operation = history[0]["operations"][0]
    assert (operation["startedAtMs"], operation["endedAtMs"], operation["status"]) == (
        1000,
        5000,
        "SUCCEEDED",
    )


def test_missing_original_service_timestamp_stays_missing():
    raw = [
        {"EventType": "StepStarted", "Id": "op", "Name": "controlled"},
        {"EventType": "StepRetryStarted", "Id": "op", "EventTimestamp": 2},
        {"EventType": "StepStopped", "Id": "op", "EventTimestamp": 3},
    ]
    history, _, _ = sources(raw).histories([])
    operation = history[0]["operations"][0]
    assert "startedAtMs" not in operation
    assert operation["endedAtMs"] == 3000
    assert operation["status"] == "STOPPED"


@pytest.fixture
def measurement(tmp_path):
    # Preserve the archived c2 baseline's observed deadline/measurement deltas.
    selected = {"victim": "original", "retry": "retry"}
    record = {
        "runId": "run",
        "caseId": "case",
        "cohortId": "cohort-1",
        "scenario": "timeout-retry",
        "executionArn": "execution",
        "selectedRequests": selected,
        "deadlineAtMs": 1794527680692.2056,
        "cleanupCutoffAtMs": 1794527685692.2056,
        "observationCutoffAtMs": 1794527686692.2056,
        "observedAtMs": 1794527688724.1567,
        "events": [{"kind": "effect", "atMs": 1794527685892.097}],
        "histories": {"execution": []},
        "sourceUris": ["memory://frozen-source"],
    }
    path = tmp_path / "measurements/deadline.json"
    path.parent.mkdir()
    path.write_text(json.dumps(record))
    context = SimpleNamespace(
        run_id="run",
        scenario=SimpleNamespace(
            case_id="case", cohort_id="cohort-1", scenario="timeout-retry"
        ),
    )
    cloud = SimpleNamespace(
        artifacts_dir=tmp_path,
        deadline_measurement=deepcopy(record),
        invocations=[{"role": "victim", "arn": "execution"}],
    )
    return context, cloud, selected, record, path


def test_persisted_measurement_exports_only_identity_bounds_and_source_reference(
    measurement,
):
    context, cloud, selected, record, path = measurement
    value, errors = _deadline_measurement(context, cloud, selected)
    assert errors == []
    assert value == {
        **{
            key: record[key]
            for key in (
                "selectedRequests",
                "deadlineAtMs",
                "cleanupCutoffAtMs",
                "observationCutoffAtMs",
                "observedAtMs",
            )
        },
        "sourceEvidenceUri": path.resolve().as_uri(),
    }
    assert "events" not in value


@pytest.mark.parametrize(
    "field",
    ["runId", "caseId", "cohortId", "scenario", "executionArn", "selectedRequests"],
)
def test_measurement_cannot_be_substituted_between_cases_or_original_requests(
    measurement, field
):
    context, cloud, selected, record, path = measurement
    record[field] = (
        {"victim": "replacement"} if field == "selectedRequests" else "other"
    )
    path.write_text(json.dumps(record))
    value, errors = _deadline_measurement(context, cloud, selected)
    assert value is None and errors


def test_measurement_must_match_the_record_written_before_release(measurement):
    context, cloud, selected, _, _ = measurement
    cloud.deadline_measurement["observedAtMs"] += 1
    value, errors = _deadline_measurement(context, cloud, selected)
    assert value is None and errors


def test_missing_measurement_is_legacy_only_if_no_snapshot_was_recorded(measurement):
    context, cloud, selected, _, path = measurement
    path.unlink()
    assert _deadline_measurement(context, cloud, selected)[1]
    cloud.deadline_measurement = None
    assert _deadline_measurement(context, cloud, selected) == (None, [])


def test_reloading_archived_measurement_does_not_require_the_old_driver_process(
    measurement,
):
    context, cloud, selected, _, _ = measurement
    cloud.deadline_measurement = None
    value, errors = _deadline_measurement(context, cloud, selected)
    assert not errors and value["selectedRequests"] == selected


def test_assembler_keeps_post_measurement_events_in_the_main_evidence(measurement):
    context, cloud, selected, record, _ = measurement
    context.scenario = SimpleNamespace(
        case_id="case",
        cohort_id="cohort-1",
        scenario="timeout-retry",
        requirement_id="python-lmi-11",
        coverage_profile="python-lifecycle",
        otel_view=None,
        export_mode=None,
        behavior_budgets={
            "cleanupMs": 5000,
            "observationGraceMs": 1000,
            "retryMs": 40000,
        },
    )
    context.profile = SimpleNamespace(
        artifact_id="sdk",
        configuration_id="config",
        resource_id="function",
        native_concurrency=2,
        recovery_policy="reuse-worker",
    )
    cloud.manifest = {"conformanceRevision": "conformance", "invocationTimeout": 60}
    cloud.observation_ended_at = 1794527693
    cloud.retired_at = cloud.observation_ended_at
    cloud.source_uris = {"memory://source"}
    cloud.control_records = []
    cloud._save = lambda *args: None
    cloud.invocations[0]["marker"] = "marker"
    cloud.invocations[0]["payload"] = {"scenario": "deadline"}
    events = [
        {
            "kind": "effect",
            "requestId": "original",
            "ownerRequestId": "original",
            "executionArn": "execution",
            "atMs": record["cleanupCutoffAtMs"] + 200,
        },
        {
            "kind": "checkpoint_exit",
            "requestId": "original",
            "ownerRequestId": "original",
            "executionArn": "execution",
            "atMs": record["observedAtMs"] + 1000,
        },
    ]
    captured = SimpleNamespace(
        observations=lambda: (deepcopy(events), []),
        histories=lambda _: ([], {}, []),
        attempts=lambda _: (
            [{"requestId": "original", "executionArn": "execution"}],
            selected,
            [],
        ),
    )
    with patch("lmi_tests.shared_evidence.CaseSources.capture", return_value=captured):
        value = build_evidence(context, cloud)
    assert [event["atMs"] for event in value["events"]] == [
        event["atMs"] for event in events
    ]
    assert (
        value["regression"]["deadlineMeasurement"]["observedAtMs"]
        == record["observedAtMs"]
    )
