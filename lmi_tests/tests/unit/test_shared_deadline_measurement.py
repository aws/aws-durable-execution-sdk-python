# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Normal victim release must follow the immutable deadline measurement."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from lmi_tests import shared_scenarios as module
from lmi_tests.evidence import CollectionError, PlacementError
from lmi_tests.tests.unit.test_cloud import driver as driver


class MeasurementCloud:
    def __init__(self, context, clock, *, retry_observed=True, write_fails=False):
        self.context, self.clock = context, clock
        self.concurrency = 1
        self.manifest = {"invocationTimeout": 60, "driverTimeout": 1}
        self.actions = []
        self.events = [
            {
                "phase": "WRAPPER_ENTER",
                "request": "original",
                "execution": "execution",
                "time": 0,
            }
        ]
        if retry_observed:
            self.events.append(
                {
                    "phase": "WRAPPER_ENTER",
                    "request": "retry",
                    "execution": "execution",
                    "time": 52,
                }
            )
        self.raw_histories = {"execution": [{"EventType": "InvocationCompleted"}]}
        self.source_uris = {"s3://case/measurement-source"}
        self.write_fails = write_fails
        self.snapshot = None

    def poll(self, predicate, **options):
        observed = predicate()
        if not observed:
            raise CollectionError(options.get("message", "measurement wait incomplete"))
        return observed

    def platform_timeout(self, item, request):
        self.actions.append(("timeout", request))

    def for_item(self, item):
        return self.events

    def refresh(self, **options):
        return self.events

    def history(self, item):
        self.actions.append(("history", item["arn"]))
        return self.raw_histories[item["arn"]]

    def record_deadline_measurement(self, snapshot):
        if self.write_fails:
            raise OSError("evidence storage failed")
        self.snapshot = deepcopy(snapshot)
        self.actions.append(("measurement", self.clock.now))

    def release(self, gate):
        self.actions.append(("release", gate, self.clock.now))

    def wait_terminal(self, item):
        self.actions.append(("terminal", item["arn"]))
        self.events.append(
            {"phase": "WRAPPER_RETURN", "request": "retry", "status": "FAILED"}
        )
        self.raw_histories[item["arn"]].append({"EventType": "ExecutionFailed"})
        return {"Status": "FAILED"}

    def start(self, fixture, **options):
        role = options["role"]
        self.context.record_attempt(role, role=role, requestId=role)
        self.actions.append(("start", role))
        return {"arn": role, "marker": role}

    def finish(self, item, status):
        self.actions.append(("finish", item["arn"], status))


def measured_driver(
    monkeypatch,
    *,
    scenario="timeout-retry",
    now=100,
    retry_observed=True,
    write_fails=False,
):
    clock = SimpleNamespace(now=now)
    monkeypatch.setattr(module.time, "time", lambda: clock.now)
    roles = (
        ("victim", "retry") if scenario == "timeout-retry" else ("victim", "recovery")
    )
    profile = Profile("native-1", "function", 1, 1, "sdk", "configuration")
    context = ScenarioContext(
        "run",
        Scenario(
            "case",
            "lmi-lifecycle-11",
            profile.profile_id,
            scenario,
            roles=roles,
            behavior_budgets={
                "cleanupMs": 5000,
                "observationGraceMs": 1000,
                "retryMs": 2000,
            },
        ),
        profile,
    )
    context.record_attempt("original", role="victim", requestId="original")
    if scenario == "timeout-retry" and retry_observed:
        context.record_attempt("retry", role="retry", requestId="retry")
    cloud = MeasurementCloud(
        context, clock, retry_observed=retry_observed, write_fails=write_fails
    )
    result = module.ScenarioDriver(context, cloud)
    result.items["victim"] = {"arn": "execution", "marker": "victim"}
    result.boundary = {
        "deadline": 50,
        "request": "original",
        "environment": "original-environment",
    }
    return result, cloud, clock


def test_completed_measurement_is_persisted_before_normal_release_and_terminal_wait(
    monkeypatch,
):
    scenario, cloud, clock = measured_driver(monkeypatch)
    scenario._deadline()
    kinds = [action[0] for action in cloud.actions]
    assert kinds.index("measurement") < kinds.index("release") < kinds.index("terminal")
    assert [action[1] for action in cloud.actions if action[0] == "release"] == [
        scenario.gate("fault")
    ]
    assert cloud.snapshot["selectedRequests"] == {
        "victim": "original",
        "retry": "retry",
    }
    assert cloud.snapshot["cleanupCutoffAtMs"] == 55_000
    assert cloud.snapshot["observationCutoffAtMs"] == 56_000
    assert cloud.snapshot["observedAtMs"] == clock.now * 1000
    assert cloud.snapshot["sourceUris"] == ["s3://case/measurement-source"]
    assert not any(
        event["phase"] == "WRAPPER_RETURN" for event in cloud.snapshot["events"]
    )
    assert cloud.snapshot["histories"]["execution"] == [
        {"EventType": "InvocationCompleted"}
    ]
    assert not any(action[0] == "finish" for action in cloud.actions)


def test_victim_fault_cannot_be_released_before_cutoff_plus_observation_grace(
    monkeypatch,
):
    scenario, cloud, _ = measured_driver(monkeypatch, now=55.5)
    with pytest.raises(CollectionError):
        scenario._deadline()
    assert cloud.snapshot is None
    assert not any(action[0] in {"release", "terminal"} for action in cloud.actions)


def test_victim_fault_cannot_be_released_before_same_execution_retry_is_observed(
    monkeypatch,
):
    scenario, cloud, _ = measured_driver(monkeypatch, retry_observed=False)
    with pytest.raises(CollectionError, match="real service retry"):
        scenario._deadline()
    assert cloud.snapshot is None
    assert not any(action[0] in {"release", "terminal"} for action in cloud.actions)


def test_original_recovery_placement_failure_is_not_repaired_by_victim_release(
    monkeypatch,
):
    scenario, cloud, _ = measured_driver(monkeypatch, scenario="deadline-recovery")
    scenario.wait_overlap = Mock(
        side_effect=PlacementError("original environment unavailable")
    )
    with pytest.raises(PlacementError, match="original environment"):
        scenario._deadline()
    assert cloud.snapshot is None
    assert not any(action[0] in {"release", "terminal"} for action in cloud.actions)


def test_measurement_storage_failure_keeps_normal_fault_release_unperformed(
    monkeypatch,
):
    scenario, cloud, _ = measured_driver(monkeypatch, write_fails=True)
    with pytest.raises(OSError, match="evidence storage"):
        scenario._deadline()
    assert not any(action[0] in {"release", "terminal"} for action in cloud.actions)


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"])
def test_terminal_observation_preserves_actual_outcome_and_waits_for_all_wrappers(
    driver, status
):
    driver.manifest["driverTimeout"] = 1
    driver.wait = Mock()
    driver.lam.get_durable_execution = Mock(
        side_effect=[{"Status": "RUNNING"}, {"Status": status}]
    )
    opened = [
        {"phase": "WRAPPER_ENTER", "request": "original"},
        {"phase": "WRAPPER_ENTER", "request": "retry"},
    ]
    closed = [
        *opened,
        {"phase": "WRAPPER_RAISE", "request": "original"},
        {"phase": "WRAPPER_RETURN", "request": "retry"},
    ]
    driver.for_item = Mock(side_effect=[opened, closed])
    driver.history = Mock(return_value=[])
    observed = driver.wait_terminal({"arn": "execution", "marker": "victim"})
    assert observed == {"Status": status}
    assert driver.for_item.call_count == 2
    assert driver.execution_results["execution"] == observed
    driver.history.assert_called_once()


def test_terminal_service_status_does_not_replace_missing_native_wrapper_exit(driver):
    driver.manifest["driverTimeout"] = 1
    driver.lam.get_durable_execution = Mock(return_value={"Status": "FAILED"})
    driver.for_item = Mock(
        return_value=[{"phase": "WRAPPER_ENTER", "request": "still-running"}]
    )

    def poll(predicate, **options):
        result = predicate()
        if not result:
            raise CollectionError(options["message"])
        return result

    driver.poll = poll
    with pytest.raises(CollectionError, match="wrapper exits"):
        driver.wait_terminal({"arn": "execution", "marker": "victim"})


def test_persisted_measurement_does_not_follow_mutated_later_observations(driver):
    snapshot = {
        "selectedRequests": {"victim": "original"},
        "events": [{"phase": "EFFECT", "time": 54}],
    }
    driver.record_deadline_measurement(snapshot)
    snapshot["events"][0]["time"] = 99
    assert driver.deadline_measurement["events"][0]["time"] == 54
    assert (driver.artifacts_dir / "measurements/deadline.json").exists()
