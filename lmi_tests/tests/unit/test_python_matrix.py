# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The Python regression profile cannot pass with missing configuration slots."""

from dataclasses import replace
from pathlib import Path
from runpy import run_path

import pytest

from lmi_tests.python_catalog import requirements, required_cases
from aws_durable_execution_conformance_tests.lmi.matrix import (
    describe_case,
    duplicate_coverage,
    missing_cases,
)

BASE_SCENARIOS = {
    "native-concurrency",
    "replay-callback-retry",
    "warm-lifecycle",
    "early-completion-parallel",
    "early-completion-map",
    "early-completion-nested",
    "checkpoint-settlement",
    "deadline-recovery",
    "timeout-retry",
    "pending-cleanup",
    "nested-progress",
    "return-inflight",
    "failure-inflight",
    "late-operation",
}
OTEL_SCENARIOS = {
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
}


def test_python_lifecycle_has_fourteen_variants_on_both_native_configurations():
    cases = required_cases("lifecycle")
    assert len(cases) == 28
    assert {case.scenario for case in cases} == BASE_SCENARIOS
    assert {
        (case.scenario, case.native_concurrency, case.otel_view) for case in cases
    } == {
        (scenario, concurrency, None)
        for scenario in BASE_SCENARIOS
        for concurrency in (1, 2)
    }
    assert not {"python-lmi-2", "python-lmi-3", "python-lmi-7", "python-lmi-8"} & {
        case.requirement_id for case in cases
    }


def test_python_otel_adds_twelve_variants_in_both_views_without_replacing_baseline():
    cases = required_cases("lifecycle-otel")
    baseline = required_cases("lifecycle")
    instrumented = [case for case in cases if case.otel_view is not None]
    assert len(cases) == 76
    assert len(instrumented) == 48
    assert {case.key for case in baseline} <= {case.key for case in cases}
    assert {
        (case.scenario, case.native_concurrency, case.otel_view)
        for case in instrumented
    } == {
        (scenario, concurrency, view)
        for scenario in OTEL_SCENARIOS
        for concurrency in (1, 2)
        for view in ("invocation", "execution")
    }
    assert missing_cases(cases, [case.key for case in cases]) == []
    assert duplicate_coverage(case.key for case in cases) == set()


def test_declared_but_unexercised_c1_cannot_be_filled_with_more_c2_results():
    cases = required_cases("lifecycle")
    c2 = [case.key for case in cases if case.native_concurrency == 2]
    missing = missing_cases(cases, c2 + c2)
    assert len(missing) == 14
    assert {case.native_concurrency for case in missing} == {1}
    assert duplicate_coverage(c2 + c2) == set(c2)


def test_relabeling_native_concurrency_or_view_does_not_cover_the_original_slot():
    cases = required_cases("lifecycle-otel")
    selected = next(
        case
        for case in cases
        if case.scenario == "runtime-carrier-sampled"
        and case.native_concurrency == 1
        and case.otel_view == "invocation"
    )
    observed = [case.key for case in cases if case != selected]
    for invalid in (
        replace(selected, native_concurrency=2),
        replace(selected, native_concurrency=8),
        replace(selected, otel_view="execution"),
        replace(selected, otel_view=None),
        replace(selected, scenario="runtime-carrier-unsampled"),
        replace(selected, requirement_id="python-lmi-8"),
    ):
        assert missing_cases(cases, [*observed, invalid.key]) == [selected]


def test_otel_coverage_cannot_replace_uninstrumented_lifecycle_or_other_views():
    cases = required_cases("lifecycle-otel")
    observed = [case.key for case in cases if case.otel_view == "invocation"]
    missing = missing_cases(cases, observed)
    assert len(missing) == 52
    assert {case.otel_view for case in missing} == {None, "execution"}


def test_peer_roles_follow_real_python_worker_configuration():
    cases = required_cases("lifecycle")
    indexed = {(case.scenario, case.native_concurrency): case for case in cases}
    for name in ("native-concurrency", "nested-progress"):
        assert indexed[name, 1].roles == ("victim",)
        assert indexed[name, 2].roles == ("victim", "peer")
        assert indexed[name, 1].minimum_native_concurrency == 1
        assert indexed[name, 2].minimum_native_concurrency == 2
    assert indexed["deadline-recovery", 1].roles == ("victim", "recovery")
    assert set(indexed["deadline-recovery", 2].roles) == {
        "victim",
        "peer",
        "recovery",
        "recovery-peer",
    }
    assert "peer" not in indexed["pending-cleanup", 1].roles
    assert "peer" in indexed["pending-cleanup", 2].roles
    assert indexed["replay-callback-retry", 2].minimum_native_concurrency == 1


def test_blocked_faults_and_resource_baselines_require_exclusive_windows():
    cases = required_cases("lifecycle-otel")
    shared = {
        "replay-callback-retry",
        "runtime-carrier-sampled",
        "runtime-carrier-unsampled",
    }
    assert all(case.exclusive == (case.scenario not in shared) for case in cases)
    for case in cases:
        assert case.native_concurrency is not None
        assert case.minimum_native_concurrency <= case.native_concurrency
        assert case.source_tests
        assert all(
            test.startswith("lmi_tests/tests/e2e/") for test in case.source_tests
        )
    deadline = next(case for case in cases if case.scenario == "deadline-recovery")
    assert deadline.budget_names == ("observationGraceMs", "cleanupMs", "recoveryMs")
    assert describe_case(deadline) == "python-lmi-4/deadline-recovery/native-1/no-otel"


def test_expanded_catalog_exposes_selector_union_and_python_owned_ids():
    catalog = requirements("lifecycle-otel")
    assert len(catalog) == 13
    assert set(catalog["python-lmi-7"]["requiredScenarios"]) == {
        "runtime-carrier-sampled",
        "runtime-carrier-unsampled",
        "otel-warm-lifecycle",
        "otel-concurrent",
    }
    for definition in catalog.values():
        assert definition["runtimes"] == ["python"]
        assert definition["id"].startswith("python-lmi-")


def test_every_expanded_slot_declares_the_validator_behavior_budget_names():
    cases = required_cases("lifecycle-otel")
    for case in cases:
        assert "observationGraceMs" in case.budget_names
        assert not {
            "cleanupGraceMs",
            "placementTimeoutMs",
            "retryTimeoutMs",
            "progressTimeoutMs",
        } & set(case.budget_names)
        if case.scenario == "warm-lifecycle":
            assert set(case.budget_names) == {
                "observationGraceMs",
                "maxThreadGrowth",
                "maxFdGrowth",
                "maxRssGrowthBytes",
            }
        if case.scenario.startswith("early-completion-"):
            assert set(case.budget_names) == {"observationGraceMs", "cleanupMs"}
        if case.scenario == "timeout-retry":
            assert set(case.budget_names) - {"exportMs"} == {
                "observationGraceMs",
                "cleanupMs",
                "retryMs",
            }
        assert ("exportMs" in case.budget_names) == (case.otel_view is not None)


@pytest.mark.parametrize("native", [1, 2])
def test_warm_lifecycle_positive_control_fits_the_complete_default_role_plan(native):
    build = run_path(str(Path(__file__).with_name("test_python_lifecycle.py")))[
        "build_evidence"
    ]
    evidence = build("warm-lifecycle", native)
    required = next(
        case
        for case in required_cases("lifecycle")
        if case.scenario == "warm-lifecycle" and case.native_concurrency == native
    )
    assert len(required.roles) == 12
    assert set(evidence["selectedRequests"]) == set(required.roles)
    assert {attempt["role"] for attempt in evidence["attempts"]} == set(required.roles)


def test_otel_warm_defaults_plan_all_six_executions_and_two_native_resumes():
    expected = {
        "victim",
        "warm-1",
        "warm-2",
        "warm-2-resume",
        "warm-3",
        "warm-4",
        "warm",
        "warm-resume",
    }
    cells = [
        case
        for case in required_cases("lifecycle-otel")
        if case.scenario == "otel-warm-lifecycle"
    ]
    assert len(cells) == 4
    assert all(set(case.roles) == expected for case in cells)


@pytest.mark.parametrize("capability", ["unknown", "", None])
def test_unknown_python_capability_is_rejected(capability):
    with pytest.raises(ValueError):
        requirements(capability)
    with pytest.raises(ValueError):
        required_cases(capability)
