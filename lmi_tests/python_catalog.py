# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Python-owned LMI requirements and coverage using the shared runner machinery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.lmi.app import SuiteDefinition
from aws_durable_execution_conformance_tests.lmi.matrix import CaseRequirement


@dataclass(frozen=True)
class ScenarioDefinition:
    selector: str
    roles: tuple[str, ...]
    slots: int
    exclusive: bool
    budget_names: tuple[str, ...]
    source_tests: tuple[str, ...]
    roles_at_two: tuple[str, ...] | None = None
    slots_at_two: int | None = None


@dataclass(frozen=True)
class Requirement:
    number: int
    description: str
    contract: str
    baseline: tuple[ScenarioDefinition, ...]
    instrumented: tuple[ScenarioDefinition, ...]


# These are Python SDK regression contracts, not shared conformance requirements.
_REQUIREMENTS = (
    Requirement(
        number=1,
        description="Configured native worker capacity and execution isolation",
        contract="Verify genuine native concurrency one and two deployments independently. Establish one original request per configured worker under a bounded barrier; at concurrency two require distinct worker processes overlapping in the same environment. Preserve the selected original requests, results and service histories. A single request to a concurrency-two deployment cannot replace the concurrency-one baseline.",
        baseline=(
            ScenarioDefinition(
                selector="native-concurrency",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_actual_environment_process_concurrency",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
        ),
        instrumented=(),
    ),
    Requirement(
        number=10,
        description="Synchronous checkpoint settlement at the branch boundary",
        contract="Hold an actual SDK checkpoint request in flight while a child branch completes. The durable wrapper must preserve the supported checkpoint and child-scope settlement boundary. Require independently observed request entry, held interval, service response and wrapper exit; a mocked checkpoint flag or a callback without a real dispatch is insufficient.",
        baseline=(
            ScenarioDefinition(
                selector="checkpoint-settlement",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_real_synchronous_checkpoint_settles_before_branch_join",
                ),
            ),
        ),
        instrumented=(),
    ),
    Requirement(
        number=11,
        description="Platform timeout retries preserve completed work",
        contract="Cross the configured platform invocation timeout and observe a service-started retry with a different request ID for the same durable execution. Completed successful side effects remain checkpointed and are not repeated. Record interrupted work per attempt without imposing exactly-once execution on at-least-once steps; an independently invoked replacement execution cannot satisfy retry.",
        baseline=(
            ScenarioDefinition(
                selector="timeout-retry",
                roles=("victim", "retry"),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "cleanupMs", "retryMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_service_timeout_retry_does_not_repeat_completed_step",
                ),
            ),
        ),
        instrumented=(
            ScenarioDefinition(
                selector="timeout-retry",
                roles=("victim", "retry"),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "cleanupMs", "retryMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_service_timeout_retry_does_not_repeat_completed_step",
                    "lmi_tests/tests/e2e/test_observability.py::test_plugin_enabled_lifecycle_boundaries",
                ),
            ),
        ),
    ),
    Requirement(
        number=12,
        description="Abandoned child rejects a late durable operation",
        contract="After a parent or sibling result abandons a child, allow that residual child to attempt another public durable operation. Prove the attempt was rejected before its user body and checkpoint dispatch; keep the operation absent from service history. Retain the original invocation and scope identities and distinguish fault release from the assertion boundary.",
        baseline=(
            ScenarioDefinition(
                selector="late-operation",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_abandoned_child_rejects_late_durable_operation",
                ),
            ),
        ),
        instrumented=(
            ScenarioDefinition(
                selector="late-operation",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_abandoned_child_rejects_late_durable_operation",
                    "lmi_tests/tests/e2e/test_observability.py::test_plugin_enabled_lifecycle_boundaries",
                ),
            ),
        ),
    ),
    Requirement(
        number=13,
        description="Root return and failure settle in-flight work",
        contract="Exercise successful root return and root failure while an admitted child operation remains in flight. Observe root completion, child cleanup, request settlement and wrapper exit independently, preserving the original returned result or failure. At native concurrency two, keep a selected healthy peer progressing in the same environment without inheriting the victim scope.",
        baseline=(
            ScenarioDefinition(
                selector="return-inflight",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_root_return_or_failure_settles_inflight_work",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
            ScenarioDefinition(
                selector="failure-inflight",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_root_return_or_failure_settles_inflight_work",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
        ),
        instrumented=(
            ScenarioDefinition(
                selector="return-inflight",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_root_return_or_failure_settles_inflight_work",
                    "lmi_tests/tests/e2e/test_observability.py::test_plugin_enabled_lifecycle_boundaries",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
            ScenarioDefinition(
                selector="failure-inflight",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_root_return_or_failure_settles_inflight_work",
                    "lmi_tests/tests/e2e/test_observability.py::test_plugin_enabled_lifecycle_boundaries",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
        ),
    ),
    Requirement(
        number=14,
        description="Pending waits for root cleanup before replay",
        contract="Suspend through the public durable API while controlled work remains in the root cleanup path. Observe root-finally entry and exit, actual PENDING wrapper completion, service resume and stable checkpoint history. The wrapper must honor the supported scope-cleanup boundary and replay must not repeat the completed operation body. Native concurrency two includes an established healthy peer.",
        baseline=(
            ScenarioDefinition(
                selector="pending-cleanup",
                roles=("victim", "resume"),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_pending_waits_for_root_finally_and_then_replays",
                ),
                roles_at_two=("victim", "resume", "peer"),
                slots_at_two=2,
            ),
        ),
        instrumented=(
            ScenarioDefinition(
                selector="pending-cleanup",
                roles=("victim", "resume"),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_pending_waits_for_root_finally_and_then_replays",
                    "lmi_tests/tests/e2e/test_observability.py::test_plugin_enabled_lifecycle_boundaries",
                ),
                roles_at_two=("victim", "resume", "peer"),
                slots_at_two=2,
            ),
        ),
    ),
    Requirement(
        number=15,
        description="Nested durable work progresses with bounded branch capacity",
        contract="Use the real public child, map and parallel APIs with a single sibling slot to establish nested progress. Launch one root per configured native worker and positively establish the required environment and process placement. Nested work must progress within its declared test budget without starving behind its owning branch, preserving result attribution for every original root.",
        baseline=(
            ScenarioDefinition(
                selector="nested-progress",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "progressMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_nested_single_lane_pools_progress_for_all_runtime_workers",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
        ),
        instrumented=(),
    ),
    Requirement(
        number=4,
        description="Deadline admission and original environment capacity recovery",
        contract="Observe a real platform timeout of the original victim with controlled work in flight. At native concurrency two, establish a healthy peer in the same environment and verify its progress and successful completion. Before fault release, demand every configured worker slot in the original environment and prove full capacity recovers within the declared deadline-relative cleanup budget. Preserve victim admission and stale-side-effect assertions independently of recovery and final retirement.",
        baseline=(
            ScenarioDefinition(
                selector="deadline-recovery",
                roles=("victim", "recovery"),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "cleanupMs", "recoveryMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_invocation_deadline_isolation_and_capacity_recovery",
                ),
                roles_at_two=("victim", "peer", "recovery", "recovery-peer"),
                slots_at_two=2,
            ),
        ),
        instrumented=(
            ScenarioDefinition(
                selector="deadline-recovery",
                roles=("victim", "recovery"),
                slots=1,
                exclusive=True,
                budget_names=(
                    "observationGraceMs",
                    "cleanupMs",
                    "recoveryMs",
                    "exportMs",
                ),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_invocation_deadline_isolation_and_capacity_recovery",
                    "lmi_tests/tests/e2e/test_observability.py::test_plugin_enabled_lifecycle_boundaries",
                ),
                roles_at_two=("victim", "peer", "recovery", "recovery-peer"),
                slots_at_two=2,
            ),
        ),
    ),
    Requirement(
        number=5,
        description="Callback suspension and retry preserve checkpointed work",
        contract="Exercise real durable callback suspension, operation retry and stored success and failure results. Resume the same durable execution through the service callback path, retaining every invocation and checkpoint history. Completed user bodies and side effects must not repeat on replay, and retry effects follow the configured per-attempt contract.",
        baseline=(
            ScenarioDefinition(
                selector="replay-callback-retry",
                roles=("victim", "resume"),
                slots=1,
                exclusive=False,
                budget_names=("observationGraceMs",),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_checkpoint_replay_callback_and_retry",
                ),
            ),
        ),
        instrumented=(),
    ),
    Requirement(
        number=6,
        description="Warm success, failure and suspension resource cleanup",
        contract="Exercise a repeated sequence of successful, failed and suspended/resumed public handlers. Identify actual process reuse and compare invocation-owned state and process resource baselines over the sequence. Verify branch/thread ownership and cleanup, preserving failure and replay outcomes; an unrelated process cannot replace the observed warm worker.",
        baseline=(
            ScenarioDefinition(
                selector="warm-lifecycle",
                roles=(
                    "victim",
                    "warm",
                    "warm-2",
                    "warm-2-resume",
                    "warm-3",
                    "warm-4",
                    "warm-5",
                    "warm-5-resume",
                    "warm-6",
                    "warm-7",
                    "warm-8",
                    "warm-8-resume",
                ),
                slots=1,
                exclusive=True,
                budget_names=(
                    "observationGraceMs",
                    "maxThreadGrowth",
                    "maxFdGrowth",
                    "maxRssGrowthBytes",
                ),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_warm_success_failure_and_suspension_cleanup",
                ),
            ),
        ),
        instrumented=(),
    ),
    Requirement(
        number=7,
        description="Runtime carriers, warm telemetry and native worker attribution",
        contract="In both OTel views and both native configurations, separately verify genuine sampled and unsampled runtime carriers, identified warm success/failure/replay, and one original request per configured worker. Preserve caller Root and sampling correlation and execution attribution. At concurrency two, ending one request must not end a held healthy peer; controlled malformed or missing-carrier unit fixtures cannot substitute for native runtime evidence.",
        baseline=(),
        instrumented=(
            ScenarioDefinition(
                selector="runtime-carrier-sampled",
                roles=("victim",),
                slots=1,
                exclusive=False,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_observability.py::test_native_runtime_carrier_and_sampling",
                ),
            ),
            ScenarioDefinition(
                selector="runtime-carrier-unsampled",
                roles=("victim",),
                slots=1,
                exclusive=False,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_observability.py::test_native_runtime_carrier_and_sampling",
                ),
            ),
            ScenarioDefinition(
                selector="otel-warm-lifecycle",
                roles=(
                    "victim",
                    "warm-1",
                    "warm-2",
                    "warm-2-resume",
                    "warm-3",
                    "warm-4",
                    "warm",
                    "warm-resume",
                ),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_observability.py::test_warm_process_cleanup_and_replay",
                ),
            ),
            ScenarioDefinition(
                selector="otel-concurrent",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_observability.py::test_concurrent_worker_attribution_and_peer_end",
                ),
                roles_at_two=("victim", "peer"),
                slots_at_two=2,
            ),
        ),
    ),
    Requirement(
        number=8,
        description="Exporter failure recovery and held-export settlement",
        contract="In both OTel views and both native configurations, separately observe an exporter failure followed by a healthy invocation and an intentionally held export. Record provider/exporter ownership and all export outcomes. Successful wrapper/OTel cleanup cannot precede the held export settlement; driver release does not turn an earlier failure or incomplete collection into a pass.",
        baseline=(),
        instrumented=(
            ScenarioDefinition(
                selector="export-error-recovery",
                roles=("victim", "warm"),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_observability.py::test_export_failure_then_healthy_invocation",
                ),
            ),
            ScenarioDefinition(
                selector="held-export",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "exportMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_observability.py::test_held_export_must_settle_before_successful_cleanup",
                ),
            ),
        ),
    ),
    Requirement(
        number=9,
        description="Early completion closes losing branch scopes",
        contract="For parallel, map and nested public operations configured to finish after a winning branch, observe the winner and controlled losing user work separately. The supported wrapper and branch cleanup boundaries must satisfy the declared cleanup budget before the driver releases the fault. Record actual task and request dispatch observations; final success alone cannot prove cleanup.",
        baseline=(
            ScenarioDefinition(
                selector="early-completion-parallel",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "cleanupMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_early_completion_reclaims_losing_branches",
                ),
            ),
            ScenarioDefinition(
                selector="early-completion-map",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "cleanupMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_early_completion_reclaims_losing_branches",
                ),
            ),
            ScenarioDefinition(
                selector="early-completion-nested",
                roles=("victim",),
                slots=1,
                exclusive=True,
                budget_names=("observationGraceMs", "cleanupMs"),
                source_tests=(
                    "lmi_tests/tests/e2e/test_lifecycle.py::test_early_completion_reclaims_losing_branches",
                ),
            ),
        ),
        instrumented=(),
    ),
)


def requirements(capability: str = "lifecycle-otel") -> dict[str, dict[str, Any]]:
    if capability not in ("lifecycle", "lifecycle-otel"):
        raise ValueError("Unknown Python lifecycle capability")
    result = {}
    for item in _REQUIREMENTS:
        definitions = item.baseline + (
            item.instrumented if capability == "lifecycle-otel" else ()
        )
        if not definitions:
            continue
        identity = f"python-lmi-{item.number}"
        result[identity] = {
            "id": identity,
            "description": item.description,
            "contract": item.contract,
            "mandatory": True,
            "capability": "lifecycle" if item.baseline else "otel",
            "runtimes": ["python"],
            "requiredScenarios": list(
                dict.fromkeys(case.selector for case in definitions)
            ),
        }
    return result


def required_cases(capability: str = "lifecycle-otel") -> tuple[CaseRequirement, ...]:
    requirements(capability)
    cases = []
    for item in _REQUIREMENTS:
        groups: tuple[
            tuple[tuple[ScenarioDefinition, ...], tuple[str | None, ...]], ...
        ] = ((item.baseline, (None,)),)
        if capability == "lifecycle-otel":
            groups += ((item.instrumented, ("invocation", "execution")),)
        for definitions, views in groups:
            for case in definitions:
                for native in (1, 2):
                    roles = (
                        case.roles_at_two
                        if native == 2 and case.roles_at_two is not None
                        else case.roles
                    )
                    slots = (
                        case.slots_at_two
                        if native == 2 and case.slots_at_two is not None
                        else case.slots
                    )
                    for view in views:
                        cases.append(
                            CaseRequirement(
                                requirement_id=f"python-lmi-{item.number}",
                                scenario=case.selector,
                                native_concurrency=native,
                                otel_view=view,
                                roles=roles,
                                exclusive=case.exclusive,
                                minimum_native_concurrency=slots,
                                source_tests=case.source_tests,
                                budget_names=case.budget_names,
                            )
                        )
    return tuple(cases)


def validate(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    from aws_durable_execution_conformance_tests.lmi.evidence import (
        validate_common_evidence,
    )
    from lmi_tests.python_lifecycle import validate_lifecycle_regressions

    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown Python lifecycle validation phase"]
    if (
        not isinstance(evidence, Mapping)
        or evidence.get("coverageProfile") != "python-lifecycle"
    ):
        return [
            "INCOMPLETE: Python lifecycle validation requires its SDK-owned coverage profile"
        ]
    if evidence.get("runtime") != "python":
        return ["INCOMPLETE: Python lifecycle contracts apply only to the Python SDK"]
    profile = evidence.get("profile")
    if not isinstance(profile, Mapping) or profile.get("recoveryPolicy") not in (
        "reuse-worker",
        "replace-worker",
    ):
        return [
            "INCOMPLETE: Python lifecycle evidence requires an explicit supported recoveryPolicy"
        ]
    errors = validate_common_evidence(
        evidence, phase=phase, requirement_ids=tuple(requirements())
    )
    errors.extend(validate_lifecycle_regressions(evidence, phase=phase))
    if evidence.get("otelView") is not None:
        from lmi_tests.python_otel import validate_lifecycle_telemetry

        errors.extend(validate_lifecycle_telemetry(evidence, phase=phase))
    return errors


def suite_definition(capability: str = "lifecycle-otel") -> SuiteDefinition:
    return SuiteDefinition(
        coverage_profile="python-lifecycle",
        suite_name="python-lmi",
        requirements=requirements(capability),
        cases=required_cases(capability),
        validator=validate,
        evidence_policy_key="regression",
        nonnegative_budgets=frozenset(
            {"maxThreadGrowth", "maxFdGrowth", "maxRssGrowthBytes"}
        ),
    )


def load_manifest(path: Path) -> dict[str, Any]:
    from aws_durable_execution_conformance_tests.lmi.app import (
        load_manifest as load_shared_manifest,
    )

    manifest = json.loads(path.read_text())
    if not isinstance(manifest, Mapping) or manifest.get("runtime") != "python":
        raise ValueError("Python lifecycle manifests require runtime python")
    if manifest.get("coverageProfile") != "python-lifecycle":
        raise ValueError(
            "Python lifecycle manifests require the SDK-owned coverage profile"
        )
    profiles = manifest.get("profiles")
    if isinstance(profiles, list):
        for profile in profiles:
            if not isinstance(profile, Mapping) or profile.get(
                "recovery_policy"
            ) not in ("reuse-worker", "replace-worker"):
                raise ValueError(
                    "Python lifecycle profiles require an explicit recovery_policy"
                )
    capability = manifest.get("capabilityProfile")
    if not isinstance(capability, str):
        raise ValueError(
            "Python lifecycle manifests require an explicit capabilityProfile"
        )
    return load_shared_manifest(path, suite=suite_definition(capability))
