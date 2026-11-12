# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Actual fixture exports reach shared acceptance through the local service."""

import json
import time

import pytest

from aws_durable_execution_conformance_tests.lmi.app import validate
from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import fixture, telemetry
from lmi_tests.cloud import Cloud
from lmi_tests.shared_evidence import build_evidence
from lmi_tests.tests.unit.test_shared_observations import install_ledger, shared_payload
from lmi_tests.tests.unit.test_shared_telemetry import HEADER


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("mode", ["sync", "async-bounded-flush"])
@pytest.mark.parametrize(
    "scenario", ["export-success", "export-failure-recovery", "held-export"]
)
def test_real_fixture_exports_satisfy_shared_mode_contract(
    monkeypatch, tmp_path, view, mode, scenario
):
    ledger = install_ledger(monkeypatch)
    monkeypatch.setenv("_X_AMZN_TRACE_ID", HEADER)
    telemetry._RUNTIMES.clear()
    factory = telemetry.Runtime
    # Test-only processor configuration. The deployed fixture defaults to 5 s;
    # its separately declared acceptance budget is 10 s.
    monkeypatch.setattr(
        telemetry,
        "Runtime",
        lambda selected_view, selected_mode="sync": factory(
            selected_view, selected_mode, flush_timeout_ms=20
        ),
    )
    budgets = {"exportCompletionMs": 5000, "observationGraceMs": 1}
    if mode == "async-bounded-flush":
        budgets["flushWaitMs"] = 100
    roles = ("victim", "warm") if scenario == "export-failure-recovery" else ("victim",)
    context = ScenarioContext(
        "unit",
        Scenario(
            "case",
            "lmi-lifecycle-16",
            "c1",
            scenario,
            roles=roles,
            otel_view=view,
            coverage_profile="export-lifecycle",
            export_mode=mode,
            behavior_budgets=budgets,
        ),
        Profile("c1", "local-function", 1, 1, "actual-local-sdk", "local-config"),
    )
    cloud = object.__new__(Cloud)
    cloud.manifest = {
        "commit": "tested-commit",
        "conformanceRevision": "local-pinned-conformance",
        "invocationTimeout": 30,
        "wheels": {
            "aws_durable_execution_sdk_python_otel-local.whl": "local-plugin-source"
        },
    }
    cloud.invocations, cloud.control_records = [], []
    cloud.request_roles, cloud.raw_histories, cloud.execution_results = {}, {}, {}
    cloud.artifacts_dir = tmp_path
    cloud._save = lambda name, data: (tmp_path / name).write_text(json.dumps(data))
    held_count = 0
    try:
        with DurableFunctionTestRunner(
            fixture.handler, poll_interval=0.01, skip_time=False
        ) as runner:
            for role in roles:
                payload = shared_payload("success")
                payload.update(
                    marker=role,
                    role=role,
                    coverageProfile="export-lifecycle",
                    sharedScenario=scenario,
                    otelView=view,
                    exportMode=mode,
                    behaviorBudgets=budgets,
                    otel_export="held"
                    if scenario == "held-export"
                    else "failure"
                    if scenario == "export-failure-recovery" and role == "victim"
                    else "success",
                    otel_export_gate="export-gate",
                )
                prefix = payload["controlPrefix"]
                for gate in ("release-all", "export-gate"):
                    if prefix + gate not in ledger.objects:
                        started = time.time()
                        ledger.put_object(
                            Bucket="private-evidence", Key=prefix + gate, Body=b"hold"
                        )
                        cloud.control_records.append(
                            {
                                "gate": gate,
                                "action": "hold",
                                "started": started,
                                "time": time.time(),
                            }
                        )
                context.record_attempt(role, role=role)
                arn = runner.run_async(
                    input=payload, execution_name=role, execution_timeout=15
                )
                try:
                    if scenario == "held-export":
                        ledger.wait_for("blocked", timeout=3)
                        if mode == "async-bounded-flush":
                            waited = ledger.wait_for("flush_wait_completed", timeout=3)
                            assert waited["observation"]["outcome"] == "timeout"
                            ledger.wait_for("wrapper_return", timeout=3)
                            assert not any(
                                record["observation"]["kind"] == "export_completed"
                                for record in ledger.observations()
                            )
                        else:
                            assert not any(
                                record["observation"]["kind"] == "wrapper_return"
                                for record in ledger.observations()
                            )
                finally:
                    if scenario == "held-export":
                        started = time.time()
                        ledger.put_object(
                            Bucket="private-evidence",
                            Key=prefix + "export-gate",
                            Body=b"release",
                        )
                        cloud.control_records.append(
                            {
                                "gate": "export-gate",
                                "action": "release",
                                "started": started,
                                "time": time.time(),
                            }
                        )
                result = runner.wait_for_result(arn, timeout=10)
                assert result.status.value == "SUCCEEDED"
                history = runner.get_execution_history(
                    arn, include_execution_data=True
                ).to_dict()["Events"]
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    complete = [
                        record
                        for record in ledger.observations()
                        if record["execution"] == arn
                        and record["observation"]["kind"] == "export_quiescence"
                    ]
                    if complete:
                        break
                    time.sleep(0.001)
                assert complete, "No observed export/publication quiescence"
                entry = next(
                    record
                    for record in ledger.records()
                    if record["execution"] == arn and record["phase"] == "WRAPPER_ENTER"
                )
                request = entry["request"]
                context.update_attempt(role, requestId=request, executionArn=arn)
                cloud.request_roles[request] = role
                cloud.invocations.append(
                    {
                        "marker": role,
                        "arn": arn,
                        "payload": payload,
                        "role": role,
                        "trace_header": HEADER,
                        "native_requests": [request],
                    }
                )
                cloud.raw_histories[arn] = history
                cloud.execution_results[arn] = {
                    "Status": result.execution_status.value,
                    "Result": result.result,
                }
                held_count += sum(
                    record["execution"] == arn
                    and record["observation"]["kind"] == "export_held"
                    for record in ledger.observations()
                )
        for gate in ("release-all", "export-gate"):
            started = time.time()
            ledger.put_object(
                Bucket="private-evidence", Key=prefix + gate, Body=b"release"
            )
            cloud.control_records.append(
                {
                    "gate": gate,
                    "action": "release",
                    "started": started,
                    "time": time.time(),
                }
            )
        raw = ledger.records()
        cloud.events = {str(index): record for index, record in enumerate(raw)}
        cloud.source_uris = {
            "memory://actual-sdk-ledger/" + key
            for key in ledger.objects
            if "/events/" in key
        }
        (tmp_path / "events.json").write_text(json.dumps(raw))
        time.sleep(0.002)
        cloud.observation_ended_at = time.time()
        cloud.retired_at = time.time()
        evidence = build_evidence(context, cloud, phase="final")
        assert evidence["collectionErrors"] == []
        assert validate(evidence) == []
        if scenario == "held-export":
            # Later legitimate exports happen after the gate has opened. They
            # stay in evidence without being falsely labeled as held work.
            assert held_count == 1
            assert (
                len(
                    [
                        event
                        for event in evidence["events"]
                        if event["kind"] == "export_started"
                    ]
                )
                > held_count
            )
    finally:
        for runtime in telemetry._RUNTIMES.values():
            runtime.provider.shutdown()
        telemetry._RUNTIMES.clear()
