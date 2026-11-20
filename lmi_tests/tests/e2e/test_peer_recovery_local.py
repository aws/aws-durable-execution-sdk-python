"""The real SDK checkpoints preparation before the controlled recovery fault."""

import json
import time

import pytest

from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import fixture
from lmi_tests.tests.unit.test_shared_observations import install_ledger, shared_payload


def test_preparation_gate_precedes_fault_and_keeps_one_original_barrier(monkeypatch):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("deadline")
    payload.update(
        sharedScenario="peer-recovery", preparation_gate="prepare", gate="fault"
    )
    prefix = payload["controlPrefix"]
    for gate in ("prepare", "fault", "release-all"):
        ledger.objects[prefix + gate] = b"hold"
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        arn = runner.run_async(input=json.dumps(payload), execution_timeout=15)
        prepared = ledger.wait_for("blocked")["observation"]
        assert prepared["gateId"] == "prepare"
        assert not [
            e
            for e in ledger.observations()
            if e["observation"]["kind"] == "barrier_arrived"
        ]
        assert not [
            e
            for e in ledger.observations()
            if e["observation"]["kind"] == "work_in_flight"
        ]
        ledger.objects[prefix + "prepare"] = b"release"
        fault = ledger.wait_for("work_in_flight")["observation"]
        assert fault["gateId"] == "fault"
        ledger.objects[prefix + "fault"] = b"release"
        result = runner.wait_for_result(arn, timeout=15)
    assert result.result == '"marker"'
    observations = [event["observation"] for event in ledger.observations()]
    barriers = [event for event in observations if event["kind"] == "barrier_arrived"]
    assert len(barriers) == 1 and barriers[0]["gateId"] == "prepare"
    released = next(
        e
        for e in observations
        if e["kind"] == "fault_released" and e["gateId"] == "prepare"
    )
    assert barriers[0]["atMs"] == released["atMs"]
    assert prepared["atMs"] < barriers[0]["atMs"] <= fault["atMs"]
    checkpoint = next(
        event
        for event in observations
        if event["kind"] == "checkpoint_ack"
        and result.get_step("prepare-recovery").operation_id in event["operationIds"]
    )
    assert prepared["atMs"] < checkpoint["atMs"] <= fault["atMs"]
    assert not [event for event in observations if event["kind"] == "observation_error"]


def test_service_retry_replays_preparation_without_reinjecting_the_original_fault(
    monkeypatch,
):
    from aws_durable_execution_sdk_python.exceptions import RetryableSerDesError

    ledger = install_ledger(monkeypatch)
    payload = shared_payload("deadline")
    payload.update(
        sharedScenario="peer-recovery", preparation_gate="prepare", gate="fault"
    )
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + "prepare"] = b"release"
    ledger.objects[prefix + "fault"] = b"hold"
    ledger.objects[prefix + "release-all"] = b"hold"
    gate = fixture.Trace.gate
    fault_requests = []

    def interrupted(trace, name, **options):
        if name == "fault":
            fault_requests.append(trace.identity["request"])
            # Trigger the SDK's actual invocation-retry path after preparation
            # checkpointed, leaving controlled-io incomplete for replay.
            raise RetryableSerDesError("transient interruption")
        return gate(trace, name, **options)

    monkeypatch.setattr(fixture.Trace, "gate", interrupted)
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        result = runner.run(input=json.dumps(payload), execution_timeout=20)
    assert result.result == '"marker"'
    assert len(fault_requests) == 1
    observations = ledger.observations()
    raised = next(
        e["observation"]
        for e in observations
        if e["request"] == fault_requests[0]
        and e["observation"]["kind"] == "wrapper_raise"
    )
    original_probes = [
        e["observation"]
        for e in observations
        if e["request"] == fault_requests[0]
        and e["observation"]["kind"] == "admission_probe"
    ]
    assert original_probes and all(e["atMs"] >= raised["atMs"] for e in original_probes)
    skipped = [
        e for e in observations if e["observation"]["kind"] == "fault_not_reapplied"
    ]
    assert len(skipped) == 1
    assert skipped[0]["request"] != fault_requests[0]
    assert skipped[0]["observation"]["originalRequestId"] == fault_requests[0]
    assert not [
        e
        for e in observations
        if e["request"] == skipped[0]["request"]
        and e["observation"]["kind"] == "admission_probe"
    ]


@pytest.mark.parametrize("role", ["peer", "recovery"])
def test_off_target_cohort_member_uses_normal_control_for_driver_classification(
    monkeypatch, role
):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("barrier")
    payload.update(
        role=role,
        sharedScenario="peer-recovery",
        target_environment="different-environment",
        gate=role,
    )
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + role] = ledger.objects[prefix + "release-all"] = b"hold"
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        arn = runner.run_async(input=json.dumps(payload), execution_timeout=15)
        try:
            blocked = ledger.wait_for("blocked")
            assert blocked["observation"]["gateId"] == role
            assert blocked["environment"] != payload["target_environment"]
        finally:
            ledger.objects[prefix + "release-all"] = b"release"
        result = runner.wait_for_result(arn, timeout=15)
    assert result.result == '"marker"'
    steps = {row["observation"].get("name") for row in ledger.observations()}
    assert "controlled-io" in steps
    assert "placement-admission" not in steps
    assert "misplaced-peer-control" not in steps


@pytest.mark.parametrize("aborted", [False, True])
def test_staggered_native_overlap_attests_only_after_both_heartbeats(
    monkeypatch, aborted
):
    ledger = install_ledger(monkeypatch)
    victim = shared_payload("barrier")
    victim.update(sharedScenario="native-overlap", gate="cohort")
    peer = {**victim, "role": "peer", "marker": "peer"}
    prefix = victim["controlPrefix"]
    ledger.objects[prefix + "cohort"] = ledger.objects[prefix + "release-all"] = b"hold"

    def wait_for_pair():
        limit = time.monotonic() + 10
        while time.monotonic() < limit:
            raw = ledger.records()
            blocked = {row["role"]: row for row in raw if row["phase"] == "BLOCKED"}
            if len(blocked) == 2:
                peer_blocked = blocked["peer"]["time"]
                if {
                    row["role"]
                    for row in raw
                    if row["phase"] == "ALIVE" and row["time"] >= peer_blocked
                } == {"victim", "peer"}:
                    return blocked
            time.sleep(0.01)
        raise AssertionError("Original cohort did not publish two ordered heartbeats")

    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        original = runner.run_async(input=json.dumps(victim), execution_timeout=15)
        try:
            ledger.wait_for("alive")
            second = runner.run_async(input=json.dumps(peer), execution_timeout=15)
            blocked = wait_for_pair()
            assert blocked["victim"]["time"] < blocked["peer"]["time"]
            assert not any(
                row["observation"]["kind"] == "barrier_arrived"
                for row in ledger.observations()
            )
            gate = "release-all" if aborted else "cohort"
            ledger.objects[prefix + gate] = b"release"
            assert runner.wait_for_result(original, timeout=15).result == '"marker"'
            assert runner.wait_for_result(second, timeout=15).result == '"peer"'
        finally:
            ledger.objects[prefix + "release-all"] = b"release"

    arrivals = [
        row
        for row in ledger.observations()
        if row["observation"]["kind"] == "barrier_arrived"
    ]
    expected_roles = [] if aborted else ["peer", "victim"]
    assert sorted(row["role"] for row in arrivals) == expected_roles
    assert all(
        row["observation"]["atMs"] > blocked["peer"]["time"] * 1000 for row in arrivals
    )


def test_lost_io_exit_evidence_preserves_real_gate_completion(monkeypatch):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("barrier")
    payload["gate"] = "gate"
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + "gate"] = b"release"
    ledger.objects[prefix + "release-all"] = b"hold"
    put = ledger.put_object

    def store(**kwargs):
        if json.loads(kwargs["Body"])["phase"] == "IO_EXIT":
            raise OSError("evidence store unavailable")
        return put(**kwargs)

    monkeypatch.setattr(ledger, "put_object", store)
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        result = runner.run(input=json.dumps(payload), execution_timeout=15)
    assert result.result == '"marker"'
    assert any(
        row["observation"]["kind"] == "fault_released" for row in ledger.observations()
    )
    assert not any(
        row["observation"]["kind"] == "io_exit" for row in ledger.observations()
    )
    assert not any(row["phase"] == "SEALED" for row in ledger.records())
