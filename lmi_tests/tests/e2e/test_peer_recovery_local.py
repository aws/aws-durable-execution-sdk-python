"""The real SDK checkpoints preparation before the controlled recovery fault."""

import json

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
