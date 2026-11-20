"""Run the actual Lambda fixture through the public in-process SDK service."""

from __future__ import annotations

import json

import pytest
from collections import Counter

from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import fixture
from lmi_tests.tests.unit.test_shared_observations import install_ledger, shared_payload


def values(ledger, kind):
    return [
        record["observation"]
        for record in ledger.observations()
        if record["observation"]["kind"] == kind
    ]


def assert_sealed(ledger, request):
    records = [event for event in ledger.records() if event["request"] == request]
    seal = records[-1]
    assert seal["phase"] == "SEALED"
    assert seal["sourceProtocol"] == 2
    assert "observation" not in seal
    assert [event["phase"] for event in records].count("SEALED") == 1
    captures = sorted(
        event["observation"]["captureSequence"]
        for event in records
        if event["phase"] == "CONFORMANCE"
    )
    assert captures == list(range(1, seal["throughCapture"] + 1))
    assert [event["sequence"] for event in records] == list(range(1, len(records) + 1))
    return seal


def test_actual_fixture_public_hooks_bind_bodies_checkpoints_and_root_lifetime(
    monkeypatch,
):
    ledger = install_ledger(monkeypatch)
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        result = runner.run(input=json.dumps(shared_payload()), execution_timeout=30)
    assert result.result == '"marker"'
    assert values(ledger, "observation_error") == []
    assert_sealed(ledger, ledger.records()[0]["request"])
    body = values(ledger, "body")
    assert (
        len(body) == 1
        and body[0]["operationId"] == result.get_step("success").operation_id
    )
    assert body[0]["parentId"] == result.get_step("success").parent_id
    assert body[0]["operationId"] in {
        operation
        for ack in values(ledger, "checkpoint_ack")
        for operation in ack["operationIds"]
    }
    assert {item["callId"] for item in values(ledger, "checkpoint_call")} == {
        item["callId"] for item in values(ledger, "checkpoint_exit")
    }
    assert {item["taskId"] for item in values(ledger, "task_start")} == {
        item["taskId"] for item in values(ledger, "task_exit")
    }
    assert (
        next(item for item in values(ledger, "task_exit") if item["taskId"] == "root")[
            "atMs"
        ]
        <= values(ledger, "wrapper_return")[0]["atMs"]
    )


def test_actual_service_replay_keeps_operation_identity_and_skips_completed_bodies(
    monkeypatch,
):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("replay")
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        arn = runner.run_async(input=json.dumps(payload), execution_timeout=30)
        callback = runner.wait_for_callback(arn, name="completion", timeout=20)
        runner.send_callback_success(callback, result=b"marker")
        result = runner.wait_for_result(arn, timeout=20)
    assert result.result == '"marker"'
    assert values(ledger, "observation_error") == []
    bodies = values(ledger, "body")
    assert Counter(item["operation"] for item in bodies) == {
        "success": 1,
        "failure": 1,
        "retry": 2,
        "item": 3,
        "left": 1,
        "right": 1,
    }
    assert {
        item["operationId"] for item in bodies if item["operation"] == "success"
    } == {result.get_step("success").operation_id}
    starts = values(ledger, "invocation_start")
    assert len(starts) >= 2 and any(not item["isFirstInvocation"] for item in starts)
    replay = next(item for item in starts if not item["isFirstInvocation"])
    assert any(
        item["operationId"] == result.get_step("success").operation_id
        and item["status"] == "SUCCEEDED"
        for item in replay["operations"]
    )
    assert "PENDING" in {item["outcome"] for item in values(ledger, "wrapper_return")}
    observations = ledger.observations()
    for request in {event["request"] for event in observations}:
        assert_sealed(ledger, request)
    for record in observations:
        if record["observation"]["kind"] != "pending":
            continue
        wrapper = next(
            item["observation"]
            for item in observations
            if item["request"] == record["request"]
            and item["observation"]["kind"] == "wrapper_return"
        )
        assert record["observation"]["atMs"] == wrapper["atMs"]


def test_warm_invocation_observes_late_work_under_its_original_request(monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key

    ledger = install_ledger(monkeypatch)
    owner = shared_payload("warm-owner")
    owner["sharedScenario"] = "warm-reuse"
    owner["caseId"] = "local-warm-reuse"
    owner["controlPrefix"] = f"runs/unit/cases/{case_key(owner['caseId'])}/control/"
    ledger.objects[owner["controlPrefix"] + "release-all"] = b"hold"
    warm = {**owner, "scenario": "warm-probe", "role": "warm", "marker": "warm-marker"}
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        first = runner.run(input=json.dumps(owner), execution_timeout=15)
        assert not any(event["phase"] == "SEALED" for event in ledger.records())
        second = runner.run(input=json.dumps(warm), execution_timeout=15)
    assert first.result == '"marker"'
    assert second.result == '"warm-marker"'
    raw = ledger.records()
    original = next(
        e for e in raw if e["phase"] == "WRAPPER_RETURN" and e["role"] == "victim"
    )
    entered = next(
        e for e in raw if e["phase"] == "WRAPPER_ENTER" and e["role"] == "warm"
    )
    ended = next(
        e for e in raw if e["phase"] == "WRAPPER_RETURN" and e["role"] == "warm"
    )
    late = next(
        e for e in raw if e.get("observation", {}).get("kind") == "late_activity"
    )
    assert late["request"] == original["request"]
    assert late["execution"] == original["execution"]
    assert late["observation"]["observedDuringRequestId"] == entered["request"]
    assert original["time"] < entered["time"] <= late["time"] <= ended["time"]
    assert_sealed(ledger, entered["request"])
    # The real SDK can leave its post-closure operation probe waiting.
    # Such an owner must remain unsealed until its own worker exits.
    owner_rows = [event for event in raw if event["request"] == original["request"]]
    owner_exits = [
        event
        for event in owner_rows
        if event.get("observation", {}).get("kind") == "task_exit"
        and event["observation"].get("taskId") == "late-callback"
    ]
    assert any(event["phase"] == "SEALED" for event in owner_rows) == bool(owner_exits)


@pytest.mark.parametrize("lost_phase", ["WRAPPER_ENTER", "USER_EXIT", "STORED_FAILURE"])
def test_source_write_failure_preserves_real_suspend_and_resume(
    monkeypatch, lost_phase
):
    ledger = install_ledger(monkeypatch)
    put = ledger.put_object

    def store(**kwargs):
        if json.loads(kwargs["Body"])["phase"] == lost_phase:
            raise OSError("evidence store unavailable")
        return put(**kwargs)

    monkeypatch.setattr(ledger, "put_object", store)
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        arn = runner.run_async(
            input=json.dumps(shared_payload("replay")), execution_timeout=30
        )
        callback = runner.wait_for_callback(arn, name="completion", timeout=20)
        runner.send_callback_success(callback, result=b"marker")
        result = runner.wait_for_result(arn, timeout=20)
        history = runner.get_execution_history(
            arn, include_execution_data=True
        ).to_dict()["Events"]
    assert result.result == '"marker"'
    completion_ids = {
        event["InvocationCompletedDetails"]["RequestId"]
        for event in history
        if event["EventType"] == "InvocationCompleted"
    }
    assert len(completion_ids) > 1
    assert not any(row["phase"] == "SEALED" for row in ledger.records())
