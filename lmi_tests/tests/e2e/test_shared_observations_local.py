"""Run the actual Lambda fixture through the public in-process SDK service."""

from __future__ import annotations

import json
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


def test_actual_fixture_public_hooks_bind_bodies_checkpoints_and_root_lifetime(
    monkeypatch,
):
    ledger = install_ledger(monkeypatch)
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        result = runner.run(input=json.dumps(shared_payload()), execution_timeout=30)
    assert result.result == '"marker"'
    assert values(ledger, "observation_error") == []
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


def test_warm_invocation_observes_late_work_under_its_original_request(monkeypatch):
    ledger = install_ledger(monkeypatch)
    owner = shared_payload("warm-owner")
    owner["sharedScenario"] = "warm-reuse"
    warm = {**owner, "scenario": "warm-probe", "role": "warm", "marker": "warm-marker"}
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        first = runner.run(input=json.dumps(owner), execution_timeout=15)
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
