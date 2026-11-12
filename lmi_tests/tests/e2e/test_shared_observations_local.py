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


def test_actual_checkpoint_response_settles_before_join_observation(monkeypatch):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("checkpoint")
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + "marker-checkpoint"] = b"hold"
    ledger.objects[prefix + "release-all"] = b"hold"
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        arn = runner.run_async(input=json.dumps(payload), execution_timeout=30)
        held = ledger.wait_for("ack_held")["observation"]
        ledger.wait_for("blocked")
        assert not [
            item
            for item in values(ledger, "checkpoint_exit")
            if item["callId"] == held["callId"]
        ]
        assert values(ledger, "branch_join") == []
        ledger.objects[prefix + "marker-checkpoint"] = b"release"
        result = runner.wait_for_result(arn, timeout=20)
    assert result.result == '"marker"'
    assert values(ledger, "observation_error") == []
    exited = next(
        item
        for item in values(ledger, "checkpoint_exit")
        if item["callId"] == held["callId"]
    )
    joined = values(ledger, "branch_join")[0]
    assert held["operationId"] in {
        op.operation_id
        for op in result.get_context("checkpoint-pool")
        .child_operations[0]
        .child_operations
    }
    assert joined["scopeId"] == result.get_context("checkpoint-pool").operation_id
    assert (
        exited["atMs"] <= joined["atMs"] <= values(ledger, "wrapper_return")[0]["atMs"]
    )


def test_actual_nested_public_calls_supply_configuration_ids(monkeypatch):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("nested-progress")
    payload["gate"] = "entry"
    ledger.objects[payload["controlPrefix"] + "entry"] = b"release"
    ledger.objects[payload["controlPrefix"] + "release-all"] = b"hold"
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        result = runner.run(input=json.dumps(payload), execution_timeout=30)
    assert result.result == '"marker"'
    assert values(ledger, "observation_error") == []
    configured = values(ledger, "operation_config")
    assert len(configured) == 2 and all(
        item["maxConcurrency"] == 1 for item in configured
    )
    started = {item["operationId"]: item for item in values(ledger, "operation_start")}
    assert {started[item["operationId"]]["name"] for item in configured} == {
        "inner-map",
        "outer-parallel",
    }
    assert values(ledger, "progress")[0]["value"] == [
        ["marker:0", "marker:1"],
        "marker",
    ]


def test_actual_parent_closure_is_captured_before_late_child_attempt(monkeypatch):
    ledger = install_ledger(monkeypatch)
    payload = shared_payload("late-operation")
    prefix = payload["controlPrefix"]
    ledger.objects[prefix + "marker-loser"] = b"hold"
    ledger.objects[prefix + "marker-loser-started"] = b"hold"
    ledger.objects[prefix + "release-all"] = b"hold"
    with DurableFunctionTestRunner(handler=fixture.handler) as runner:
        arn = runner.run_async(input=json.dumps(payload), execution_timeout=30)
        winner = ledger.wait_for("winner_selected")["observation"]
        ledger.objects[prefix + "marker-loser"] = b"release"
        runner.wait_for_result(arn, timeout=20)
    assert values(ledger, "observation_error") == []
    attempted = values(ledger, "late_attempt")[0]
    closed = next(
        item
        for item in values(ledger, "scope_closed")
        if item["scopeId"] == attempted["scopeId"]
    )
    assert closed["atMs"] <= winner["atMs"] < attempted["atMs"]
    # Observe the actual SDK result, without fixing or hiding a lifecycle defect.
    outcomes = values(ledger, "late_rejected") + values(ledger, "late_accepted")
    assert len(outcomes) == 1 and outcomes[0]["scopeId"] == attempted["scopeId"]
