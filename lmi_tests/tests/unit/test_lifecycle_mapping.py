"""All seven shared contracts reach their actual SDK fixture and preserve audit facts."""

from copy import deepcopy
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi.matrix import required_cases
from lmi_tests.shared_scenarios import ScenarioDriver, FIXTURES
from lmi_tests.shared_evidence import _decoded, assertion_events, retirement_snapshot
from lmi_tests.tests.unit.test_shared_adapter import ScriptCloud, context


@pytest.mark.parametrize(
    "cell", required_cases("python", "lifecycle"), ids=lambda c: c.scenario
)
def test_each_shared_case_uses_its_required_original_roles(cell):
    cloud = ScriptCloud(cell.minimum_native_concurrency)
    driver = ScenarioDriver(
        context(
            scenario=cell.scenario,
            roles=cell.roles,
            native=cell.minimum_native_concurrency,
        ),
        cloud,
    )
    driver.wait_overlap = Mock(return_value=[])
    driver.start()
    driver.place()
    driver.execute()
    launches = [action for action in cloud.actions if action[0] == "start"]
    assert {item[2]["role"] for item in launches} == set(cell.roles) - {"resume"}
    assert (
        next(item for item in launches if item[2]["role"] == "victim")[1]
        == FIXTURES[cell.scenario]
    )
    assert all(
        "otel_view" not in item[2] and "export_mode" not in item[2] for item in launches
    )
    if cell.scenario == "peer-recovery":
        actions = cloud.actions
        recovery_finished = next(
            i for i, a in enumerate(actions) if a == ("finish", "recovery", "SUCCEEDED")
        )
        peer_released = actions.index(("release", driver.gate("peer")))
        assert recovery_finished < peer_released
        assert ("release", driver.gate("fault")) not in actions
    if cell.scenario == "normal-failure":
        assert ("finish", "victim", "FAILED") in cloud.actions


def test_scalar_and_wrapped_service_payloads_have_the_same_persisted_meaning():
    assert (
        _decoded({"Truncated": False, "Payload": '"value"'})
        == _decoded('"value"')
        == "value"
    )
    assert (
        _decoded({"Truncated": False, "Payload": "callback bytes"}) == "callback bytes"
    )
    assert _decoded('{"Payload":"business field"}') == {"Payload": "business field"}
    with pytest.raises(ValueError, match="Truncated"):
        _decoded({"Truncated": True, "Payload": '"partial"'})


@pytest.mark.parametrize(
    "scenario,target",
    [
        ("normal-return", "root"),
        ("normal-failure", "root"),
        ("warm-reuse", "late-callback"),
    ],
)
def test_singular_assertion_boundary_does_not_discard_other_task_lifetimes(
    scenario, target
):
    original = [
        {"kind": kind, "taskId": task, "requestId": "victim", "atMs": time}
        for task in ("root", "user-task-1", "late-callback")
        for kind, time in (("task_start", 1), ("task_exit", 2))
    ]
    retained = deepcopy(original)
    events = assertion_events(original, scenario, {"victim": "victim"})
    assert [e["taskId"] for e in events if e["kind"] == "task_exit"] == [target]
    assert len(events) == len(original)
    assert original == retained
    restored = retirement_snapshot({"events": events})["events"]
    assert [
        {k: v for k, v in e.items() if k != "sourceKind"} for e in restored
    ] == original
