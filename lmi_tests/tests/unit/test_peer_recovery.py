"""Recovery scheduling uses original native deadlines, not late I/O or retries."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from lmi_tests import shared_scenarios
from lmi_tests.evidence import PlacementError
from lmi_tests.shared_scenarios import ScenarioDriver
from lmi_tests.tests.unit.test_shared_adapter import ScriptCloud


class RecoveryCloud(ScriptCloud):
    def __init__(self):
        super().__init__(2)
        self.now = 0.0
        self.events = []
        self.retiring = False
        self.configuration = "c2"
        self.manifest["driverTimeout"] = 120

    def emit(self, item, phase, **extra):
        request = item["native_requests"][0]
        value = {
            "phase": phase,
            "time": self.now,
            "deadline": item["deadline"],
            "marker": item["marker"],
            "request": request,
            "execution": item["arn"],
            "environment": "environment",
            "process": item["process"],
            "pid": item["pid"],
            "gate": item.get("held_gate"),
            "sequence": 1 + sum(e["request"] == request for e in self.events),
            **extra,
        }
        self.events.append(value)
        return value

    def start(self, fixture, **options):
        item = super().start(fixture, **options)
        role = options["role"]
        item.update(
            native_requests=[f"request-{role}"],
            deadline=self.now + 60,
            process="peer-worker" if role == "peer" else "victim-worker",
            pid=2 if role == "peer" else 1,
            payload={"scenario": fixture, **options},
            held_gate=options.get("preparation_gate") or options.get("gate"),
        )
        self.emit(item, "WRAPPER_ENTER")
        self.now += 3 if role == "recovery" else 0.25
        self.emit(item, "BLOCKED")
        return item

    def phase(self, item, phase, **options):
        return [
            e
            for e in self.events
            if e["marker"] == item["marker"] and e["phase"] == phase
        ]

    def for_item(self, item):
        return [e for e in self.refresh() if e["marker"] == item["marker"]]

    def refresh(self, **options):
        # Deliberately allow I/O liveness after a native deadline: this must not
        # be accepted as proof that the original invocation is still healthy.
        for item in self.invocations:
            if not item.get("finished"):
                self.emit(item, "ALIVE")
        return list(self.events)

    def poll(self, predicate, seconds=30, **options):
        deadline = self.now + seconds
        while True:
            value = predicate()
            if value:
                return value
            if self.now >= deadline:
                raise AssertionError(options.get("message", "poll timed out"))
            self.now = min(deadline, self.now + 0.5)

    def release(self, gate):
        super().release(gate)
        for item in self.invocations:
            if item["payload"].get("preparation_gate") == gate:
                self.emit(item, "IO_EXIT")
                item["held_gate"] = item["payload"]["gate"]
                self.emit(item, "BLOCKED")
                self.emit(
                    item,
                    "CONFORMANCE",
                    observation={"kind": "work_in_flight", "atMs": self.now * 1000},
                )

    def finish(self, item, status="SUCCEEDED"):
        assert self.now < item["deadline"], "The selected original invocation expired"
        super().finish(item, status)
        self.now += 0.25
        self.emit(item, "WRAPPER_RETURN", status=status)
        item["finished"] = True

    def wait_terminal(self, item):
        assert self.retiring, "An active victim must reach assertions before cleanup"
        return super().wait_terminal(item)

    def wait(self, seconds):
        self.now += seconds

    def collect(self, **options):
        self.actions.append(("collect",))

    def settle_case(self):
        self.retiring = True
        self.actions.append(("settle",))
        self.wait_terminal(self.invocations[0])


def recovery_driver(monkeypatch):
    cloud = RecoveryCloud()
    monkeypatch.setattr(
        shared_scenarios, "time", SimpleNamespace(time=lambda: cloud.now)
    )
    profile = Profile("c2", "function", 2, 2, "sdk", "configuration")
    scenario = Scenario(
        "case",
        "lmi-lifecycle-4",
        "c2",
        "peer-recovery",
        roles=("victim", "peer", "recovery"),
    )
    return ScenarioDriver(ScenarioContext("run", scenario, profile), cloud), cloud


def test_peer_has_recovery_headroom_and_fault_is_armed_after_cohort_placement(
    monkeypatch,
):
    driver, cloud = recovery_driver(monkeypatch)
    driver.start()
    assert [i["marker"] for i in cloud.invocations] == ["victim"]
    driver.place()
    victim, peer = cloud.invocations
    assert peer["deadline"] - victim["deadline"] >= 30
    armed = next(
        e
        for e in cloud.events
        if e.get("observation", {}).get("kind") == "work_in_flight"
    )
    peer_ready = next(
        e for e in cloud.events if e["phase"] == "BLOCKED" and e["marker"] == "peer"
    )
    assert peer_ready["time"] <= armed["time"] < victim["deadline"]
    driver.execute()
    assert ("finish", "peer", "SUCCEEDED") in cloud.actions
    assert not any(action[0] == "terminal" for action in cloud.actions)
    assert ("release", driver.gate("fault")) not in cloud.actions
    driver.collect()
    driver.quiescent = Mock(return_value=True)
    driver.retire()
    assert cloud.actions.index(("collect",)) < cloud.actions.index(("settle",))


def test_late_io_and_a_peer_retry_cannot_replace_the_expired_original(monkeypatch):
    driver, cloud = recovery_driver(monkeypatch)
    driver.start()
    driver.place()
    peer = driver.items["peer"]
    cloud.now = peer["deadline"] + 1
    driver.launch("recovery", "barrier", gate=driver.gate("recovery"))
    original = peer["native_requests"][0]
    peer["native_requests"].append("peer-retry")
    cloud.events.append(
        {
            **next(e for e in cloud.events if e["request"] == original),
            "request": "peer-retry",
            "time": cloud.now,
            "deadline": cloud.now + 60,
        }
    )
    with pytest.raises(PlacementError, match="Original peer invocation expired"):
        driver.wait_overlap(("peer", "recovery"), "environment")


def test_original_peer_cannot_expire_while_waiting_for_recovery_completion(monkeypatch):
    driver, cloud = recovery_driver(monkeypatch)
    driver.start()
    driver.place()
    finish = cloud.finish

    def delayed_finish(item, status="SUCCEEDED"):
        finish(item, status)
        if item["marker"] == "recovery":
            cloud.now = driver.peer_boundary["deadline"] + 1

    cloud.finish = delayed_finish
    with pytest.raises(PlacementError, match="Original healthy peer expired"):
        driver.execute()
    assert ("release", driver.gate("peer")) not in cloud.actions


def test_expired_victim_does_not_start_a_peer_or_arm_the_fault(monkeypatch):
    driver, cloud = recovery_driver(monkeypatch)
    driver.start()
    cloud.now = cloud.invocations[0]["deadline"] + 1
    with pytest.raises(PlacementError, match="Original victim expired"):
        driver.place()
    assert [item["marker"] for item in cloud.invocations] == ["victim"]
    assert ("release", driver.gate("prepare")) not in cloud.actions
