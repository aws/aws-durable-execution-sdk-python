import copy

import pytest

from lmi_tests import evidence


def event(phase, time=1, **fields):
    return {
        "phase": phase,
        "time": time,
        "sequence": int(time * 10),
        "marker": "a",
        "request": "r1",
        "execution": "exec1",
        "environment": "env1",
        "process": "p1",
        "pid": 10,
        "deadline": 10,
        "gate": "gate",
        **fields,
    }


def alive(events):
    return events + [
        {**e, "phase": "ALIVE", "time": max(v["time"] for v in events) + 1}
        for e in events
        if e["phase"] == "BLOCKED"
    ]


def test_overlap_requires_distinct_processes_requests_and_executions():
    events = [
        event("BLOCKED"),
        event(
            "BLOCKED",
            2,
            marker="b",
            process="p2",
            pid=11,
            request="r2",
            execution="exec2",
        ),
    ]
    assert evidence.overlap(alive(events), {"a", "b"}, 2) == "env1"
    with pytest.raises(evidence.PlacementError):
        evidence.overlap(events, {"a", "b"}, 2)  # starts alone do not prove overlap
    for key in ("process", "pid", "request", "execution"):
        invalid = copy.deepcopy(events)
        invalid[1][key] = invalid[0][key]
        with pytest.raises(evidence.PlacementError):
            evidence.overlap(alive(invalid), {"a", "b"}, 2)


def test_different_environment_and_sequential_invocations_cannot_pass():
    a = event("BLOCKED")
    b = event(
        "BLOCKED", 3, marker="b", process="p2", pid=11, request="r2", execution="exec2"
    )
    for events in ([a, {**b, "environment": "env2"}], [a, event("IO_EXIT", 2), b]):
        with pytest.raises(evidence.PlacementError):
            evidence.overlap(alive(events), {"a", "b"}, 2)
    with pytest.raises(evidence.PlacementError):
        evidence.overlap(alive([a, b]), {"a", "b"}, 2, environment="original-env")
