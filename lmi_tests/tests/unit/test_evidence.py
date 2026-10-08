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


def resource_snapshot(extra=False):
    return {
        "threads": [{"id": 1, "name": "MainThread"}]
        + ([{"id": 2, "name": "dex-handler_0"}] if extra else []),
        "fds": 8,
        "rss": 1000,
    }


def lifecycle_events():
    return [
        event("WRAPPER_ENTER", resources=resource_snapshot()),
        event("WINNER_SELECTED", 2),
        event("USER_EXIT", 3),
        event("WRAPPER_RETURN", 4, status="SUCCEEDED", resources=resource_snapshot()),
    ]


def race_events():
    return lifecycle_events() + [
        event("BLOCKED", 1.1, gate="a-loser"),
        event("EFFECT", 1.2, gate="a-loser"),
        event("WINNER_READY", 1.3),
    ]


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


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "PENDING"])
def test_lifecycle_accepts_clean_success_failure_and_suspension(status):
    events = lifecycle_events()
    events[-1]["status"] = status
    evidence.lifecycle(events)


@pytest.mark.parametrize(
    "phase",
    [
        "BODY",
        "EFFECT",
        "CHECKPOINT_CALL",
        "CHECKPOINT_ACK",
        "CHECKPOINT_EXIT",
        "POLL_EXIT",
    ],
)
def test_late_activity_fails(phase):
    with pytest.raises(AssertionError, match="Late invocation"):
        evidence.lifecycle(lifecycle_events() + [event(phase, 5)])


def test_return_with_threads_or_without_user_exit_fails():
    events = lifecycle_events()
    events[-1]["resources"] = resource_snapshot(True)
    with pytest.raises(AssertionError, match="workers survived"):
        evidence.lifecycle(events)
    with pytest.raises(AssertionError, match="User code still running"):
        evidence.lifecycle([e for e in lifecycle_events() if e["phase"] != "USER_EXIT"])


def test_winner_without_wrapper_return_and_emergency_release_fail():
    with pytest.raises(AssertionError, match="wrapper pinned"):
        evidence.early_completion(
            [e for e in race_events() if e["phase"] != "WRAPPER_RETURN"], 5
        )
    with pytest.raises(AssertionError, match="emergency"):
        evidence.early_completion(race_events() + [event("ESCAPE", 3)], 5)
    with pytest.raises(AssertionError, match="grace"):
        evidence.early_completion(race_events(), 1)
    evidence.early_completion(race_events(), 5)


def test_early_completion_rejects_the_cloud_false_positive():
    with pytest.raises(evidence.CollectionError, match="not held"):
        evidence.early_completion(
            [e for e in race_events() if e["phase"] != "EFFECT"], 5
        )
    with pytest.raises(evidence.CollectionError, match="not held"):
        evidence.early_completion(
            race_events() + [event("IO_EXIT", 1.25, gate="a-loser")], 5
        )
    with pytest.raises(evidence.CollectionError, match="403"):
        evidence.early_completion(
            race_events()
            + [event("CONTROL_ERROR", gate="a-loser", error="403 Forbidden")],
            5,
        )
    # SDK cancellation after the winning step is ready, before the public
    # parallel call returns, is legitimate cleanup rather than a fixture error.
    evidence.early_completion(
        race_events() + [event("IO_EXIT", 1.5, gate="a-loser")], 5
    )


def test_checkpoint_hold_requires_real_blocking_and_correlates_the_request():
    held = [event("ACK_HELD", 1), event("BLOCKED", 2, gate="checkpoint")]
    evidence.checkpoint_held(held, "checkpoint")
    with pytest.raises(evidence.CollectionError, match="not held"):
        evidence.checkpoint_held(held[:1], "checkpoint")
    with pytest.raises(AssertionError, match="before the held"):
        evidence.checkpoint_held(held + [event("WRAPPER_RETURN", 3)], "checkpoint")
    # An unrelated request's return is not evidence that this waiter settled.
    evidence.checkpoint_held(
        held + [event("WRAPPER_RETURN", 3, request="other")], "checkpoint"
    )


@pytest.mark.parametrize(
    "phase", ["BODY", "EFFECT", "CHECKPOINT_CALL", "CHECKPOINT_ACK"]
)
def test_expired_invocation_cannot_continue_activity(phase):
    with pytest.raises(AssertionError, match="#741"):
        evidence.deadline([event("WRAPPER_ENTER"), event(phase, 16)], "r1", 5)
    # A distinct attempt may repeat interrupted at-least-once work.
    evidence.deadline([event("WRAPPER_ENTER"), event(phase, 16, request="r2")], "r1", 5)


def test_warm_reuse_is_required_and_resource_growth_fails():
    with pytest.raises(evidence.PlacementError):
        evidence.warm_resources(lifecycle_events())
    events = []
    for index in range(3):
        events += [
            {**e, "request": f"r{index}", "time": e["time"] + index * 10}
            for e in lifecycle_events()
        ]
    evidence.warm_resources(events)
    events[-1]["resources"]["fds"] = 100
    with pytest.raises(AssertionError, match="descriptors"):
        evidence.warm_resources(events)


def test_replay_rejects_repeated_bodies_changed_identity_and_missing_history():
    events = lifecycle_events()
    events[-1]["status"] = "PENDING"
    events += [
        {**e, "request": "r2", "time": e["time"] + 10} for e in lifecycle_events()
    ]
    events += [
        event("BODY", operation=name, value="a", attempt=attempt)
        for name, attempt in [
            ("success", 1),
            ("failure", 1),
            ("retry", 1),
            ("retry", 2),
        ]
    ]
    events += [
        event("STORED_FAILURE", message="expected:a", error_type="ValueError")
        for _ in range(2)
    ]
    history = [
        {"Name": "success", "Id": "s", "EventType": "StepSucceeded"},
        {"Name": "failure", "Id": "f", "EventType": "StepFailed"},
        *[
            {"EventType": kind}
            for kind in ("WaitSucceeded", "CallbackSucceeded", "ContextSucceeded")
        ],
    ]
    evidence.replay(events, history, "a")
    for bad_events, bad_history in [
        (events + [event("BODY", operation="success", value="a")], history),
        (events, history[1:]),
        (
            events,
            history
            + [{"Name": "success", "Id": "different", "EventType": "StepStarted"}],
        ),
    ]:
        with pytest.raises(AssertionError):
            evidence.replay(bad_events, bad_history, "a")


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "PENDING"])
def test_scope_exit_rejects_return_before_owned_work_exits(status):
    events = lifecycle_events() + [event("BLOCKED", 1.1)]
    events[3]["status"] = status
    with pytest.raises(AssertionError, match="still active"):
        evidence.scope_exit(events, "gate", status)
    evidence.scope_exit(events + [event("IO_EXIT", 3.5)], "gate", status)


def test_suspension_requires_finally_before_pending_and_preserves_replay():
    events = [
        event("WRAPPER_ENTER", resources=resource_snapshot()),
        event("BODY", 1.1, operation="success"),
        event("CLEANUP_ENTER", 1.2),
        event("CLEANUP_EXIT", 2),
        event("USER_EXIT", 2.1),
        event("WRAPPER_RETURN", 2.2, status="PENDING", resources=resource_snapshot()),
        event("WRAPPER_ENTER", 3, request="r2", resources=resource_snapshot()),
        event("USER_EXIT", 4, request="r2"),
        event(
            "WRAPPER_RETURN",
            5,
            request="r2",
            status="SUCCEEDED",
            resources=resource_snapshot(),
        ),
    ]
    history = [{"EventType": "WaitSucceeded"}]
    evidence.suspension_cleanup(events, history)
    wrong = [
        {**e, "sequence": 30} if e["phase"] == "CLEANUP_EXIT" else e for e in events
    ]
    with pytest.raises(AssertionError, match="finally"):
        evidence.suspension_cleanup(wrong, history)
    with pytest.raises(AssertionError, match="repeated"):
        evidence.suspension_cleanup(
            events + [event("BODY", 3.1, request="r2", operation="success")], history
        )


def test_progress_requires_correct_nested_results_within_budget():
    events = lifecycle_events() + [
        event("PROGRESS_BEGIN", 1.1),
        event("PROGRESS", 2, value=[["a:0", "a:1"], "a"]),
    ]
    evidence.progress(events, "a")
    with pytest.raises(AssertionError, match="crossed"):
        evidence.progress(events, "different")
    with pytest.raises(AssertionError, match="budget"):
        evidence.progress(events, "a", seconds=0.1)


def test_late_rejection_after_a_side_effect_is_not_a_pass():
    events = race_events() + [
        event("LATE_ATTEMPT", 2.1),
        event("LATE_REJECTED", 2.2, error="OrphanedChildException"),
    ]
    evidence.late_operation(events, [])
    with pytest.raises(AssertionError, match="late user side effect"):
        evidence.late_operation(
            events + [event("BODY", 2.15, operation="late-work")], []
        )
    with pytest.raises(AssertionError, match="accepted"):
        evidence.late_operation(events + [event("LATE_ACCEPTED", 2.2)], [])
    with pytest.raises(AssertionError, match="reached the service"):
        evidence.late_operation(
            events, [{"Name": "late-work", "EventType": "StepStarted"}]
        )
