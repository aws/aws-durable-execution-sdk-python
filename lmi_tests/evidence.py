# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Assertions on service and external evidence, including negative controls."""


class ProvisioningError(RuntimeError):
    """Unsupported configuration, unavailable capacity, or failed deployment."""


class PlacementError(RuntimeError):
    """The required runtime placement was not established; never a test pass."""


class CollectionError(RuntimeError):
    """Evidence is missing/unavailable; distinct from an SDK regression assertion."""


def select(events, phase, marker=None):
    return [
        e
        for e in events
        if e["phase"] == phase and (marker is None or e["marker"] == marker)
    ]


def overlap(events, markers, count, environment=None):
    blocked = [e for e in select(events, "BLOCKED") if e["marker"] in markers]
    for start in blocked:
        env = start["environment"]
        if environment is not None and env != environment:
            continue
        active = []
        for candidate in blocked:
            if candidate["environment"] != env or candidate["time"] > start["time"]:
                continue
            alive = [
                e
                for e in events
                if e["phase"] in {"ALIVE", "EFFECT"}
                and all(
                    e.get(k) == candidate.get(k)
                    for k in ("request", "environment", "process", "gate")
                )
                and e["time"] >= candidate["time"]
            ]
            # Missing exit records (e.g. a retired process) do not prove liveness.
            if not alive or max(e["time"] for e in alive) < start["time"]:
                continue
            exits = [
                e
                for e in select(events, "IO_EXIT")
                if e["request"] == candidate["request"]
                and e.get("gate") == candidate.get("gate")
                and e["time"] >= candidate["time"]
            ]
            if not exits or min(e["time"] for e in exits) > start["time"]:
                active.append(candidate)
        if (
            len({e["request"] for e in active}) >= count
            and len({e["process"] for e in active}) >= count
            and len({e["pid"] for e in active}) >= count
            and len({e["execution"] for e in active}) >= count
        ):
            return env
    raise PlacementError(f"No {count}-process overlap in one execution environment")


def lifecycle(events):
    returned = select(events, "WRAPPER_RETURN") + select(events, "WRAPPER_RAISE")
    assert returned, "No wrapper exit observed"
    assert not select(events, "ESCAPE"), "Test emergency timeout released SDK work"
    for end in returned:
        local = [e for e in events if e["request"] == end["request"]]
        begins = select(local, "WRAPPER_ENTER")
        assert len(begins) == 1, "Missing/duplicate wrapper entry"
        baseline = {t["id"] for t in begins[0]["resources"]["threads"]}
        residual = [
            t
            for t in end["resources"]["threads"]
            if t["id"] not in baseline
            and (
                t["name"].startswith("dex-")
                or t["name"].startswith("ThreadPoolExecutor")
            )
        ]
        assert not residual, (
            f"Invocation-owned workers survived wrapper exit: {residual}"
        )
        assert any(
            e["sequence"] < end["sequence"] for e in select(local, "USER_EXIT")
        ), "User code still running"
        assert not [
            e
            for e in local
            if e["sequence"] > end["sequence"]
            and e["phase"]
            in {
                "BODY",
                "EFFECT",
                "CHECKPOINT_CALL",
                "CHECKPOINT_ACK",
                "CHECKPOINT_EXIT",
                "POLL_CALL",
                "POLL_EXIT",
            }
        ], "Late invocation activity"


def replay(events, history, marker):
    assert len({e["request"] for e in select(events, "WRAPPER_ENTER")}) >= 2, (
        "No real replay"
    )
    assert any(e["status"] == "PENDING" for e in select(events, "WRAPPER_RETURN")), (
        "No suspension"
    )
    for name, terminal in [("success", "StepSucceeded"), ("failure", "StepFailed")]:
        bodies = [e for e in select(events, "BODY") if e["operation"] == name]
        assert len(bodies) == 1 and bodies[0]["value"] == marker, (
            f"Completed {name} repeated/contaminated"
        )
        entries = [e for e in history if e.get("Name") == name]
        assert sum(e["EventType"] == terminal for e in entries) == 1, (
            f"Missing {terminal}"
        )
        assert len({e["Id"] for e in entries}) == 1, "Operation identity changed"
    failures = select(events, "STORED_FAILURE")
    assert len(failures) >= 2 and all(
        e["message"] == "expected:" + marker and e["error_type"] == "ValueError"
        for e in failures
    )
    attempts = [
        e["attempt"] for e in select(events, "BODY") if e["operation"] == "retry"
    ]
    assert attempts == [1, 2], f"Unexpected retry side effects: {attempts}"
    assert {"WaitSucceeded", "CallbackSucceeded", "ContextSucceeded"} <= {
        e["EventType"] for e in history
    }
    lifecycle(events)


def early_completion(events, grace):
    winners = select(events, "WINNER_SELECTED")
    assert winners, "Completion policy did not select a winner"
    winner = winners[0]
    returns = [
        e for e in select(events, "WRAPPER_RETURN") if e["request"] == winner["request"]
    ]
    assert returns, "#741: result computed but wrapper pinned by losing branch"
    assert returns[0]["time"] <= winner["time"] + grace, "#741: cleanup exceeded grace"
    assert not [
        e for e in select(events, "EFFECT") if e["time"] > winner["time"] + grace
    ], "#741: abandoned scope still performs side effects"
    lifecycle(events)


def deadline(events, request, grace):
    local = [e for e in events if e["request"] == request]
    entry = select(local, "WRAPPER_ENTER")[0]
    cutoff = entry["deadline"] + grace
    assert not [
        e
        for e in local
        if e["time"] > cutoff
        and e["phase"] in {"BODY", "EFFECT", "CHECKPOINT_CALL", "CHECKPOINT_ACK"}
    ], "#741: work continued after invocation deadline + cleanup grace"
    assert not select(local, "ESCAPE"), "Emergency release is not SDK deadline handling"
    # A runtime-supported worker retirement can have no final diagnostic. Recovery
    # and full-capacity placement must therefore also be asserted by the driver.


def warm_resources(events, min_reuses=3):
    processes = {}
    for event in select(events, "WRAPPER_RETURN"):
        processes.setdefault((event["environment"], event["process"]), []).append(event)
    reused = [
        sorted(v, key=lambda e: e["time"])
        for v in processes.values()
        if len(v) >= min_reuses
    ]
    if not reused:
        raise PlacementError("No observed warm Python process reuse")
    for samples in reused:
        # First invocation initializes legitimate client/serialization caches.
        baseline = samples[1]["resources"]
        for sample in samples[2:]:
            current = sample["resources"]
            assert len(current["threads"]) <= len(baseline["threads"]) + 1
            assert current["fds"] <= baseline["fds"] + 8, "File descriptors accumulate"
            assert current["rss"] <= baseline["rss"] + 32 * 1024 * 1024, (
                "RSS grew beyond cache allowance"
            )
    lifecycle(events)
