# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real LMI regressions: #741 assertions intentionally fail until the SDK fix."""

import time
import uuid

import pytest

from lmi_tests import evidence


def test_checkpoint_replay_callback_and_retry(cloud):
    item = cloud.start("replay")
    callback = item["runner"].wait_for_callback(
        item["arn"], name="completion", timeout=90
    )
    item["runner"].send_callback_success(callback, result=item["marker"].encode())
    history = cloud.finish(item)
    evidence.replay(cloud.for_item(item), history, item["marker"])


def capacity_barrier(cloud, fixture="normal", environment=None, seconds=30):
    count = cloud.manifest["concurrency"]
    gate = "capacity-" + uuid.uuid4().hex
    items = [cloud.start("barrier", fixture, gate) for _ in range(count)]
    env, events = wait_overlap(cloud, items, count, environment, seconds)
    cloud.release(gate)
    for item in items:
        cloud.finish(item)
        evidence.lifecycle(cloud.for_item(item))
    return env, evidence.select(events, "BLOCKED")


def wait_overlap(cloud, items, count, environment=None, seconds=30):
    markers = {i["marker"] for i in items}

    def observed():
        events = [e for e in cloud.refresh() if e["marker"] in markers]
        if environment is not None and any(
            e["environment"] != environment for e in evidence.select(events, "BLOCKED")
        ):
            raise evidence.PlacementError(
                "Recovery probes reached a replacement/unrelated environment"
            )
        try:
            return evidence.overlap(events, markers, count, environment), events
        except evidence.PlacementError:
            return None

    return cloud.poll(
        observed,
        seconds=seconds,
        category=AssertionError if environment is not None else evidence.PlacementError,
        message="#741: full original-environment worker capacity did not recover"
        if environment is not None
        else "Live worker overlap was not established in the required environment",
    )


def test_actual_environment_process_concurrency(cloud):
    # c1 is a genuine baseline; c2 must establish different PIDs/process UUIDs
    # and distinct durable execution ARNs inside the same /tmp environment.
    capacity_barrier(cloud)


def test_warm_success_failure_and_suspension_cleanup(cloud):
    items = []
    for scenario in ["success", "failure", "replay"] * 3:
        item = cloud.start(scenario)
        items.append(item)
        if scenario == "replay":
            callback = item["runner"].wait_for_callback(
                item["arn"], name="completion", timeout=90
            )
            item["runner"].send_callback_success(
                callback, result=item["marker"].encode()
            )
        cloud.finish(item, "FAILED" if scenario == "failure" else "SUCCEEDED")
    markers = {i["marker"] for i in items}
    evidence.warm_resources([e for e in cloud.refresh() if e["marker"] in markers])


@pytest.mark.parametrize("scenario", ["parallel", "map", "nested"])
def test_early_completion_reclaims_losing_branches(cloud, scenario):
    item = cloud.start(scenario)
    winner = cloud.phase(item, "WINNER_SELECTED")[0]
    # Observation is deliberately completed BEFORE any driver release signal.
    cutoff = winner["time"] + cloud.manifest["cleanupGrace"]
    cloud.poll(lambda: time.time() >= cutoff + 1, seconds=15)
    events = cloud.for_item(item)
    evidence.early_completion(events, cloud.manifest["cleanupGrace"])
    cloud.finish(item)


def test_real_synchronous_checkpoint_settles_before_branch_join(cloud):
    item = cloud.start("checkpoint")
    cloud.phase(item, "BLOCKED")
    held = cloud.for_item(item)
    evidence.checkpoint_held(held, item["marker"] + "-checkpoint")
    cloud.release(item["marker"] + "-checkpoint")
    cloud.finish(item)
    evidence.lifecycle(cloud.for_item(item))


def test_invocation_deadline_isolation_and_capacity_recovery(cloud):
    fault = cloud.start("deadline", "deadline", "fault-" + uuid.uuid4().hex)
    blocked = cloud.phase(fault, "BLOCKED")[0]
    if cloud.manifest["concurrency"] > 1:
        healthy_gate = "healthy-" + uuid.uuid4().hex
        healthy = cloud.start("barrier", "deadline", healthy_gate)
        cloud.phase(healthy, "BLOCKED")
        wait_overlap(cloud, [fault, healthy], 2, blocked["environment"])
        cloud.release(healthy_gate)
        cloud.finish(healthy)
        evidence.lifecycle(cloud.for_item(healthy))
    # Queue recovery demand at the runtime deadline, before waiting for the
    # eventually consistent service history. History delivery latency must not
    # consume the worker recovery budget.
    cloud.poll(lambda: time.time() >= blocked["deadline"] - 1, seconds=15)
    # Put every worker slot under demand without releasing the fault. A different
    # environment cannot satisfy recovery, even if the service replaces capacity.
    recovery_started = time.time()
    try:
        _env, probes = capacity_barrier(
            cloud, "deadline", blocked["environment"], seconds=15
        )
    finally:
        cloud.platform_timeout(fault, blocked["request"])
    bound = blocked["deadline"] + cloud.manifest["cleanupGrace"]
    if recovery_started > bound:
        raise evidence.CollectionError(
            "Driver was too late to measure the recovery budget"
        )
    assert max(e["time"] for e in probes) <= bound, (
        f"#741: worker recovery exceeded deadline + grace; probes started at {recovery_started}"
    )
    cloud.poll(lambda: time.time() > bound + 1, seconds=15)
    evidence.deadline(
        cloud.for_item(fault), blocked["request"], cloud.manifest["cleanupGrace"]
    )


def test_service_timeout_retry_does_not_repeat_completed_step(cloud):
    fault = cloud.start("deadline", "deadline", "retry-" + uuid.uuid4().hex)
    first = cloud.phase(fault, "BLOCKED")[0]
    cloud.platform_timeout(fault, first["request"])
    # Diagnose stale side effects before waiting for a retry that may itself be
    # unable to start because the old attempt has pinned the only worker.
    cutoff = first["deadline"] + cloud.manifest["cleanupGrace"]
    cloud.poll(lambda: time.time() > cutoff + 1, seconds=15)
    evidence.deadline(
        cloud.for_item(fault), first["request"], cloud.manifest["cleanupGrace"]
    )

    def retry_observed():
        entries = evidence.select(cloud.for_item(fault), "WRAPPER_ENTER")
        return entries if len({e["request"] for e in entries}) >= 2 else None

    # Only a retry of the SAME durable execution satisfies this assertion. A new
    # execution started by pytest must not be mislabeled as a service replay.
    cloud.poll(
        retry_observed,
        seconds=40,
        message="Service retry precondition unavailable: no second request for the same execution",
    )
    events = cloud.for_item(fault)
    completed = [
        e for e in evidence.select(events, "BODY") if e["operation"] == "success"
    ]
    assert len(completed) == 1, "Completed side effect repeated across timeout/retry"
    assert {e["execution"] for e in events} == {fault["arn"]}
    evidence.deadline(events, first["request"], cloud.manifest["cleanupGrace"])
    # Interrupted blocked-io effects are recorded per request, not incorrectly
    # constrained to exactly-once under at-least-once step semantics.


def start_healthy_anchor(cloud):
    if cloud.manifest["concurrency"] == 1:
        return None, None
    gate = "anchor-" + uuid.uuid4().hex
    item = cloud.start("barrier", gate=gate)
    cloud.phase(item, "BLOCKED")
    return item, gate


def healthy_during(cloud, anchor, victim, boundary):
    if anchor is None:
        return
    wait_overlap(cloud, [anchor, victim], 2)
    cloud.poll(
        lambda: [
            e
            for e in cloud.for_item(anchor)
            if e["phase"] == "ALIVE" and e["time"] >= boundary["time"]
        ],
        message="Healthy peer did not progress while victim cleanup was held",
    )


def finish_anchor(cloud, anchor, gate):
    if anchor is not None:
        cloud.release(gate)
        cloud.finish(anchor)
        evidence.lifecycle(cloud.for_item(anchor))


def test_pending_waits_for_root_finally_and_then_replays(cloud):
    anchor, anchor_gate = start_healthy_anchor(cloud)
    item = cloud.start("suspend-cleanup")
    blocked = cloud.phase(item, "BLOCKED")[0]
    healthy_during(cloud, anchor, item, blocked)
    gate = item["marker"] + "-cleanup"
    evidence.scope_exit(cloud.for_item(item), gate)
    cloud.release(gate)
    history = cloud.finish(item)
    evidence.suspension_cleanup(cloud.for_item(item), history)
    finish_anchor(cloud, anchor, anchor_gate)


def test_nested_single_lane_pools_progress_for_all_runtime_workers(cloud):
    gate = "progress-" + uuid.uuid4().hex
    items = [
        cloud.start("nested-progress", gate=gate)
        for _ in range(cloud.manifest["concurrency"])
    ]
    wait_overlap(cloud, items, len(items))
    cloud.release(gate)
    for item in items:
        cloud.phase(
            item,
            "PROGRESS",
            seconds=20,
            category=AssertionError,
            message="Nested child/map/parallel work starved behind its own branch pool",
        )
        cloud.finish(item)
        evidence.progress(cloud.for_item(item), item["marker"])


@pytest.mark.parametrize(
    "scenario,status",
    [("return-inflight", "SUCCEEDED"), ("failure-inflight", "FAILED")],
)
def test_root_return_or_failure_settles_inflight_work(cloud, scenario, status):
    anchor, anchor_gate = start_healthy_anchor(cloud)
    item = cloud.start(scenario)
    cloud.phase(item, "WINNER_SELECTED")
    root_exit = cloud.phase(item, "USER_EXIT")[0]
    healthy_during(cloud, anchor, item, root_exit)
    gate = item["marker"] + "-loser"
    evidence.held_loser(cloud.for_item(item))
    evidence.scope_exit(cloud.for_item(item), gate)
    cloud.release(gate)
    cloud.finish(item, status)
    if status == "FAILED":
        failure = cloud.lam.get_durable_execution(DurableExecutionArn=item["arn"])[
            "Error"
        ]
        assert (
            failure["ErrorType"] == "ValueError"
            and failure["ErrorMessage"] == "expected:" + item["marker"]
        )
    evidence.scope_exit(cloud.for_item(item), gate, status)
    finish_anchor(cloud, anchor, anchor_gate)


def test_abandoned_child_rejects_late_durable_operation(cloud):
    item = cloud.start("late-operation")
    cloud.phase(item, "WINNER_SELECTED")
    evidence.held_loser(cloud.for_item(item))
    cloud.release(item["marker"] + "-loser")
    cloud.poll(
        lambda: [
            e
            for e in cloud.for_item(item)
            if e["phase"] in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
        ],
        message="Residual branch did not settle after release",
    )
    history = cloud.history(item)
    evidence.late_operation(cloud.for_item(item), history)
    cloud.finish(item)
