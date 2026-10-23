"""Native LMI observability acceptance; controlled carriers stay in unit tests."""

import time
import uuid

import pytest

from lmi_tests import evidence, telemetry_evidence
from lmi_tests.tests.e2e import test_lifecycle as lifecycle


@pytest.fixture(params=["invocation", "execution"])
def view(request):
    return request.param


def header(sampled="1"):
    return (
        f"Root=1-{int(time.time()):08x}-{uuid.uuid4().hex[:24]};"
        f"Parent={uuid.uuid4().hex[:16]};Sampled={sampled}"
    )


def finish(cloud, item, *, scenario="success", export_outcome="success"):
    if scenario == "replay":
        callback = item["runner"].wait_for_callback(
            item["arn"], name="completion", timeout=90
        )
        item["runner"].send_callback_success(callback, result=item["marker"].encode())
    history = cloud.finish(item, "FAILED" if scenario == "failure" else "SUCCEEDED")
    events = cloud.for_item(item)
    telemetry_evidence.invocation(events, expected_outcome=export_outcome)
    telemetry_evidence.terminal(events, failed=scenario == "failure")
    first = evidence.select(events, "OTEL_ENTER")[0]
    actual = telemetry_evidence.parse_header(first["carrier"])
    sent = telemetry_evidence.parse_header(item["trace_header"])
    assert actual["trace_id"] == sent["trace_id"], (
        "Lambda did not preserve the caller's X-Ray Root"
    )
    assert actual["sampled"] == sent["sampled"], (
        "Lambda did not preserve explicit sampling"
    )
    if scenario == "replay":
        evidence.replay(events, history, item["marker"])
    return events


@pytest.mark.parametrize("sampled", ["0", "1"])
def test_native_runtime_carrier_and_sampling(cloud, view, sampled):
    item = cloud.start("success", otel_view=view, trace_header=header(sampled))
    finish(cloud, item)


def test_warm_process_cleanup_and_replay(cloud, view):
    events = []
    for scenario in ["success", "failure", "replay"] * 2:
        item = cloud.start(scenario, otel_view=view, trace_header=header())
        events.extend(finish(cloud, item, scenario=scenario))
    telemetry_evidence.warm_reuse(events)
    telemetry_evidence.invocation(events)


def test_concurrent_worker_attribution_and_peer_end(cloud, view):
    items = [
        cloud.start(
            "barrier",
            gate="otel-" + uuid.uuid4().hex,
            otel_view=view,
            trace_header=header(),
        )
        for _ in range(cloud.concurrency)
    ]
    lifecycle.wait_overlap(cloud, items, cloud.concurrency)
    for index, item in enumerate(items):
        blocked = cloud.phase(item, "BLOCKED")[0]
        cloud.release(blocked["gate"])
        finish(cloud, item)
        for peer in items[index + 1 :]:
            peer_events = cloud.for_item(peer)
            assert not evidence.select(peer_events, "OTEL_EXIT"), (
                "Ending A ended a held peer"
            )
            assert evidence.select(peer_events, "ALIVE"), (
                "No healthy-peer progress evidence"
            )


def test_export_failure_then_healthy_invocation(cloud, view):
    failed = cloud.start(
        "success", otel_view=view, trace_header=header(), otel_export="failure"
    )
    finish(cloud, failed, export_outcome="failure")
    healthy = cloud.start("success", otel_view=view, trace_header=header())
    finish(cloud, healthy)


def test_held_export_must_settle_before_successful_cleanup(cloud, view):
    item = cloud.start(
        "success", otel_view=view, trace_header=header(), otel_export="held"
    )
    cloud.phase(item, "OTEL_EXPORT_HELD")
    assert not evidence.select(cloud.for_item(item), "OTEL_EXIT")
    cloud.release(item["marker"] + "-export")
    finish(cloud, item, export_outcome="held")


@pytest.mark.parametrize(
    "case",
    [
        "deadline",
        "timeout-retry",
        "pending-cleanup",
        "late-operation",
        "return-inflight",
        "failure-inflight",
    ],
)
def test_plugin_enabled_lifecycle_boundaries(cloud, view, case, monkeypatch):
    # Run the original assertions with real OTel registration. Runtime defects
    # remain failures; neither cleanup nor instrumentation turns them into passes.
    original = cloud.start

    def start(*args, **kwargs):
        return original(*args, **kwargs, otel_view=view, trace_header=header())

    monkeypatch.setattr(cloud, "start", start)
    if case == "deadline":
        lifecycle.test_invocation_deadline_isolation_and_capacity_recovery(cloud)
    elif case == "timeout-retry":
        lifecycle.test_service_timeout_retry_does_not_repeat_completed_step(cloud)
    elif case == "pending-cleanup":
        lifecycle.test_pending_waits_for_root_finally_and_then_replays(cloud)
    elif case == "late-operation":
        lifecycle.test_abandoned_child_rejects_late_durable_operation(cloud)
    else:
        lifecycle.test_root_return_or_failure_settles_inflight_work(
            cloud, case, "FAILED" if case == "failure-inflight" else "SUCCEEDED"
        )
    for item in cloud.invocations:
        telemetry_evidence.invocation(cloud.for_item(item))
