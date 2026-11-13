# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Ownership, native-request journaling and cancellation of the shared driver."""

import asyncio
import json
from pathlib import Path
from threading import Event
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from lmi_tests import cloud as cloud_module
from lmi_tests import shared_adapter as adapter_module
from lmi_tests.evidence import CollectionError, PlacementError, ProvisioningError
from lmi_tests.shared_scenarios import ScenarioDriver


FUNCTION = "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"
EXECUTION = FUNCTION + "/durable-execution/name/id"
PROFILE = Profile("native-1", FUNCTION, 1, 1, "sdk-sha", "configuration-sha")


def context(
    case_id="case",
    scenario="replay-callback-retry",
    roles=("victim", "resume"),
    *,
    native=1,
    view=None,
    mode=None,
):
    profile = Profile(
        f"native-{native}", FUNCTION, native, native, "sdk-sha", "configuration-sha"
    )
    return ScenarioContext(
        "run",
        Scenario(
            case_id,
            "lmi-lifecycle-16" if mode else "python-lmi-5",
            profile.profile_id,
            scenario,
            roles=roles,
            otel_view=view,
            export_mode=mode,
            coverage_profile="export-lifecycle" if mode else "python-lifecycle",
        ),
        profile,
    )


def deployment():
    return {
        "functions": {"c1": FUNCTION, "c2": FUNCTION + "-c2"},
        "concurrencies": {"c1": 1, "c2": 2},
        "region": "us-west-2",
        "run": "run",
        "bucket": "private-evidence",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
        "driverTimeout": 1,
    }


@pytest.fixture
def cloud(monkeypatch, tmp_path):
    clients = {name: Mock() for name in ("lambda", "s3", "logs")}
    clients["lambda"].invoke.return_value = {
        "StatusCode": 202,
        "DurableExecutionArn": EXECUTION,
    }
    clients["s3"].get_paginator.return_value.paginate.return_value = []
    monkeypatch.setattr(cloud_module, "client", lambda service: clients[service])
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python_testing.runner.boto3.client",
        lambda *a, **kw: clients["lambda"],
    )
    return cloud_module.Cloud(
        deployment(), "c1", artifacts_dir=tmp_path / "case", case_context=context()
    )


def enter(item, request, at):
    return {
        "phase": "WRAPPER_ENTER",
        "marker": item["marker"],
        "request": request,
        "execution": EXECUTION,
        "time": at,
        "sequence": 0,
        "run": "run",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
    }


def refresh_entries(cloud, *events):
    cloud.events.update({f"events/{event['request']}": event for event in events})
    return cloud.refresh()


def test_direct_launch_is_journaled_before_public_runner_transport(cloud):
    observed = []

    def invoke(**kwargs):
        observed.append(kwargs)
        assert len(cloud.case_context.attempts) == 1
        assert "requestId" not in cloud.case_context.attempts[0]
        return {"StatusCode": 202, "DurableExecutionArn": EXECUTION}

    cloud.lam.invoke.side_effect = invoke
    item = cloud.start(
        "replay",
        role="victim",
        resume_roles=("resume",),
        dispatcher_payload=cloud.case_context.dispatcher("victim"),
    )
    payload = json.loads(observed[0]["Payload"])
    assert observed[0]["FunctionName"] == FUNCTION
    assert payload["scenario"] == "replay"
    assert payload["sharedScenario"] == "replay-callback-retry"
    assert payload["caseId"] == "case"
    assert payload["controlPrefix"] == cloud.control_prefix
    assert "/cases/" in cloud.control_prefix
    assert item["native_requests"] == []
    refresh_entries(cloud, enter(item, "original", 1))
    assert cloud.case_context.attempts[0]["requestId"] == "original"


def test_service_replays_are_journaled_without_another_invoke(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(
        cloud,
        enter(item, "first", 1),
        enter(item, "second", 2),
        enter(item, "third", 3),
    )
    assert cloud.lam.invoke.call_count == 1
    assert [
        (attempt["role"], attempt["attempt"], attempt["requestId"])
        for attempt in cloud.case_context.attempts
    ] == [("victim", 1, "first"), ("resume", 1, "second"), ("resume", 2, "third")]
    assert cloud.request_roles == {
        "first": "victim",
        "second": "resume",
        "third": "resume",
    }
    cloud.refresh()
    assert len(cloud.case_context.attempts) == 3


def test_lost_invoke_response_retains_launch_and_recovers_identity(cloud):
    cloud.lam.invoke.side_effect = TimeoutError("response lost")
    with pytest.raises(CollectionError, match="API failed"):
        cloud.start("success", role="victim")
    item = cloud.invocations[0]
    assert item["arn"] is None
    record = json.loads(
        (cloud.artifacts_dir / f"invocations/{item['marker']}.json").read_text()
    )
    assert "response lost" in record["error"]
    refresh_entries(cloud, enter(item, "original", 1))
    assert item["arn"] == EXECUTION
    assert cloud.case_context.attempts[0]["requestId"] == "original"
    assert cloud.lam.invoke.call_count == 1


def test_replacement_execution_cannot_rebind_a_launched_marker(cloud):
    item = cloud.start("success", role="victim")
    event = enter(item, "replacement", 1)
    event["execution"] = EXECUTION + "-other"
    with pytest.raises(CollectionError, match="original durable execution"):
        refresh_entries(cloud, event)


def test_case_controls_and_collection_cannot_touch_another_case(cloud, tmp_path):
    cloud.hold("own-gate")
    with pytest.raises(CollectionError, match="not owned"):
        cloud.release("another-gate")
    with pytest.raises(CollectionError, match="another case"):
        cloud.refresh(markers=["another-marker"])
    with pytest.raises(CollectionError, match="another case"):
        cloud.refresh(markers=[])
    with pytest.raises(CollectionError, match="only its own"):
        cloud.collect(full_run=True)
    cloud.release_all()
    keys = [call.kwargs["Key"] for call in cloud.s3.put_object.call_args_list]
    assert all(key.startswith(cloud.control_prefix) for key in keys)
    assert not (tmp_path / "events.json").exists()


def test_unlaunched_case_does_not_scan_shared_event_prefix(cloud):
    assert cloud.refresh() == []
    cloud.s3.get_paginator.assert_not_called()


def test_cancellation_stops_waits_and_new_transports(cloud):
    cloud.cancel()
    with pytest.raises(CollectionError, match="cancelled"):
        cloud.wait(30)
    with pytest.raises(CollectionError, match="cancelled"):
        cloud.start("success", role="victim")
    cloud.lam.invoke.assert_not_called()
    cloud.begin_retirement()
    with pytest.raises(CollectionError, match="retiring"):
        cloud.start("success", role="victim")


class FakeCloud:
    instances = []

    def __init__(self, manifest, configuration, *, artifacts_dir, case_context=None):
        self.manifest, self.configuration = manifest, configuration
        self.artifacts_dir, self.case_context = Path(artifacts_dir), case_context
        self.cancel_event = Event()
        self.actions = []
        self.instances.append(self)

    def verify(self):
        self.actions.append("verify")

    def check_active(self):
        if self.cancel_event.is_set():
            raise CollectionError("cancelled")

    def cancel(self):
        self.cancel_event.set()

    def begin_retirement(self):
        self.actions.append("begin-retirement")
        self.cancel_event.clear()

    def release_all(self):
        self.actions.append("release")


def adapter(tmp_path, *, factory=FakeCloud, ownership=None):
    return adapter_module.SharedAdapter(
        {"sdk": {"deployment": deployment(), "artifacts_dir": str(tmp_path)}},
        ownership=ownership or Mock(spec=["assert_owned", "finish_profile", "release"]),
        evidence_builder=lambda context, cloud, *, phase: {
            "retirement": {"owner": context.scenario.case_id, "phase": phase}
        },
        cloud_factory=factory,
    )


def test_prepare_checks_persistent_ownership_and_actual_profile(tmp_path):
    owner = Mock(spec=["assert_owned", "finish_profile", "release"])
    value = adapter(tmp_path, ownership=owner)
    from lmi_tests.shared_contract import configuration_identity

    valid = Profile(
        PROFILE.profile_id,
        PROFILE.resource_id,
        1,
        1,
        "sdk-sha",
        configuration_identity(deployment(), "c1"),
    )
    asyncio.run(value.prepare(valid))
    with pytest.raises(ProvisioningError, match="configuration identity"):
        asyncio.run(value.prepare(PROFILE))
    assert all(call.args == (FUNCTION,) for call in owner.assert_owned.call_args_list)
    assert FakeCloud.instances[-1].actions == ["verify"]
    bad = Profile("bad", FUNCTION, 1, 2, "sdk-sha", "configuration-sha")
    with pytest.raises(ProvisioningError, match="Native concurrency"):
        asyncio.run(value.prepare(bad))


def test_lost_ownership_prevents_any_driver_launch(tmp_path):
    owner = Mock(spec=["assert_owned", "finish_profile", "release"])
    owner.assert_owned.side_effect = RuntimeError("lease lost")
    value = adapter(tmp_path, ownership=owner)
    with pytest.raises(RuntimeError, match="lease lost"):
        asyncio.run(value.start(context()))
    assert value.cases == {}


def test_cancelled_worker_is_joined_before_case_cleanup(monkeypatch, tmp_path):
    entered, unblock, exited = Event(), Event(), Event()

    class Driver:
        def __init__(self, context, cloud, **kwargs):
            self.cloud = cloud

        def start(self):
            entered.set()
            unblock.wait(2)
            try:
                self.cloud.check_active()
            finally:
                exited.set()

        def retire(self):
            assert exited.is_set()
            return True

    monkeypatch.setattr(adapter_module, "ScenarioDriver", Driver)

    async def exercise():
        value = adapter(tmp_path)
        ctx = context()
        starting = asyncio.create_task(value.start(ctx))
        assert await asyncio.to_thread(entered.wait, 1)
        starting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starting
        releasing = asyncio.create_task(value.release(ctx))
        await asyncio.sleep(0)
        assert "release" not in value.cases[ctx.scenario.case_id].driver.cloud.actions
        unblock.set()
        await releasing
        assert exited.is_set()
        assert await value.retire(ctx)
        assert ctx.evidence["retirement"] == {"owner": "case", "phase": "final"}

    asyncio.run(exercise())


def test_finish_records_quarantine_without_releasing_the_whole_run(tmp_path):
    owner = Mock(spec=["assert_owned", "finish_profile", "release"])
    value = adapter(tmp_path, ownership=owner)
    asyncio.run(value.finish(PROFILE, quiescent=False))
    owner.finish_profile.assert_called_once_with(FUNCTION, False)
    assert not owner.release.called


class ScriptCloud:
    def __init__(self, native=1):
        self.concurrency = native
        self.manifest = {"driverTimeout": 1, "invocationTimeout": 60}
        self.actions = []
        self.callback_records = []
        self.invocations = []
        self.raw_histories = {}
        self.source_uris = set()

    def record_deadline_measurement(self, snapshot):
        self.actions.append(("measurement", snapshot))

    def wait_terminal(self, item):
        self.actions.append(("terminal", item["marker"]))
        return {"Status": "FAILED"}

    def refresh(self, **kwargs):
        return [event for item in self.invocations for event in self.for_item(item)]

    def start(self, fixture, **kwargs):
        item = {
            "marker": kwargs["role"],
            "arn": f"execution:{kwargs['role']}",
            "runner": Mock(),
        }
        item["runner"].wait_for_callback.return_value = "callback-id"
        self.actions.append(("start", fixture, kwargs))
        self.invocations.append(item)
        return item

    def phase(self, item, phase, **kwargs):
        self.actions.append(("phase", item["marker"], phase))
        return [
            {
                "phase": phase,
                "time": 0,
                "deadline": 0,
                "environment": "environment",
                "request": "request",
                "status": "PENDING",
            }
        ]

    def poll(self, predicate, **kwargs):
        self.actions.append(("poll", kwargs.get("message")))
        return predicate() or [
            {"phase": "PLACEMENT_ACCEPTED", "environment": "environment"}
        ]

    def release(self, gate):
        self.actions.append(("release", gate))

    def finish(self, item, status="SUCCEEDED"):
        self.actions.append(("finish", item["marker"], status))

    def for_item(self, item):
        return [
            {
                "phase": "WRAPPER_RETURN",
                "status": "PENDING",
                "time": 1,
                "request": "first",
            },
            {"phase": "WRAPPER_ENTER", "request": "first"},
            {"phase": "WRAPPER_ENTER", "request": "second"},
            {
                "phase": "CONFORMANCE",
                "observation": {"kind": "flush_wait_completed", "outcome": "timeout"},
            },
            {"phase": "CONFORMANCE", "observation": {"kind": "export_completed"}},
            {"phase": "LATE_REJECTED"},
            {"phase": "ALIVE", "time": 2},
        ]

    def platform_timeout(self, item, request):
        self.actions.append(("platform-timeout", item["arn"]))

    def history(self, item):
        self.actions.append(("history", item["arn"]))

    def check_active(self):
        pass


def test_callback_uses_same_execution_and_actual_pending_before_response():
    cloud = ScriptCloud()
    driver = ScenarioDriver(context(), cloud)
    driver.start()
    driver.place()
    driver.execute()
    assert len(cloud.invocations) == 1
    runner = cloud.invocations[0]["runner"]
    runner.wait_for_callback.assert_called_once_with(
        "execution:victim", name="completion", timeout=1
    )
    runner.send_callback_success.assert_called_once_with(
        "callback-id", result=b"victim"
    )
    assert cloud.callback_records[0]["executionArn"] == "execution:victim"


@pytest.mark.parametrize(
    "scenario,fixture",
    [
        ("early-completion-parallel", "parallel"),
        ("early-completion-map", "map"),
        ("early-completion-nested", "nested"),
    ],
)
def test_early_completion_observations_do_not_release_losing_fault(scenario, fixture):
    cloud = ScriptCloud()
    driver = ScenarioDriver(context(scenario=scenario, roles=("victim",)), cloud)
    driver.start()
    driver.place()
    driver.execute()
    assert cloud.actions[0][1] == fixture
    assert not any(action[0] == "release" for action in cloud.actions)


@pytest.mark.parametrize("mode", ["sync", "async-bounded-flush"])
def test_held_async_export_observes_real_timeout_and_wrapper_before_release(mode):
    cloud = ScriptCloud()
    driver = ScenarioDriver(
        context(
            scenario="held-export", roles=("victim",), view="invocation", mode=mode
        ),
        cloud,
    )
    driver.start()
    driver.place()
    driver.execute()
    released = next(
        index for index, action in enumerate(cloud.actions) if action[0] == "release"
    )
    preceding = cloud.actions[:released]
    assert ("phase", "victim", "OTEL_EXPORT_HELD") in preceding
    assert (("phase", "victim", "WRAPPER_RETURN") in preceding) == (
        mode == "async-bounded-flush"
    )
    assert cloud.actions[0][2]["otel_export"] == "held"


def test_missed_original_placement_is_not_retried_as_another_victim():
    cloud = ScriptCloud(2)
    cloud.poll = lambda *args, **kwargs: [{"phase": "PLACEMENT_MISS"}]
    driver = ScenarioDriver(
        context(scenario="return-inflight", roles=("victim", "peer"), native=2), cloud
    )
    driver.start()
    with pytest.raises(PlacementError, match="Original victim"):
        driver.place()
    assert len(cloud.invocations) == 2


@pytest.mark.parametrize(
    "scenario,count", [("warm-lifecycle", 9), ("otel-warm-lifecycle", 6)]
)
def test_warm_sequence_uses_real_callback_resumes_not_extra_driver_invocations(
    scenario, count
):
    from lmi_tests.python_catalog import required_cases

    cell = next(
        case
        for case in required_cases("lifecycle-otel")
        if case.scenario == scenario and case.native_concurrency == 1
    )
    cloud = ScriptCloud()
    driver = ScenarioDriver(
        context(scenario=scenario, roles=cell.roles, view=cell.otel_view), cloud
    )
    driver.start()
    driver.place()
    driver.execute()
    launches = [action for action in cloud.actions if action[0] == "start"]
    assert len(launches) == count
    assert [action[1] for action in launches] == ["success", "failure", "replay"] * (
        count // 3
    )
    assert len(cloud.callback_records) == count // 3
    assert len({item["arn"] for item in cloud.invocations}) == count


@pytest.mark.parametrize("native", [1, 2])
def test_deadline_recovery_demands_original_environment_before_fault_release(native):
    roles = (
        ("victim", "recovery")
        if native == 1
        else ("victim", "peer", "recovery", "recovery-peer")
    )
    cloud = ScriptCloud(native)
    driver = ScenarioDriver(
        context(scenario="deadline-recovery", roles=roles, native=native), cloud
    )
    driver.wait_overlap = Mock(return_value=[])
    for role in roles:
        driver.context.record_attempt(
            role, role=role, requestId="request" if role == "victim" else role
        )
    driver.start()
    driver.place()
    driver.execute()
    expected = ("recovery",) if native == 1 else ("recovery", "recovery-peer")
    driver.wait_overlap.assert_called_with(expected, "environment")
    measured = next(
        index
        for index, action in enumerate(cloud.actions)
        if action[0] == "measurement"
    )
    released = next(
        index
        for index, action in enumerate(cloud.actions)
        if action == ("release", driver.gate("fault"))
    )
    assert measured < released
    assert ("platform-timeout", "execution:victim") in cloud.actions


def test_timeout_retry_never_launches_a_replacement_execution():
    cloud = ScriptCloud()
    driver = ScenarioDriver(
        context(scenario="timeout-retry", roles=("victim", "retry")), cloud
    )
    driver.context.record_attempt("original", role="victim", requestId="request")
    driver.context.record_attempt("retry", role="retry", requestId="second")
    driver.start()
    driver.place()
    driver.execute()
    assert len(cloud.invocations) == 1
    assert cloud.actions[0][2]["resume_roles"] == ("retry",)
    assert [action[1] for action in cloud.actions if action[0] == "release"] == [
        driver.gate("fault")
    ]


@pytest.mark.parametrize(
    "scenario,gate_suffix,expected_status",
    [
        ("checkpoint-settlement", "checkpoint", "SUCCEEDED"),
        ("pending-cleanup", "cleanup", "SUCCEEDED"),
        ("return-inflight", "loser", "SUCCEEDED"),
        ("failure-inflight", "loser", "FAILED"),
        ("late-operation", "loser", "SUCCEEDED"),
    ],
)
def test_scope_drivers_observe_real_boundary_before_releasing_its_control(
    scenario, gate_suffix, expected_status
):
    cloud = ScriptCloud()
    roles = ("victim", "resume") if scenario == "pending-cleanup" else ("victim",)
    driver = ScenarioDriver(context(scenario=scenario, roles=roles), cloud)
    driver.start()
    driver.place()
    driver.execute()
    release = next(
        index
        for index, action in enumerate(cloud.actions)
        if action == ("release", f"victim-{gate_suffix}")
    )
    finished = next(
        index
        for index, action in enumerate(cloud.actions)
        if action == ("finish", "victim", expected_status)
    )
    assert release < finished
    assert any(action[0] == "phase" for action in cloud.actions[:release])


def test_each_case_has_independent_artifact_namespace(monkeypatch, tmp_path):
    class Driver:
        def __init__(self, context, cloud, **kwargs):
            self.cloud = cloud

        def start(self):
            pass

    monkeypatch.setattr(adapter_module, "ScenarioDriver", Driver)

    async def exercise():
        value = adapter(tmp_path)
        await asyncio.gather(value.start(context("one")), value.start(context("two")))
        first = value.cases["one"].driver.cloud
        second = value.cases["two"].driver.cloud
        assert first.artifacts_dir != second.artifacts_dir
        assert first.case_context.scenario.case_id == "one"
        assert second.case_context.scenario.case_id == "two"
        with pytest.raises(CollectionError, match="replace"):
            await value.start(context("one"))

    asyncio.run(exercise())


def settled_cloud(*, view=None):
    cloud = ScriptCloud()
    cloud.invocations = [{"arn": "execution", "native_requests": ["native"]}]
    events = [
        {"phase": "WRAPPER_ENTER", "request": "native"},
        {"phase": "WRAPPER_RETURN", "request": "native"},
        {
            "phase": "CONFORMANCE",
            "observation": {
                "kind": "task_start",
                "taskId": "root",
                "requestId": "native",
            },
        },
        {
            "phase": "CONFORMANCE",
            "observation": {
                "kind": "task_exit",
                "taskId": "root",
                "requestId": "native",
            },
        },
    ]
    cloud.refresh = lambda **kwargs: events
    driver = ScenarioDriver(context(view=view), cloud)
    return driver, events


@pytest.mark.parametrize(
    "start,end,identity",
    [
        ("task_start", "task_exit", "taskId"),
        ("io_start", "io_exit", "taskId"),
        ("checkpoint_call", "checkpoint_exit", "callId"),
        ("poll_call", "poll_exit", "callId"),
        ("export_started", "export_completed", "exportId"),
        ("flush_started", "flush_completed", "flushId"),
    ],
)
def test_retirement_requires_every_real_task_call_and_export_to_exit(
    start, end, identity
):
    driver, events = settled_cloud()
    assert driver.quiescent()
    event = {"requestId": "native", identity: "outstanding"}
    events.append({"phase": "CONFORMANCE", "observation": {**event, "kind": start}})
    assert not driver.quiescent()
    events.append({"phase": "CONFORMANCE", "observation": {**event, "kind": end}})
    assert driver.quiescent()


def test_export_quiescence_needs_positive_publication_evidence_even_when_unsampled():
    driver, events = settled_cloud(view="invocation")
    assert not driver.quiescent()
    observation = {
        "kind": "export_quiescence",
        "requestId": "native",
        "pendingExportIds": [],
        "pendingFlushIds": [],
        "publicationCount": 1,
    }
    events.append({"phase": "CONFORMANCE", "observation": observation})
    assert not driver.quiescent()
    observation["publicationCount"] = 0
    assert driver.quiescent()


def test_unidentified_launch_or_missing_root_hook_cannot_claim_retirement():
    driver, events = settled_cloud()
    driver.cloud.invocations[0]["native_requests"] = []
    assert not driver.quiescent()
    driver.cloud.invocations[0]["native_requests"] = ["native"]
    events[:] = [
        event
        for event in events
        if event.get("observation", {}).get("kind") != "task_exit"
    ]
    assert not driver.quiescent()


def test_wrong_function_response_is_never_adopted_for_case_cleanup(cloud):
    cloud.lam.invoke.return_value = {
        "StatusCode": 202,
        "DurableExecutionArn": "arn:another-function/durable-execution/name/id",
    }
    with pytest.raises(CollectionError, match="API failed"):
        cloud.start("success", role="victim")
    assert cloud.invocations[0]["arn"] is None


def test_canonical_observations_keep_authoritative_outer_identity_and_hook_mismatch():
    from lmi_tests.shared_scenarios import canonical

    records = canonical(
        [
            {
                "phase": "CONFORMANCE",
                "request": "outer",
                "execution": "execution",
                "observation": {"kind": "task_exit", "taskId": "root"},
            },
            {
                "phase": "CONFORMANCE",
                "request": "outer",
                "execution": "execution",
                "observation": {
                    "kind": "invocation_start",
                    "requestId": "wrong-hook-request",
                },
            },
        ]
    )
    assert records[0]["requestId"] == records[0]["ownerRequestId"] == "outer"
    assert records[0]["executionArn"] == "execution"
    assert records[1]["requestId"] == "wrong-hook-request"
    assert records[1]["ownerRequestId"] == "outer"
