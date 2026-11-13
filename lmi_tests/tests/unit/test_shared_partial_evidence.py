# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Partial observations remain separate from facts collected after cleanup."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch


from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from lmi_tests.shared_adapter import SharedAdapter, _Case
from lmi_tests.shared_evidence import build_evidence


def context():
    profile = Profile("c1", "function", 1, 1, "sdk", "configuration")
    return ScenarioContext(
        "run",
        Scenario(
            "case",
            "lmi-lifecycle-3",
            "c1",
            "deadline-during-step",
            roles=("victim", "retry"),
            coverage_profile="standard",
        ),
        profile,
    )


def adapter(tmp_path, *, read_fails=False):
    calls = []
    cloud = SimpleNamespace(observation_ended_at=None)

    def read(**options):
        calls.append(("read", options))
        if read_fails:
            raise OSError("source unavailable")

    cloud.collect = read
    cloud._save = lambda name, value: calls.append(("save", name, deepcopy(value)))
    cloud.begin_retirement = lambda: calls.append(("retirement",))
    cloud.release_all = lambda: calls.append(("release",))
    driver = SimpleNamespace(
        cloud=cloud,
        collect=Mock(side_effect=AssertionError("must not wait for quiescence")),
        retire=lambda: True,
    )

    def build(ctx, observed, *, phase):
        calls.append(("build", phase))
        return {
            "events": [{"kind": "effect"}]
            if phase == "observations"
            else [{"kind": "effect"}, {"kind": "task_exit"}],
            "collection": {"watermarks": {"lifecycle": 1}},
            "collectionErrors": [],
            **({"retirement": {"resources": []}} if phase == "final" else {}),
        }

    result = SharedAdapter(
        {"sdk": {"deployment": {}, "artifacts_dir": str(tmp_path)}},
        ownership=Mock(),
        evidence_builder=build,
    )
    result.cases["case"] = _Case(driver)
    return result, cloud, calls


def test_partial_capture_reads_once_without_waiting_for_cleanup(tmp_path):
    value, cloud, calls = adapter(tmp_path)
    partial = asyncio.run(value.collect_partial(context()))
    assert [call[0] for call in calls] == ["read", "build", "save"]
    assert calls[0] == ("read", {"full_run": False})
    assert calls[-1][1] == "shared-partial-evidence.json"
    assert cloud.observation_ended_at is not None
    assert partial["events"] == [{"kind": "effect"}]


def test_partial_read_error_keeps_cached_observations_but_no_complete_watermark(
    tmp_path,
):
    value, _, _ = adapter(tmp_path, read_fails=True)
    partial = asyncio.run(value.collect_partial(context()))
    assert partial["events"] == [{"kind": "effect"}]
    assert partial["collection"]["watermarks"] == {}
    assert any("source unavailable" in error for error in partial["collectionErrors"])


def test_sdk_retirement_does_not_replace_the_pre_release_events(tmp_path):
    async def exercise():
        value, _, _ = adapter(tmp_path)
        ctx = context()
        ctx.evidence = deepcopy(await value.collect_partial(ctx))
        await value.release(ctx)
        assert await value.retire(ctx) is True
        assert ctx.evidence["events"] == [{"kind": "effect"}]
        assert ctx.retirement_evidence["events"] == [
            {"kind": "effect"},
            {"kind": "task_exit"},
        ]
        assert ctx.evidence["retirement"] == ctx.retirement_evidence["retirement"]

    asyncio.run(exercise())


def test_final_read_bound_alone_does_not_invent_quiescence(tmp_path):
    ctx = context()
    ctx.scenario = Scenario(
        "case",
        "lmi-lifecycle-1",
        "c1",
        "native-overlap",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud = SimpleNamespace(
        manifest={"invocationTimeout": 60, "conformanceRevision": "conformance"},
        observation_ended_at=None,
        retired_at=2,
        source_uris={"memory://source"},
        control_records=[],
        invocations=[],
        artifacts_dir=tmp_path,
        _save=lambda *args: None,
    )
    event = {
        "kind": "task_start",
        "taskId": "root",
        "requestId": "request",
        "ownerRequestId": "request",
        "executionArn": "execution",
        "atMs": 1000,
    }
    sources = SimpleNamespace(
        observations=lambda: ([event], []),
        histories=lambda _: ([], {}, []),
        attempts=lambda _: (
            [{"requestId": "request", "executionArn": "execution"}],
            {"victim": "request"},
            [],
        ),
    )
    cloud.execution_results = {"execution": {"Status": "STOPPED"}}
    with patch("lmi_tests.shared_evidence.CaseSources.capture", return_value=sources):
        evidence = build_evidence(ctx, cloud, phase="final")
    assert evidence["observationEndedAtMs"] == 2000
    assert evidence["collection"]["watermarks"] == {}
    assert evidence["collectionErrors"]
    invocation = next(
        item
        for item in evidence["retirement"]["resources"]
        if item["kind"] == "invocation"
    )
    assert invocation["outcome"] == "UNKNOWN"
