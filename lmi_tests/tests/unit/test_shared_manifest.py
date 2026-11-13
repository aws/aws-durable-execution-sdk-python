# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Plans pin the shared matrix and immutable SDK/deployment inputs."""

import json
from unittest.mock import patch

import pytest

from aws_durable_execution_conformance_tests.lmi.app import load_manifest
from lmi_tests.shared import make_manifest, verify_checkout
from lmi_tests.shared_contract import CONFORMANCE_REVISION, configuration_identity


def deployment():
    return {
        "run": "run-1",
        "stack": "stack",
        "region": "us-west-2",
        "account": "123456789012",
        "runtime": "python3.14",
        "role": "arn:role",
        "bucket": "private",
        "commit": "sdk-commit",
        "provider": "arn:aws:lambda:us-west-2:123456789012:capacity-provider:provider",
        "codeSha256": "bundle-sha",
        "invocationTimeout": 60,
        "executionTimeout": 240,
        "driverTimeout": 120,
        "concurrencies": {"c1": 1, "c2": 2},
        "functions": {
            "c1": "arn:function:c1:$LATEST.PUBLISHED",
            "c2": "arn:function:c2:$LATEST.PUBLISHED",
        },
        "wheels": {
            "aws_durable_execution_sdk_python-3.0.0.whl": "core-sha",
        },
        "sharedLease": {
            "bucket": "private",
            "key": "ownership/stack/shared-run.json",
            "runId": "run-1",
            "token": "fence",
            "resources": ["c1", "c2"],
        },
    }


def test_generated_manifest_covers_the_seven_shared_scenarios(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.app import coverage_inventory

    source = deployment()
    plan = make_manifest(source, "standard", tmp_path)
    assert len(plan["scenarios"]) == 7
    assert {c["requirement_id"] for c in plan["scenarios"]} == {
        f"lmi-lifecycle-{i}" for i in range(1, 8)
    }
    assert {c["profile_id"] for c in plan["scenarios"]} == {"c1", "c2"}
    assert all(
        "otel_view" not in c and "export_mode" not in c for c in plan["scenarios"]
    )
    assert plan["sdk"]["deployment"]["conformanceRevision"] == CONFORMANCE_REVISION
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan
    assert coverage_inventory(plan)["missing"] == []
    assert (
        next(c for c in plan["scenarios"] if c["scenario"] == "normal-failure")[
            "requirement_id"
        ]
        == "lmi-lifecycle-7"
    )
    plan["sdk"]["deployment"]["commit"] = "mutated"
    assert source["commit"] == "sdk-commit"


def test_no_unowned_or_unidentified_artifact_can_be_planned(tmp_path):
    source = deployment()
    source.pop("sharedLease")
    with pytest.raises(ValueError, match="ownership"):
        make_manifest(source, "standard", tmp_path)
    source = deployment()
    source["wheels"] = {}
    with pytest.raises(ValueError, match="wheel"):
        make_manifest(source, "standard", tmp_path)


def test_configuration_identity_changes_with_the_tested_settings():
    source = deployment()
    original = configuration_identity(source, "c1")
    assert original != configuration_identity(source, "c2")
    source["codeSha256"] = "other-bundle"
    assert original != configuration_identity(source, "c1")


def test_only_the_exact_clean_shared_revision_is_accepted(tmp_path):
    with patch("lmi_tests.shared.subprocess.check_output", side_effect=["wrong\n"]):
        with pytest.raises(ValueError, match="pinned"):
            verify_checkout(tmp_path)
    with patch(
        "lmi_tests.shared.subprocess.check_output",
        side_effect=[CONFORMANCE_REVISION + "\n", " M packages/validator.py\n"],
    ):
        with pytest.raises(ValueError, match="local changes"):
            verify_checkout(tmp_path)
    with patch(
        "lmi_tests.shared.subprocess.check_output",
        side_effect=[CONFORMANCE_REVISION + "\n", ""],
    ):
        verify_checkout(tmp_path)
