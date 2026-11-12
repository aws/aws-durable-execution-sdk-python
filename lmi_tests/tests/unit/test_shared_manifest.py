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
            "aws_durable_execution_sdk_python_otel-2.0.0.whl": "otel-sha",
        },
        "sharedLease": {
            "bucket": "private",
            "key": "ownership/stack/shared-run.json",
            "runId": "run-1",
            "token": "fence",
            "resources": ["c1", "c2"],
        },
    }


@pytest.mark.parametrize(
    "profile,count", [("python-lifecycle", 76), ("export-lifecycle", 24)]
)
def test_generated_manifest_covers_every_required_slot(tmp_path, profile, count):
    source = deployment()
    plan = make_manifest(source, profile, tmp_path)
    assert len(plan["scenarios"]) == count
    assert {case["profile_id"] for case in plan["scenarios"]} == {"c1", "c2"}
    assert plan["sdk"]["deployment"]["conformanceRevision"] == CONFORMANCE_REVISION
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan
    plan["sdk"]["deployment"]["commit"] = "mutated"
    assert source["commit"] == "sdk-commit"


def test_no_unowned_or_unidentified_artifact_can_be_planned(tmp_path):
    source = deployment()
    source.pop("sharedLease")
    with pytest.raises(ValueError, match="ownership"):
        make_manifest(source, "python-lifecycle", tmp_path)
    source = deployment()
    source["wheels"] = {}
    with pytest.raises(ValueError, match="wheel"):
        make_manifest(source, "python-lifecycle", tmp_path)


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
