# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Cleanup never uses a stale receipt or mutates work before ownership checks."""

import hashlib
import io
import json
from unittest.mock import Mock

import pytest

from lmi_tests import shared
from lmi_tests.shared_lock import OwnershipError


def test_stale_receipt_cannot_skip_current_ownership_verification(
    monkeypatch, tmp_path
):
    deployment = {"run": "same-run", "sharedLease": {"token": "current"}}
    lease = Mock()
    lease.to_manifest.return_value = deployment["sharedLease"]
    lease._read.side_effect = OwnershipError("current ownership must be verified")
    monkeypatch.setattr(shared.RunLease, "from_manifest", lambda value: lease)
    receipt = tmp_path / "ownership-released.json"
    receipt.write_text(json.dumps({"runId": "same-run", "lease": {"token": "old"}}))
    with pytest.raises(OwnershipError):
        shared.cleanup(deployment, tmp_path)
    lease._read.assert_called_once()
    lease._read.reset_mock()
    receipt.write_text(json.dumps({"runId": "same-run", "lease": {"token": "current"}}))
    assert shared.cleanup(deployment, tmp_path) == 0
    lease._read.assert_not_called()


@pytest.mark.parametrize("foreign", [True, False])
def test_case_ownership_is_checked_before_control_release(
    monkeypatch, tmp_path, foreign
):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    owned = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "sharedLease": {},
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
    }
    lease = Mock()
    lease._read.return_value = ({}, "etag")
    lease._owned.return_value = {"profiles": {target: "active"}}
    monkeypatch.setattr(shared.RunLease, "from_manifest", lambda value: lease)
    case = {"case_id": "case", "cohort_id": "cohort", "profile_id": "c1"}
    plan = {
        "runId": "run",
        "profiles": [{"profile_id": "c1", "resource_id": target}],
        "scenarios": [case],
    }
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(json.dumps(plan))
    identity = hashlib.sha256(b"case:cohort").hexdigest()[:20]
    invocations = output / "source" / "cases" / identity / "invocations"
    invocations.mkdir(parents=True)
    (invocations / "intent.json").write_text(
        json.dumps(
            {
                "payload": {"run": "run", "caseId": "case"},
                "arn": "arn:foreign" if foreign else owned,
            }
        )
    )
    storage, service = Mock(), Mock()
    prefix = f"runs/run/cases/{identity}/control/"
    storage.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": prefix + "release-all"}]}
    ]
    storage.get_object.return_value = {"Body": io.BytesIO(b"hold")}
    service.get_durable_execution.return_value = {"Status": "RUNNING"}
    monkeypatch.setattr(
        shared, "client", lambda name, region: storage if name == "s3" else service
    )
    assert shared.cleanup(deployment, tmp_path) == 1
    if foreign:
        storage.put_object.assert_not_called()
        service.stop_durable_execution.assert_not_called()
    else:
        storage.put_object.assert_called_once_with(
            Bucket="private", Key=prefix + "release-all", Body=b"release"
        )
        service.stop_durable_execution.assert_called_once_with(
            DurableExecutionArn=owned
        )
    lease.finish_profile.assert_called_once_with(target, False)
    lease.release.assert_not_called()


def test_failed_lease_acquisition_skips_cleanup_without_touching_another_run(
    monkeypatch, tmp_path
):
    from_manifest = Mock(side_effect=AssertionError("No lease was acquired"))
    client = Mock(side_effect=AssertionError("Unowned resources must not be touched"))
    monkeypatch.setattr(shared.RunLease, "from_manifest", from_manifest)
    monkeypatch.setattr(shared, "client", client)
    assert shared.cleanup({"run": "new-run", "functions": {}}, tmp_path) == 0
    assert (
        json.loads((tmp_path / "cleanup.json").read_text())["status"]
        == "SKIPPED_NO_LEASE"
    )
    assert not (tmp_path / "ownership-released.json").exists()
    from_manifest.assert_not_called()
    client.assert_not_called()


@pytest.mark.parametrize("evidence", ["owned", "functions", "case-plan"])
def test_missing_lease_with_prior_activity_is_not_treated_as_unacquired(
    monkeypatch, tmp_path, evidence
):
    deployment = {"run": "run", "functions": {}}
    if evidence == "case-plan":
        output = tmp_path / "standard"
        output.mkdir()
        (output / "manifest.json").write_text("{}")
    else:
        deployment[evidence] = True if evidence == "owned" else {"c1": "arn:owned"}
    client = Mock(
        side_effect=AssertionError("Cannot mutate resources without ownership")
    )
    monkeypatch.setattr(shared, "client", client)
    with pytest.raises(OwnershipError, match="cannot verify ownership"):
        shared.cleanup(deployment, tmp_path)
    client.assert_not_called()
