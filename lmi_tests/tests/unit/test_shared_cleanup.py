# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Run-scoped cleanup needs no distributed lock and never touches foreign work."""

import hashlib
import io
import json
from unittest.mock import Mock

import pytest

from lmi_tests import shared
from lmi_tests.evidence import CollectionError


def test_no_recorded_cases_skip_aws_cleanup(monkeypatch, tmp_path):
    client = Mock(side_effect=AssertionError("No case work was recorded"))
    monkeypatch.setattr(shared, "client", client)
    assert shared.cleanup({"run": "run", "functions": {}}, tmp_path) == 0
    assert json.loads((tmp_path / "cleanup.json").read_text())["status"] == "NO_CASES"
    client.assert_not_called()


@pytest.mark.parametrize("foreign", [True, False])
def test_case_identity_is_checked_before_control_release(
    monkeypatch, tmp_path, foreign
):
    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    owned = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
    }
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
    invocations = output / "source/cases" / identity / "invocations"
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
    storage.get_object.side_effect = lambda **kwargs: {"Body": io.BytesIO(b"hold")}
    service.get_durable_execution.return_value = {"Status": "RUNNING"}
    monkeypatch.setattr(
        shared, "client", lambda name, region: storage if name == "s3" else service
    )
    assert shared.cleanup(deployment, tmp_path) == int(foreign)
    report = json.loads((tmp_path / "cleanup.json").read_text())
    assert report["status"] == ("FAILED" if foreign else "CLEANUP_REQUESTED")
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
    assert all(
        call.kwargs["Key"].startswith(prefix)
        for call in storage.get_object.call_args_list
    )


@pytest.mark.parametrize("wrong", ["run", "target"])
def test_cleanup_rejects_foreign_manifest_before_aws_calls(
    monkeypatch, tmp_path, wrong
):
    deployment = {"run": "run", "functions": {"c1": "arn:owned"}}
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "other" if wrong == "run" else "run",
                "profiles": [
                    {
                        "profile_id": "c1",
                        "resource_id": "arn:foreign"
                        if wrong == "target"
                        else "arn:owned",
                    }
                ],
                "scenarios": [{"profile_id": "c1", "case_id": "case"}],
            }
        )
    )
    client = Mock(side_effect=AssertionError("Foreign targets must not be touched"))
    monkeypatch.setattr(shared, "client", client)
    with pytest.raises(CollectionError):
        shared.cleanup(deployment, tmp_path)
    client.assert_not_called()


def test_one_identified_execution_does_not_hide_another_unreconciled_invoke(
    monkeypatch, tmp_path
):
    from pathlib import Path

    target = "arn:aws:lambda:us-west-2:123:function:owned:$LATEST.PUBLISHED"
    execution = target + "/durable-execution/name/id"
    deployment = {
        "run": "run",
        "bucket": "private",
        "region": "us-west-2",
        "functions": {"c1": target},
    }
    case = {"case_id": "case", "profile_id": "c1"}
    output = tmp_path / "standard"
    output.mkdir()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "runId": "run",
                "profiles": [{"profile_id": "c1", "resource_id": target}],
                "scenarios": [case],
            }
        )
    )
    owner = hashlib.sha256(b"case:cohort-1").hexdigest()[:20]
    intents = output / "source/cases" / owner / "invocations"
    intents.mkdir(parents=True)
    known, missing = intents / "known.json", intents / "missing.json"
    payload = {"run": "run", "caseId": "case"}
    known.write_text(
        json.dumps({"payload": {**payload, "marker": "known"}, "arn": execution})
    )
    missing.write_text(
        json.dumps({"payload": {**payload, "marker": "missing"}, "arn": None})
    )
    original_glob = Path.glob
    monkeypatch.setattr(
        Path,
        "glob",
        lambda path, pattern: (
            iter([known, missing]) if path == intents else original_glob(path, pattern)
        ),
    )
    storage, service = Mock(), Mock()
    storage.get_paginator.return_value.paginate.return_value = []
    service.get_durable_execution.return_value = {"Status": "SUCCEEDED"}
    monkeypatch.setattr(
        shared, "client", lambda name, region: storage if name == "s3" else service
    )
    assert shared.cleanup(deployment, tmp_path) == 1
    errors = json.loads((tmp_path / "cleanup.json").read_text())["errors"]
    assert errors == ["Unreconciled Invoke: case/missing"]
