# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Ownership must survive run failure and conditional-write races."""

import io
import json

from botocore.exceptions import ClientError
import pytest

from lmi_tests.shared_lock import OwnershipError, RunLease


class Store:
    def __init__(self):
        self.record = None
        self.version = 0
        self.race = False

    def get_object(self, **kwargs):
        if self.record is None:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {
            "Body": io.BytesIO(json.dumps(self.record).encode()),
            "ETag": str(self.version),
        }

    def put_object(self, **kwargs):
        if (
            self.race
            or (kwargs.get("IfNoneMatch") == "*" and self.record is not None)
            or ("IfMatch" in kwargs and kwargs["IfMatch"] != str(self.version))
        ):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.record = json.loads(kwargs["Body"])
        self.version += 1


def test_run_holds_all_targets_until_both_profiles_retire():
    store = Store()
    lease = RunLease.acquire("private", "stack", "run", ("c1", "c2"), s3=store)
    lease.assert_owned("c1")
    with pytest.raises(OwnershipError):
        RunLease.acquire("private", "stack", "other", ("c1", "c2"), s3=store)
    lease.finish_profile("c1", quiescent=True)
    with pytest.raises(OwnershipError):
        lease.release()
    lease.finish_profile("c2", quiescent=True)
    lease.release()
    replacement = RunLease.acquire("private", "stack", "next", ("c1", "c2"), s3=store)
    replacement.assert_owned("c2")
    with pytest.raises(OwnershipError):
        lease.assert_owned("c1")


def test_quarantine_cannot_be_erased_by_later_success_or_clock_expiry():
    store = Store()
    lease = RunLease.acquire("private", "stack", "run", ("c1", "c2"), s3=store)
    lease.finish_profile("c1", quiescent=False)
    lease.finish_profile("c1", quiescent=True)
    lease.assert_owned("c2")
    with pytest.raises(OwnershipError):
        lease.assert_owned("c1")
    with pytest.raises(OwnershipError):
        lease.release()
    with pytest.raises(OwnershipError):
        RunLease.acquire("private", "stack", "new", ("c1", "c2"), s3=store)


def test_conditional_write_race_does_not_steal_ownership():
    store = Store()
    store.race = True
    with pytest.raises(OwnershipError):
        RunLease.acquire("private", "stack", "run", ("c1",), s3=store)
    assert store.record is None


def test_unknown_resources_and_malformed_records_fail_closed():
    store = Store()
    lease = RunLease.acquire("private", "stack", "run", ("c1",), s3=store)
    with pytest.raises(OwnershipError):
        lease.finish_profile("foreign", quiescent=True)
    with pytest.raises(OwnershipError):
        lease.assert_owned("foreign")
    store.record = {"schemaVersion": 999}
    with pytest.raises(OwnershipError):
        lease.assert_owned("c1")


def test_reusing_a_settled_profile_retains_the_lease_for_the_new_group():
    store = Store()
    lease = RunLease.acquire("private", "stack", "run", ("c1",), s3=store)
    lease.finish_profile("c1", quiescent=True)
    lease.assert_owned("c1")
    with pytest.raises(OwnershipError):
        lease.release()
    lease.finish_profile("c1", quiescent=True)
    lease.release()


@pytest.mark.parametrize("state", ["unknown", None, 1, {"unverified": True}])
def test_unknown_retirement_state_cannot_be_claimed_or_cleared(state):
    store = Store()
    lease = RunLease.acquire("private", "stack", "run", ("c1",), s3=store)
    store.record["profiles"]["c1"] = state
    with pytest.raises(OwnershipError):
        lease.assert_owned("c1")
    with pytest.raises(OwnershipError):
        lease.finish_profile("c1", True)
    assert store.record["profiles"]["c1"] == state


def test_blocked_acquisition_identifies_owner_without_exposing_fence_or_mutating_lock():
    store = Store()
    owner = RunLease.acquire("private", "stack", "original-run", ("c1", "c2"), s3=store)
    owner.finish_profile("c1", False)
    before = json.dumps(store.record, sort_keys=True)
    with pytest.raises(OwnershipError) as error:
        RunLease.acquire("private", "stack", "new-run", ("c1", "c2"), s3=store)
    assert "original-run" in str(error.value)
    assert "quarantined" in str(error.value)
    assert owner.token not in str(error.value)
    assert json.dumps(store.record, sort_keys=True) == before
