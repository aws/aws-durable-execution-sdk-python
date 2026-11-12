# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Conditional S3 ownership for one persistent SDK deployment.

The lease token fences concurrent runs; it is not an authentication credential.
S3 access continues to require the caller's normal SigV4/IAM authorization.
There is deliberately no expiry-based stealing of a run with residual work.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class OwnershipError(RuntimeError):
    """A persistent target cannot be safely acquired, updated, or released."""


@dataclass(frozen=True)
class RunLease:
    bucket: str
    key: str
    run_id: str
    token: str
    resources: tuple[str, ...]
    s3: Any

    @classmethod
    def acquire(
        cls,
        bucket: str,
        stack: str,
        run_id: str,
        resources: tuple[str, ...],
        *,
        s3: Any = None,
    ) -> RunLease:
        lease = cls(
            bucket,
            f"ownership/{stack}/shared-run.json",
            run_id,
            uuid.uuid4().hex,
            tuple(sorted(resources)),
            s3
            if s3 is not None
            else boto3.client(
                "s3",
                config=Config(
                    connect_timeout=3,
                    read_timeout=10,
                    retries={"total_max_attempts": 2},
                ),
            ),
        )
        previous, etag = lease._read()
        if previous is not None and (
            previous.get("state") != "released"
            or not isinstance(previous.get("profiles"), dict)
            or not previous["profiles"]
            or any(value != "quiescent" for value in previous["profiles"].values())
        ):
            raise OwnershipError(
                "Persistent LMI targets are owned or quarantined by another run"
            )
        record = {
            "schemaVersion": 1,
            "runId": run_id,
            "token": lease.token,
            "state": "owned",
            "profiles": {resource: "active" for resource in lease.resources},
        }
        try:
            lease._write(record, etag)
        except ClientError as error:
            if error.response["Error"]["Code"] in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            }:
                raise OwnershipError(
                    "Another run acquired the persistent LMI targets"
                ) from error
            raise
        return lease

    @classmethod
    def from_manifest(cls, sdk: Mapping[str, Any]) -> RunLease:
        record = sdk.get("lease")
        if not isinstance(record, dict):
            raise OwnershipError(
                "Shared LMI execution requires an acquired persistent run lease"
            )
        required = ("bucket", "key", "runId", "token", "resources")
        if any(key not in record for key in required):
            raise OwnershipError("Run lease manifest is incomplete")
        deployment = sdk["deployment"]
        if (
            record["bucket"] != deployment["bucket"]
            or record["runId"] != deployment["run"]
            or record["key"] != f"ownership/{deployment['stack']}/shared-run.json"
            or set(record["resources"]) != set(deployment["functions"].values())
        ):
            raise OwnershipError("Run lease does not belong to this SDK deployment")
        return cls(
            record["bucket"],
            record["key"],
            record["runId"],
            record["token"],
            tuple(record["resources"]),
            boto3.client(
                "s3",
                region_name=sdk["deployment"]["region"],
                config=Config(
                    connect_timeout=3,
                    read_timeout=10,
                    retries={"total_max_attempts": 2},
                ),
            ),
        )

    def to_manifest(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "key": self.key,
            "runId": self.run_id,
            "token": self.token,
            "resources": list(self.resources),
        }

    def _read(self) -> tuple[dict[str, Any] | None, str | None]:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key)
        except ClientError as error:
            if error.response["Error"]["Code"] in {"NoSuchKey", "404", "NotFound"}:
                return None, None
            raise
        with response["Body"] as body:
            record = json.loads(body.read())
        if (
            not isinstance(record, dict)
            or type(record.get("schemaVersion")) is not int
            or record["schemaVersion"] != 1
        ):
            raise OwnershipError("Stored run ownership is malformed")
        return record, response["ETag"]

    def _write(self, record: Mapping[str, Any], etag: str | None) -> None:
        condition = {"IfMatch": etag} if etag is not None else {"IfNoneMatch": "*"}
        self.s3.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=json.dumps(record, sort_keys=True).encode(),
            ContentType="application/json",
            **condition,
        )

    def _owned(self, record: Mapping[str, Any] | None) -> dict[str, Any]:
        if (
            not isinstance(record, dict)
            or record.get("state") != "owned"
            or record.get("runId") != self.run_id
            or record.get("token") != self.token
            or not isinstance(record.get("profiles"), dict)
            or set(record["profiles"]) != set(self.resources)
            or any(
                not isinstance(value, str)
                or value not in {"active", "quiescent", "quarantined"}
                for value in record["profiles"].values()
            )
        ):
            raise OwnershipError(
                "Persistent target ownership no longer matches this run"
            )
        return record

    def assert_owned(self, resource_id: str) -> None:
        """Verify the owner and mark a previously settled profile active again."""
        for _ in range(3):
            record, etag = self._read()
            record = self._owned(record)
            if (
                resource_id not in self.resources
                or record["profiles"][resource_id] == "quarantined"
            ):
                raise OwnershipError(
                    "The requested function is not owned or has residual work"
                )
            if record["profiles"][resource_id] == "active":
                return
            record["profiles"][resource_id] = "active"
            try:
                self._write(record, etag)
                return
            except ClientError as error:
                if error.response["Error"]["Code"] not in {
                    "PreconditionFailed",
                    "ConditionalRequestConflict",
                }:
                    raise
        raise OwnershipError("Could not reactivate the owned profile without a race")

    def finish_profile(self, resource_id: str, quiescent: bool) -> None:
        if resource_id not in self.resources:
            raise OwnershipError("Cannot retire another run's function")
        for _ in range(3):
            record, etag = self._read()
            record = self._owned(record)
            previous = record["profiles"][resource_id]
            record["profiles"][resource_id] = (
                "quarantined"
                if not quiescent or previous == "quarantined"
                else "quiescent"
            )
            try:
                self._write(record, etag)
                return
            except ClientError as error:
                if error.response["Error"]["Code"] not in {
                    "PreconditionFailed",
                    "ConditionalRequestConflict",
                }:
                    raise
        raise OwnershipError("Could not record retirement without racing another owner")

    def release(self) -> None:
        record, etag = self._read()
        record = self._owned(record)
        if any(value != "quiescent" for value in record["profiles"].values()):
            raise OwnershipError(
                "Unretired LMI work retains persistent target ownership"
            )
        record["state"] = "released"
        self._write(record, etag)
