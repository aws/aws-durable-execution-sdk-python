# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""S3 Workflow Insight exporter."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    sanitize,
)


class S3Partitioning(StrEnum):
    """S3 key partitioning scheme. The values match the JS S3 exporter's options."""

    # year=YYYY/month=MM/day=DD/ derived from the record startTime (default)
    DATE = "date"
    # function=<functionName>/
    FUNCTION_NAME = "function-name"
    # no partition prefix
    NONE = "none"


# Accepted string inputs, kept in lockstep with the enum values above. The
# constructor is typed as this ``Literal`` union (never bare ``str``) so a typoed
# scheme fails a static type check, while ``S3Partitioning(partitioning)`` in
# ``__init__`` normalizes any accepted value to the matching enum member and
# raises ``ValueError`` for an invalid dynamic string.
S3PartitioningInput = Literal["date", "function-name", "none"]


class S3Exporter:
    """Writes canonical ``operations``-array records to S3.

    Port of the JS ``S3Exporter``. Each record is a JSON object keyed by
    execution name, so updates to the same execution overwrite the same object.
    Emits the lossless ``operations`` array (``OPERATIONS_ARRAY``).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "workflow-insight/",
        partitioning: S3Partitioning | S3PartitioningInput = S3Partitioning.DATE,
        region: str | None = None,
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        # ``S3Partitioning(x)`` is idempotent for members, accepts the exact
        # JS-style strings, and raises ``ValueError`` for an unrecognized dynamic
        # string so an invalid scheme fails at construction rather than silently
        # falling through to no partitioning.
        self.partitioning = S3Partitioning(partitioning)
        self.max_record_size_bytes = (
            5_000_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("s3", region_name=region) if region else boto3.client("s3")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        key = self._build_key(record)
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=compact_dumps(record).encode("utf-8"),
            ContentType="application/json",
        )

    def flush(self) -> None:
        return None

    def _build_key(self, record: dict[str, Any]) -> str:
        file_name = (
            sanitize(
                record.get("executionName") or record.get("executionArn") or "record"
            )
            + ".json"
        )
        return f"{self.prefix}{self._partition(record)}{file_name}"

    def _partition(self, record: dict[str, Any]) -> str:
        if self.partitioning == S3Partitioning.FUNCTION_NAME:
            return f"function={sanitize(record.get('functionName', ''))}/"
        if self.partitioning == S3Partitioning.DATE:
            start = str(record.get("startTime", ""))
            # YYYY-MM-DD... -> year=YYYY/month=MM/day=DD/
            if len(start) >= 10 and start[4] == "-" and start[7] == "-":
                return f"year={start[0:4]}/month={start[5:7]}/day={start[8:10]}/"
            return ""
        return ""
