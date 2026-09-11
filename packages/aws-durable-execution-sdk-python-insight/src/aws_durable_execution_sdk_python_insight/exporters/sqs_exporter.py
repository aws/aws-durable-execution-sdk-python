# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""SQS Workflow Insight exporter."""

from __future__ import annotations

import hashlib
from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import compact_dumps
from aws_durable_execution_sdk_python_insight.operations_index import (
    OperationsFormat,
    OperationsFormatInput,
    apply_operations_format,
)


# SQS caps MessageGroupId and MessageDeduplicationId at 128 characters.
_MAX_ID_LENGTH = 128


class SQSExporter:
    """Sends each record as one SQS message with SendMessage.

    The rendered record is the message body; ``status`` and ``functionName``
    are message attributes. On a FIFO queue (URL ending in ``.fifo``) the group
    id defaults to ``executionArn`` and the deduplication id is
    ``executionArn:emittedAt``; an id longer than 128 characters is replaced by
    its SHA-256 hex digest so the message is never rejected.
    """

    def __init__(
        self,
        queue_url: str,
        message_group_id: str | None = None,
        region: str | None = None,
        operations_format: OperationsFormat | OperationsFormatInput = (
            OperationsFormat.ARRAY
        ),
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.queue_url = queue_url
        self.message_group_id = message_group_id
        self.is_fifo = queue_url.endswith(".fifo")
        self.operations_format = OperationsFormat(operations_format)
        self.max_record_size_bytes: int | None = (
            256_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("sqs", region_name=region)
                if region
                else boto3.client("sqs")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return apply_operations_format(record, self.operations_format)

    def export(self, record: dict[str, Any]) -> None:
        kwargs: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MessageBody": compact_dumps(self.render(record)),
            "MessageAttributes": {
                "status": {"DataType": "String", "StringValue": record["status"]},
                "functionName": {
                    "DataType": "String",
                    "StringValue": record["functionName"],
                },
            },
        }
        if self.is_fifo:
            kwargs["MessageGroupId"] = _bounded_id(
                self.message_group_id or record["executionArn"]
            )
            kwargs["MessageDeduplicationId"] = _bounded_id(
                f"{record['executionArn']}:{record['emittedAt']}"
            )
        self._client.send_message(**kwargs)

    def flush(self) -> None:
        return None


def _bounded_id(value: str) -> str:
    """Return ``value`` if it fits the SQS 128-character id limit, else its SHA-256."""
    if len(value) <= _MAX_ID_LENGTH:
        return value
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
