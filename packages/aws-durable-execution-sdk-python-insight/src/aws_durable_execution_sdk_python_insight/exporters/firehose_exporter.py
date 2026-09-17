# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Kinesis Data Firehose Workflow Insight exporter."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import compact_dumps
from aws_durable_execution_sdk_python_insight.operations_index import (
    OperationsFormat,
    OperationsFormatInput,
    apply_operations_format,
)


class FirehoseExporter:
    """Sends each record to a Firehose delivery stream with PutRecord.

    The data is one JSON line with a trailing newline, so records that Firehose
    concatenates into a single object stay parseable as NDJSON.
    """

    def __init__(
        self,
        delivery_stream_name: str,
        region: str | None = None,
        operations_format: OperationsFormat | OperationsFormatInput = (
            OperationsFormat.ARRAY
        ),
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.delivery_stream_name = delivery_stream_name
        self.operations_format = OperationsFormat(operations_format)
        self.max_record_size_bytes: int | None = (
            1_000_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("firehose", region_name=region)
                if region
                else boto3.client("firehose")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return apply_operations_format(record, self.operations_format)

    def export(self, record: dict[str, Any]) -> None:
        data = (compact_dumps(self.render(record)) + "\n").encode("utf-8")
        self._client.put_record(
            DeliveryStreamName=self.delivery_stream_name, Record={"Data": data}
        )

    def flush(self) -> None:
        return None
