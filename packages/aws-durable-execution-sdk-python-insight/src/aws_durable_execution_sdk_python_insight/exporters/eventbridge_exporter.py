# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""EventBridge Workflow Insight exporter."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    parse_iso_datetime,
)
from aws_durable_execution_sdk_python_insight.operations_index import (
    OperationsFormat,
    OperationsFormatInput,
    apply_operations_format,
)


class EventBridgeExporter:
    """Publishes each record as one EventBridge event with PutEvents.

    ``Source`` is configurable, ``DetailType`` is the record status
    (``SUCCEEDED``, ``RUNNING``, ``FAILED``), ``Detail`` is the rendered record
    and ``Time`` is the record's ``emittedAt``.
    """

    def __init__(
        self,
        event_bus_name: str = "default",
        source: str = "aws.durable-execution.insight",
        region: str | None = None,
        operations_format: OperationsFormat | OperationsFormatInput = (
            OperationsFormat.ARRAY
        ),
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.event_bus_name = event_bus_name
        self.source = source
        self.operations_format = OperationsFormat(operations_format)
        self.max_record_size_bytes: int | None = (
            256_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("events", region_name=region)
                if region
                else boto3.client("events")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return apply_operations_format(record, self.operations_format)

    def export(self, record: dict[str, Any]) -> None:
        result = self._client.put_events(
            Entries=[
                {
                    "EventBusName": self.event_bus_name,
                    "Source": self.source,
                    "DetailType": record["status"],
                    "Detail": compact_dumps(self.render(record)),
                    "Time": parse_iso_datetime(record["emittedAt"]),
                }
            ]
        )
        if (result or {}).get("FailedEntryCount", 0) > 0:
            entries = result.get("Entries") or [{}]
            entry = entries[0]
            msg = (
                "EventBridge PutEvents failed: "
                f"{entry.get('ErrorCode')} — {entry.get('ErrorMessage')}"
            )
            raise RuntimeError(msg)

    def flush(self) -> None:
        return None
