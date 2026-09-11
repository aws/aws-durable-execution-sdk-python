# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``EventBridgeExporter`` (fake client, no AWS)."""

from __future__ import annotations

import datetime
import json
from typing import Any

import pytest

from aws_durable_execution_sdk_python_insight import EventBridgeExporter
from aws_durable_execution_sdk_python_insight.exporters.eventbridge_exporter import (
    EventBridgeExporter as EventBridgeExporterFromModule,
)


def _record(**overrides: Any) -> dict[str, Any]:
    """A complete SUCCEEDED record; keyword arguments override fields."""
    record: dict[str, Any] = {
        "recordType": "WorkflowInsight",
        "schemaVersion": "1.0",
        "emittedAt": "2026-07-15T12:00:00.000Z",
        "executionArn": (
            "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
            "/durable-execution/my-exec/inv-1"
        ),
        "executionName": "my-exec",
        "functionName": "fn",
        "functionQualifier": "$LATEST",
        "region": "us-east-1",
        "accountId": "123456789012",
        "status": "SUCCEEDED",
        "startTime": "2026-07-15T11:59:58.000Z",
        "endTime": "2026-07-15T12:00:00.000Z",
        "durationMs": 2000,
        "operations": [
            {
                "id": "op-1",
                "name": "fetch-user",
                "type": "STEP",
                "subType": "Step",
                "status": "SUCCEEDED",
                "durationMs": 12,
            }
        ],
    }
    record.update(overrides)
    return record


class FakeEventsClient:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._response = response or {"FailedEntryCount": 0, "Entries": [{}]}

    def put_events(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return self._response


def test_public_import_path_and_defaults() -> None:
    assert EventBridgeExporter is EventBridgeExporterFromModule
    exporter = EventBridgeExporter(client=FakeEventsClient())
    assert exporter.event_bus_name == "default"
    assert exporter.source == "aws.durable-execution.insight"
    assert exporter.operations_format == "array"
    assert exporter.max_record_size_bytes == 256_000
    exporter.flush()  # no buffering: a no-op


def test_publishes_single_event_with_status_detail_type() -> None:
    client = FakeEventsClient()
    record = _record()
    exporter = EventBridgeExporter(client=client)
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(client.calls) == 1
    entries = client.calls[0]["Entries"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["EventBusName"] == "default"
    assert entry["Source"] == "aws.durable-execution.insight"
    assert entry["DetailType"] == "SUCCEEDED"
    assert entry["Time"] == datetime.datetime(
        2026, 7, 15, 12, 0, 0, tzinfo=datetime.UTC
    )
    detail = entry["Detail"]
    assert ", " not in detail and '": ' not in detail
    assert json.loads(detail) == record
    assert isinstance(json.loads(detail)["operations"], list)


def test_by_name_format_with_custom_bus_and_source() -> None:
    client = FakeEventsClient()
    EventBridgeExporter(
        event_bus_name="insight-bus",
        source="my.source",
        operations_format="by-name",
        client=client,
    ).export(_record(status="FAILED"))
    entry = client.calls[0]["Entries"][0]
    assert entry["EventBusName"] == "insight-bus"
    assert entry["Source"] == "my.source"
    assert entry["DetailType"] == "FAILED"
    detail = json.loads(entry["Detail"])
    assert "operations" not in detail
    assert detail["operationsByName"]["fetch-user"]["count"] == 1


def test_failed_entry_raises() -> None:
    client = FakeEventsClient(
        response={
            "FailedEntryCount": 1,
            "Entries": [{"ErrorCode": "ThrottlingException", "ErrorMessage": "slow"}],
        }
    )
    exporter = EventBridgeExporter(client=client)
    with pytest.raises(
        RuntimeError,
        match=r"EventBridge PutEvents failed: ThrottlingException — slow",
    ):
        exporter.export(_record())
