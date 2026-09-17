# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``CloudWatchLogsExporter`` (fake client, no AWS)."""

from __future__ import annotations

import datetime
import json
import re
from typing import Any

import pytest

from aws_durable_execution_sdk_python_insight import CloudWatchLogsExporter
from aws_durable_execution_sdk_python_insight.exporters.cloudwatch_logs_exporter import (
    CloudWatchLogsExporter as CloudWatchLogsExporterFromModule,
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


class ResourceAlreadyExistsException(Exception):
    """Stand-in for the modeled botocore exception (matched by class name)."""


class GenericClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code, "Message": code}}


class FakeLogsClient:
    def __init__(self, create_error: Exception | None = None) -> None:
        self.creates: list[dict[str, Any]] = []
        self.puts: list[dict[str, Any]] = []
        self._create_error = create_error

    def create_log_stream(self, **kwargs: Any) -> None:
        self.creates.append(kwargs)
        if self._create_error is not None:
            raise self._create_error

    def put_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.puts.append(kwargs)
        return {}


def test_public_import_path_and_defaults() -> None:
    assert CloudWatchLogsExporter is CloudWatchLogsExporterFromModule
    exporter = CloudWatchLogsExporter(log_group_name="/g", client=FakeLogsClient())
    assert exporter.max_record_size_bytes == 256_000
    assert exporter.log_stream_prefix == "workflow-insight/"
    exporter.flush()  # no buffering: a no-op


def test_creates_dated_stream_then_puts_operations_by_name_event() -> None:
    client = FakeLogsClient()
    exporter = CloudWatchLogsExporter(log_group_name="/insight/records", client=client)
    before_ms = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
    exporter.export(_record())

    assert len(client.creates) == 1
    assert len(client.puts) == 1
    create = client.creates[0]
    assert create["logGroupName"] == "/insight/records"
    assert re.fullmatch(r"workflow-insight/\d{4}/\d{2}/\d{2}", create["logStreamName"])
    today = datetime.datetime.now(datetime.UTC).strftime("%Y/%m/%d")
    assert create["logStreamName"] == f"workflow-insight/{today}"

    put = client.puts[0]
    assert put["logGroupName"] == "/insight/records"
    assert put["logStreamName"] == create["logStreamName"]
    assert len(put["logEvents"]) == 1
    event = put["logEvents"][0]
    assert isinstance(event["timestamp"], int)
    assert event["timestamp"] >= before_ms
    message = event["message"]
    assert ", " not in message and '": ' not in message
    parsed = json.loads(message)
    assert parsed["operationsByName"]["fetch-user"]["count"] == 1
    assert "operations" not in parsed


def test_custom_prefix_is_used_in_stream_name() -> None:
    client = FakeLogsClient()
    CloudWatchLogsExporter(
        log_group_name="/g", log_stream_prefix="wi-", client=client
    ).export(_record())
    assert client.creates[0]["logStreamName"].startswith("wi-")


def test_stream_is_created_once_across_exports() -> None:
    client = FakeLogsClient()
    exporter = CloudWatchLogsExporter(log_group_name="/g", client=client)
    exporter.export(_record())
    exporter.export(_record())
    assert len(client.creates) == 1
    assert len(client.puts) == 2


@pytest.mark.parametrize(
    "error",
    [
        ResourceAlreadyExistsException("exists"),
        GenericClientError("ResourceAlreadyExistsException"),
    ],
)
def test_already_exists_from_create_is_swallowed(error: Exception) -> None:
    client = FakeLogsClient(create_error=error)
    exporter = CloudWatchLogsExporter(log_group_name="/g", client=client)
    exporter.export(_record())
    exporter.export(_record())
    assert len(client.creates) == 1
    assert len(client.puts) == 2


def test_other_create_errors_propagate() -> None:
    client = FakeLogsClient(create_error=GenericClientError("AccessDeniedException"))
    exporter = CloudWatchLogsExporter(log_group_name="/g", client=client)
    with pytest.raises(GenericClientError):
        exporter.export(_record())
    assert client.puts == []
