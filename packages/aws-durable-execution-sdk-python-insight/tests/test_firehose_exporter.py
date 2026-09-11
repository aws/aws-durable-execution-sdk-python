# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``FirehoseExporter`` (fake client, no AWS)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from aws_durable_execution_sdk_python_insight import FirehoseExporter, OperationsFormat
from aws_durable_execution_sdk_python_insight.exporters.firehose_exporter import (
    FirehoseExporter as FirehoseExporterFromModule,
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


class FakeFirehoseClient:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []

    def put_record(self, **kwargs: Any) -> dict[str, Any]:
        self.puts.append(kwargs)
        return {}


def test_public_import_path_and_defaults() -> None:
    assert FirehoseExporter is FirehoseExporterFromModule
    exporter = FirehoseExporter(delivery_stream_name="s", client=FakeFirehoseClient())
    assert exporter.max_record_size_bytes == 1_000_000
    assert exporter.operations_format is OperationsFormat.ARRAY
    exporter.flush()  # no buffering: a no-op


def test_puts_single_ndjson_record_with_trailing_newline() -> None:
    client = FakeFirehoseClient()
    record = _record()
    exporter = FirehoseExporter(delivery_stream_name="insight-stream", client=client)
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(client.puts) == 1
    put = client.puts[0]
    assert put["DeliveryStreamName"] == "insight-stream"
    data = put["Record"]["Data"]
    assert isinstance(data, bytes)
    expected = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
    assert data == expected.encode("utf-8")
    assert data.endswith(b"\n")
    assert len(data.rstrip(b"\n").split(b"\n")) == 1
    assert isinstance(json.loads(data)["operations"], list)


def test_by_name_format_renders_operations_by_name() -> None:
    client = FakeFirehoseClient()
    FirehoseExporter(
        delivery_stream_name="s", operations_format="by-name", client=client
    ).export(_record())
    parsed = json.loads(client.puts[0]["Record"]["Data"])
    assert "operations" not in parsed
    assert parsed["operationsByName"]["fetch-user"]["count"] == 1


def test_both_format_keeps_array_and_adds_map() -> None:
    client = FakeFirehoseClient()
    FirehoseExporter(
        delivery_stream_name="s", operations_format=OperationsFormat.BOTH, client=client
    ).export(_record())
    parsed = json.loads(client.puts[0]["Record"]["Data"])
    assert isinstance(parsed["operations"], list)
    assert parsed["operationsByName"]["fetch-user"]["count"] == 1


def test_invalid_operations_format_raises() -> None:
    with pytest.raises(ValueError):
        FirehoseExporter(
            delivery_stream_name="s",
            operations_format="list",  # type: ignore[arg-type]  # dynamic invalid value
            client=FakeFirehoseClient(),
        )
