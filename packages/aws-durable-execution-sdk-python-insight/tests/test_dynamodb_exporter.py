# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``DynamoDBExporter`` (fake client, no AWS)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from aws_durable_execution_sdk_python_insight import DynamoDBExporter
from aws_durable_execution_sdk_python_insight.exporters.dynamodb_exporter import (
    DynamoDBExporter as DynamoDBExporterFromModule,
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


class FakeDynamoDBClient:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []

    def put_item(self, **kwargs: Any) -> None:
        self.puts.append(kwargs)


def test_public_import_path() -> None:
    assert DynamoDBExporter is DynamoDBExporterFromModule


def test_defaults_and_render_is_operations_by_name() -> None:
    exporter = DynamoDBExporter(table_name="insight", client=FakeDynamoDBClient())
    assert exporter.max_record_size_bytes == 400_000
    assert exporter.partition_key == "pk"
    assert exporter.sort_key == "sk"
    shaped = exporter.render(_record())
    assert "operations" not in shaped
    assert shaped["operationsByName"]["fetch-user"]["count"] == 1
    exporter.flush()  # no buffering: a no-op


def test_export_writes_history_item_keyed_by_arn_and_emitted_at() -> None:
    client = FakeDynamoDBClient()
    record = _record()
    DynamoDBExporter(table_name="insight", client=client).export(record)

    assert len(client.puts) == 1
    put = client.puts[0]
    assert put["TableName"] == "insight"
    item = put["Item"]
    assert item["pk"] == {"S": record["executionArn"]}
    assert item["sk"] == {"S": "2026-07-15T12:00:00.000Z"}
    assert "operations" not in item
    assert item["durationMs"] == {"N": "2000"}
    summary = item["operationsByName"]["M"]["fetch-user"]["M"]
    assert summary["count"] == {"N": "1"}
    assert summary["type"] == {"S": "STEP"}


def test_export_upserts_without_sort_key_and_custom_partition_key() -> None:
    client = FakeDynamoDBClient()
    exporter = DynamoDBExporter(
        table_name="insight",
        partition_key="executionArnKey",
        sort_key="",
        client=client,
    )
    assert exporter.sort_key is None
    record = _record()
    exporter.export(record)
    item = client.puts[0]["Item"]
    assert item["executionArnKey"] == {"S": record["executionArn"]}
    assert "sk" not in item


def test_export_marshals_floats_as_numbers() -> None:
    client = FakeDynamoDBClient()
    DynamoDBExporter(table_name="insight", client=client).export(
        _record(output={"ratio": 0.25, "n": 3, "ok": True, "none": None})
    )
    output = client.puts[0]["Item"]["output"]["M"]
    assert output["ratio"] == {"N": "0.25"}
    assert Decimal(output["ratio"]["N"]) == Decimal("0.25")
    assert output["n"] == {"N": "3"}
    assert output["ok"] == {"BOOL": True}
    assert output["none"] == {"NULL": True}
