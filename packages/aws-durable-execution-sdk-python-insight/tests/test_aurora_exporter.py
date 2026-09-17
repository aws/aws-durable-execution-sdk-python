# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``AuroraExporter`` (fake RDS Data API client, no AWS)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from aws_durable_execution_sdk_python_insight import AuroraEngine, AuroraExporter
from aws_durable_execution_sdk_python_insight.exporters.aurora_exporter import (
    AuroraExporter as AuroraExporterFromModule,
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


class FakeRdsDataClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {}


def _params(call: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {p["name"]: p["value"] for p in call["parameters"]}


def _exporter(client: FakeRdsDataClient, **kw: Any) -> AuroraExporter:
    base: dict[str, Any] = {
        "resource_arn": "arn:aws:rds:us-east-1:123456789012:cluster:c",
        "secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:s",
        "database": "insight",
        "engine": "postgresql",
        "client": client,
    }
    base.update(kw)
    return AuroraExporter(**base)


def test_public_import_path_and_defaults() -> None:
    assert AuroraExporter is AuroraExporterFromModule
    exporter = _exporter(FakeRdsDataClient())
    assert exporter.max_record_size_bytes == 1_000_000
    assert exporter.table == "workflow_insight"
    assert exporter.engine is AuroraEngine.POSTGRESQL
    assert _exporter(FakeRdsDataClient(), engine=AuroraEngine.MYSQL).engine == "mysql"
    exporter.flush()  # no buffering: a no-op


def test_postgres_upsert_with_typed_casts_and_bound_parameters() -> None:
    client = FakeRdsDataClient()
    record = _record()
    exporter = _exporter(client)
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["resourceArn"] == "arn:aws:rds:us-east-1:123456789012:cluster:c"
    assert call["secretArn"] == "arn:aws:secretsmanager:us-east-1:123456789012:secret:s"
    assert call["database"] == "insight"
    sql = call["sql"]
    assert "INSERT INTO workflow_insight" in sql
    assert "ON CONFLICT (execution_arn) DO UPDATE" in sql
    assert ":start_time::timestamptz" in sql
    assert ":record_json::jsonb" in sql

    params = _params(call)
    assert [p["name"] for p in call["parameters"]] == [
        "execution_arn",
        "execution_name",
        "function_name",
        "status",
        "start_time",
        "end_time",
        "duration_ms",
        "record_json",
        "emitted_at",
    ]
    assert params["execution_arn"] == {"stringValue": record["executionArn"]}
    assert params["execution_name"] == {"stringValue": "my-exec"}
    assert params["function_name"] == {"stringValue": "fn"}
    assert params["status"] == {"stringValue": "SUCCEEDED"}
    assert params["start_time"] == {"stringValue": "2026-07-15T11:59:58.000Z"}
    assert params["end_time"] == {"stringValue": "2026-07-15T12:00:00.000Z"}
    assert params["duration_ms"] == {"longValue": 2000}
    assert params["emitted_at"] == {"stringValue": "2026-07-15T12:00:00.000Z"}
    record_json = params["record_json"]["stringValue"]
    assert ", " not in record_json and '": ' not in record_json
    assert json.loads(record_json) == record


def test_mysql_dialect_has_no_casts_and_custom_table() -> None:
    client = FakeRdsDataClient()
    _exporter(client, engine="mysql", table="custom_table").export(_record())
    sql = client.calls[0]["sql"]
    assert "INSERT INTO custom_table" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "::timestamptz" not in sql
    assert "ON CONFLICT" not in sql


def test_absent_nullable_fields_are_is_null() -> None:
    client = FakeRdsDataClient()
    record = _record(status="RUNNING")
    del record["executionName"]
    del record["endTime"]
    del record["durationMs"]
    _exporter(client).export(record)
    params = _params(client.calls[0])
    assert params["execution_name"] == {"isNull": True}
    assert params["end_time"] == {"isNull": True}
    assert params["duration_ms"] == {"isNull": True}


def test_invalid_identifier_or_engine_fails_at_construction() -> None:
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _exporter(FakeRdsDataClient(), table="bad-table; drop")
    with pytest.raises(ValueError):
        _exporter(FakeRdsDataClient(), engine="oracle")
