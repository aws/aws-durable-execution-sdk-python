# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``RedshiftExporter`` (fake Redshift Data API client, no AWS)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from aws_durable_execution_sdk_python_insight import RedshiftExporter
from aws_durable_execution_sdk_python_insight.exporters.redshift_exporter import (
    RedshiftExporter as RedshiftExporterFromModule,
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


class FakeRedshiftDataClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"Id": "stmt-1"}


def _params(call: dict[str, Any]) -> dict[str, str]:
    return {p["name"]: p["value"] for p in call["Parameters"]}


def test_public_import_path_and_defaults() -> None:
    assert RedshiftExporter is RedshiftExporterFromModule
    exporter = RedshiftExporter(
        database="insight", workgroup_name="wg", client=FakeRedshiftDataClient()
    )
    assert exporter.max_record_size_bytes == 1_000_000
    assert exporter.fq_table == "public.workflow_insight"
    exporter.flush()  # no buffering: a no-op


def test_merge_joins_on_source_column_subquery_with_typed_casts() -> None:
    client = FakeRedshiftDataClient()
    record = _record()
    exporter = RedshiftExporter(
        database="insight", workgroup_name="insight-wg", client=client
    )
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["WorkgroupName"] == "insight-wg"
    assert call["Database"] == "insight"
    assert "ClusterIdentifier" not in call
    assert "DbUser" not in call
    assert "SecretArn" not in call

    sql = call["Sql"]
    assert "MERGE INTO public.workflow_insight USING (" in sql
    assert "ON public.workflow_insight.execution_arn = src.execution_arn" in sql
    assert ":execution_arn::varchar AS execution_arn" in sql
    assert "USING (SELECT 1)" not in sql
    assert ":start_time::timestamptz AS start_time" in sql
    assert ":end_time::timestamptz AS end_time" in sql
    assert ":duration_ms::bigint AS duration_ms" in sql
    assert ":execution_name::varchar AS execution_name" in sql
    assert ":emitted_at::timestamptz AS emitted_at" in sql
    assert "JSON_PARSE(:record_json) AS record_json" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql

    params = _params(call)
    assert params["execution_arn"] == record["executionArn"]
    assert params["function_name"] == "fn"
    assert params["status"] == "SUCCEEDED"
    assert params["start_time"] == "2026-07-15T11:59:58.000Z"
    assert params["end_time"] == "2026-07-15T12:00:00.000Z"
    assert params["duration_ms"] == "2000"
    assert params["execution_name"] == "my-exec"
    assert params["emitted_at"] == "2026-07-15T12:00:00.000Z"
    assert all(isinstance(v, str) for v in params.values())
    assert ", " not in params["record_json"]
    assert json.loads(params["record_json"]) == record


def test_absent_nullable_fields_become_typed_null_literals() -> None:
    client = FakeRedshiftDataClient()
    record = _record(status="RUNNING")
    del record["executionName"]
    del record["endTime"]
    del record["durationMs"]
    RedshiftExporter(
        database="insight",
        cluster_identifier="insight-cluster",
        db_user="admin",
        secret_arn="arn:aws:secretsmanager:us-east-1:123456789012:secret:s",
        client=client,
    ).export(record)

    call = client.calls[0]
    assert call["ClusterIdentifier"] == "insight-cluster"
    assert call["DbUser"] == "admin"
    assert call["SecretArn"] == "arn:aws:secretsmanager:us-east-1:123456789012:secret:s"
    assert "WorkgroupName" not in call
    assert "NULL::timestamptz AS end_time" in call["Sql"]
    assert "NULL::bigint AS duration_ms" in call["Sql"]
    assert "NULL::varchar AS execution_name" in call["Sql"]
    params = _params(call)
    assert "end_time" not in params
    assert "duration_ms" not in params
    assert "execution_name" not in params


def test_custom_schema_and_table_are_validated() -> None:
    exporter = RedshiftExporter(
        database="d",
        workgroup_name="wg",
        schema="analytics",
        table="wi_records",
        client=FakeRedshiftDataClient(),
    )
    assert exporter.fq_table == "analytics.wi_records"
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        RedshiftExporter(
            database="d",
            workgroup_name="wg",
            schema="a.b",
            client=FakeRedshiftDataClient(),
        )


def test_requires_workgroup_or_cluster() -> None:
    with pytest.raises(
        ValueError, match="exactly one of workgroup_name or cluster_identifier"
    ):
        RedshiftExporter(database="insight", client=FakeRedshiftDataClient())


def test_rejects_both_workgroup_and_cluster() -> None:
    with pytest.raises(ValueError, match="exactly one of"):
        RedshiftExporter(
            database="insight",
            workgroup_name="wg",
            cluster_identifier="cluster",
            client=FakeRedshiftDataClient(),
        )
