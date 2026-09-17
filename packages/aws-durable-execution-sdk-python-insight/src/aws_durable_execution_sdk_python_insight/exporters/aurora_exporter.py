# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Aurora (RDS Data API) Workflow Insight exporter."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    sql_identifier,
)


class AuroraEngine(StrEnum):
    """Database engine; selects the upsert dialect."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


# Accepted string inputs, kept in lockstep with the enum values above.
AuroraEngineInput = Literal["postgresql", "mysql"]

_COLUMNS = (
    "execution_arn, execution_name, function_name, status, start_time, "
    "end_time, duration_ms, record_json, emitted_at"
)


def _string_or_null(value: Any) -> dict[str, Any]:
    return {"stringValue": value} if value else {"isNull": True}


class AuroraExporter:
    """Upserts one row per execution through the RDS Data API.

    Rows are keyed by ``execution_arn``; a later export for the same execution
    overwrites the row. The full record is stored as JSON in ``record_json``.
    The cluster must have the Data API enabled.
    """

    def __init__(
        self,
        resource_arn: str,
        secret_arn: str,
        database: str,
        engine: AuroraEngine | AuroraEngineInput,
        table: str = "workflow_insight",
        region: str | None = None,
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.resource_arn = resource_arn
        self.secret_arn = secret_arn
        self.database = database
        self.table = sql_identifier(table)
        self.engine = AuroraEngine(engine)
        self.max_record_size_bytes: int | None = (
            1_000_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("rds-data", region_name=region)
                if region
                else boto3.client("rds-data")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        sql = (
            self._postgres_upsert()
            if self.engine == AuroraEngine.POSTGRESQL
            else self._mysql_upsert()
        )
        duration = record.get("durationMs")
        self._client.execute_statement(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            database=self.database,
            sql=sql,
            parameters=[
                {
                    "name": "execution_arn",
                    "value": {"stringValue": record["executionArn"]},
                },
                {
                    "name": "execution_name",
                    "value": _string_or_null(record.get("executionName")),
                },
                {
                    "name": "function_name",
                    "value": {"stringValue": record["functionName"]},
                },
                {"name": "status", "value": {"stringValue": record["status"]}},
                {"name": "start_time", "value": {"stringValue": record["startTime"]}},
                {"name": "end_time", "value": _string_or_null(record.get("endTime"))},
                {
                    "name": "duration_ms",
                    "value": {"longValue": duration}
                    if duration is not None
                    else {"isNull": True},
                },
                {
                    "name": "record_json",
                    "value": {"stringValue": compact_dumps(record)},
                },
                {"name": "emitted_at", "value": {"stringValue": record["emittedAt"]}},
            ],
        )

    def flush(self) -> None:
        return None

    def _postgres_upsert(self) -> str:
        return (
            f"INSERT INTO {self.table}\n"
            f"      ({_COLUMNS})\n"
            "    VALUES\n"
            "      (:execution_arn, :execution_name, :function_name, :status, "
            ":start_time::timestamptz, :end_time::timestamptz, :duration_ms, "
            ":record_json::jsonb, :emitted_at::timestamptz)\n"
            "    ON CONFLICT (execution_arn) DO UPDATE SET\n"
            "      status = EXCLUDED.status,\n"
            "      end_time = EXCLUDED.end_time,\n"
            "      duration_ms = EXCLUDED.duration_ms,\n"
            "      record_json = EXCLUDED.record_json,\n"
            "      emitted_at = EXCLUDED.emitted_at"
        )

    def _mysql_upsert(self) -> str:
        return (
            f"INSERT INTO {self.table}\n"
            f"      ({_COLUMNS})\n"
            "    VALUES\n"
            "      (:execution_arn, :execution_name, :function_name, :status, "
            ":start_time, :end_time, :duration_ms, :record_json, :emitted_at)\n"
            "    ON DUPLICATE KEY UPDATE\n"
            "      status = VALUES(status),\n"
            "      end_time = VALUES(end_time),\n"
            "      duration_ms = VALUES(duration_ms),\n"
            "      record_json = VALUES(record_json),\n"
            "      emitted_at = VALUES(emitted_at)"
        )
