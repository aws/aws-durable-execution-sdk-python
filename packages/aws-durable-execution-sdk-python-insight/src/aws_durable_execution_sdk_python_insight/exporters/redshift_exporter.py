# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Redshift (Data API) Workflow Insight exporter."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    sql_identifier,
)


_COLUMNS = (
    "execution_arn, execution_name, function_name, status, start_time, "
    "end_time, duration_ms, record_json, emitted_at"
)


class RedshiftExporter:
    """Upserts one row per execution through the Redshift Data API.

    Works with Redshift Serverless (``workgroup_name``) and provisioned
    clusters (``cluster_identifier``). Rows are merged on ``execution_arn`` and
    the full record lands in the ``record_json`` SUPER column. The statement is
    submitted and not awaited.
    """

    def __init__(
        self,
        database: str,
        workgroup_name: str | None = None,
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        secret_arn: str | None = None,
        table: str = "workflow_insight",
        schema: str = "public",
        region: str | None = None,
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        if bool(workgroup_name) == bool(cluster_identifier):
            msg = (
                "RedshiftExporter: provide exactly one of workgroup_name or "
                "cluster_identifier."
            )
            raise ValueError(msg)
        self.database = database
        self.fq_table = f"{sql_identifier(schema)}.{sql_identifier(table)}"
        self.workgroup_name = workgroup_name
        self.cluster_identifier = cluster_identifier
        self.db_user = db_user
        self.secret_arn = secret_arn
        self.max_record_size_bytes: int | None = (
            1_000_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("redshift-data", region_name=region)
                if region
                else boto3.client("redshift-data")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        parameters: list[dict[str, str]] = [
            {"name": "execution_arn", "value": record["executionArn"]},
            {"name": "function_name", "value": record["functionName"]},
            {"name": "status", "value": record["status"]},
            {"name": "start_time", "value": record["startTime"]},
            {"name": "record_json", "value": compact_dumps(record)},
            {"name": "emitted_at", "value": record["emittedAt"]},
        ]

        # The Data API takes neither NULL nor empty-string parameter values, so
        # an absent nullable field becomes a typed NULL literal in the source
        # projection instead of a bound parameter.
        if record.get("endTime"):
            parameters.append({"name": "end_time", "value": record["endTime"]})
            end_time_sel = ":end_time::timestamptz"
        else:
            end_time_sel = "NULL::timestamptz"
        if record.get("durationMs") is not None:
            parameters.append(
                {"name": "duration_ms", "value": str(record["durationMs"])}
            )
            duration_sel = ":duration_ms::bigint"
        else:
            duration_sel = "NULL::bigint"
        if record.get("executionName"):
            parameters.append(
                {"name": "execution_name", "value": record["executionName"]}
            )
            exec_name_sel = ":execution_name::varchar"
        else:
            exec_name_sel = "NULL::varchar"

        # The source row is a subquery and MERGE joins on a source column:
        # Redshift rejects a MERGE whose join is on a parameter or constant
        # (NestedLoop). Time columns are cast to timestamptz and record_json is
        # JSON_PARSEd into the SUPER column.
        sql = (
            f"MERGE INTO {self.fq_table} USING (\n"
            "      SELECT\n"
            "        :execution_arn::varchar AS execution_arn,\n"
            f"        {exec_name_sel} AS execution_name,\n"
            "        :function_name::varchar AS function_name,\n"
            "        :status::varchar AS status,\n"
            "        :start_time::timestamptz AS start_time,\n"
            f"        {end_time_sel} AS end_time,\n"
            f"        {duration_sel} AS duration_ms,\n"
            "        JSON_PARSE(:record_json) AS record_json,\n"
            "        :emitted_at::timestamptz AS emitted_at\n"
            "    ) AS src\n"
            f"    ON {self.fq_table}.execution_arn = src.execution_arn\n"
            "    WHEN MATCHED THEN UPDATE SET\n"
            "      status = src.status,\n"
            "      end_time = src.end_time,\n"
            "      duration_ms = src.duration_ms,\n"
            "      record_json = src.record_json,\n"
            "      emitted_at = src.emitted_at\n"
            "    WHEN NOT MATCHED THEN INSERT\n"
            f"      ({_COLUMNS})\n"
            "    VALUES\n"
            "      (src.execution_arn, src.execution_name, src.function_name, "
            "src.status, src.start_time, src.end_time, src.duration_ms, "
            "src.record_json, src.emitted_at)"
        )

        kwargs: dict[str, Any] = {
            "Database": self.database,
            "Sql": sql,
            "Parameters": parameters,
        }
        # Unset optional targets are omitted rather than passed as None.
        if self.workgroup_name:
            kwargs["WorkgroupName"] = self.workgroup_name
        if self.cluster_identifier:
            kwargs["ClusterIdentifier"] = self.cluster_identifier
        if self.db_user:
            kwargs["DbUser"] = self.db_user
        if self.secret_arn:
            kwargs["SecretArn"] = self.secret_arn
        self._client.execute_statement(**kwargs)

    def flush(self) -> None:
        return None
