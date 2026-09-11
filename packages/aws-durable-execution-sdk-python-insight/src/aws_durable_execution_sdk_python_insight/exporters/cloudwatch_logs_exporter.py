# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""CloudWatch Logs (PutLogEvents) Workflow Insight exporter."""

from __future__ import annotations

import datetime
import time
from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import compact_dumps
from aws_durable_execution_sdk_python_insight.operations_index import (
    with_operations_by_name,
)


def _is_already_exists(exc: BaseException) -> bool:
    if type(exc).__name__ == "ResourceAlreadyExistsException":
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code")
        return bool(code == "ResourceAlreadyExistsException")
    return False


class CloudWatchLogsExporter:
    """Writes ``operationsByName`` records to a chosen log group with PutLogEvents.

    Unlike ``LambdaLogExporter`` this targets any log group. One log stream is
    created per UTC day, named ``{log_stream_prefix}{YYYY}/{MM}/{DD}``.
    """

    def __init__(
        self,
        log_group_name: str,
        log_stream_prefix: str = "workflow-insight/",
        region: str | None = None,
        max_record_size_bytes: int | None = None,
        client: Any = None,
    ) -> None:
        self.log_group_name = log_group_name
        self.log_stream_prefix = log_stream_prefix
        self.max_record_size_bytes: int | None = (
            256_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        self._created_streams: set[str] = set()
        if client is not None:
            self._client = client
        else:
            import boto3  # deferred: boto3 is provided by the Lambda runtime

            self._client = (
                boto3.client("logs", region_name=region)
                if region
                else boto3.client("logs")
            )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return with_operations_by_name(record)

    def export(self, record: dict[str, Any]) -> None:
        stream_name = self._build_stream_name()
        self._ensure_stream(stream_name)
        self._client.put_log_events(
            logGroupName=self.log_group_name,
            logStreamName=stream_name,
            logEvents=[
                {
                    "timestamp": int(time.time() * 1000),
                    "message": compact_dumps(self.render(record)),
                }
            ],
        )

    def flush(self) -> None:
        return None

    def _build_stream_name(self) -> str:
        now = datetime.datetime.now(datetime.UTC)
        return f"{self.log_stream_prefix}{now:%Y/%m/%d}"

    def _ensure_stream(self, stream_name: str) -> None:
        if stream_name in self._created_streams:
            return
        try:
            self._client.create_log_stream(
                logGroupName=self.log_group_name, logStreamName=stream_name
            )
        except Exception as exc:
            if not _is_already_exists(exc):
                raise
        self._created_streams.add(stream_name)
