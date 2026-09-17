# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""OpenTelemetry (OTLP/HTTP logs) Workflow Insight exporter."""

from __future__ import annotations

import datetime
from enum import StrEnum
from typing import Any, Literal

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    http_send,
    parse_iso_datetime,
)
from aws_durable_execution_sdk_python_insight.operations_index import (
    OperationsFormat,
    OperationsFormatInput,
    apply_operations_format,
)


class OTelProtocol(StrEnum):
    """OTLP transport encoding."""

    HTTP_JSON = "http/json"
    HTTP_PROTOBUF = "http/protobuf"


# Accepted string inputs, kept in lockstep with the enum values above.
OTelProtocolInput = Literal["http/json", "http/protobuf"]

_SCOPE_NAME = "aws-durable-execution-sdk-python-insight"
_SEVERITY = {"FAILED": 17, "RUNNING": 9, "SUCCEEDED": 9}  # ERROR / INFO / INFO
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)


def _kv(key: str, value: str | int) -> dict[str, Any]:
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    return {"key": key, "value": {"stringValue": value}}


def _to_nano(iso: str) -> str:
    # Keep the record's full microsecond precision (integer math, no float).
    delta = parse_iso_datetime(iso) - _EPOCH
    seconds = delta.days * 86_400 + delta.seconds
    return str(seconds * 1_000_000_000 + delta.microseconds * 1_000)


class OTelExporter:
    """Posts each record as one OTLP log record to an OTLP/HTTP endpoint.

    Record identity fields become resource and log attributes; the record
    itself, rendered per ``operations_format``, is the log body. Only
    ``http/json`` is supported; ``http/protobuf`` raises at construction.
    """

    def __init__(
        self,
        endpoint: str,
        headers: dict[str, str] | None = None,
        protocol: OTelProtocol | OTelProtocolInput = OTelProtocol.HTTP_JSON,
        operations_format: OperationsFormat | OperationsFormatInput = (
            OperationsFormat.ARRAY
        ),
        max_record_size_bytes: int | None = None,
    ) -> None:
        if OTelProtocol(protocol) == OTelProtocol.HTTP_PROTOBUF:
            msg = "OTelExporter: http/protobuf is not yet supported. Use http/json."
            raise ValueError(msg)
        self.endpoint = endpoint
        self.headers = dict(headers or {})
        self.operations_format = OperationsFormat(operations_format)
        self.max_record_size_bytes: int | None = (
            1_000_000 if max_record_size_bytes is None else max_record_size_bytes
        )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        # The whole request is measured: the record sits inside body.stringValue,
        # so the size limit covers both the record and the OTLP envelope.
        return self._build_payload(record)

    def export(self, record: dict[str, Any]) -> None:
        body = compact_dumps(self._build_payload(record)).encode("utf-8")
        headers = {"Content-Type": "application/json", **self.headers}
        status, reason, _ = http_send("POST", self.endpoint, headers, body)
        if not 200 <= status < 300:
            msg = f"OTelExporter: OTLP endpoint returned {status} {reason}"
            raise RuntimeError(msg)

    def flush(self) -> None:
        return None

    def _build_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        """Build an OTLP ``ExportLogsServiceRequest``."""
        function_name = record.get("functionName", "")
        status = record.get("status", "")
        return {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            _kv("service.name", function_name),
                            _kv("cloud.region", record.get("region", "")),
                            _kv("cloud.account.id", record.get("accountId", "")),
                            _kv("faas.name", function_name),
                            _kv("faas.version", record.get("functionQualifier", "")),
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {
                                "name": _SCOPE_NAME,
                                "version": record.get("schemaVersion", ""),
                            },
                            "logRecords": [
                                {
                                    "timeUnixNano": _to_nano(record["emittedAt"]),
                                    "severityNumber": _SEVERITY.get(status, 0),
                                    "severityText": status,
                                    "body": {
                                        "stringValue": compact_dumps(
                                            apply_operations_format(
                                                record, self.operations_format
                                            )
                                        )
                                    },
                                    "attributes": [
                                        _kv(
                                            "workflow.execution_arn",
                                            record["executionArn"],
                                        ),
                                        _kv(
                                            "workflow.execution_name",
                                            record.get("executionName") or "",
                                        ),
                                        _kv("workflow.status", status),
                                        _kv(
                                            "workflow.duration_ms",
                                            record.get("durationMs") or 0,
                                        ),
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
