# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""HTTP / webhook Workflow Insight exporter."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    http_send,
)
from aws_durable_execution_sdk_python_insight.operations_index import (
    OperationsFormat,
    OperationsFormatInput,
    apply_operations_format,
)


class HttpMethod(StrEnum):
    """Request method. ``PUT`` suits endpoints that upsert by URL path."""

    POST = "POST"
    PUT = "PUT"


# Accepted string inputs, kept in lockstep with the enum values above.
HttpMethodInput = Literal["POST", "PUT"]


class HttpExporter:
    """Sends each record as a JSON body to any HTTP endpoint.

    The endpoint must answer 2xx; any other status raises. ``timeout_ms``
    bounds the whole request (default 10 seconds). ``max_record_size_bytes``
    has no default because a generic endpoint has no known limit.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        method: HttpMethod | HttpMethodInput = HttpMethod.POST,
        timeout_ms: int = 10_000,
        operations_format: OperationsFormat | OperationsFormatInput = (
            OperationsFormat.ARRAY
        ),
        max_record_size_bytes: int | None = None,
    ) -> None:
        self.url = url
        self.method = HttpMethod(method)
        self.headers = dict(headers or {})
        self.timeout_ms = timeout_ms
        self.operations_format = OperationsFormat(operations_format)
        self.max_record_size_bytes = max_record_size_bytes

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return apply_operations_format(record, self.operations_format)

    def export(self, record: dict[str, Any]) -> None:
        body = compact_dumps(self.render(record)).encode("utf-8")
        headers = {"Content-Type": "application/json", **self.headers}
        status, reason, _ = http_send(
            self.method.value, self.url, headers, body, timeout=self.timeout_ms / 1000
        )
        if not 200 <= status < 300:
            msg = f"HttpExporter: endpoint returned {status} {reason}"
            raise RuntimeError(msg)

    def flush(self) -> None:
        return None
