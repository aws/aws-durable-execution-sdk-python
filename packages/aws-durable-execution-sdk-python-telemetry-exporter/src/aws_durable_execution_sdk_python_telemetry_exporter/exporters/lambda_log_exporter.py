# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lambda log (CloudWatch) Durable Execution Telemetry exporter."""

from __future__ import annotations

import datetime
from typing import Any

from aws_durable_execution_sdk_python_telemetry_exporter.exporters._common import (
    compact_dumps,
)
from aws_durable_execution_sdk_python_telemetry_exporter.operations_index import (
    with_operations_by_name,
)


class LambdaLogExporter:
    """Writes ``operationsByName`` records to the function's own log group.

    Port of the JS ``LambdaLogExporter``, which calls ``console.log(JSON.stringify(
    withOperationsByName(record)))``. In the Lambda JSON log format the Node.js
    runtime wraps that line in a ``{timestamp, level, requestId, message}``
    envelope, and the record readers for this destination query
    ``level = "INFO"`` and parse ``message``. The Python runtime captures
    ``print`` output as plain text in both log formats, so this exporter writes
    the envelope itself: one stdout line of compact JSON with the keys
    ``timestamp`` (UTC ISO-8601, millisecond precision, ``Z``), ``level``
    (``"INFO"``) and ``message`` (the compact record JSON, as a string). The
    request id is not available to exporters, so ``requestId`` is omitted.
    Requires no extra IAM.

    ``render`` returns this envelope, so ``max_record_size_bytes`` (default
    256,000) limits the printed line itself (in UTF-8 bytes, without the
    trailing newline). Truncation runs once, against the envelope, including
    the JSON escaping of ``message``. U+2028 and U+2029 are written as
    ``\\u2028`` and ``\\u2029`` so tools that split on them keep one record per
    line. CloudWatch Logs accepts events of up to 1 MB.
    """

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        self.max_record_size_bytes: int | None = (
            256_000 if max_record_size_bytes is None else max_record_size_bytes
        )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        timestamp = (
            datetime.datetime.now(datetime.UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        return {
            "timestamp": timestamp,
            "level": "INFO",
            "message": compact_dumps(with_operations_by_name(record)),
        }

    def export(self, record: dict[str, Any]) -> None:
        line = (
            compact_dumps(self.render(record))
            .replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029")
        )
        # print (not logging): Lambda leaves stdout lines as written in both
        # the Text and JSON log formats, so the envelope is never wrapped twice.
        print(line, flush=True)  # noqa: T201

    def flush(self) -> None:
        return None
