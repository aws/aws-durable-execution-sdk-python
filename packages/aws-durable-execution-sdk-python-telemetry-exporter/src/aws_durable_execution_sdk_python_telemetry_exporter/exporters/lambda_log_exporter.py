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

    ``max_record_size_bytes`` limits the rendered record (the ``message``
    string), as for every exporter; the envelope keys and the JSON escaping of
    ``message`` add bytes on top of it. The line can be up to about twice the
    record (escaping doubles each ``"`` and ``\\``), about 20% more for a
    typical record. CloudWatch Logs accepts events of up to 1 MB, so a record
    at the default 256 KB limit fits.
    """

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        self.max_record_size_bytes: int | None = (
            256_000 if max_record_size_bytes is None else max_record_size_bytes
        )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return with_operations_by_name(record)

    def export(self, record: dict[str, Any]) -> None:
        timestamp = (
            datetime.datetime.now(datetime.UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        envelope = {
            "timestamp": timestamp,
            "level": "INFO",
            "message": compact_dumps(self.render(record)),
        }
        # print (not logging): Lambda leaves stdout lines as written in both
        # the Text and JSON log formats, so the envelope is never wrapped twice.
        print(compact_dumps(envelope), flush=True)  # noqa: T201

    def flush(self) -> None:
        return None
