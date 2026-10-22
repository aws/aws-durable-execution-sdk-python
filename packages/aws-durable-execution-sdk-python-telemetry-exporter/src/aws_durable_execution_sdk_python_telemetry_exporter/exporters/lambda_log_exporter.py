# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lambda log (CloudWatch) Durable Execution Telemetry exporter."""

from __future__ import annotations

import datetime
import re
from typing import Any

from aws_durable_execution_sdk_python_telemetry_exporter.exporters._common import (
    compact_dumps,
)
from aws_durable_execution_sdk_python_telemetry_exporter.operations_index import (
    with_operations_by_name,
)


# U+2028 and U+2029 (line breaks to some tools) and lone surrogates (no UTF-8
# encoding, so printing one raises). A JSON line holds them only inside
# strings, where a \uXXXX escape is valid and decodes to the same text.
_ESCAPED_IN_LINE = re.compile("[\u2028\u2029\ud800-\udfff]")


def _escape(match: re.Match[str]) -> str:
    return f"\\u{ord(match.group()):04x}"


def _line(rendered: Any) -> str:
    return _ESCAPED_IN_LINE.sub(_escape, compact_dumps(rendered))


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
    the JSON escaping of ``message``; ``measure`` returns the size of that
    exact line. U+2028 and U+2029 are written as ``\\u2028`` and ``\\u2029``
    so tools that split on them keep one record per line, and a lone surrogate
    is written as its ``\\uXXXX`` escape so printing it never raises.
    CloudWatch Logs accepts events of up to 1 MB.
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

    def measure(self, rendered: Any) -> int | None:
        """Return the UTF-8 size of the line printed for ``rendered``."""
        try:
            return len(_line(rendered).encode("utf-8"))
        except (TypeError, ValueError):
            return None

    def export(self, record: dict[str, Any]) -> None:
        line = _line(self.render(record))
        # print (not logging): Lambda leaves stdout lines as written in both
        # the Text and JSON log formats, so the envelope is never wrapped twice.
        print(line, flush=True)  # noqa: T201

    def flush(self) -> None:
        return None
