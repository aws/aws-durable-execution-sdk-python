# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lambda log (CloudWatch) Workflow Insight exporter."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python_insight.exporters._common import compact_dumps
from aws_durable_execution_sdk_python_insight.operations_index import (
    with_operations_by_name,
)


class LambdaLogExporter:
    """Writes ``operationsByName`` records to the function's own log group via ``print``.

    Port of the JS ``LambdaLogExporter``: ``console.log(JSON.stringify(
    withOperationsByName(record)))``. Requires no extra IAM. Emits the name-keyed
    summary map (``OPERATIONS_BY_NAME``).
    """

    def __init__(self, max_record_size_bytes: int | None = None) -> None:
        self.max_record_size_bytes = (
            256_000 if max_record_size_bytes is None else max_record_size_bytes
        )

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return with_operations_by_name(record)

    def export(self, record: dict[str, Any]) -> None:
        # Raw JSON line to stdout -> the function's CloudWatch log group. The
        # conformance CloudWatch sink json.loads each line (and unwraps the
        # Lambda structured-log envelope when present).
        print(compact_dumps(self.render(record)), flush=True)  # noqa: T201

    def flush(self) -> None:
        return None
