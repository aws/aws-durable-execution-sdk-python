# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Filesystem Workflow Insight exporter."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    sanitize,
)
from aws_durable_execution_sdk_python_insight.operations_index import (
    OperationsFormat,
    OperationsFormatInput,
    apply_operations_format,
)


class FileMode(StrEnum):
    """File layout."""

    # append every record to one ``{YYYY-MM-DD}.ndjson`` file per day (default)
    NDJSON = "ndjson"
    # one pretty-printed ``{executionName}.json`` per execution, overwritten
    JSON = "json"


# Accepted string inputs, kept in lockstep with the enum values above.
FileModeInput = Literal["ndjson", "json"]


class FileExporter:
    """Writes records under a directory (EFS mount, a mounted share, or ``/tmp``).

    ``ndjson`` appends one compact line per record to ``{directory}/{date}.ndjson``
    where the date comes from ``emittedAt``. ``json`` writes
    ``{directory}/{executionName}.json`` and overwrites it on every export.
    ``max_record_size_bytes`` has no default.
    """

    def __init__(
        self,
        directory: str | Path,
        mode: FileMode | FileModeInput = FileMode.NDJSON,
        operations_format: OperationsFormat | OperationsFormatInput = (
            OperationsFormat.ARRAY
        ),
        max_record_size_bytes: int | None = None,
    ) -> None:
        self.directory = Path(directory)
        self.mode = FileMode(mode)
        self.operations_format = OperationsFormat(operations_format)
        self.max_record_size_bytes = max_record_size_bytes
        self._dir_created = False

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return apply_operations_format(record, self.operations_format)

    def export(self, record: dict[str, Any]) -> None:
        self._ensure_dir()
        formatted = self.render(record)
        if self.mode == FileMode.NDJSON:
            date = str(record["emittedAt"])[:10]  # YYYY-MM-DD
            path = self.directory / f"{date}.ndjson"
            with path.open("a", encoding="utf-8") as handle:
                handle.write(compact_dumps(formatted) + "\n")
        else:
            file_name = (
                sanitize(record.get("executionName") or record["executionArn"])
                + ".json"
            )
            (self.directory / file_name).write_text(
                json.dumps(formatted, indent=2, ensure_ascii=False), encoding="utf-8"
            )

    def flush(self) -> None:
        return None

    def _ensure_dir(self) -> None:
        if self._dir_created:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        self._dir_created = True
