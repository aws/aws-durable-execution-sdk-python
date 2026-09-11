# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``FileExporter`` (``tmp_path``, no AWS)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from aws_durable_execution_sdk_python_insight import FileExporter, FileMode
from aws_durable_execution_sdk_python_insight.exporters.file_exporter import (
    FileExporter as FileExporterFromModule,
)


def _record(**overrides: Any) -> dict[str, Any]:
    """A complete SUCCEEDED record; keyword arguments override fields."""
    record: dict[str, Any] = {
        "recordType": "WorkflowInsight",
        "schemaVersion": "1.0",
        "emittedAt": "2026-07-15T12:00:00.000Z",
        "executionArn": (
            "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
            "/durable-execution/my-exec/inv-1"
        ),
        "executionName": "my-exec",
        "functionName": "fn",
        "functionQualifier": "$LATEST",
        "region": "us-east-1",
        "accountId": "123456789012",
        "status": "SUCCEEDED",
        "startTime": "2026-07-15T11:59:58.000Z",
        "endTime": "2026-07-15T12:00:00.000Z",
        "durationMs": 2000,
        "operations": [
            {
                "id": "op-1",
                "name": "fetch-user",
                "type": "STEP",
                "subType": "Step",
                "status": "SUCCEEDED",
                "durationMs": 12,
            }
        ],
    }
    record.update(overrides)
    return record


def test_public_import_path_and_defaults(tmp_path: Path) -> None:
    assert FileExporter is FileExporterFromModule
    exporter = FileExporter(directory=tmp_path)
    assert exporter.mode is FileMode.NDJSON
    assert exporter.operations_format == "array"
    assert exporter.max_record_size_bytes is None
    exporter.flush()  # no buffering: a no-op
    with pytest.raises(ValueError):
        FileExporter(
            directory=tmp_path,
            mode="csv",  # type: ignore[arg-type]  # dynamic invalid value
        )


def test_ndjson_appends_date_partitioned_compact_lines(tmp_path: Path) -> None:
    directory = tmp_path / "nested" / "insight"
    exporter = FileExporter(directory=str(directory))
    first = _record()
    second = _record(executionName="other", status="FAILED")
    assert exporter.render(first) is first
    exporter.export(first)
    exporter.export(second)

    files = sorted(p.name for p in directory.iterdir())
    assert files == ["2026-07-15.ndjson"]
    content = (directory / "2026-07-15.ndjson").read_text(encoding="utf-8")
    assert content.endswith("\n")
    lines = content.split("\n")
    assert lines[-1] == ""
    assert len(lines) == 3
    assert lines[0] == json.dumps(first, separators=(",", ":"), ensure_ascii=False)
    assert json.loads(lines[1])["executionName"] == "other"
    assert isinstance(json.loads(lines[0])["operations"], list)


def test_ndjson_uses_emitted_at_date_per_file(tmp_path: Path) -> None:
    exporter = FileExporter(directory=tmp_path)
    exporter.export(_record(emittedAt="2026-07-16T00:00:00.000Z"))
    exporter.export(_record())
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "2026-07-15.ndjson",
        "2026-07-16.ndjson",
    ]


def test_json_mode_writes_pretty_file_per_execution_and_overwrites(
    tmp_path: Path,
) -> None:
    exporter = FileExporter(directory=tmp_path, mode="json")
    exporter.export(_record(status="RUNNING"))
    exporter.export(_record(status="SUCCEEDED"))

    assert sorted(p.name for p in tmp_path.iterdir()) == ["my-exec.json"]
    content = (tmp_path / "my-exec.json").read_text(encoding="utf-8")
    assert "\n  " in content  # 2-space pretty print
    assert content == json.dumps(
        _record(status="SUCCEEDED"), indent=2, ensure_ascii=False
    )
    assert json.loads(content)["status"] == "SUCCEEDED"


def test_json_mode_sanitizes_name_and_falls_back_to_arn(tmp_path: Path) -> None:
    exporter = FileExporter(directory=tmp_path, mode=FileMode.JSON)
    exporter.export(_record(executionName="exec/with space"))
    record = _record()
    del record["executionName"]
    exporter.export(record)
    names = sorted(p.name for p in tmp_path.iterdir())
    assert "exec_with_space.json" in names
    assert any(n.startswith("arn_aws_lambda") and n.endswith(".json") for n in names)


def test_by_name_format_applies_to_written_record(tmp_path: Path) -> None:
    FileExporter(directory=tmp_path, operations_format="by-name").export(_record())
    line = (tmp_path / "2026-07-15.ndjson").read_text(encoding="utf-8").splitlines()[0]
    parsed = json.loads(line)
    assert "operations" not in parsed
    assert parsed["operationsByName"]["fetch-user"]["count"] == 1


def test_directory_is_created_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Path] = []
    real_mkdir = Path.mkdir

    def counting_mkdir(self: Path, *args: Any, **kwargs: Any) -> None:
        calls.append(self)
        real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", counting_mkdir)
    exporter = FileExporter(directory=tmp_path / "d")
    exporter.export(_record())
    exporter.export(_record())
    assert calls == [tmp_path / "d"]
