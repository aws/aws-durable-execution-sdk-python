# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the first-party exporters (no AWS; a fake S3 client is used).

These lock the behavior preserved when ``exporters.py`` was split into the
``exporters`` package (one module per destination). Imports are exercised via
both the public re-export path and the concrete submodules.
"""

from __future__ import annotations

import json
from typing import Any

from aws_durable_execution_sdk_python_insight import LambdaLogExporter, S3Exporter
from aws_durable_execution_sdk_python_insight.exporters import (
    LambdaLogExporter as LambdaLogExporterFromPkg,
)
from aws_durable_execution_sdk_python_insight.exporters.s3_exporter import (
    S3Exporter as S3ExporterFromModule,
)


def _record(**kw: Any) -> dict[str, Any]:
    base = {
        "recordType": "WorkflowInsight",
        "schemaVersion": "1.0",
        "executionArn": "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1",
        "executionName": "exec-1",
        "functionName": "my-fn",
        "status": "SUCCEEDED",
        "startTime": "2026-01-01T00:00:00.000Z",
        "operations": [
            {
                "id": "a",
                "name": "greet",
                "type": "STEP",
                "subType": "Step",
                "status": "SUCCEEDED",
            }
        ],
    }
    base.update(kw)
    return base


class FakeS3Client:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> None:
        self.puts.append(kwargs)


# -- re-export compatibility --------------------------------------------------


def test_public_import_paths_resolve_same_classes():
    assert LambdaLogExporter is LambdaLogExporterFromPkg
    assert S3Exporter is S3ExporterFromModule


# -- LambdaLogExporter --------------------------------------------------------


def test_lambda_log_default_size_and_render_is_operations_by_name():
    exporter = LambdaLogExporter()
    assert exporter.max_record_size_bytes == 256_000
    shaped = exporter.render(_record())
    assert "operations" not in shaped
    assert shaped["operationsByName"]["greet"]["count"] == 1


def test_lambda_log_export_prints_compact_json_line(capsys):
    LambdaLogExporter().export(_record())
    out = capsys.readouterr().out.strip()
    # single compact JSON line, no whitespace separators
    assert ", " not in out and '": ' not in out
    parsed = json.loads(out)
    assert parsed["recordType"] == "WorkflowInsight"
    assert "operationsByName" in parsed


def test_lambda_log_custom_size_and_flush_noop():
    exporter = LambdaLogExporter(max_record_size_bytes=1234)
    assert exporter.max_record_size_bytes == 1234
    assert exporter.flush() is None


# -- S3Exporter ---------------------------------------------------------------


def test_s3_render_is_identity_and_body_is_compact():
    client = FakeS3Client()
    exporter = S3Exporter(bucket="b", client=client)
    rec = _record()
    assert exporter.render(rec) is rec
    exporter.export(rec)
    assert len(client.puts) == 1
    put = client.puts[0]
    assert put["Bucket"] == "b"
    assert put["ContentType"] == "application/json"
    body = put["Body"].decode("utf-8")
    assert ", " not in body and '": ' not in body
    assert json.loads(body)["executionName"] == "exec-1"


def test_s3_default_size_and_date_partition_key():
    client = FakeS3Client()
    S3Exporter(bucket="b", client=client).export(_record())
    key = client.puts[0]["Key"]
    assert key == "workflow-insight/year=2026/month=01/day=01/exec-1.json"
    assert S3Exporter(bucket="b", client=client).max_record_size_bytes == 5_000_000


def test_s3_function_name_partition_and_sanitization():
    client = FakeS3Client()
    exporter = S3Exporter(
        bucket="b", partitioning="function-name", prefix="wi/", client=client
    )
    exporter.export(_record(functionName="fn/weird name", executionName="exec/1"))
    key = client.puts[0]["Key"]
    assert key == "wi/function=fn_weird_name/exec_1.json"


def test_s3_key_falls_back_to_arn_then_record():
    client = FakeS3Client()
    exporter = S3Exporter(bucket="b", partitioning="none", client=client)
    rec = _record()
    del rec["executionName"]
    exporter.export(rec)
    # falls back to the (sanitized) executionArn
    assert client.puts[0]["Key"].endswith(".json")
    assert "arn_aws_lambda" in client.puts[0]["Key"]
