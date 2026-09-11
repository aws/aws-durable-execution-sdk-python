# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``SQSExporter`` (fake client, no AWS)."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from aws_durable_execution_sdk_python_insight import SQSExporter
from aws_durable_execution_sdk_python_insight.exporters.sqs_exporter import (
    SQSExporter as SQSExporterFromModule,
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


STANDARD_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/insight"
FIFO_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/insight.fifo"


class FakeSqsClient:
    def __init__(self) -> None:
        self.sends: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.sends.append(kwargs)
        return {"MessageId": "m-1"}


def test_public_import_path_and_defaults() -> None:
    assert SQSExporter is SQSExporterFromModule
    exporter = SQSExporter(queue_url=STANDARD_URL, client=FakeSqsClient())
    assert exporter.max_record_size_bytes == 256_000
    assert exporter.operations_format == "array"
    assert exporter.is_fifo is False
    exporter.flush()  # no buffering: a no-op


def test_standard_queue_message_has_attributes_and_no_fifo_fields() -> None:
    client = FakeSqsClient()
    record = _record()
    exporter = SQSExporter(queue_url=STANDARD_URL, client=client)
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(client.sends) == 1
    send = client.sends[0]
    assert send["QueueUrl"] == STANDARD_URL
    assert "MessageGroupId" not in send
    assert "MessageDeduplicationId" not in send
    assert send["MessageAttributes"] == {
        "status": {"DataType": "String", "StringValue": "SUCCEEDED"},
        "functionName": {"DataType": "String", "StringValue": "fn"},
    }
    body = send["MessageBody"]
    assert ", " not in body and '": ' not in body
    assert json.loads(body) == record


def test_fifo_queue_sets_group_and_dedup_ids() -> None:
    client = FakeSqsClient()
    record = _record()
    exporter = SQSExporter(queue_url=FIFO_URL, client=client)
    assert exporter.is_fifo is True
    exporter.export(record)
    send = client.sends[0]
    assert send["MessageGroupId"] == record["executionArn"]
    assert send["MessageDeduplicationId"] == (
        f"{record['executionArn']}:2026-07-15T12:00:00.000Z"
    )


def test_fifo_honors_explicit_message_group_id() -> None:
    client = FakeSqsClient()
    SQSExporter(queue_url=FIFO_URL, message_group_id="grp", client=client).export(
        _record()
    )
    assert client.sends[0]["MessageGroupId"] == "grp"


def test_by_name_format_renders_operations_by_name() -> None:
    client = FakeSqsClient()
    SQSExporter(
        queue_url=STANDARD_URL, operations_format="by-name", client=client
    ).export(_record())
    body = json.loads(client.sends[0]["MessageBody"])
    assert "operations" not in body
    assert body["operationsByName"]["fetch-user"]["count"] == 1


def test_fifo_ids_over_128_chars_are_hashed() -> None:
    long_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:"
        + "a-very-long-function-name-" * 3
        + ":$LATEST/durable-execution/"
        + "e" * 64
        + "/inv-1"
    )
    assert len(long_arn) > 128
    client = FakeSqsClient()
    record = _record(executionArn=long_arn)
    SQSExporter(queue_url=FIFO_URL, client=client).export(record)
    send = client.sends[0]
    expected_group = hashlib.sha256(long_arn.encode("utf-8")).hexdigest()
    expected_dedup = hashlib.sha256(
        f"{long_arn}:{record['emittedAt']}".encode()
    ).hexdigest()
    assert send["MessageGroupId"] == expected_group
    assert send["MessageDeduplicationId"] == expected_dedup
    assert len(send["MessageGroupId"]) <= 128
    assert len(send["MessageDeduplicationId"]) <= 128
    # short ids are still passed through untouched
    short = _record()
    SQSExporter(queue_url=FIFO_URL, client=client).export(short)
    assert client.sends[1]["MessageGroupId"] == short["executionArn"]
