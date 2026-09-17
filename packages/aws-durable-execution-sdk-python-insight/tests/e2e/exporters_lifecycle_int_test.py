# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end test: first-party exporters receive records through the real plugin.

Drives the plugin through the repository's LOCAL durable runner
(``DurableFunctionTestRunner``) and the real ``@durable_execution`` lifecycle
with three shipped exporters attached at once:

* ``FileExporter`` in ``ndjson`` mode with ``operations_format="by-name"`` and
  ``on-change`` emission, so each delivered snapshot and the terminal record
  land as separate lines in one dated file (the export worker may coalesce
  RUNNING snapshots, so their count is not asserted);
* ``FileExporter`` in ``json`` mode, so the per-execution file is overwritten
  and ends holding only the terminal record;
* ``SQSExporter`` with an injected recording client and a small
  ``max_record_size_bytes``, so the plugin's size limiter truncates the copy
  that exporter receives without touching the copies the others receive.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)

from aws_durable_execution_sdk_python_insight import (
    FileExporter,
    SQSExporter,
    WorkflowInsightConfig,
    workflow_insight,
)
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


_STEP_NAMES = ("first", "second", "third")


class _RecordingSqsClient:
    def __init__(self) -> None:
        self.sends: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.sends.append(kwargs)
        return {"MessageId": "m"}


def _three_steps_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    for name in _STEP_NAMES:
        context.step(_step_returning(name), name=name)
    return "done"


def _step_returning(value: str) -> Callable[[Any], str]:
    return lambda _step_ctx: value


def test_exporters_receive_records_through_the_plugin_lifecycle(
    tmp_path: Path,
) -> None:
    ndjson_dir = tmp_path / "ndjson"
    json_dir = tmp_path / "json"
    sqs_client = _RecordingSqsClient()
    sqs = SQSExporter(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/insight",
        max_record_size_bytes=800,
        client=sqs_client,
    )
    plugin = workflow_insight(
        WorkflowInsightConfig(
            exporters=[
                FileExporter(directory=ndjson_dir, operations_format="by-name"),
                FileExporter(directory=json_dir, mode="json"),
                sqs,
            ],
            emit_mode="on-change",
        )
    )
    handler = durable_execution(_three_steps_handler, plugins=[plugin])

    with DurableFunctionTestRunner(handler=handler, execution_timeout=15) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}")
    assert result.status is InvocationStatus.SUCCEEDED

    # ndjson + by-name: one line per delivered emission, name-keyed operations,
    # terminal last. The export worker coalesces pending on-change snapshots, so
    # the number of RUNNING lines is not fixed; every line must still be well
    # formed and the terminal record must be the last one written.
    ndjson_files = list(ndjson_dir.iterdir())
    assert len(ndjson_files) == 1
    lines = ndjson_files[0].read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines]
    assert len(records) >= 1
    assert all("operations" not in r and "operationsByName" in r for r in records)
    assert {r["status"] for r in records[:-1]} <= {"RUNNING"}
    terminal = records[-1]
    assert terminal["status"] == "SUCCEEDED"
    assert set(terminal["operationsByName"]) == set(_STEP_NAMES)
    assert all(
        terminal["operationsByName"][n]["status"] == "SUCCEEDED" for n in _STEP_NAMES
    )

    # json mode: overwritten per emission, so the file holds the terminal record.
    json_files = list(json_dir.iterdir())
    assert len(json_files) == 1
    final = json.loads(json_files[0].read_text(encoding="utf-8"))
    assert final["status"] == "SUCCEEDED"
    assert [op["name"] for op in final["operations"]] == list(_STEP_NAMES)
    assert final["executionArn"] == terminal["executionArn"]

    # SQS with a small limit: the same emissions arrive, and the terminal copy is
    # truncated to fit while the file copies above stayed intact.
    assert len(sqs_client.sends) == len(records)
    sqs_terminal = json.loads(sqs_client.sends[-1]["MessageBody"])
    assert sqs_terminal["status"] == "SUCCEEDED"
    assert sqs_terminal.get("truncated") is True
    assert len(sqs_client.sends[-1]["MessageBody"].encode("utf-8")) <= 800
    assert "truncated" not in final
    assert sqs_client.sends[-1]["MessageAttributes"]["status"]["StringValue"] == (
        "SUCCEEDED"
    )
