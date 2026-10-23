# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Record timestamps use one fixed-width UTC format: ``YYYY-MM-DDTHH:MM:SS.sssZ``.

Stores compare these values as text. Text order equals time order only when
every value has the same width. So every record timestamp has exactly 3
fraction digits and a literal ``Z``.
"""

from __future__ import annotations

import datetime
import re
from typing import Any

import pytest

from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationEndInfo,
    OperationType,
)

from aws_durable_execution_sdk_python_telemetry_exporter import (
    DurableExecutionTelemetryConfig,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_telemetry_exporter import (
    plugin as plugin_module,
)
from aws_durable_execution_sdk_python_telemetry_exporter._export_scheduler import (
    _ExportState,
)
from aws_durable_execution_sdk_python_telemetry_exporter.plugin import (
    _format_record_timestamp,
    _iso,
)

RECORD_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST"
    "/durable-execution/exec-1/inv-1"
)
UTC = datetime.UTC
PLUS_0530 = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


@pytest.mark.parametrize(
    ("ts", "expected"),
    [
        # Plain isoformat() omits the fraction when microseconds are 0.
        (
            datetime.datetime(2026, 10, 23, 18, 42, 40, microsecond=0, tzinfo=UTC),
            "2026-10-23T18:42:40.000Z",
        ),
        # Microseconds are truncated to milliseconds.
        (
            datetime.datetime(2026, 10, 23, 18, 42, 40, 784119, tzinfo=UTC),
            "2026-10-23T18:42:40.784Z",
        ),
        # Truncation does not round up to the next second.
        (
            datetime.datetime(2026, 10, 23, 18, 42, 40, 999999, tzinfo=UTC),
            "2026-10-23T18:42:40.999Z",
        ),
        (
            datetime.datetime(2026, 10, 23, 18, 42, 40, 500, tzinfo=UTC),
            "2026-10-23T18:42:40.000Z",
        ),
        # A non-UTC aware datetime is converted to UTC.
        (
            datetime.datetime(2026, 10, 24, 0, 12, 40, 784119, tzinfo=PLUS_0530),
            "2026-10-23T18:42:40.784Z",
        ),
    ],
)
def test_format_record_timestamp(ts: datetime.datetime, expected: str) -> None:
    assert _format_record_timestamp(ts) == expected
    assert _iso(ts) == expected
    assert RECORD_TIMESTAMP.match(expected)


def test_iso_returns_none_for_non_datetime() -> None:
    assert _iso(None) is None
    assert _iso("2026-10-23T18:42:40Z") is None


def test_fixed_width_text_order_matches_time_order() -> None:
    times = [
        datetime.datetime(2026, 10, 23, 18, 42, 40, 0, tzinfo=UTC),
        datetime.datetime(2026, 10, 23, 18, 42, 40, 500000, tzinfo=UTC),
        datetime.datetime(2026, 10, 23, 18, 42, 41, 0, tzinfo=UTC),
    ]
    texts = [_format_record_timestamp(t) for t in times]
    assert sorted(texts) == texts


class _CaptureExporter:
    max_record_size_bytes: int | None = None

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, rendered: Any) -> None:
        self.records.append(rendered)

    def flush(self) -> None:
        return None


def test_every_record_timestamp_is_fixed_width() -> None:
    exec_start = datetime.datetime(2026, 10, 23, 18, 42, 40, microsecond=0, tzinfo=UTC)
    op_start = datetime.datetime(2026, 10, 24, 0, 12, 40, 784119, tzinfo=PLUS_0530)
    op_end = datetime.datetime(2026, 10, 23, 18, 42, 46, 999999, tzinfo=UTC)
    op = OperationEndInfo(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name="greet",
        parent_id=None,
        start_time=op_start,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=op_end,
        result=None,
        error=None,
        attempt=1,
    )
    exporter = _CaptureExporter()
    factory = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter])
    )
    start = InvocationStartInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=exec_start,
        execution_input="World",
        operations={},
    )
    plugin = factory.create_plugin(start)
    plugin.on_invocation_start(start)
    plugin.on_invocation_end(
        InvocationEndInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=exec_start,
            status=InvocationStatus.SUCCEEDED,
            error=None,
            execution_result='"Hello, World!"',
            operations={op.operation_id: op},
        )
    )

    assert len(exporter.records) == 1
    record = exporter.records[0]
    values = [record["emittedAt"], record["startTime"], record["endTime"]]
    for entry in record["operations"]:
        values.extend([entry["startTime"], entry["endTime"]])
    assert len(values) == 5
    for value in values:
        assert RECORD_TIMESTAMP.match(value), value

    assert record["startTime"] == "2026-10-23T18:42:40.000Z"
    assert record["operations"][0]["startTime"] == "2026-10-23T18:42:40.784Z"
    assert record["operations"][0]["endTime"] == "2026-10-23T18:42:46.999Z"


# 2026-10-23T18:42:40.123Z in epoch milliseconds.
FROZEN_MS = 1792780960123


def _on_change_emitted_at(
    monkeypatch: pytest.MonkeyPatch, clock_ms: list[int]
) -> list[str]:
    """Run one on-change invocation and return each record's emittedAt.

    The invocation emits RUNNING (start), RUNNING (operation change) and the
    terminal record. ``clock_ms`` supplies the time each build reads, in order.
    The scheduler can merge a pending record into a newer one. So the test
    drains after every hook, and the exporter receives all three records.
    """
    readings = iter(clock_ms)
    monkeypatch.setattr(plugin_module, "_utc_now_ms", lambda: next(readings))
    op = OperationEndInfo(
        operation_id="op-1",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name="greet",
        parent_id=None,
        start_time=None,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=None,
        result=None,
        error=None,
        attempt=1,
    )
    exporter = _CaptureExporter()
    factory = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(exporters=[exporter], emit_mode="on-change")
    )

    def drain() -> None:
        factory._scheduler.drain(_ExportState())

    start = InvocationStartInfo(
        request_id=None,
        execution_arn=ARN,
        is_first_invocation=True,
        execution_start_time=None,
        execution_input="World",
        operations={},
    )
    plugin = factory.create_plugin(start)
    plugin.on_invocation_start(start)
    drain()
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=ARN,
            updated_operations={op.operation_id: op},
            operations={op.operation_id: op},
        )
    )
    drain()
    plugin.on_invocation_end(
        InvocationEndInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=None,
            status=InvocationStatus.SUCCEEDED,
            error=None,
            execution_result='"Hello, World!"',
            operations={op.operation_id: op},
        )
    )
    drain()

    assert [r["status"] for r in exporter.records] == [
        "RUNNING",
        "RUNNING",
        "SUCCEEDED",
    ]
    return [r["emittedAt"] for r in exporter.records]


def test_emitted_at_strictly_increases_when_the_clock_is_frozen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _on_change_emitted_at(monkeypatch, [FROZEN_MS] * 3) == [
        "2026-10-23T18:42:40.123Z",
        "2026-10-23T18:42:40.124Z",
        "2026-10-23T18:42:40.125Z",
    ]


def test_emitted_at_uses_the_clock_once_it_passes_the_last_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [FROZEN_MS, FROZEN_MS, FROZEN_MS + 10]
    assert _on_change_emitted_at(monkeypatch, clock) == [
        "2026-10-23T18:42:40.123Z",
        "2026-10-23T18:42:40.124Z",
        "2026-10-23T18:42:40.133Z",
    ]


def test_emitted_at_strictly_increases_when_the_clock_moves_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [FROZEN_MS + 5, FROZEN_MS, FROZEN_MS + 6]
    assert _on_change_emitted_at(monkeypatch, clock) == [
        "2026-10-23T18:42:40.128Z",
        "2026-10-23T18:42:40.129Z",
        "2026-10-23T18:42:40.130Z",
    ]


def test_utc_now_ms_truncates_to_whole_milliseconds() -> None:
    before = datetime.datetime.now(UTC)
    now_ms = plugin_module._utc_now_ms()
    after = datetime.datetime.now(UTC)
    assert isinstance(now_ms, int)
    text = _format_record_timestamp(
        plugin_module._EPOCH + now_ms * plugin_module._ONE_MS
    )
    assert _format_record_timestamp(before) <= text <= _format_record_timestamp(after)
