# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end test: record fields that are aligned with the JS exporter.

Drives the plugin through the LOCAL durable runner (``DurableFunctionTestRunner``)
and the real ``@durable_execution`` lifecycle, so the checkpointed values the
plugin decodes (step results, ErrorObjects) are produced by the SDK itself:

* an opted-in step result is the plain JSON value, not the serializer envelope;
* ``error.name`` is the simple name of the user's error type, for a failure on
  the first invocation and for one reached after a retry (a later invocation
  that replays the checkpointed failure);
* a RUNNING record built at invocation end (retry suspension) carries
  ``endTime``/``durationMs``, while start and operation-change records do not.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.retries import (
    JitterStrategy,
    RetryStrategyConfig,
    create_retry_strategy,
)

from aws_durable_execution_sdk_python_telemetry_exporter import (
    ContentConfig,
    ContentOperations,
    DurableExecutionTelemetryConfig,
    EmitMode,
    OperationOverride,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


class PaymentError(Exception):
    """A user-defined error raised from step code."""


class _CaptureExporter:
    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self.records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        return None


def _run(handler_fn: Any, config: DurableExecutionTelemetryConfig) -> None:
    handler = durable_execution(
        handler_fn, plugins=[durable_execution_telemetry(config)]
    )
    with DurableFunctionTestRunner(handler=handler, execution_timeout=30) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}")
    assert result.status in (InvocationStatus.SUCCEEDED, InvocationStatus.FAILED)


def test_opted_in_result_is_plain_json() -> None:
    def handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        context.step(
            lambda _s: {"amount": 99.99, "items": [{"sku": "a"}]}, name="charge"
        )
        return "done"

    capture = _CaptureExporter()
    _run(
        handler,
        DurableExecutionTelemetryConfig(
            exporters=[capture],
            content=ContentConfig(
                operations=ContentOperations(
                    overrides=[OperationOverride("charge", result=lambda r: r)]
                )
            ),
        ),
    )
    (record,) = capture.records
    (charge,) = [op for op in record["operations"] if op["name"] == "charge"]
    assert charge["result"] == {"amount": 99.99, "items": [{"sku": "a"}]}


def _failing_handler(attempts: int) -> Any:
    config = StepConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=attempts,
                initial_delay=Duration.from_seconds(1),
                jitter_strategy=JitterStrategy.NONE,
            )
        )
    )

    def handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        def charge(_s: Any) -> str:
            msg = "card declined"
            raise PaymentError(msg)

        context.step(charge, name="charge", config=config)
        return "unreachable"

    return handler


def _terminal_failure(
    attempts: int, emit_mode: EmitMode = EmitMode.ON_COMPLETE
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    capture = _CaptureExporter()
    _run(
        _failing_handler(attempts),
        DurableExecutionTelemetryConfig(exporters=[capture], emit_mode=emit_mode),
    )
    terminal = capture.records[-1]
    assert terminal["status"] == "FAILED"
    return terminal, capture.records


def test_error_name_is_user_type_on_first_run_and_after_replay() -> None:
    first_run, _ = _terminal_failure(attempts=1)
    after_retry, _ = _terminal_failure(attempts=2)
    for record in (first_run, after_retry):
        assert record["error"] == {"name": "PaymentError", "message": "card declined"}
        (charge,) = record["operations"]
        assert charge["error"] == {"name": "PaymentError", "message": "card declined"}


def test_running_record_at_retry_suspension_has_end_time() -> None:
    _, records = _terminal_failure(attempts=2, emit_mode=EmitMode.ON_CHANGE)
    running_with_end = [
        r for r in records if r["status"] == "RUNNING" and "endTime" in r
    ]
    # The first invocation ends PENDING while the step waits to retry; that
    # invocation-end record is RUNNING with endTime/durationMs, like JS.
    assert len(running_with_end) >= 1
    for record in running_with_end:
        assert isinstance(record["durationMs"], int)
        assert "output" not in record and "error" not in record
