# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Public callback failures preserve error-detail semantics across stores."""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import OperationEndInfo, OperationType
from aws_durable_execution_sdk_python_testing.model import (
    GetDurableExecutionHistoryResponse,
    events_to_operations,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from opentelemetry import context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC_DIR = Path(__file__).resolve().parents[1] / "src"


def _run_public_callback_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    filesystem: bool,
    error_kind: str,
) -> dict[str, object]:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    callback_errors: list[ErrorObject | None] = []
    original_end = plugin.on_operation_end

    def observe_error(info: OperationEndInfo) -> None:
        if info.operation_type is OperationType.CALLBACK:
            callback_errors.append(info.error)
        original_end(info)

    monkeypatch.setattr(plugin, "on_operation_end", observe_error)
    original_context = context.get_current()
    common_spec = importlib.util.spec_from_file_location(
        "common", SRC_DIR / "common.py"
    )
    assert common_spec is not None and common_spec.loader is not None
    common = importlib.util.module_from_spec(common_spec)
    monkeypatch.setitem(sys.modules, "common", common)
    common_spec.loader.exec_module(common)
    monkeypatch.setattr(common, "otel_plugin", lambda: plugin)
    spec = importlib.util.spec_from_file_location(
        "otel_17_wait_for_callback_failure",
        SRC_DIR / "otel_17_wait_for_callback_failure.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    error = {
        "omitted": None,
        "empty": ErrorObject(message=None, type=None, data=None, stack_trace=None),
        "rich": ErrorObject(
            message="explicit failure",
            type="CallbackFailure",
            data=None,
            stack_trace=None,
        ),
        "empty-message": ErrorObject(
            message="", type=None, data=None, stack_trace=None
        ),
        "empty-stack": ErrorObject(message=None, type=None, data=None, stack_trace=[]),
        "empty-type": ErrorObject(message=None, type="", data=None, stack_trace=None),
        "empty-data": ErrorObject(message=None, type=None, data="", stack_trace=None),
    }[error_kind]
    try:
        store = FileSystemExecutionStore.create(tmp_path) if filesystem else None
        with DurableFunctionTestRunner(
            handler=module.handler,
            store=store,
            poll_interval=0.01,
            execution_timeout=20,
            skip_time=False,
        ) as runner:
            arn = runner.run_async(
                input=json.dumps({"scenario": "wait-for-callback-failure"})
            )
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                events = runner.get_execution_history(
                    arn, include_execution_data=True
                ).events
                starts = [
                    event for event in events if event.event_type == "CallbackStarted"
                ]
                if starts and any(
                    event.event_type == "InvocationCompleted"
                    and event.event_id > starts[0].event_id
                    for event in events
                ):
                    details = starts[0].callback_started_details
                    assert details is not None and details.callback_id is not None
                    callback_id = details.callback_id
                    break
                time.sleep(0.01)
            else:
                raise AssertionError("Callback did not suspend before failure")
            if error_kind == "omitted":
                runner.send_callback_failure(callback_id)
            else:
                runner.send_callback_failure(callback_id, error=error)
            result = runner.wait_for_result(arn, timeout=10)
            with_data = runner.get_execution_history(arn, include_execution_data=True)
            without_data = runner.get_execution_history(
                arn, include_execution_data=False
            )

        assert result.status.value == "FAILED"
        spans = exporter.get_finished_spans()
        leaves = [
            span
            for span in spans
            if (span.attributes or {}).get("durable.operation.type") == "CALLBACK"
            and (span.attributes or {}).get("durable.operation.status") == "FAILED"
        ]
        assert len(leaves) == 1
        has_details = error_kind not in {"omitted", "empty"}
        assert [
            item.to_dict() if item is not None else None for item in callback_errors
        ] == [error.to_dict() if has_details and error is not None else None]
        if has_details and not filesystem:
            assert callback_errors[0] is error
        assert leaves[0].status.status_code.name == (
            "ERROR" if has_details else "UNSET"
        )
        assert [event.name for event in leaves[0].events] == (
            ["exception"] if has_details else []
        )
        # The public failed future still raises, so its enclosing context fails.
        parents = [
            span
            for span in spans
            if span.name == "otel-failed-callback"
            and (span.attributes or {}).get("durable.operation.status") == "FAILED"
        ]
        assert len(parents) == 1
        assert parents[0].status.status_code.name == "ERROR"
        invocations = sorted(
            [span for span in spans if span.name == "Invocation"],
            key=lambda span: span.start_time or 0,
        )
        statuses = [
            (span.attributes or {}).get("durable.invocation.status")
            for span in invocations
        ]
        assert statuses == ["PENDING", "FAILED"]
        assert result.error is not None
        assert context.get_current() == original_context
        for history in (with_data, without_data):
            wire = history.to_dict()
            decoded = GetDurableExecutionHistoryResponse.from_dict(wire)
            original_error = next(
                event["CallbackFailedDetails"]["Error"]
                for event in wire["Events"]
                if event["EventType"] == "CallbackFailed"
            )
            for candidate in (history, decoded):
                assert (
                    next(
                        event.to_dict()["CallbackFailedDetails"]["Error"]
                        for event in candidate.events
                        if event.event_type == "CallbackFailed"
                    )
                    == original_error
                )
                callback = next(
                    operation
                    for operation in events_to_operations(candidate.events)
                    if operation.callback_details is not None
                )
                details = callback.callback_details
                assert details is not None
                assert (
                    details.error.to_dict() if details.error is not None else None
                ) == (error.to_dict() if has_details and error is not None else None)
        return {
            "status": result.status.value,
            "caller_error": dict(result.error.to_dict()),
            "invocation_statuses": statuses,
            "submitted_error": dict(error.to_dict()) if error is not None else {},
            "history_error": next(
                event.to_dict()["CallbackFailedDetails"]["Error"]
                for event in with_data.events
                if event.event_type == "CallbackFailed"
            ),
            "metadata_error": next(
                event.to_dict()["CallbackFailedDetails"]["Error"]
                for event in without_data.events
                if event.event_type == "CallbackFailed"
            ),
        }
    finally:
        provider.shutdown()


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize(
    "error_kind",
    [
        "omitted",
        "empty",
        "rich",
        "empty-message",
        "empty-stack",
        "empty-type",
        "empty-data",
    ],
)
def test_public_callback_failure_preserves_error_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    error_kind: str,
) -> None:
    memory = _run_public_callback_failure(
        monkeypatch, tmp_path / "memory", plugin_type, False, error_kind
    )
    filesystem = _run_public_callback_failure(
        monkeypatch, tmp_path / "filesystem", plugin_type, True, error_kind
    )
    assert memory == filesystem
    expected_payload = memory["submitted_error"]
    for outcome in [memory, filesystem]:
        # Actual AWS history retains an empty Payload object for this failure,
        # independently of the SDK-facing absent callback error.
        # Retain the existing flags for nonempty errors; only the observed
        # no-details projection is changed here.
        assert outcome["history_error"] == {
            "Payload": expected_payload,
            "Truncated": bool(expected_payload),
        }
        # Preserve this API's existing metadata-only projection.
        assert outcome["metadata_error"] == (
            {"Payload": expected_payload, "Truncated": True}
            if expected_payload
            else {"Truncated": True}
        )
