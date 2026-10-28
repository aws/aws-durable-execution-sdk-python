"""Unvisited callback updates survive the public handler's invocation lifecycle."""

from datetime import UTC, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import logging
from threading import Event
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.identifier import OperationIdNamespace
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ContextDetails,
    OperationSubType,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import InvocationStartInfo
from opentelemetry import context as otel_context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    ExecutionOtelPlugin,
    InvocationOtelPluginFactory,
    InvocationOtelPlugin,
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)


EXECUTION_ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:completion:1"
    "/durable-execution/unvisited-callback/execution-id"
)
CALLBACK_ID = "unvisited-callback"


@pytest.mark.parametrize(
    "factory_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("status", [OperationStatus.SUCCEEDED, OperationStatus.FAILED])
@pytest.mark.parametrize("parent_visibility", ["root", "missing", "completed"])
def test_unvisited_input_completion_exports_before_invocation_closes(
    factory_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    status: OperationStatus,
    parent_visibility: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Use service-shaped resume input without fabricating plugin callbacks.

    A handler can finish without revisiting an updated callback, such as a
    callback in a sibling branch after another branch determines the result.
    A later invocation may still contain its terminal checkpoint without
    naming it as newly updated. Only the first invocation should export it.
    """
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    caplog.set_level(logging.WARNING)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = factory_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    start = datetime.now(UTC) - timedelta(seconds=10)
    error = (
        ErrorObject(
            type="CallbackFailed",
            message="callback failed while suspended",
            data=None,
            stack_trace=None,
        )
        if status is OperationStatus.FAILED
        else None
    )
    parent_id = OperationIdNamespace().create_id_for_step(1)
    operations = [
        Operation(
            operation_id="execution-id",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
            start_timestamp=start,
            execution_details=ExecutionDetails(input_payload='{"result": "winner"}'),
        ),
        Operation(
            operation_id=CALLBACK_ID,
            operation_type=OperationType.CALLBACK,
            status=status,
            name="unvisited callback",
            parent_id=(
                parent_id
                if parent_visibility == "completed"
                else "missing-context"
                if parent_visibility == "missing"
                else None
            ),
            start_timestamp=start + timedelta(seconds=1),
            end_timestamp=start + timedelta(seconds=2),
            callback_details=CallbackDetails(
                callback_id="service-callback-id",
                result='"callback-result"' if error is None else None,
                error=error,
            ),
        ),
    ]
    if parent_visibility == "completed":
        operations.append(
            Operation(
                operation_id=parent_id,
                operation_type=OperationType.CONTEXT,
                sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
                name="cached parent",
                status=OperationStatus.SUCCEEDED,
                start_timestamp=start,
                end_timestamp=start + timedelta(seconds=1),
                context_details=ContextDetails(result='"winner"'),
            )
        )
    invocation: dict[str, Any] = {
        "DurableExecutionArn": EXECUTION_ARN,
        "CheckpointToken": "resume-token",
        "InitialExecutionState": {
            "Operations": [operation.to_json_dict() for operation in operations],
            "NextMarker": "",
        },
        "UpdatedOperationIds": [CALLBACK_ID, "unknown-callback"],
    }
    lambda_context = Mock(
        aws_request_id="first-request",
        client_context=None,
        identity=None,
        _epoch_deadline_time_in_ms=0,
        invoked_function_arn="completion:1",
        tenant_id=None,
    )
    counts_during_handler: list[int] = []

    def completed_parent(_context: DurableContext) -> str:
        pytest.fail(
            "A completed parent must return its saved result without repeating its body"
        )

    def body(event: dict[str, str], context: DurableContext) -> str:
        counts_during_handler.append(
            sum(
                (span.attributes or {}).get("durable.operation.id") == CALLBACK_ID
                for span in exporter.get_finished_spans()
            )
        )
        if parent_visibility == "completed":
            return context.run_in_child_context(completed_parent, name="cached parent")
        return event["result"]

    handler = durable_execution(body, plugins=[factory])
    service = Mock()
    before = otel_context.get_current()
    try:
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
            return_value=service,
        ):
            result = handler(invocation, lambda_context)
            assert result == {"Status": "SUCCEEDED", "Result": '"winner"'}
            spans = exporter.get_finished_spans()
            (callback_span,) = [
                span
                for span in spans
                if (span.attributes or {}).get("durable.operation.id") == CALLBACK_ID
            ]
            (invocation_span,) = [span for span in spans if span.name == "Invocation"]
            assert spans.index(callback_span) < spans.index(invocation_span)
            assert callback_span.attributes is not None
            assert callback_span.attributes["durable.operation.status"] == status.value
            assert callback_span.status.status_code is (
                trace.StatusCode.ERROR if error else trace.StatusCode.OK
            )
            assert len(callback_span.events) == int(error is not None)
            assert callback_span.context is not None
            assert callback_span.parent is not None
            assert invocation_span.context is not None
            assert callback_span.context.trace_id == invocation_span.context.trace_id

            if parent_visibility == "completed":
                expected_parent_id = operation_id_to_span_id(EXECUTION_ARN, parent_id)
            elif factory_type is InvocationOtelPluginFactory:
                expected_parent_id = invocation_span.context.span_id
            elif parent_visibility == "missing":
                expected_parent_id = operation_id_to_span_id(
                    EXECUTION_ARN, "missing-context"
                )
            else:
                (workflow_span,) = [span for span in spans if span.name == "Workflow"]
                assert workflow_span.context is not None
                expected_parent_id = workflow_span.context.span_id
            assert callback_span.parent.span_id == expected_parent_id

            invocation["UpdatedOperationIds"] = []
            lambda_context.aws_request_id = "second-request"
            assert handler(invocation, lambda_context) == result

        service.checkpoint.assert_not_called()
        assert counts_during_handler == [0, 1]
        assert [
            span
            for span in exporter.get_finished_spans()
            if (span.attributes or {}).get("durable.operation.id") == CALLBACK_ID
        ] == [callback_span]
        fallback_warnings = [
            record
            for record in caplog.records
            if "fallback parent" in record.getMessage()
            and CALLBACK_ID in record.getMessage()
        ]
        assert len(fallback_warnings) == int(parent_visibility == "missing")
        assert otel_context.get_current() == before
    finally:
        provider.shutdown()


@pytest.mark.parametrize(
    "factory_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_overlapping_invocations_and_warm_reuse_own_their_pending_completions(
    factory_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = factory_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    instances: dict[str, ExecutionOtelPlugin | InvocationOtelPlugin] = {}
    original_create = factory.create_plugin

    def create(info: InvocationStartInfo) -> ExecutionOtelPlugin | InvocationOtelPlugin:
        plugin = original_create(info)
        assert info.request_id is not None
        instances[info.request_id] = plugin
        return plugin

    monkeypatch.setattr(factory, "create_plugin", create)
    ready = {request: Event() for request in ("a", "b")}
    release = {request: Event() for request in ("a", "b")}

    def body(event: dict[str, str], _context: DurableContext) -> str:
        request = event["request"]
        if request in ready:
            ready[request].set()
            assert release[request].wait(5)
        return request

    handler = durable_execution(body, plugins=[factory])
    start = datetime.now(UTC) - timedelta(seconds=5)

    def invoke(request: str) -> dict[str, Any]:
        operations = [
            Operation(
                operation_id="execution-id-" + request,
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
                start_timestamp=start,
                execution_details=ExecutionDetails(
                    input_payload='{"request":"' + request + '"}'
                ),
            ),
            Operation(
                operation_id=CALLBACK_ID,
                operation_type=OperationType.CALLBACK,
                name="unvisited callback",
                status=OperationStatus.SUCCEEDED,
                start_timestamp=start,
                end_timestamp=start + timedelta(seconds=1),
                callback_details=CallbackDetails(
                    callback_id="callback", result='"saved"'
                ),
            ),
        ]
        event = {
            "DurableExecutionArn": EXECUTION_ARN + "-" + request,
            "CheckpointToken": "resume-token",
            "InitialExecutionState": {
                "Operations": [operation.to_json_dict() for operation in operations],
            },
            "UpdatedOperationIds": [CALLBACK_ID],
        }
        context = Mock(
            aws_request_id=request,
            client_context=None,
            identity=None,
            _epoch_deadline_time_in_ms=0,
            invoked_function_arn="completion:1",
            tenant_id=None,
        )
        return handler(event, context)

    service = Mock()
    before = otel_context.get_current()
    try:
        with (
            patch(
                "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
                return_value=service,
            ),
            ThreadPoolExecutor(max_workers=2) as callers,
        ):
            first = callers.submit(invoke, "a")
            second = callers.submit(invoke, "b")
            try:
                assert ready["a"].wait(5) and ready["b"].wait(5)
                release["a"].set()
                assert first.result(5) == {"Status": "SUCCEEDED", "Result": '"a"'}
                assert not instances["a"]._completions.pending
                assert CALLBACK_ID in instances["b"]._completions.pending
                assert instances["b"]._tracing_enabled
                release["b"].set()
                assert second.result(5) == {"Status": "SUCCEEDED", "Result": '"b"'}
                assert invoke("c") == {"Status": "SUCCEEDED", "Result": '"c"'}
            finally:
                release["a"].set()
                release["b"].set()
        callbacks = [
            span
            for span in exporter.get_finished_spans()
            if span.name == "unvisited callback"
        ]
        assert len(callbacks) == 3
        assert {
            span.attributes["durable.execution.arn"]
            for span in callbacks
            if span.attributes
        } == {EXECUTION_ARN + "-" + request for request in ("a", "b", "c")}
        assert len({span.context.trace_id for span in callbacks if span.context}) == 3
        for plugin in instances.values():
            assert not plugin._completions.pending
            assert not plugin._completions.children_by_parent
            assert not plugin._completed_input_parent_ids
            assert not plugin._completed_parent_spans
            assert not plugin._completions.in_flight
            assert not plugin._completions.delivered
            assert not plugin._tracing_enabled
        service.checkpoint.assert_not_called()
        assert otel_context.get_current() == before
    finally:
        release["a"].set()
        release["b"].set()
        provider.shutdown()
