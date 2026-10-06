"""End-to-end invocation-view OTel coverage for wait/resume."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python import plugin as core_plugin_api
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    derive_workflow_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagators.aws.aws_xray_propagator import AwsXRayPropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


EXECUTION_ARN = "test-arn/execution-otel-wait-resume"
EXECUTION_START = datetime(2026, 8, 27, 5, 11, 47, tzinfo=UTC)
XRAY_TRACE_HEADER = (
    "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
)
XRAY_TRACE_ID = int("5759e988bd862e3fe1be46a994272793", 16)
XRAY_PARENT_SPAN_ID = int("53995c3f42cd8ad8", 16)


def _lambda_context() -> Mock:
    context = Mock()
    context.aws_request_id = "test-request-id"
    context.client_context = None
    context.identity = None
    context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    context.invoked_function_arn = "test-arn"
    context.tenant_id = None
    return context


def _event(
    operations: list[Operation],
    updated_operation_ids: list[str] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "DurableExecutionArn": EXECUTION_ARN,
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [operation.to_json_dict() for operation in operations],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }
    if updated_operation_ids is not None:
        event["UpdatedOperationIds"] = updated_operation_ids
    return event


def _execution_operation() -> Operation:
    return Operation(
        operation_id="execution-otel-wait-resume",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=EXECUTION_START,
        execution_details=ExecutionDetails(input_payload="{}"),
    )


def _checkpoint_store(initial_operations: list[Operation]):
    operations = {operation.operation_id: operation for operation in initial_operations}

    def checkpoint(
        durable_execution_arn,  # noqa: ARG001
        checkpoint_token,  # noqa: ARG001
        updates,
        client_token="token",  # noqa: S107, ARG001
    ) -> CheckpointOutput:
        for update in updates:
            now = datetime.now(UTC)
            previous = operations.get(update.operation_id)
            if update.action is OperationAction.START:
                operations[update.operation_id] = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    sub_type=update.sub_type,
                    start_timestamp=now,
                )
            elif update.action is OperationAction.SUCCEED:
                base = previous or Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    sub_type=update.sub_type,
                    start_timestamp=now,
                )
                operations[update.operation_id] = replace(
                    base,
                    status=OperationStatus.SUCCEEDED,
                    end_timestamp=now,
                    step_details=(
                        StepDetails(result=update.payload, attempt=1)
                        if update.operation_type is OperationType.STEP
                        else base.step_details
                    ),
                )

        return CheckpointOutput(
            checkpoint_token="new-token",
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=list(operations.values())
            ),
        )

    return checkpoint, operations


@pytest.mark.parametrize(
    "plugin_type",
    [InvocationOtelPlugin, ExecutionOtelPlugin],
)
def test_otel_wait_resume_spans_share_default_xray_execution_trace(
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[InvocationOtelPlugin] | type[ExecutionOtelPlugin],
) -> None:
    monkeypatch.setenv("_X_AMZN_TRACE_ID", XRAY_TRACE_HEADER)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
        )
    )

    @durable_step
    def complete_after_resume(_step_context) -> str:
        return "resumed"

    def handler_impl(_event: Any, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(1), name="otel-wait")
        return context.step(complete_after_resume(), name="otel-after-resume")

    handler = durable_execution(handler_impl, plugins=[plugin])

    initial_operations = [_execution_operation()]
    first_checkpoint, first_operations = _checkpoint_store(initial_operations)

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = first_checkpoint
        mock_client_class.initialize_client.return_value = mock_client

        first_result = handler(_event(initial_operations), _lambda_context())

    assert first_result["Status"] == InvocationStatus.PENDING.value
    wait_operation = next(
        operation
        for operation in first_operations.values()
        if operation.name == "otel-wait"
    )
    completed_wait = replace(
        wait_operation,
        status=OperationStatus.SUCCEEDED,
        end_timestamp=datetime.now(UTC),
        sub_type=OperationSubType.WAIT,
    )

    second_operations = [_execution_operation(), completed_wait]
    second_checkpoint, _ = _checkpoint_store(second_operations)

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = second_checkpoint
        mock_client_class.initialize_client.return_value = mock_client

        second_result = handler(
            _event(
                second_operations,
                updated_operation_ids=[completed_wait.operation_id],
            ),
            _lambda_context(),
        )

    assert second_result["Status"] == InvocationStatus.SUCCEEDED.value

    spans = exporter.get_finished_spans()
    durable_spans = [
        span
        for span in spans
        if span.name in {"Workflow", "Invocation", "otel-wait", "otel-after-resume"}
    ]
    trace_ids = {span.context.trace_id for span in durable_spans}
    assert trace_ids == {XRAY_TRACE_ID}

    workflow = next(span for span in spans if span.name == "Workflow")
    invocations = [span for span in spans if span.name == "Invocation"]
    waits = [span for span in spans if span.name == "otel-wait"]
    after_resume = next(span for span in spans if span.name == "otel-after-resume")

    assert len(invocations) >= 2
    if plugin_type is InvocationOtelPlugin:
        assert len(waits) >= 2  # one segment per invocation
    else:
        assert len(waits) == 1  # one span per operation
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert workflow.parent is not None
    assert workflow.parent.span_id == XRAY_PARENT_SPAN_ID
    assert {span.parent.span_id for span in invocations if span.parent} == {
        XRAY_PARENT_SPAN_ID
    }

    assert after_resume.parent is not None
    if plugin_type is InvocationOtelPlugin:
        assert after_resume.parent.span_id in {
            span.context.span_id for span in invocations
        }
    else:
        assert after_resume.parent.span_id == workflow.context.span_id
    completed_wait_span = next(
        span
        for span in waits
        if span.parent is not None
        and span.parent.span_id == after_resume.parent.span_id
    )
    assert completed_wait_span.end_time is not None
    assert after_resume.start_time is not None
    assert completed_wait_span.end_time <= after_resume.start_time


@pytest.mark.parametrize(
    ("plugin_type", "extra_context_plugin"),
    [
        (InvocationOtelPlugin, False),
        (InvocationOtelPlugin, True),
        (ExecutionOtelPlugin, False),
    ]
    # Execution-view caller isolation requires the coordinated newer core.
    # Released 2.0.x retains the pre-existing same-order teardown limitation;
    # the legacy lane continues checking its supported combinations above.
    + (
        [(ExecutionOtelPlugin, True)]
        if getattr(
            core_plugin_api, "DURABLE_INSTRUMENTATION_HANDLER_CONTEXT_API_VERSION", None
        )
        == 1
        else []
    ),
)
@pytest.mark.parametrize("reverse_plugins", [False, True])
@pytest.mark.parametrize("fail_after_resume", [False, True])
@pytest.mark.parametrize("ambient_kind", ["same", "unrelated", "absent"])
def test_handler_user_spans_inherit_context_across_resume_and_failure(
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[InvocationOtelPlugin] | type[ExecutionOtelPlugin],
    fail_after_resume: bool,
    ambient_kind: str,
    extra_context_plugin: bool,
    reverse_plugins: bool,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.setenv("_X_AMZN_TRACE_ID", XRAY_TRACE_HEADER)
    # The documented PyPI compatibility environment deliberately uses an older
    # core. Keep exercising its supported operation tracing and lifecycle while
    # asserting the new handler contract only when that core exposes the scope.
    supports_handler_context = (
        getattr(
            core_plugin_api, "DURABLE_INSTRUMENTATION_HANDLER_CONTEXT_API_VERSION", None
        )
        == 1
    )
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    )
    tracer = provider.get_tracer("customer")
    before_context = otel_context.get_current()
    calls: list[str] = []

    def user_span(name: str) -> None:
        # Ordinary instrumentation: the SDK/caller supplies the active parent.
        span = tracer.start_span(name)
        span.end()

    def step_body(_step_context: Any) -> str:
        calls.append("step")
        user_span("step-user")
        return "saved"

    def handler_body(_event: Any, context: DurableContext) -> str:
        if extra_context_plugin:
            assert baggage.get_baggage("customer") == (
                "present" if supports_handler_context else None
            )
        user_span("handler-entry")
        saved = context.step(step_body, name="before-wait")
        user_span("handler-after-step")
        context.wait(Duration.from_seconds(1), name="context-wait")
        user_span("handler-after-resume")
        if fail_after_resume:
            raise ValueError("handler failed after resume")
        return saved

    # An unrelated plugin may own a caller-thread OTel baggage scope. The
    # invocation-view fallback must never become part of its saved token.
    from opentelemetry import baggage

    class BaggagePlugin(DurableInstrumentationPlugin):
        token: Any = None

        def on_invocation_start(self, _info: Any) -> None:
            self.token = otel_context.attach(baggage.set_baggage("customer", "present"))

        def on_invocation_end(self, _info: Any) -> None:
            otel_context.detach(self.token)
            self.token = None

    plugins: list[DurableInstrumentationPlugin] = [plugin]
    if extra_context_plugin:
        plugins.append(BaggagePlugin())
    if reverse_plugins:
        plugins.reverse()
    handler = durable_execution(handler_body, plugins=plugins)
    remote = AwsXRayPropagator().extract({"X-Amzn-Trace-Id": XRAY_TRACE_HEADER})
    assert trace.get_current_span(remote).get_span_context().trace_id == XRAY_TRACE_ID
    initial_operations = [_execution_operation()]
    checkpoint, operations = _checkpoint_store(initial_operations)
    ambient_ids: list[int] = []
    host_context = remote if ambient_kind == "same" else otel_context.Context()
    try:
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient"
        ) as client_class:
            client = Mock()
            client.checkpoint = checkpoint
            client_class.initialize_client.return_value = client
            # Standard host instrumentation supplies a same-trace Lambda span.
            host_scope = (
                tracer.start_as_current_span("lambda-first", context=host_context)
                if ambient_kind != "absent"
                else nullcontext()
            )
            with host_scope:
                host = trace.get_current_span()
                ambient_ids.append(host.get_span_context().span_id)
                first = handler(_event(initial_operations), _lambda_context())
                assert (
                    trace.get_current_span().get_span_context()
                    == host.get_span_context()
                )
        assert first["Status"] == InvocationStatus.PENDING.value
        assert otel_context.get_current() == before_context
        resumed_operations = [
            replace(
                operation,
                status=OperationStatus.SUCCEEDED,
                end_timestamp=datetime.now(UTC),
            )
            if operation.name == "context-wait"
            else operation
            for operation in operations.values()
        ]
        wait_id = next(
            operation.operation_id
            for operation in resumed_operations
            if operation.name == "context-wait"
        )
        checkpoint, _ = _checkpoint_store(resumed_operations)
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient"
        ) as client_class:
            client = Mock()
            client.checkpoint = checkpoint
            client_class.initialize_client.return_value = client
            host_scope = (
                tracer.start_as_current_span("lambda-resume", context=host_context)
                if ambient_kind != "absent"
                else nullcontext()
            )
            with host_scope:
                host = trace.get_current_span()
                ambient_ids.append(host.get_span_context().span_id)
                resumed = handler(
                    _event(resumed_operations, updated_operation_ids=[wait_id]),
                    _lambda_context(),
                )
                assert (
                    trace.get_current_span().get_span_context()
                    == host.get_span_context()
                )
        assert resumed["Status"] == (
            InvocationStatus.FAILED.value
            if fail_after_resume
            else InvocationStatus.SUCCEEDED.value
        )
        assert calls == ["step"]
        assert otel_context.get_current() == before_context
        spans = exporter.get_finished_spans()
        expected_parents = (
            [None, None]
            if not supports_handler_context
            else [derive_workflow_span_id(EXECUTION_ARN)] * 2
            if plugin_type is ExecutionOtelPlugin
            else ambient_ids
            if ambient_kind == "same"
            else [
                span.context.span_id
                for span in spans
                if span.name == "Invocation" and span.context is not None
            ]
        )
        for name in ("handler-entry", "handler-after-step"):
            users = [span for span in spans if span.name == name]
            assert len(users) == 2
            assert [span.parent.span_id if span.parent else None for span in users] == (
                expected_parents
            )
            assert all(
                span.context is not None
                and (
                    (span.context.trace_id == XRAY_TRACE_ID) == supports_handler_context
                )
                for span in users
            )
        after_resume = next(
            span for span in spans if span.name == "handler-after-resume"
        )
        assert (
            after_resume.parent.span_id if after_resume.parent else None
        ) == expected_parents[1]
        step_user = next(span for span in spans if span.name == "step-user")
        assert step_user.parent is not None
        assert any(
            span.name == "before-wait attempt 1"
            and span.context is not None
            and span.context.span_id == step_user.parent.span_id
            for span in spans
        )
    finally:
        provider.shutdown()
