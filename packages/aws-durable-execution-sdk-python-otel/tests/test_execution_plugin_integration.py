"""In-process integration tests for ExecutionOtelPlugin.

Drives the full plugin lifecycle against a real TracerProvider +
InMemorySpanExporter for the two deployment shapes:

* Community collector layer: the caller supplies a provider.
* ADOT layer: the ADOT Lambda layer supplies the global provider.

Both paths keep Workflow and Invocation on one execution trace, parented to a
shared execution ancestor.
"""

from __future__ import annotations

from datetime import UTC, datetime

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStatus,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import (
    ProxyTracerProvider,
    SpanKind,
    TracerProvider as ApiTracerProvider,
)

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
    derive_execution_root_span_id,
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"
OP_ID = "step-1"
OP_NAME = "fetch-user"
XRAY_TRACE_HEADER = (
    "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
)
XRAY_TRACE_ID = int("5759e988bd862e3fe1be46a994272793", 16)
EXECUTION_TRACE_ID = int("65937d253aa8c3f7ffe36c50d65b1a6d", 16)


@pytest.fixture(autouse=True)
def _assert_otel_context_balanced():
    """Assert each test leaves the OTel thread-local context as it found it."""
    before = otel_context.get_current()
    yield
    assert otel_context.get_current() == before, (
        "test leaked OTel context state: an attach() was not detached"
    )


def _provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


class _AdotParentInspectionProcessor(SpanProcessor):
    """Exercise the SDK-only parent fields accessed by the ADOT processor."""

    def on_start(self, span, parent_context=None) -> None:
        parent = trace.get_current_span(parent_context)
        if not parent.get_span_context().is_valid:
            return
        if isinstance(parent, ReadableSpan):
            _ = parent.attributes
        else:
            parent_kind = getattr(parent, "kind", None)
            parent_attributes = getattr(parent, "attributes", {})
            _ = parent_kind
            _ = parent_attributes.get("aws.trace.id")
        if getattr(parent, "kind", None) is SpanKind.SERVER:
            _ = getattr(parent, "kind", None)

    def on_end(self, span: ReadableSpan) -> None:
        return

    def shutdown(self) -> None:
        return

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def _invocation_start() -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )


def _invocation_end(
    status: InvocationStatus = InvocationStatus.SUCCEEDED,
) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
        status=status,
        error=None,
    )


def _run_step_lifecycle(plugin: ExecutionOtelPlugin) -> None:
    """Drive a single completed STEP (operation + one attempt) through a plugin."""
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=OP_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=OP_NAME,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )


def _config_for_provider(
    uses_global_provider: bool,
    provider: TracerProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> OtelPluginConfig:
    if uses_global_provider:
        monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    return OtelPluginConfig(
        tracer_provider=None if uses_global_provider else provider,
        context_extractor=lambda _: None,
        enrich_logger=False,
    )


@pytest.mark.parametrize(
    "uses_global_provider",
    [False, True],
    ids=["explicit", "global"],
)
def test_unrelated_root_spans_keep_provider_id_generation(
    uses_global_provider: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unrelated scopes receive fresh roots throughout a durable invocation."""
    provider, _ = _provider()
    provider_generator = provider.id_generator
    unrelated_tracer = provider.get_tracer("unrelated-library")
    before = unrelated_tracer.start_span("before", context=Context())

    plugin = ExecutionOtelPlugin(
        _config_for_provider(uses_global_provider, provider, monkeypatch)
    )
    assert provider.id_generator is provider_generator
    assert isinstance(plugin._id_generator, DeterministicIdGenerator)

    plugin.on_invocation_start(_invocation_start())
    workflow = plugin._workflow_span
    assert workflow is not None
    during = unrelated_tracer.start_span("during", context=Context())
    plugin.on_invocation_end(_invocation_end())
    after = unrelated_tracer.start_span("after", context=Context())

    trace_ids = {
        before.get_span_context().trace_id,
        during.get_span_context().trace_id,
        after.get_span_context().trace_id,
        workflow.get_span_context().trace_id,
    }
    assert len(trace_ids) == 4

    before.end()
    during.end()
    after.end()


def test_global_proxy_binds_sdk_provider_before_first_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plugin created before global SDK setup binds when invocation starts."""
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", None)
    current_provider: list[ApiTracerProvider] = [ProxyTracerProvider()]
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: current_provider[0])
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )

    provider, exporter = _provider()
    current_provider[0] = provider
    plugin.on_invocation_start(_invocation_start())
    plugin.on_invocation_end(_invocation_end())

    assert {span.name for span in exporter.get_finished_spans()} == {
        "Invocation",
        "Workflow",
    }


def test_parent_placeholder_supports_adot_style_parent_inspection() -> None:
    """A vendor processor can inspect a deferred parent without an exception."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(_AdotParentInspectionProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )

    plugin.on_invocation_start(_invocation_start())
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())

    assert {span.name for span in exporter.get_finished_spans()} == {
        "Invocation",
        "Workflow",
        OP_NAME,
        f"{OP_NAME} attempt 1",
    }


def test_global_proxy_disables_entire_invocation_until_sdk_provider_is_ready(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Provider setup midway through an invocation cannot produce a partial trace."""
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", None)
    current_provider: list[ApiTracerProvider] = [ProxyTracerProvider()]
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: current_provider[0])
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    provider, exporter = _provider()

    plugin.on_invocation_start(_invocation_start())
    assert "telemetry is disabled for this invocation" in caplog.text

    current_provider[0] = provider
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())
    assert exporter.get_finished_spans() == ()

    plugin.on_invocation_start(_invocation_start())
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())
    assert {span.name for span in exporter.get_finished_spans()} == {
        "Invocation",
        "Workflow",
        OP_NAME,
        f"{OP_NAME} attempt 1",
    }


# ---------------------------------------------------------------------------
# Community collector layer (plugin owns the provider)
# ---------------------------------------------------------------------------
def test_community_layer_full_lifecycle_is_workflow_rooted():
    provider, exporter = _provider()
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )

    plugin.on_invocation_start(_invocation_start())
    _run_step_lifecycle(plugin)
    plugin.on_invocation_end(_invocation_end())

    finished = exporter.get_finished_spans()
    spans = {s.name: s for s in finished}
    workflow = spans["Workflow"]
    invocation = spans["Invocation"]
    operation = spans[OP_NAME]
    attempt = spans[f"{OP_NAME} attempt 1"]

    # Workflow is parented to the synthetic execution ancestor with the
    # deterministic workflow span id.
    assert workflow.parent is not None
    assert workflow.parent.span_id == derive_execution_root_span_id(EXECUTION_ARN)
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert (
        workflow.attributes["durable.execution.status"]
        == InvocationStatus.SUCCEEDED.value
    )

    # Without extracted context, Invocation shares the synthetic execution root.
    assert invocation.parent is not None
    assert invocation.context.trace_id == workflow.context.trace_id
    assert invocation.parent.span_id == workflow.parent.span_id

    # Operation span: deterministic id, parented under Workflow, linked to invocation.
    assert operation.context.span_id == operation_id_to_span_id(EXECUTION_ARN, OP_ID)
    assert operation.parent.span_id == workflow.context.span_id
    assert invocation.context.span_id in {
        link.context.span_id for link in operation.links
    }

    # Attempt span is a child of the operation span.
    assert attempt.parent.span_id == operation.context.span_id

    # The operation span is exported exactly once.
    assert len([s for s in finished if s.name == OP_NAME]) == 1


# ---------------------------------------------------------------------------
# ADOT layer (default provider; ambient invocation span)
# ---------------------------------------------------------------------------
def test_adot_layer_full_lifecycle_ignores_different_trace_ambient_span(monkeypatch):
    provider, exporter = _provider()
    # Simulate the ADOT layer having configured the global TracerProvider.
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )

    # Simulate the ambient Lambda invocation span the ADOT layer creates.
    ambient = provider.get_tracer("adot").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start())
        _run_step_lifecycle(plugin)
        plugin.on_invocation_end(_invocation_end())
    finally:
        otel_context.detach(token)
        ambient.end()

    finished = exporter.get_finished_spans()
    spans = {s.name: s for s in finished}
    workflow = spans["Workflow"]
    invocation = spans["Invocation"]
    operation = spans[OP_NAME]

    # Invocation ignores the different-trace ambient ADOT span and stays on the
    # execution trace.
    assert invocation.parent is not None
    assert workflow.parent is not None
    assert invocation.parent.span_id != ambient.get_span_context().span_id
    assert invocation.context.trace_id != ambient.get_span_context().trace_id
    assert invocation.context.trace_id == workflow.context.trace_id
    assert invocation.parent.span_id == workflow.parent.span_id
    assert invocation.attributes["durable.invocation.first"] is True

    # Operation span still uses the deterministic id and links to the durable
    # invocation span (which is itself parented to the ambient ADOT span).
    assert operation.context.span_id == operation_id_to_span_id(EXECUTION_ARN, OP_ID)
    assert invocation.context.span_id in {
        link.context.span_id for link in operation.links
    }

    # The operation span is exported exactly once.
    assert len([s for s in finished if s.name == OP_NAME]) == 1


def test_second_plugin_uses_execution_trace_id_independent_of_xray(monkeypatch):
    """Workflow trace IDs remain deterministic and separate from X-Ray."""
    provider, exporter = _provider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    config = OtelPluginConfig(
        context_extractor=lambda _: None,
        enrich_logger=False,
    )
    first_plugin = ExecutionOtelPlugin(config)
    target_plugin = ExecutionOtelPlugin(config)
    monkeypatch.setenv("_X_AMZN_TRACE_ID", XRAY_TRACE_HEADER)

    if target_plugin._tracer is first_plugin._tracer:
        assert target_plugin._id_generator is first_plugin._id_generator
    target_plugin.on_invocation_start(_invocation_start())
    target_plugin.on_invocation_end(_invocation_end())

    workflow = next(
        span for span in exporter.get_finished_spans() if span.name == "Workflow"
    )
    assert workflow.context.trace_id == EXECUTION_TRACE_ID
    assert workflow.context.trace_id != XRAY_TRACE_ID
