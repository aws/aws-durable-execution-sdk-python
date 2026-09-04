"""Tests for the OpenTelemetry durable execution plugin."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
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
from opentelemetry import baggage, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON, Sampler
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    StatusCode,
    TraceFlags,
    TraceState,
)

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ExtractedContext,
    Sampling,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    _to_otel_trace_id,
    derive_execution_root_span_id,
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.log_filter import OtelContextLogFilter
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _assert_otel_context_balanced():
    """Assert each test leaves the OTel thread-local context as it found it.

    The plugin pairs every context.attach() with a context.detach(), so no
    global reset is needed to keep tests isolated. Asserting the invariant here
    turns a plugin lifecycle leak into a test failure instead of hiding it.
    """
    before = otel_context.get_current()
    yield
    assert otel_context.get_current() == before, (
        "test leaked OTel context state: an attach() was not detached"
    )


def _create_plugin() -> tuple[InvocationOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    return _create_plugin_with_sampler()


def _create_plugin_with_sampler(
    sampler: Sampler | None = None,
    context_extractor=lambda _: None,
) -> tuple[InvocationOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider(sampler=sampler)
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=trace_provider,
            context_extractor=context_extractor,
        )
    )
    return plugin, exporter


def _invocation_start_info(
    is_first_invocation: bool = True,
) -> InvocationStartInfo:
    """Create standard invocation start info for tests."""
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=is_first_invocation,
    )


def _invocation_end_info(
    status: InvocationStatus = InvocationStatus.SUCCEEDED,
) -> InvocationEndInfo:
    """Create standard invocation end info for tests."""
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
        status=status,
        error=None,
    )


def _user_function_start_info(
    operation_id: str,
    attempt: int = 1,
    parent_id: str | None = None,
    operation_type: OperationType = OperationType.STEP,
) -> UserFunctionStartInfo:
    """Create standard user function start info for tests."""
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=f"step-{operation_id}",
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
    )


def _user_function_end_info(
    operation_id: str,
    outcome: UserFunctionOutcome = UserFunctionOutcome.SUCCEEDED,
    attempt: int = 1,
    parent_id: str | None = None,
    operation_type: OperationType = OperationType.STEP,
) -> UserFunctionEndInfo:
    """Create standard user function end info for tests."""
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=f"step-{operation_id}",
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
        outcome=outcome,
        end_time=END_TIME,
        error=None,
    )


def _user_function_incomplete_info(
    operation_id: str,
    attempt: int = 1,
    parent_id: str | None = None,
    operation_type: OperationType = OperationType.STEP,
) -> UserFunctionEndInfo:
    """Create user function end info for an incomplete execution."""
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=f"step-{operation_id}",
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=attempt,
        outcome=UserFunctionOutcome.INCOMPLETE,
        end_time=END_TIME,
        error=None,
    )


def test_extract_attributes_uses_structural_event_attributes():
    plugin, _ = _create_plugin()

    invocation_attributes = plugin._extract_attributes(
        SimpleNamespace(is_first_invocation=False)
    )
    assert invocation_attributes["durable.invocation.first"] is False

    operation_attributes = plugin._extract_attributes(
        SimpleNamespace(
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        )
    )
    assert (
        operation_attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )

    user_function_attributes = plugin._extract_attributes(
        SimpleNamespace(
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
            is_replay_children=False,
        )
    )
    assert "durable.operation.status" not in user_function_attributes


def test_invocation_start_and_end_emit_invocation_span():
    """Verify invocation lifecycle callbacks create and finish the span."""
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    assert plugin._get_span(None) is not None

    plugin.on_invocation_end(_invocation_end_info())

    spans = exporter.get_finished_spans()
    spans_by_name = {span.name: span for span in spans}
    # Terminal invocation also exports the Workflow span.
    assert set(spans_by_name) == {"Invocation", "Workflow"}
    invocation = spans_by_name["Invocation"]
    assert invocation.kind is SpanKind.INTERNAL
    assert invocation.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert invocation.attributes["durable.invocation.first"] is True
    assert (
        invocation.attributes["durable.invocation.status"]
        == InvocationStatus.SUCCEEDED.value
    )
    workflow = spans_by_name["Workflow"]
    assert invocation.parent is not None
    assert workflow.parent is not None
    assert invocation.context.trace_id == workflow.context.trace_id
    assert invocation.parent.span_id == workflow.parent.span_id
    assert invocation.parent.span_id == derive_execution_root_span_id(EXECUTION_ARN)
    assert plugin._get_span(None) is None


def test_invocation_start_without_execution_start_time_disables_tracing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    plugin, exporter = _create_plugin()
    info = InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=None,
        is_first_invocation=True,
    )

    plugin.on_invocation_start(info)
    plugin.on_invocation_end(_invocation_end_info())

    assert "requires InvocationStartInfo.execution_start_time" in caplog.text
    assert exporter.get_finished_spans() == ()


def test_invocation_start_without_execution_arn_disables_tracing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    plugin, exporter = _create_plugin()
    info = InvocationStartInfo(
        request_id="request-1",
        execution_arn=None,
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )

    plugin.on_invocation_start(info)
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="after-rejected-start",
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="after rejected start",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    assert "requires InvocationStartInfo.execution_arn" in caplog.text
    assert exporter.get_finished_spans() == ()


def test_invocation_start_without_sampler_disables_tracing(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin, exporter = _create_plugin()

    def bind_without_sampler() -> bool:
        plugin._sampling_delegate = None
        return True

    monkeypatch.setattr(plugin, "_bind_sdk_tracer", bind_without_sampler)

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="after-rejected-start",
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="after rejected start",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    assert "No sampler available" in caplog.text
    assert exporter.get_finished_spans() == ()


def test_invocation_span_ignores_different_trace_ambient_span():
    plugin, exporter = _create_plugin()

    ambient = plugin._provider.get_tracer("ambient").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        plugin.on_invocation_end(_invocation_end_info())
    finally:
        otel_context.detach(token)
        ambient.end()

    spans = {span.name: span for span in exporter.get_finished_spans()}
    invocation = spans["Invocation"]
    workflow = spans["Workflow"]
    assert invocation.parent is not None
    assert workflow.parent is not None
    assert invocation.parent.span_id != ambient.get_span_context().span_id
    assert invocation.context.trace_id != ambient.get_span_context().trace_id
    assert invocation.context.trace_id == workflow.context.trace_id
    assert invocation.parent.span_id == workflow.parent.span_id


def test_log_filter_uses_invocation_trace_when_ambient_trace_is_rejected():
    plugin, _ = _create_plugin()
    ambient = plugin._provider.get_tracer("ambient").start_span("lambda-invocation")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="message",
            args=(),
            exc_info=None,
        )

        OtelContextLogFilter(plugin).filter(record)

        invocation_span = plugin._get_span(None)
        assert invocation_span is not None
        invocation_context = invocation_span.get_span_context()
        assert record.traceId == format(invocation_context.trace_id, "032x")
        assert record.spanId == format(invocation_context.span_id, "016x")
        assert record.traceId != format(ambient.get_span_context().trace_id, "032x")
    finally:
        plugin.on_invocation_end(_invocation_end_info())
        otel_context.detach(token)
        ambient.end()


def test_invocation_span_parents_to_same_trace_ambient_span():
    plugin, exporter = _create_plugin()
    canonical_trace_id = _to_otel_trace_id(EXECUTION_ARN, START_TIME)
    trace_state = TraceState([("vendor", "opaque")])
    ambient_context = SpanContext(
        trace_id=canonical_trace_id,
        span_id=int("1234567890abcdef", 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=trace_state,
    )
    ambient = NonRecordingSpan(ambient_context)
    token = otel_context.attach(trace.set_span_in_context(ambient, Context()))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        plugin.on_invocation_end(_invocation_end_info())
    finally:
        otel_context.detach(token)

    spans = {span.name: span for span in exporter.get_finished_spans()}
    invocation = spans["Invocation"]
    workflow = spans["Workflow"]
    assert invocation.parent is not None
    assert workflow.parent is not None
    assert invocation.context.trace_id == workflow.context.trace_id
    assert invocation.context.trace_state == trace_state
    assert workflow.context.trace_state == trace_state
    assert invocation.parent.span_id == ambient_context.span_id
    assert workflow.parent.span_id == derive_execution_root_span_id(EXECUTION_ARN)


def test_pre_terminal_placeholder_preserves_same_trace_tracestate():
    """The Workflow placeholder and operation links carry ambient tracestate."""
    plugin, exporter = _create_plugin()
    canonical_trace_id = _to_otel_trace_id(EXECUTION_ARN, START_TIME)
    trace_state = TraceState([("vendor", "opaque")])
    ambient_context = SpanContext(
        trace_id=canonical_trace_id,
        span_id=int("1234567890abcdef", 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=trace_state,
    )
    ambient = NonRecordingSpan(ambient_context)
    token = otel_context.attach(trace.set_span_in_context(ambient, Context()))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        assert plugin._workflow_span is not None
        assert plugin._workflow_span.get_span_context().trace_state == trace_state
        # A cross-invocation completion links the deterministic operation context.
        plugin.on_operation_end(
            OperationEndInfo(
                operation_id="wait-existing",
                operation_type=OperationType.WAIT,
                sub_type=OperationSubType.WAIT,
                name="existing-wait",
                parent_id=None,
                start_time=START_TIME,
                is_replayed=False,
                status=OperationStatus.SUCCEEDED,
                end_time=END_TIME,
                error=None,
            )
        )
        plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))
    finally:
        otel_context.detach(token)

    span = next(s for s in exporter.get_finished_spans() if s.name == "existing-wait")
    operation_link = next(
        link
        for link in span.links
        if link.context.span_id
        == operation_id_to_span_id(EXECUTION_ARN, "wait-existing")
    )
    assert operation_link.context.trace_state == trace_state


def test_extracted_remote_parent_is_execution_ancestor():
    remote_trace_id = int("5759e988bd862e3fe1be46a994272793", 16)
    remote_parent_id = int("53995c3f42cd8ad8", 16)
    plugin, exporter = _create_plugin_with_sampler(
        context_extractor=lambda _: ExtractedContext(
            trace_id=remote_trace_id,
            parent_span_id=remote_parent_id,
            sampling=Sampling.SAMPLED,
        )
    )

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    spans = {span.name: span for span in exporter.get_finished_spans()}
    workflow = spans["Workflow"]
    invocation = spans["Invocation"]
    assert workflow.context.trace_id == remote_trace_id
    assert invocation.context.trace_id == remote_trace_id
    assert workflow.parent is not None
    assert invocation.parent is not None
    assert workflow.parent.span_id == remote_parent_id
    assert invocation.parent.span_id == remote_parent_id


def test_backend_sampled_overrides_local_always_off_sampler():
    remote_trace_id = int("5759e988bd862e3fe1be46a994272793", 16)
    remote_parent_id = int("53995c3f42cd8ad8", 16)
    plugin, exporter = _create_plugin_with_sampler(
        sampler=ALWAYS_OFF,
        context_extractor=lambda _: ExtractedContext(
            trace_id=remote_trace_id,
            parent_span_id=remote_parent_id,
            sampling=Sampling.SAMPLED,
        ),
    )

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    assert {span.name for span in exporter.get_finished_spans()} == {
        "Invocation",
        "Workflow",
    }


def test_backend_not_sampled_overrides_local_always_on_sampler():
    remote_trace_id = int("5759e988bd862e3fe1be46a994272793", 16)
    remote_parent_id = int("53995c3f42cd8ad8", 16)
    plugin, exporter = _create_plugin_with_sampler(
        sampler=ALWAYS_ON,
        context_extractor=lambda _: ExtractedContext(
            trace_id=remote_trace_id,
            parent_span_id=remote_parent_id,
            sampling=Sampling.NOT_SAMPLED,
        ),
    )

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    assert exporter.get_finished_spans() == ()


def test_invocation_span_records_subsequent_invocation():
    """Invocation spans preserve a false first-invocation attribute."""
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info(is_first_invocation=False))
    plugin.on_invocation_end(_invocation_end_info())

    spans = exporter.get_finished_spans()
    invocation = next(s for s in spans if s.name == "Invocation")
    assert invocation.attributes["durable.invocation.first"] is False


@pytest.mark.parametrize(
    ("invocation_status", "expected_span_status"),
    [
        (InvocationStatus.PENDING, StatusCode.OK),
        (InvocationStatus.RETRY, StatusCode.UNSET),
        (InvocationStatus.SUCCEEDED, StatusCode.OK),
        (InvocationStatus.FAILED, StatusCode.ERROR),
    ],
)
def test_invocation_span_status_reflects_execution_status(
    invocation_status: InvocationStatus,
    expected_span_status: StatusCode,
):
    """Only terminal invocation spans receive a success or failure status."""
    plugin, exporter = _create_plugin()

    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(invocation_status))

    spans = exporter.get_finished_spans()
    invocation = next(s for s in spans if s.name == "Invocation")
    attributes = invocation.attributes
    assert attributes is not None
    assert attributes["durable.invocation.status"] == invocation_status.value
    assert invocation.status.status_code is expected_span_status


def test_invocation_end_closes_callback_child_before_parent_context():
    """Invocation shutdown preserves containment for open callback spans."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    context_start_time = datetime.now(UTC)
    context_id = "callback-context"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=context_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.WAIT_FOR_CALLBACK,
            name="wait for callback",
            parent_id=None,
            start_time=context_start_time,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="callback",
            operation_type=OperationType.CALLBACK,
            sub_type=OperationSubType.CALLBACK,
            name="create callback id",
            parent_id=context_id,
            start_time=context_start_time,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )

    plugin.on_invocation_end(_invocation_end_info(InvocationStatus.PENDING))

    spans = {span.name: span for span in exporter.get_finished_spans()}
    invocation_span = spans["Invocation"]
    context_span = spans["wait for callback"]
    callback_span = spans["create callback id"]
    assert callback_span.parent is not None
    assert callback_span.parent.span_id == context_span.context.span_id
    assert callback_span.end_time <= context_span.end_time <= invocation_span.end_time


def test_operation_callbacks_emit_child_span_with_deterministic_span_id():
    """Verify non-user-function operations are traced beneath the invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-1"

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    active_wait_span = plugin._get_span(operation_id)
    assert active_wait_span is not None
    invocation_span = plugin._get_span(None)
    assert invocation_span is not None
    assert invocation_span.start_time <= active_wait_span.start_time
    assert (
        active_wait_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )
    assert (
        active_wait_span.attributes["durable.operation.subtype"]
        == OperationSubType.WAIT.value
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {span.name: span for span in exporter.get_finished_spans()}
    assert all(span.kind is SpanKind.INTERNAL for span in spans_by_name.values())
    wait_span = spans_by_name["wait-for-signal"]
    invocation_span = spans_by_name["Invocation"]
    assert wait_span.context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, operation_id
    )
    assert wait_span.parent.span_id == invocation_span.context.span_id
    assert (
        invocation_span.start_time
        <= wait_span.start_time
        <= wait_span.end_time
        <= invocation_span.end_time
    )
    assert wait_span.attributes["durable.operation.id"] == operation_id
    assert wait_span.attributes["durable.operation.type"] == OperationType.WAIT.value
    assert (
        wait_span.attributes["durable.operation.subtype"] == OperationSubType.WAIT.value
    )
    assert (
        wait_span.attributes["durable.operation.status"]
        == OperationStatus.SUCCEEDED.value
    )


def test_operation_end_without_start_links_previous_logical_operation():
    """A continuation links to the deterministic logical operation context."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-existing"
    random_span_id = int("1234567890abcdef", 16)
    plugin._id_generator._fallback_id_generator.generate_span_id = lambda: (
        random_span_id
    )

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=None,
            name="existing-wait",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "existing-wait"
    assert span.context.span_id == random_span_id
    linked_span_ids = {link.context.span_id for link in span.links}
    assert linked_span_ids == {
        derive_workflow_span_id(EXECUTION_ARN),
        operation_id_to_span_id(EXECUTION_ARN, operation_id),
    }
    assert (
        span.attributes["durable.operation.status"] == OperationStatus.SUCCEEDED.value
    )


def test_continuation_span_uses_current_start_and_end_times():
    """Continuation spans use current times within the invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span = plugin._get_span(None)
    assert invocation_span is not None
    before_callback = time.time_ns()

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id="fast-step",
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fast-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    after_callback = time.time_ns()

    span = exporter.get_finished_spans()[0]
    assert invocation_span.start_time <= span.start_time
    assert before_callback <= span.start_time <= span.end_time <= after_callback


def test_resume_operation_timestamps_do_not_precede_current_invocation():
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span = plugin._get_span(None)
    assert invocation_span is not None
    old_start_time = START_TIME
    old_end_time = START_TIME

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id="wait-resume",
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="otel-wait",
            parent_id=None,
            start_time=old_start_time,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=old_end_time,
            error=None,
        )
    )
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="after-resume",
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="otel-after-resume",
            parent_id=None,
            start_time=old_start_time,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id="after-resume",
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="otel-after-resume",
            parent_id=None,
            start_time=old_start_time,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=old_end_time,
            error=None,
        )
    )

    spans = {span.name: span for span in exporter.get_finished_spans()}
    wait_span = spans["otel-wait"]
    after_resume_span = spans["otel-after-resume"]
    assert invocation_span.start_time <= wait_span.start_time <= wait_span.end_time
    assert wait_span.end_time <= after_resume_span.start_time
    assert invocation_span.start_time <= after_resume_span.start_time
    assert after_resume_span.parent is not None
    assert after_resume_span.parent.span_id == invocation_span.context.span_id


def test_ordered_timestamps_are_thread_safe():
    plugin, _ = _create_plugin()
    base_time = START_TIME

    with ThreadPoolExecutor(max_workers=8) as executor:
        timestamps = list(
            executor.map(
                lambda _: plugin._next_ordered_timestamp(base_time),
                range(100),
            )
        )

    assert len(set(timestamps)) == len(timestamps)
    assert sorted(timestamps) == [
        int(base_time.timestamp() * 1_000_000_000) + index * 1_000
        for index in range(100)
    ]


def test_retried_operation_uses_fresh_id_and_links_previous_logical_operation():
    """Retried segments use fresh IDs and link the logical operation context."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-retried"
    random_span_id = int("abcdef1234567890", 16)
    plugin._id_generator._fallback_id_generator.generate_span_id = lambda: (
        random_span_id
    )

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="retried-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=True,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="retried-step",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "retried-step"
    assert span.context.span_id == random_span_id
    linked_span_ids = {link.context.span_id for link in span.links}
    assert linked_span_ids == {
        derive_workflow_span_id(EXECUTION_ARN),
        operation_id_to_span_id(EXECUTION_ARN, operation_id),
    }


def test_step_operation_span_parents_attempt_span():
    """STEP operations have a logical span with attempt spans beneath it."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    step_span = plugin._get_span(operation_id)
    assert step_span is not None
    assert step_span.name == "fetch-user"
    assert step_span.context.span_id == operation_id_to_span_id(
        EXECUTION_ARN, operation_id
    )

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
        )
    )
    active_attempt_span = trace.get_current_span()
    assert active_attempt_span.parent.span_id == step_span.context.span_id
    assert active_attempt_span.get_span_context().span_id != step_span.context.span_id

    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
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
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {span.name: span for span in exporter.get_finished_spans()}
    finished_step_span = spans_by_name["fetch-user"]
    attempt_span = spans_by_name["fetch-user attempt 1"]
    assert finished_step_span.kind is SpanKind.INTERNAL
    assert attempt_span.kind is SpanKind.INTERNAL
    assert attempt_span.parent.span_id == finished_step_span.context.span_id
    assert (
        finished_step_span.attributes["durable.operation.status"]
        == OperationStatus.SUCCEEDED.value
    )
    assert attempt_span.attributes["durable.attempt.number"] == 1
    assert "durable.operation.status" not in attempt_span.attributes


def test_user_function_callbacks_emit_attempt_span_attributes():
    """Verify user-function end refreshes all extractable span attributes."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"

    start_info = UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=None,
        name="fetch-user",
        parent_id=None,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )
    plugin.on_user_function_start(start_info)
    active_span = plugin._get_span("step-1:attempt:1")
    assert active_span is not None
    assert "durable.operation.status" not in active_span.attributes
    active_span.set_attributes(
        {
            "durable.operation.name": "stale-name",
            "durable.attempt.number": 99,
        }
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
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

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user attempt 1"
    assert span.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert span.attributes["durable.operation.id"] == operation_id
    assert span.attributes["durable.operation.type"] == OperationType.STEP.value
    assert span.attributes["durable.operation.name"] == "fetch-user"
    assert span.attributes["durable.attempt.number"] == 1
    assert (
        span.attributes["durable.attempt.outcome"]
        == UserFunctionOutcome.SUCCEEDED.value
    )
    assert "durable.operation.status" not in span.attributes


def test_step_attempt_span_name_includes_attempt_number():
    """Step attempt spans include the attempt number in the display name."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-retry"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=2,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=2,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user attempt 2"


def test_step_attempt_span_name_defaults_to_first_attempt():
    """Step attempt spans default to attempt 1 when no attempt is provided."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-no-attempt"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=None,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=None,
            name="fetch-user",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=None,
            outcome=UserFunctionOutcome.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.name == "fetch-user attempt 1"


@pytest.mark.parametrize(
    ("outcome", "terminal_status", "error", "expected_span_status"),
    [
        (
            UserFunctionOutcome.SUCCEEDED,
            OperationStatus.SUCCEEDED,
            None,
            StatusCode.OK,
        ),
        (
            UserFunctionOutcome.SUCCEEDED,
            OperationStatus.FAILED,
            ErrorObject(
                message="serialization failed",
                type="SerializationError",
                data=None,
                stack_trace=None,
            ),
            StatusCode.ERROR,
        ),
    ],
)
def test_context_span_waits_for_terminal_status_and_omits_attempt_attributes(
    outcome,
    terminal_status,
    error,
    expected_span_status,
):
    """Completed CONTEXT spans carry terminal status and no attempt attributes.

    durable.attempt.number and durable.attempt.outcome are meaningful for
    STEP operations (each retry is an attempt) but not for CONTEXT, so the
    plugin omits them on CONTEXT spans for cross-SDK consistency.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "ctx-1"

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="book-trip",
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
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="book-trip",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=False,
            attempt=1,
            outcome=outcome,
            end_time=END_TIME,
            error=None,
        )
    )

    active_span = plugin._get_span(operation_id)
    assert active_span is not None
    assert (
        active_span.attributes["durable.operation.status"]
        == OperationStatus.STARTED.value
    )
    assert not exporter.get_finished_spans()

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="book-trip",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=terminal_status,
            end_time=END_TIME,
            error=error,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.attributes["durable.operation.type"] == OperationType.CONTEXT.value
    assert span.attributes["durable.operation.status"] == terminal_status.value
    assert "durable.attempt.number" not in span.attributes
    assert "durable.attempt.outcome" not in span.attributes
    assert span.status.status_code is expected_span_status


def test_span_registry_helpers_can_be_called_from_multiple_threads():
    """Verify active span registry helpers are safe under concurrent access."""
    plugin, _ = _create_plugin()

    def update_span(index: int) -> None:
        operation_id = f"operation-{index}"
        plugin._set_span(operation_id, object())  # type: ignore[arg-type]
        assert plugin._get_span(operation_id) is not None
        plugin._delete_span(operation_id)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(update_span, range(100)))

    with plugin._operation_spans_lock:
        assert plugin._operation_spans == {}


# ----------------------------------------------------------------------
# on_user_function_end restores the context enclosing the user function
# ----------------------------------------------------------------------
def test_user_function_end_restores_enclosing_context():
    """Verify a completed step leaves the pre-step context current again."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id
    enclosing_context = otel_context.get_current()

    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    # Inside the step, the current span is the attempt span.
    active_attempt_span = plugin._get_span("step-1:attempt:1")
    assert active_attempt_span is not None
    assert (
        trace.get_current_span().get_span_context().span_id
        == active_attempt_span.get_span_context().span_id
    )

    plugin.on_user_function_end(_user_function_end_info(operation_id))

    # After the step, the enclosing context is restored and no scope is left
    # behind. Log correlation resolves the invocation span from the registry.
    assert otel_context.get_current() == enclosing_context
    assert plugin._context_tokens == {}
    assert plugin.get_current_span_context().span_id == invocation_span_id


def test_user_function_start_preserves_baggage_in_current_context():
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    baggage_context = baggage.set_baggage(
        "durable-test-key", "durable-test-value", otel_context.get_current()
    )
    token = otel_context.attach(baggage_context)
    try:
        plugin.on_user_function_start(_user_function_start_info("step-baggage"))

        assert baggage.get_baggage("durable-test-key") == "durable-test-value"

        plugin.on_user_function_end(_user_function_end_info("step-baggage"))
    finally:
        otel_context.detach(token)
        plugin.on_invocation_end(_invocation_end_info())


def test_user_function_end_restores_enclosing_context_on_failure():
    """Verify the enclosing context is restored even when the step fails."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    enclosing_context = otel_context.get_current()

    operation_id = "step-fail"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(
        _user_function_end_info(operation_id, outcome=UserFunctionOutcome.FAILED)
    )

    assert otel_context.get_current() == enclosing_context
    assert plugin._context_tokens == {}


def test_user_function_end_restores_enclosing_context_across_multiple_steps():
    """Verify sequential steps do not accumulate context scopes."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id
    enclosing_context = otel_context.get_current()

    for index in range(3):
        operation_id = f"step-{index}"
        plugin.on_user_function_start(_user_function_start_info(operation_id))
        plugin.on_user_function_end(_user_function_end_info(operation_id))
        # Between each step the context is back to where it started, and log
        # correlation still resolves the invocation span.
        assert otel_context.get_current() == enclosing_context
        assert plugin._context_tokens == {}
        assert plugin.get_current_span_context().span_id == invocation_span_id


# ----------------------------------------------------------------------
# get_current_span_context resolves the right span context
# ----------------------------------------------------------------------
def test_get_current_span_context_returns_none_before_invocation_start():
    """Verify no span context is returned when nothing is active."""
    plugin, _ = _create_plugin()

    assert plugin.get_current_span_context() is None


def test_get_current_span_context_returns_invocation_span_at_top_level():
    """Verify top-level code resolves to the invocation span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    span_context = plugin.get_current_span_context()
    invocation_span = plugin._get_span(None)
    assert span_context is not None
    assert span_context.span_id == invocation_span.get_span_context().span_id


def test_get_current_span_context_returns_operation_span_inside_step():
    """Verify code inside a step resolves to the attempt span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))

    span_context = plugin.get_current_span_context()
    active_attempt_span = plugin._get_span("step-1:attempt:1")
    assert span_context is not None
    assert active_attempt_span is not None
    assert span_context.span_id == active_attempt_span.get_span_context().span_id

    # The step never ends here, so invocation cleanup releases its scope.
    plugin.on_invocation_end(_invocation_end_info())


def test_get_current_span_context_returns_invocation_span_between_steps():
    """Verify between-step code resolves back to the invocation span context."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(_user_function_end_info(operation_id))

    span_context = plugin.get_current_span_context()
    invocation_span = plugin._get_span(None)
    assert span_context is not None
    assert span_context.span_id == invocation_span.get_span_context().span_id


# ----------------------------------------------------------------------
# on_user_function_end restores the ENCLOSING operation span (nested case)
# ----------------------------------------------------------------------
def test_user_function_end_restores_parent_context_span_for_nested_step():
    """Verify ending a nested step restores its enclosing child-context span.

    Inside a child context, code that runs after an inner step (e.g. between
    inner steps) must correlate to the child context span, not the invocation.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    # Enter a child context (CONTEXT operation at the top level).
    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    context_span_id = trace.get_current_span().get_span_context().span_id

    # Run an inner step whose parent is the child context.
    inner_step_id = "ctx-1-step"
    plugin.on_user_function_start(
        _user_function_start_info(inner_step_id, parent_id=context_id)
    )
    active_attempt_span = plugin._get_span("ctx-1-step:attempt:1")
    assert active_attempt_span is not None
    assert (
        trace.get_current_span().get_span_context().span_id
        == active_attempt_span.get_span_context().span_id
    )

    plugin.on_user_function_end(
        _user_function_end_info(inner_step_id, parent_id=context_id)
    )

    # After the inner step, the enclosing child-context span is current again,
    # NOT the invocation span.
    assert trace.get_current_span().get_span_context().span_id == context_span_id
    assert (
        trace.get_current_span().get_span_context().span_id
        != plugin._get_span(None).get_span_context().span_id
    )

    # The child context never ends here, so invocation cleanup releases it.
    plugin.on_invocation_end(_invocation_end_info())


def test_child_context_start_uses_invocation_time_not_durable_start_timestamp():
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span = plugin._get_span(None)
    assert invocation_span is not None

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )

    context_span = plugin._get_span(context_id)
    assert context_span is not None
    assert context_span.start_time > invocation_span.start_time

    plugin.on_invocation_end(_invocation_end_info())


def test_top_level_step_end_falls_back_to_invocation_for_correlation():
    """Verify a top-level step (parent_id=None) correlates to the invocation."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    invocation_span_id = plugin._get_span(None).get_span_context().span_id
    enclosing_context = otel_context.get_current()

    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(_user_function_end_info(operation_id))

    # No durable span is attached at the top level, so the registry fallback
    # supplies the invocation span for log correlation.
    assert otel_context.get_current() == enclosing_context
    assert not trace.get_current_span().get_span_context().is_valid
    assert plugin.get_current_span_context().span_id == invocation_span_id


def test_get_current_span_context_returns_context_span_between_nested_steps():
    """Verify between-step code inside a child context resolves to that context.

    This is the log-correlation path: after an inner step completes,
    get_current_span_context must return the enclosing child-context span so
    logs emitted between inner steps correlate to the child context.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    context_span = plugin._get_span(context_id)

    inner_step_id = "ctx-1-step"
    plugin.on_user_function_start(
        _user_function_start_info(inner_step_id, parent_id=context_id)
    )
    plugin.on_user_function_end(
        _user_function_end_info(inner_step_id, parent_id=context_id)
    )

    span_context = plugin.get_current_span_context()
    assert span_context is not None
    assert span_context.span_id == context_span.get_span_context().span_id
    assert span_context.span_id != plugin._get_span(None).get_span_context().span_id

    # The child context never ends here, so invocation cleanup releases it.
    plugin.on_invocation_end(_invocation_end_info())


def test_nested_steps_restore_context_span_across_multiple_iterations():
    """Verify each inner step restores the child-context span between iterations."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    context_span_id = trace.get_current_span().get_span_context().span_id

    for index in range(3):
        inner_step_id = f"ctx-1-step-{index}"
        plugin.on_user_function_start(
            _user_function_start_info(inner_step_id, parent_id=context_id)
        )
        plugin.on_user_function_end(
            _user_function_end_info(inner_step_id, parent_id=context_id)
        )
        # Between each inner step, the child-context span is current.
        assert trace.get_current_span().get_span_context().span_id == context_span_id

    # The child context never ends here, so invocation cleanup releases it.
    plugin.on_invocation_end(_invocation_end_info())


@pytest.mark.parametrize(
    ("status", "expected_code"),
    [
        (InvocationStatus.SUCCEEDED, StatusCode.OK),
        (InvocationStatus.FAILED, StatusCode.ERROR),
    ],
)
def test_workflow_span_exported_on_terminal(status, expected_code):
    """A terminal invocation exports a deterministic Workflow span."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status))

    workflow = next(s for s in exporter.get_finished_spans() if s.name == "Workflow")
    assert workflow.parent is not None
    assert workflow.parent.span_id == derive_execution_root_span_id(EXECUTION_ARN)
    assert workflow.kind is SpanKind.INTERNAL
    # Deterministic span id derived from the execution ARN.
    assert workflow.context.span_id == derive_workflow_span_id(EXECUTION_ARN)
    assert workflow.attributes["durable.execution.arn"] == EXECUTION_ARN
    assert workflow.attributes["durable.execution.status"] == status.value
    assert workflow.status.status_code is expected_code
    # Anchored to the execution start time.
    assert workflow.start_time == int(START_TIME.timestamp() * 1_000_000_000)


@pytest.mark.parametrize("status", [InvocationStatus.PENDING, InvocationStatus.RETRY])
def test_workflow_span_not_exported_on_non_terminal(status):
    """Non-terminal invocations do not materialize (export) the Workflow span.

    The Workflow span is a non-recording placeholder during the invocation, so a
    non-terminal status leaves nothing to export and no recording span to abandon.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info(status))

    names = [s.name for s in exporter.get_finished_spans()]
    assert "Workflow" not in names
    assert "Invocation" in names


@pytest.mark.parametrize(
    "status",
    [
        InvocationStatus.PENDING,
        InvocationStatus.RETRY,
        InvocationStatus.SUCCEEDED,
        InvocationStatus.FAILED,
    ],
)
def test_workflow_reference_is_non_recording_after_cleanup(status):
    """The retained Workflow span reference is never a recording span.

    During the invocation it is a non-recording deterministic placeholder, so
    invocation cleanup on any status leaves no recording span abandoned.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    workflow_reference = plugin._workflow_span
    assert workflow_reference is not None
    assert not workflow_reference.is_recording()

    plugin.on_invocation_end(_invocation_end_info(status))

    assert not workflow_reference.is_recording()


@pytest.mark.parametrize("status", [InvocationStatus.PENDING, InvocationStatus.RETRY])
def test_open_operation_reference_is_non_recording_after_non_terminal(status):
    """A suspended operation's retained span reference is ended, not abandoned."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    operation_reference = plugin._get_span("wait-1")
    assert operation_reference is not None

    plugin.on_invocation_end(_invocation_end_info(status))

    assert not operation_reference.is_recording()


def test_operation_span_links_to_workflow_span():
    """Operation spans link to the Workflow span while parented to invocation."""
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "wait-1"
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            name="wait-for-signal",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    plugin.on_invocation_end(_invocation_end_info())

    spans_by_name = {s.name: s for s in exporter.get_finished_spans()}
    op_span = spans_by_name["wait-for-signal"]
    workflow_span_id = derive_workflow_span_id(EXECUTION_ARN)
    linked_span_ids = {link.context.span_id for link in op_span.links}
    assert workflow_span_id in linked_span_ids
    # Still parented to the invocation span (not the Workflow span).
    assert op_span.parent is not None
    assert op_span.parent.span_id == spans_by_name["Invocation"].context.span_id


def test_replayed_context_span_links_previous_logical_operation():
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "callback-context"
    random_span_id = int("fedcba9876543210", 16)
    plugin._id_generator._fallback_id_generator.generate_span_id = lambda: (
        random_span_id
    )

    plugin.on_user_function_start(
        UserFunctionStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.WAIT_FOR_CALLBACK,
            name="wait-for-callback",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=True,
            status=OperationStatus.STARTED,
            is_replay_children=True,
            attempt=2,
        )
    )
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.WAIT_FOR_CALLBACK,
            name="wait-for-callback",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=True,
            status=OperationStatus.STARTED,
            is_replay_children=True,
            attempt=2,
            outcome=UserFunctionOutcome.INCOMPLETE,
            end_time=None,
            error=None,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.WAIT_FOR_CALLBACK,
            name="wait-for-callback",
            parent_id=None,
            start_time=START_TIME,
            is_replayed=True,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = exporter.get_finished_spans()[0]
    assert span.context.span_id == random_span_id
    linked_span_ids = {link.context.span_id for link in span.links}
    assert linked_span_ids == {
        derive_workflow_span_id(EXECUTION_ARN),
        operation_id_to_span_id(EXECUTION_ARN, operation_id),
    }


def test_checkpointed_context_first_span_uses_deterministic_id():
    plugin, exporter = _create_plugin()
    operation_id = "child-context"
    span_name = f"step-{operation_id}"
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            name=span_name,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )
    plugin.on_user_function_start(
        _user_function_start_info(
            operation_id,
            operation_type=OperationType.CONTEXT,
        )
    )
    plugin.on_user_function_end(
        _user_function_end_info(
            operation_id,
            operation_type=OperationType.CONTEXT,
        )
    )
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            name=span_name,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )

    span = next(
        span for span in exporter.get_finished_spans() if span.name == span_name
    )
    assert span.context.span_id == operation_id_to_span_id(EXECUTION_ARN, operation_id)
    plugin.on_invocation_end(_invocation_end_info())


def test_virtual_context_replay_uses_unique_linked_segments():
    plugin, exporter = _create_plugin()
    operation_id = "flat-branch"
    span_name = f"step-{operation_id}"
    logical_span_id = operation_id_to_span_id(EXECUTION_ARN, operation_id)

    for _ in range(2):
        plugin.on_invocation_start(_invocation_start_info())
        # Virtual contexts have no durable START hook.
        plugin.on_user_function_start(
            _user_function_start_info(
                operation_id,
                operation_type=OperationType.CONTEXT,
            )
        )
        plugin.on_user_function_end(
            _user_function_end_info(
                operation_id,
                operation_type=OperationType.CONTEXT,
            )
        )
        plugin.on_operation_end(
            OperationEndInfo(
                operation_id=operation_id,
                operation_type=OperationType.CONTEXT,
                sub_type=OperationSubType.PARALLEL,
                name=span_name,
                parent_id=None,
                start_time=None,
                is_replayed=False,
                status=OperationStatus.SUCCEEDED,
                end_time=END_TIME,
                error=None,
            )
        )
        plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))

    contexts = [
        span for span in exporter.get_finished_spans() if span.name == span_name
    ]
    assert len(contexts) == 2
    assert len({span.context.span_id for span in contexts}) == 2
    assert all(
        logical_span_id in {link.context.span_id for link in span.links}
        for span in contexts
    )


def test_incomplete_attempt_is_marked_when_invocation_ends():
    plugin, exporter = _create_plugin()
    operation_id = "step-suspends"
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(_user_function_incomplete_info(operation_id))

    plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))

    attempt = next(
        span
        for span in exporter.get_finished_spans()
        if span.name == f"step-{operation_id} attempt 1"
    )
    assert attempt.attributes["durable.span.truncated_at_invocation_boundary"] is True


def test_workflow_span_name_is_configurable():
    """The Workflow span name can be overridden via constructor kwarg."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=trace_provider,
            context_extractor=lambda _: None,
            workflow_span_name="MyExecution",
        )
    )
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    names = [s.name for s in exporter.get_finished_spans()]
    assert "MyExecution" in names
    assert "Workflow" not in names


# ----------------------------------------------------------------------
# Context attach/detach balance across the plugin lifecycle
# ----------------------------------------------------------------------
def test_child_context_end_restores_context_active_before_it():
    """Verify leaving a child context restores the context that enclosed it."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    enclosing_context = otel_context.get_current()

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    assert otel_context.get_current() != enclosing_context

    plugin.on_user_function_end(
        _user_function_end_info(context_id, operation_type=OperationType.CONTEXT)
    )

    assert otel_context.get_current() == enclosing_context
    assert plugin._context_tokens == {}


def test_nested_scopes_are_released_without_accumulating():
    """Verify a child context and its inner step unwind to their entry contexts."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    context_id = "ctx-1"
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    inside_context = otel_context.get_current()

    inner_step_id = "ctx-1-step"
    plugin.on_user_function_start(
        _user_function_start_info(inner_step_id, parent_id=context_id)
    )
    plugin.on_user_function_end(
        _user_function_end_info(inner_step_id, parent_id=context_id)
    )
    # The inner step restored the child-context scope, not a copy of it.
    assert otel_context.get_current() == inside_context
    assert set(plugin._context_tokens) == {context_id}

    plugin.on_user_function_end(
        _user_function_end_info(context_id, operation_type=OperationType.CONTEXT)
    )
    assert otel_context.get_current() == before_context
    assert plugin._context_tokens == {}


def test_invocation_end_releases_scope_of_suspended_user_function():
    """Verify a user function that never ends does not leak its scope.

    A suspending user function raises before ``on_user_function_end`` runs, so
    invocation cleanup is what releases the scope it attached.
    """
    plugin, _ = _create_plugin()
    before_context = otel_context.get_current()
    plugin.on_invocation_start(_invocation_start_info())

    plugin.on_user_function_start(_user_function_start_info("step-suspends"))
    assert plugin._context_tokens

    plugin.on_invocation_end(_invocation_end_info(status=InvocationStatus.PENDING))

    assert otel_context.get_current() == before_context
    assert plugin._context_tokens == {}


def test_ambient_span_is_current_again_after_full_lifecycle():
    """Verify an ambient (e.g. ADOT) span survives a full plugin lifecycle."""
    plugin, _ = _create_plugin()
    ambient_provider = TracerProvider()
    ambient = ambient_provider.get_tracer("ambient").start_span("AmbientLambda")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        plugin.on_invocation_start(_invocation_start_info())
        operation_id = "step-1"
        plugin.on_user_function_start(_user_function_start_info(operation_id))
        plugin.on_user_function_end(_user_function_end_info(operation_id))
        plugin.on_invocation_end(_invocation_end_info())

        assert (
            trace.get_current_span().get_span_context().span_id
            == ambient.get_span_context().span_id
        )
    finally:
        otel_context.detach(token)
        ambient.end()


def test_warm_invocation_reuse_does_not_accumulate_scopes():
    """Verify repeated invocations on one plugin instance stay balanced."""
    plugin, _ = _create_plugin()
    ambient_provider = TracerProvider()
    ambient = ambient_provider.get_tracer("ambient").start_span("AmbientLambda")
    token = otel_context.attach(trace.set_span_in_context(ambient))
    try:
        warm_context = otel_context.get_current()
        for index in range(3):
            plugin.on_invocation_start(_invocation_start_info())
            operation_id = f"step-{index}"
            plugin.on_user_function_start(_user_function_start_info(operation_id))
            plugin.on_user_function_end(_user_function_end_info(operation_id))
            plugin.on_invocation_end(_invocation_end_info())

            # Each invocation leaves the warm environment as it found it.
            assert otel_context.get_current() == warm_context
            assert plugin._context_tokens == {}
    finally:
        otel_context.detach(token)
        ambient.end()


def test_detach_ignores_token_attached_on_another_thread():
    """Verify a scope attached on another thread is dropped, not reset here.

    A context token can only be reset on the thread that created it, so the
    plugin drops foreign tokens instead of asking OpenTelemetry to fail.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(
            plugin.on_user_function_start, _user_function_start_info("step-1")
        ).result()

    plugin._detach_context("step-1:attempt:1")

    assert plugin._context_tokens == {}
    assert otel_context.get_current() == before_context

    plugin.on_invocation_end(_invocation_end_info())


# ----------------------------------------------------------------------
# Re-entering an operation whose scope was never released
# ----------------------------------------------------------------------
def test_reentered_child_context_does_not_leave_abandoned_span_current():
    """Verify a timed in-process resume unwinds the abandoned scope.

    A suspended child context never reaches on_user_function_end, and the
    map/parallel coordinator can resume that branch in-process, re-entering the
    same operation ID on the same thread. Without releasing the first scope, the
    second scope's detach would restore the abandoned span and leave it current.
    """
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()
    context_id = "ctx-1"

    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=context_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            name=context_id,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.STARTED,
        )
    )

    # First run: the child context suspends, so no end hook fires.
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    suspended_span = plugin._get_span(context_id)
    assert suspended_span is not None

    # Timed in-process resume re-enters the same operation.
    plugin.on_user_function_start(
        _user_function_start_info(context_id, operation_type=OperationType.CONTEXT)
    )
    assert len([key for key in plugin._context_tokens if key == context_id]) == 1
    assert plugin._get_span(context_id) is suspended_span

    plugin.on_user_function_end(
        _user_function_end_info(context_id, operation_type=OperationType.CONTEXT)
    )

    assert otel_context.get_current() == before_context
    assert context_id not in plugin._context_tokens
    assert (
        trace.get_current_span().get_span_context().span_id
        != suspended_span.get_span_context().span_id
    )

    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=context_id,
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            name=context_id,
            parent_id=None,
            start_time=START_TIME,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
            end_time=END_TIME,
            error=None,
        )
    )
    assert not suspended_span.is_recording()

    plugin.on_invocation_end(_invocation_end_info())


def test_reentered_step_attempt_releases_the_previous_scope():
    """Verify re-entering the same attempt key unwinds the previous scope."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()
    operation_id = "step-1"

    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    plugin.on_user_function_end(_user_function_end_info(operation_id))

    assert otel_context.get_current() == before_context
    assert plugin._context_tokens == {}

    plugin.on_invocation_end(_invocation_end_info())


def test_suspension_releases_the_scope_on_the_originating_worker():
    """Verify the suspending worker releases its own scope.

    A suspended user function reports no outcome, so the SDK fires
    on_user_function_end with INCOMPLETE on the thread that ran it -- the only thread that
    can reset its context token. The worker is kept alive and probed to prove it
    is left clean even though the resume lands on a different thread.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()
    operation_id = "step-1"
    span_key = "step-1:attempt:1"

    with ThreadPoolExecutor(max_workers=1) as worker:

        def suspend_on_worker() -> tuple[int, bool]:
            plugin.on_user_function_start(_user_function_start_info(operation_id))
            attached_span_id = trace.get_current_span().get_span_context().span_id
            plugin.on_user_function_end(_user_function_incomplete_info(operation_id))
            return (
                attached_span_id,
                trace.get_current_span().get_span_context().is_valid,
            )

        attached_span_id, span_still_current = worker.submit(suspend_on_worker).result()
        suspended_span = plugin._get_span(span_key)

        # The scope was released on the worker, and its span is left open.
        assert attached_span_id != 0
        assert span_still_current is False
        assert span_key not in plugin._context_tokens
        assert suspended_span is not None
        assert not exporter.get_finished_spans()

        # The timed resume lands on this thread, with nothing stale to unwind.
        plugin.on_user_function_start(_user_function_start_info(operation_id))
        assert plugin._context_tokens[span_key][0] == threading.get_ident()
        plugin.on_user_function_end(_user_function_end_info(operation_id))
        assert otel_context.get_current() == before_context

        # The originating worker is still clean.
        worker_span_valid = worker.submit(
            lambda: trace.get_current_span().get_span_context().is_valid
        ).result()
        assert worker_span_valid is False

    plugin.on_invocation_end(_invocation_end_info())


def test_nested_suspension_unwinds_scopes_in_reverse_order():
    """Verify nested suspends release inner-first and resume without stale scopes.

    The INCOMPLETE end callback fires as the exception propagates outward, so the inner
    context's scope is released before its enclosing one. On resume, ending the
    inner operation restores the resumed outer scope rather than the one captured
    for the suspended run.
    """
    plugin, exporter = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    before_context = otel_context.get_current()

    plugin.on_user_function_start(
        _user_function_start_info("ctx-outer", operation_type=OperationType.CONTEXT)
    )
    suspended_outer = plugin._get_span("ctx-outer")
    plugin.on_user_function_start(
        _user_function_start_info(
            "ctx-inner", parent_id="ctx-outer", operation_type=OperationType.CONTEXT
        )
    )
    suspended_inner = plugin._get_span("ctx-inner")
    assert suspended_outer is not None
    assert suspended_inner is not None

    # Both contexts suspend: the inner one unwinds first.
    plugin.on_user_function_end(
        _user_function_incomplete_info(
            "ctx-inner", parent_id="ctx-outer", operation_type=OperationType.CONTEXT
        )
    )
    assert trace.get_current_span() is suspended_outer

    plugin.on_user_function_end(
        _user_function_incomplete_info(
            "ctx-outer", operation_type=OperationType.CONTEXT
        )
    )
    assert otel_context.get_current() == before_context
    assert plugin._context_tokens == {}
    # Neither span is ended: both operations are still in flight.
    assert not exporter.get_finished_spans()

    # The timed in-process resume replays both contexts, outer first.
    plugin.on_user_function_start(
        _user_function_start_info("ctx-outer", operation_type=OperationType.CONTEXT)
    )
    resumed_outer = plugin._get_span("ctx-outer")
    plugin.on_user_function_start(
        _user_function_start_info(
            "ctx-inner", parent_id="ctx-outer", operation_type=OperationType.CONTEXT
        )
    )
    resumed_inner = plugin._get_span("ctx-inner")
    assert resumed_outer is not None
    assert resumed_outer is suspended_outer
    assert resumed_inner is suspended_inner
    assert trace.get_current_span() is resumed_inner

    plugin.on_user_function_end(
        _user_function_end_info(
            "ctx-inner", parent_id="ctx-outer", operation_type=OperationType.CONTEXT
        )
    )

    # The reused outer span's scope is restored.
    assert trace.get_current_span() is resumed_outer

    plugin.on_user_function_end(
        _user_function_end_info("ctx-outer", operation_type=OperationType.CONTEXT)
    )
    assert otel_context.get_current() == before_context
    assert plugin._context_tokens == {}

    plugin.on_invocation_end(_invocation_end_info())
