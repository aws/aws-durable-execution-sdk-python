"""A failing finalizer cannot prevent unrelated spans from ending."""

from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationInfo,
    OperationStatus,
    OperationType,
)
from opentelemetry import context as otel_context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)


class _FailingProcessor(SimpleSpanProcessor):
    def __init__(self, exporter, error, *, permanent=False):
        super().__init__(exporter)
        self.error = error
        self.permanent = permanent
        self.attempts = Counter()
        self.failed_contexts = []
        self.failed_end_times = []

    def on_end(self, span):
        self.attempts[span.name] += 1
        if span.name == "a" and (self.permanent or self.attempts[span.name] == 1):
            # Real SDK Span.end has already marked this span ended. Reusing
            # end() on the same object cannot deliver it to this exporter.
            self.failed_contexts.append(span.context)
            self.failed_end_times.append(span.end_time)
            raise self.error
        super().on_end(span)


def _operation(name, now, *, parent_id=None):
    return OperationInfo(
        operation_id=name,
        operation_type=OperationType.CALLBACK,
        sub_type=None,
        name=name,
        parent_id=parent_id,
        start_time=now - timedelta(seconds=2),
        end_time=now - timedelta(seconds=1),
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
    )


def _error_leaves(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for nested in error.exceptions for leaf in _error_leaves(nested)]
    return [error]


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize("permanent", [False, True], ids=["transient", "permanent"])
@pytest.mark.parametrize("missing_parent", [False, True], ids=["root", "fallback"])
def test_end_drain_reports_failure_after_other_spans_finish(
    view, permanent, missing_parent
):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    error = RuntimeError("completion a could not be exported")
    processor = _FailingProcessor(exporter, error, permanent=permanent)
    provider.add_span_processor(processor)
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    invocation = dict(
        request_id="finalization-failure",
        execution_arn="execution/finalization-failure",
        execution_start_time=now - timedelta(seconds=10),
        is_first_invocation=False,
    )
    parent_id = "missing-parent" if missing_parent else None
    operations = {
        name: _operation(name, now, parent_id=parent_id) for name in ("a", "b")
    }
    before = otel_context.get_current()
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation, operations=operations, updated_operations=operations
            )
        )
        # Neither callback is visited by the application. Both first reach the
        # exporter from invocation End, including the missing-parent queue.
        assert processor.attempts["a"] == processor.attempts["b"] == 0
        with pytest.raises(Exception) as caught:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )
        assert error in _error_leaves(caught.value)
        if not permanent:
            assert caught.value is error
        assert otel_context.get_current() == before
        assert not plugin._tracing_enabled
        assert plugin._context_tokens == {}
        assert plugin._pending_completions == plugin._pending_operation_ends == {}
        assert not plugin._ending_operation_ids

        spans = exporter.get_finished_spans()
        counts = Counter(span.name for span in spans)
        assert counts["b"] == counts["Invocation"] == counts["Workflow"] == 1
        assert processor.attempts["a"] == 2
        assert processor.attempts["b"] == 1
        assert counts["a"] == (0 if permanent else 1)
        invocation_span = next(span for span in spans if span.name == "Invocation")
        workflow_span = next(span for span in spans if span.name == "Workflow")
        expected_parent = (
            invocation_span.context.span_id
            if view is InvocationOtelPlugin
            else operation_id_to_span_id(invocation["execution_arn"], parent_id)
            if parent_id is not None
            else workflow_span.context.span_id
        )
        for span in spans:
            if span.name in ("a", "b"):
                assert span.status.status_code is StatusCode.OK
                assert span.attributes["durable.operation.status"] == "SUCCEEDED"
                assert span.parent.span_id == expected_parent
                assert span.context.trace_id == invocation_span.context.trace_id
                assert span.start_time < span.end_time <= invocation_span.end_time
        if not permanent:
            recovered = next(span for span in spans if span.name == "a")
            assert recovered.context == processor.failed_contexts[0]
            assert recovered.end_time == processor.failed_end_times[0]
        # A retained failure cannot create work after this invocation closes.
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        assert processor.attempts["a"] == 2
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize("error", [SystemExit("stop"), KeyboardInterrupt()])
def test_control_exception_is_not_retried_and_propagates_after_finalization(
    view, error
):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    processor = _FailingProcessor(exporter, error)
    provider.add_span_processor(processor)
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    invocation = dict(
        request_id="control-finalization",
        execution_arn="execution/control-finalization",
        execution_start_time=now - timedelta(seconds=10),
        is_first_invocation=False,
    )
    operations = {name: _operation(name, now) for name in ("a", "b")}
    before = otel_context.get_current()
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation, operations=operations, updated_operations=operations
            )
        )
        with pytest.raises(type(error)) as caught:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )
        assert caught.value is error
        assert otel_context.get_current() == before
        assert not plugin._tracing_enabled
        assert plugin._context_tokens == {}
        assert not plugin._pending_completions
        assert processor.attempts["a"] == 1
        counts = Counter(span.name for span in exporter.get_finished_spans())
        assert counts["a"] == 0
        assert counts["b"] == counts["Invocation"] == counts["Workflow"] == 1
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize("control", [False, True], ids=["transient", "control"])
def test_parent_registration_child_failure_keeps_recursive_drain_bounded(view, control):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    error = (
        SystemExit("stop child export")
        if control
        else RuntimeError("retry child export")
    )
    parent_contexts = []

    class Processor(_FailingProcessor):
        def on_end(self, span):
            if span.name == "a":
                # The child first reaches the processor inside the parent's
                # registration-driven drain, while that parent is recording.
                parent_span = plugin._get_span("parent")
                assert parent_span is not None and parent_span.is_recording()
                parent_contexts.append(parent_span.get_span_context())
            super().on_end(span)

    processor = Processor(exporter, error)
    provider.add_span_processor(processor)
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    invocation = dict(
        request_id="nested-finalization",
        execution_arn="execution/nested-finalization",
        execution_start_time=now - timedelta(seconds=10),
        is_first_invocation=False,
    )
    parent = OperationInfo(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="parent",
        parent_id=None,
        start_time=now - timedelta(seconds=4),
        end_time=now - timedelta(seconds=1),
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
    )
    # Parent-first input makes materializing its recording span recursively
    # drain both children before the outer pending-completion loop reaches them.
    operations = {
        "parent": parent,
        "a": _operation("a", now, parent_id="parent"),
        "b": _operation("b", now, parent_id="parent"),
    }
    before = otel_context.get_current()
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation, operations=operations, updated_operations=operations
            )
        )
        with pytest.raises(type(error)) as caught:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )
        assert caught.value is error
        assert processor.attempts["a"] == (1 if control else 2)
        assert processor.attempts["b"] == processor.attempts["parent"] == 1
        assert otel_context.get_current() == before
        assert not plugin._tracing_enabled
        assert plugin._context_tokens == {}
        assert plugin._finalization is None
        assert not plugin._pending_completions
        assert not plugin._pending_operation_ends
        assert not plugin._ending_operation_ids

        spans = exporter.get_finished_spans()
        counts = Counter(span.name for span in spans)
        assert counts["b"] == counts["parent"] == 1
        assert counts["Invocation"] == counts["Workflow"] == 1
        assert counts["a"] == (0 if control else 1)
        parent_span = next(span for span in spans if span.name == "parent")
        invocation_span = next(span for span in spans if span.name == "Invocation")
        assert all(context == parent_span.context for context in parent_contexts)
        assert parent_span.status.status_code is StatusCode.OK
        assert spans.index(parent_span) < spans.index(invocation_span)
        for child in (span for span in spans if span.name in ("a", "b")):
            assert child.parent.span_id == parent_span.context.span_id
            assert child.context.trace_id == parent_span.context.trace_id
            assert child.status.status_code is StatusCode.OK
            assert (
                parent_span.start_time
                <= child.start_time
                < child.end_time
                <= parent_span.end_time
            )
            assert spans.index(child) < spans.index(parent_span)
    finally:
        provider.shutdown()
