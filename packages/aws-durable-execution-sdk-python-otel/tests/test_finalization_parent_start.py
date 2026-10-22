"""Parent materialization failures preserve child and root finalization."""

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

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
from opentelemetry.trace import Span, StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)


def _error_leaves(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [leaf for nested in error.exceptions for leaf in _error_leaves(nested)]
    return [error]


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize("permanent", [False, True], ids=["transient", "permanent"])
def test_parent_start_failure_finishes_descendants_before_reporting_errors(
    view: type[InvocationOtelPlugin] | type[ExecutionOtelPlugin],
    permanent: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    execution_arn = "execution/finalization-parent-start"
    invocation: dict[str, Any] = dict(
        request_id="parent-start-failure",
        execution_arn=execution_arn,
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
    child = OperationInfo(
        operation_id="child",
        operation_type=OperationType.CALLBACK,
        sub_type=None,
        name="child",
        parent_id="parent",
        start_time=now - timedelta(seconds=3),
        end_time=now - timedelta(seconds=2),
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
    )
    operations = {"parent": parent, "child": child}
    before = otel_context.get_current()
    error = RuntimeError("parent span could not start")
    attempts: Counter[str] = Counter()
    original_start = plugin._start_span

    def start_span(*args: Any, **kwargs: Any) -> Span:
        operation_id = kwargs.get("operation_id")
        if operation_id == "parent":
            attempts[operation_id] += 1
            if permanent or attempts[operation_id] == 1:
                raise error
        return original_start(*args, **kwargs)

    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation, operations=operations, updated_operations=operations
            )
        )
        monkeypatch.setattr(plugin, "_start_span", start_span)
        with pytest.raises(Exception) as caught:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )

        assert _error_leaves(caught.value) == [error] * (2 if permanent else 1)
        assert attempts["parent"] == 2
        assert otel_context.get_current() == before
        assert not plugin._tracing_enabled
        assert plugin._context_tokens == {}
        assert plugin._finalization is None
        assert not plugin._pending_completions
        assert not plugin._pending_operation_ends
        assert not plugin._ending_operation_ids

        spans = exporter.get_finished_spans()
        counts = Counter(span.name for span in spans)
        assert counts["child"] == counts["Invocation"] == counts["Workflow"] == 1
        assert counts["parent"] == (0 if permanent else 1)
        assert ("parent" in plugin._ended_operation_ids) is not permanent
        child_span = next(span for span in spans if span.name == "child")
        invocation_span = next(span for span in spans if span.name == "Invocation")
        assert child_span.parent is not None
        assert child_span.context is not None
        assert invocation_span.context is not None
        assert child_span.context.trace_id == invocation_span.context.trace_id
        assert child_span.status.status_code is StatusCode.OK
        assert child_span.attributes is not None
        assert child_span.attributes["durable.operation.status"] == "SUCCEEDED"
        assert spans.index(child_span) < spans.index(invocation_span)

        if permanent:
            # Known completed input still supplies the logical parent identity,
            # without falsely claiming that its recording span was exported.
            assert child_span.parent.span_id == operation_id_to_span_id(
                execution_arn, "parent"
            )
        else:
            parent_span = next(span for span in spans if span.name == "parent")
            assert parent_span.context is not None
            assert child_span.parent.span_id == parent_span.context.span_id
            assert parent_span.status.status_code is StatusCode.OK
            assert parent_span.attributes is not None
            assert parent_span.attributes["durable.operation.status"] == "SUCCEEDED"
            assert parent_span.start_time is not None
            assert parent_span.end_time is not None
            assert child_span.start_time is not None
            assert child_span.end_time is not None
            assert (
                parent_span.start_time
                <= child_span.start_time
                < child_span.end_time
                <= parent_span.end_time
            )
            assert spans.index(child_span) < spans.index(parent_span)
            assert spans.index(parent_span) < spans.index(invocation_span)
    finally:
        provider.shutdown()
