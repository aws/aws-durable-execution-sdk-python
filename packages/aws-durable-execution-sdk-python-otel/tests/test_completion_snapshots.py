"""Input, replay and checkpoint observations share an invocation's export ledger."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStatus,
    OperationType,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.fixture(params=[ExecutionOtelPlugin, InvocationOtelPlugin])
def trace_case(request):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = request.param(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    invocation = dict(
        request_id="request",
        execution_arn="execution/snapshot-delivery",
        execution_start_time=datetime.now(UTC) - timedelta(seconds=2),
        is_first_invocation=False,
    )
    try:
        yield plugin, exporter, invocation
    finally:
        if plugin._tracing_enabled:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
            )
        provider.shutdown()


def completion(operation_id="b", parent_id=None, operation_type=OperationType.CALLBACK):
    now = datetime.now(UTC)
    return OperationEndInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=operation_id,
        parent_id=parent_id,
        start_time=now - timedelta(seconds=1),
        end_time=now,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        result="saved",
    )


@pytest.mark.parametrize("input_update", [False, True])
@pytest.mark.parametrize("visited", [False, True])
def test_snapshot_and_natural_notifications_export_once(
    trace_case, input_update, visited
):
    plugin, exporter, invocation = trace_case
    end = completion()
    active = OperationInfo(
        operation_id="active",
        operation_type=OperationType.CALLBACK,
        sub_type=None,
        name="active",
        parent_id=None,
        start_time=end.start_time,
        is_replayed=False,
        status=OperationStatus.STARTED,
    )
    updates = {"b": end, "active": active}
    plugin.on_invocation_start(
        InvocationStartInfo(
            **invocation,
            operations=updates,
            updated_operations=updates if input_update else {},
        )
    )
    change = OperationChangeInfo(
        execution_arn=invocation["execution_arn"],
        operations=updates,
        updated_operations=updates,
    )
    plugin.on_operation_change(change)
    if visited:
        plugin.on_operation_end(end)
        plugin.on_operation_end(end)
    plugin.on_operation_change(change)
    before_end = [span for span in exporter.get_finished_spans() if span.name == "b"]
    assert len(before_end) == int(visited)
    plugin.on_invocation_end(
        InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
    )
    spans = exporter.get_finished_spans()
    assert len([span for span in spans if span.name == "b"]) == 1
    assert not [span for span in spans if span.name == "active"]
    assert plugin._pending_completions == plugin._pending_operation_ends == {}


def test_concurrent_end_and_checkpoint_do_not_duplicate_export(trace_case, monkeypatch):
    plugin, exporter, invocation = trace_case
    end = completion()
    plugin.on_invocation_start(
        InvocationStartInfo(**invocation, updated_operations={"b": end})
    )
    entered, release = Event(), Event()
    original = plugin._start_span

    def block_start(*args, **kwargs):
        if kwargs.get("operation_id") == "b":
            entered.set()
            assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(plugin, "_start_span", block_start)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(plugin.on_operation_end, end)
        try:
            assert entered.wait(5)
            second = pool.submit(plugin.on_operation_end, end)
            second.result(timeout=5)
            plugin.on_operation_change(
                OperationChangeInfo(
                    execution_arn=invocation["execution_arn"],
                    operations={"b": end},
                    updated_operations={"b": end},
                )
            )
        finally:
            release.set()
        first.result(timeout=5)
    plugin.on_invocation_end(
        InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
    )
    assert (
        len([span for span in exporter.get_finished_spans() if span.name == "b"]) == 1
    )


def test_missing_ancestor_fallback_preserves_available_descendant_hierarchy(
    trace_case, caplog
):
    plugin, exporter, invocation = trace_case
    # Deliberately child-first: fallback must register its pending parent first.
    child = completion(parent_id="parent")
    parent = completion("parent", "missing-ancestor", OperationType.CONTEXT)
    plugin.on_invocation_start(
        InvocationStartInfo(
            **invocation, updated_operations={"b": child, "parent": parent}
        )
    )
    plugin.on_invocation_end(
        InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
    )
    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert spans["b"].parent.span_id == spans["parent"].context.span_id
    assert spans["b"].context.trace_id == spans["parent"].context.trace_id
    assert (
        len(
            [record for record in caplog.records if "fallback parent" in record.message]
        )
        == 1
    )
    assert plugin._pending_completions == plugin._pending_operation_ends == {}


def test_failed_invocation_start_cannot_requeue_fallback_forever(
    trace_case, monkeypatch
):
    plugin, _, invocation = trace_case

    def fail_extraction(_info):
        raise RuntimeError("context extraction failed")

    monkeypatch.setattr(plugin, "_context_extractor", fail_extraction)
    with pytest.raises(RuntimeError, match="context extraction failed"):
        plugin.on_invocation_start(InvocationStartInfo(**invocation))
    plugin.on_operation_end(completion(parent_id="missing-parent"))

    # Bound the regression itself if a future change retries a fallback without
    # an initialized trace or invocation span.
    attempts = []
    original = plugin._render_operation_end

    def bounded_render(*args, **kwargs):
        attempts.append(1)
        assert len(attempts) <= 1, "completion fallback kept requeueing"
        return original(*args, **kwargs)

    monkeypatch.setattr(plugin, "_render_operation_end", bounded_render)
    with pytest.raises(ValueError, match="available for completion fallback"):
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
        )
    assert not plugin._tracing_enabled
    assert plugin._pending_completions == plugin._pending_operation_ends == {}
