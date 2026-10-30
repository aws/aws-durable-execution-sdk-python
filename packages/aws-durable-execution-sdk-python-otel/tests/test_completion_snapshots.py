"""Input, replay and checkpoint observations share an invocation's export ledger."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import Any

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
    OperationStatus,
    OperationType,
    UserFunctionStartInfo,
    UserFunctionEndInfo,
)
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import Span as SdkSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


TraceCase = tuple[
    ExecutionOtelPlugin | InvocationOtelPlugin,
    InMemorySpanExporter,
    dict[str, Any],
]


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


@pytest.mark.parametrize(
    "status",
    [
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
        OperationStatus.CANCELLED,
    ],
)
@pytest.mark.parametrize("input_update", [False, True])
@pytest.mark.parametrize("visited", [False, True])
def test_snapshot_and_natural_notifications_export_once(
    trace_case, input_update, visited, status
):
    plugin, exporter, invocation = trace_case
    end = replace(completion(), status=status)
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
    (completed_span,) = [span for span in spans if span.name == "b"]
    assert completed_span.attributes["durable.operation.status"] == status.value
    assert not [span for span in spans if span.name == "active"]
    assert plugin._completions.pending == plugin._completions.children_by_parent == {}


def test_replay_only_metadata_is_not_a_new_completion(trace_case: TraceCase) -> None:
    plugin, exporter, invocation = trace_case
    completed = completion()
    plugin.on_invocation_start(
        InvocationStartInfo(
            **invocation,
            operations={"b": completed},
            updated_operations={"b": replace(completed, is_replayed=True)},
        )
    )
    plugin.on_invocation_end(
        InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
    )
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=invocation["execution_arn"],
            operations={"b": completed},
            updated_operations={"b": completed},
        )
    )
    assert not [span for span in exporter.get_finished_spans() if span.name == "b"]
    assert not plugin._completions.pending


@pytest.mark.parametrize("fail_child", [False, True])
def test_parent_registration_runs_child_processors_outside_lock_and_attaches_context(
    trace_case: TraceCase, fail_child: bool, caplog: pytest.LogCaptureFixture
) -> None:
    plugin, exporter, invocation = trace_case
    provider = plugin._provider
    assert isinstance(provider, TracerProvider)
    child = completion(parent_id="parent")
    with ThreadPoolExecutor(max_workers=1) as workers:

        class CheckParent(SpanProcessor):
            reject = fail_child

            def on_start(
                self, span: SdkSpan, parent_context: Context | None = None
            ) -> None:
                if span.name == "b":
                    # A processor can consult the registry on another thread.
                    # Neither parent registration nor terminal rendering owns
                    # that lock while executing the callback.
                    assert workers.submit(
                        plugin._completion_parent_span, "parent"
                    ).result(3)
                    if self.reject:
                        raise RuntimeError("child processor failed")

        processor = CheckParent()
        provider.add_span_processor(processor)
        plugin.on_invocation_start(
            InvocationStartInfo(**invocation, updated_operations={"b": child})
        )
        fields: dict[str, Any] = dict(
            operation_id="parent",
            operation_type=OperationType.CONTEXT,
            sub_type=None,
            name="parent",
            parent_id=None,
            start_time=datetime.now(UTC),
            is_replayed=True,
            status=OperationStatus.STARTED,
        )
        plugin.on_operation_start(OperationStartInfo(**fields))
        start = UserFunctionStartInfo(**fields)
        plugin.on_user_function_start(start)
        parent = plugin._get_span("parent")
        assert parent is not None
        assert trace.get_current_span().get_span_context() == parent.get_span_context()
        with provider.get_tracer("application").start_as_current_span("user"):
            pass
        (user,) = [
            span for span in exporter.get_finished_spans() if span.name == "user"
        ]
        assert user.parent == parent.get_span_context()
        assert len(
            [span for span in exporter.get_finished_spans() if span.name == "b"]
        ) == int(not fail_child)
        assert (
            any(
                "child processor failed" in record.getMessage() or record.exc_info
                for record in caplog.records
            )
            is fail_child
        )
        processor.reject = False
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        plugin.on_operation_end(
            OperationEndInfo(
                **{**fields, "status": OperationStatus.SUCCEEDED, "is_replayed": False},
                end_time=datetime.now(UTC),
            )
        )
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
        )
        # A failed child delivery remains available for finalization, while a
        # successful registration drain is not repeated.
        assert (
            len([span for span in exporter.get_finished_spans() if span.name == "b"])
            == 1
        )


def test_natural_child_end_preserves_other_pending_children(
    trace_case: TraceCase,
) -> None:
    plugin, exporter, invocation = trace_case
    plugin.on_invocation_start(InvocationStartInfo(**invocation))
    fields: dict[str, Any] = dict(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="parent",
        parent_id=None,
        start_time=datetime.now(UTC),
        is_replayed=False,
        status=OperationStatus.STARTED,
    )
    plugin.on_operation_start(OperationStartInfo(**fields))
    start = UserFunctionStartInfo(**fields)
    plugin.on_user_function_start(start)
    parent_span = plugin._get_span("parent")
    assert parent_span is not None
    parent_context = parent_span.get_span_context()
    updates = {
        name: completion(name, parent_id="parent") for name in ("first", "second")
    }
    plugin.on_operation_change(
        OperationChangeInfo(
            execution_arn=invocation["execution_arn"],
            operations=updates,
            updated_operations=updates,
        )
    )
    plugin.on_operation_end(updates["first"])
    assert [
        span.name for span in exporter.get_finished_spans() if span.name in updates
    ] == ["first"]
    plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
    plugin.on_operation_end(
        OperationEndInfo(
            **{**fields, "status": OperationStatus.SUCCEEDED},
            end_time=datetime.now(UTC),
        )
    )
    plugin.on_invocation_end(
        InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
    )
    children = [span for span in exporter.get_finished_spans() if span.name in updates]
    assert [span.name for span in children] == ["first", "second"]
    assert all(span.parent == parent_context for span in children)


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


@pytest.mark.parametrize("full_snapshot", [False, True])
def test_missing_ancestor_fallback_preserves_available_descendant_hierarchy(
    trace_case, caplog, full_snapshot
):
    plugin, exporter, invocation = trace_case
    # Deliberately child-first: fallback must register its pending parent first.
    child = completion(parent_id="parent")
    parent = completion("parent", "missing-ancestor", OperationType.CONTEXT)
    plugin.on_invocation_start(
        InvocationStartInfo(
            **invocation,
            operations={"b": child, "parent": parent} if full_snapshot else {},
            updated_operations={"b": child, "parent": parent},
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
    assert plugin._completions.pending == plugin._completions.children_by_parent == {}


def test_partial_start_releases_pending_metadata_at_end(
    trace_case: TraceCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin, _exporter, invocation = trace_case

    def fail_extraction(_info: InvocationStartInfo) -> None:
        raise RuntimeError("context extraction failed")

    monkeypatch.setattr(plugin, "_context_extractor", fail_extraction)
    with pytest.raises(RuntimeError, match="context extraction failed"):
        plugin.on_invocation_start(InvocationStartInfo(**invocation))
    plugin.on_operation_end(completion(parent_id="missing-parent"))
    with pytest.raises(ValueError, match="available for completion fallback"):
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
        )
    assert not plugin._tracing_enabled
    assert not plugin._completions.pending
    assert not plugin._completions.children_by_parent
