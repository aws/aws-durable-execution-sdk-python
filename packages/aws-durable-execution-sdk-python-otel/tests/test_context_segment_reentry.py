"""Context replay segments have their own End within a single invocation."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationEndInfo,
    OperationStartInfo,
    OperationStatus,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize(
    "checkpointed", [True, False], ids=["replay-children", "virtual"]
)
def test_replay_context_end_closes_new_segment_without_repeating_checkpoint(
    view, checkpointed
):
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
    invocation = dict(
        request_id="same-invocation",
        execution_arn="execution/context-segment-reentry",
        execution_start_time=now,
        is_first_invocation=True,
    )
    fields = dict(
        operation_id="child",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="child",
        parent_id=None,
        start_time=now,
        is_replayed=False,
        status=OperationStatus.STARTED,
    )

    def child_spans():
        return [span for span in exporter.get_finished_spans() if span.name == "child"]

    plugin.on_invocation_start(InvocationStartInfo(**invocation))
    try:
        if checkpointed:
            plugin.on_operation_start(OperationStartInfo(**fields))
        first_start = UserFunctionStartInfo(**fields)
        plugin.on_user_function_start(first_start)
        plugin.on_user_function_end(
            UserFunctionEndInfo.from_start_info(first_start, None)
        )
        first_end = OperationEndInfo(
            **{
                **fields,
                "start_time": now if checkpointed else None,
                "status": OperationStatus.SUCCEEDED,
            },
            end_time=datetime.now(UTC),
        )
        plugin.on_operation_end(first_end)
        assert len(child_spans()) == 1
        first_span = child_spans()[0]

        replay_start = UserFunctionStartInfo(
            **{**fields, "start_time": datetime.now(UTC)},
            is_replay_children=checkpointed,
        )
        plugin.on_user_function_start(replay_start)
        replay_span = plugin._get_span("child")
        assert replay_span is not None
        if view is InvocationOtelPlugin:
            assert replay_span.is_recording()
            assert replay_span.get_span_context().span_id != first_span.context.span_id

        # An old durable checkpoint may be delivered again while the new user
        # function is running. It must not close that invocation-local segment.
        if checkpointed:
            plugin.on_operation_end(first_end)
            plugin.on_operation_change(
                OperationChangeInfo(
                    execution_arn=invocation["execution_arn"],
                    updated_operations={"child": first_end},
                    operations={"child": first_end},
                )
            )
        assert len(child_spans()) == 1
        if view is InvocationOtelPlugin:
            assert replay_span.is_recording()

        plugin.on_user_function_end(
            UserFunctionEndInfo.from_start_info(replay_start, None)
        )
        replay_end = OperationEndInfo(
            **{
                **fields,
                "start_time": None,
                "is_replayed": checkpointed,
                "status": OperationStatus.SUCCEEDED,
            },
            end_time=datetime.now(UTC),
        )
        plugin.on_operation_end(replay_end)
        expected_count = 2 if view is InvocationOtelPlugin else 1
        assert len(child_spans()) == expected_count
        assert not replay_span.is_recording()
        assert all(
            span.attributes["durable.operation.status"] == "SUCCEEDED"
            and span.status.status_code is StatusCode.OK
            for span in child_spans()
        )
        if view is InvocationOtelPlugin:
            first, second = child_spans()
            assert first.context.trace_id == second.context.trace_id
            assert first.parent.span_id == second.parent.span_id
            assert (
                first.start_time < first.end_time <= second.start_time < second.end_time
            )

        # Retrying either notification without a fresh segment must export
        # nothing else, including at invocation End.
        plugin.on_operation_end(replay_end)
        plugin.on_operation_end(first_end)
        assert len(child_spans()) == expected_count
    finally:
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        provider.shutdown()
    assert len(child_spans()) == expected_count


@pytest.mark.parametrize("replayed", [False, True], ids=["virtual", "replay-children"])
def test_concurrent_context_ends_leave_finalization_with_first_caller(
    monkeypatch, replayed
):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    invocation = dict(
        request_id="concurrent-context-end",
        execution_arn="execution/concurrent-context-end",
        execution_start_time=now,
        is_first_invocation=False,
    )
    fields = dict(
        operation_id="child",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="child",
        parent_id=None,
        start_time=now,
        is_replayed=False,
        status=OperationStatus.STARTED,
    )
    finalizing = threading.Event()
    release = threading.Event()
    original_finish = plugin._finish_operation_end

    def paused_finish(info, span, fallback_parent):
        if not finalizing.is_set():
            finalizing.set()
            assert release.wait(5)
        original_finish(info, span, fallback_parent)

    monkeypatch.setattr(plugin, "_finish_operation_end", paused_finish)
    plugin.on_invocation_start(InvocationStartInfo(**invocation))
    try:
        start = UserFunctionStartInfo(**fields, is_replay_children=replayed)
        plugin.on_user_function_start(start)
        active_span = plugin._get_span("child")
        assert active_span is not None and active_span.is_recording()
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        end = OperationEndInfo(
            **{
                **fields,
                "start_time": None,
                "is_replayed": replayed,
                "status": OperationStatus.SUCCEEDED,
            },
            end_time=datetime.now(UTC),
        )
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(plugin.on_operation_end, end)
            try:
                assert finalizing.wait(5)
                duplicate = callers.submit(plugin.on_operation_end, end)
                duplicate.result(timeout=5)
                # The duplicate must not finalize the span while the winning
                # caller still owns it outside the registry lock.
                assert active_span.is_recording()
                assert not [
                    span
                    for span in exporter.get_finished_spans()
                    if span.name == "child"
                ]
            finally:
                release.set()
            first.result(timeout=5)
        assert not active_span.is_recording()
        plugin.on_operation_end(end)
        children = [
            span for span in exporter.get_finished_spans() if span.name == "child"
        ]
        assert len(children) == 1
        assert children[0].status.status_code is StatusCode.OK
        assert children[0].attributes["durable.operation.status"] == "SUCCEEDED"
        assert children[0].context.span_id == active_span.get_span_context().span_id
        assert children[0].start_time < children[0].end_time
        assert not plugin._completions.in_flight
    finally:
        release.set()
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        provider.shutdown()
