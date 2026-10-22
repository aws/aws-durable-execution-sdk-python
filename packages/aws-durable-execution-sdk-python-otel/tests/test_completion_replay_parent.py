"""Input-only completions retain actual parent segments after replay finishes."""

from datetime import UTC, datetime, timedelta

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
    OperationStatus,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("replay_children", [False, True])
def test_input_only_completion_uses_parent_segment_that_finished_during_replay(
    view, replay_children
):
    now = datetime.now(UTC)
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
    invocation = dict(
        request_id="input-only-completion",
        execution_arn="execution/replayed-parent",
        is_first_invocation=False,
        execution_start_time=now,
    )
    parent_fields = dict(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="parent",
        parent_id=None,
        start_time=now,
        is_replayed=True,
        status=OperationStatus.STARTED,
    )
    child = OperationInfo(
        operation_id="child",
        operation_type=OperationType.CALLBACK,
        sub_type=None,
        name="child",
        parent_id="parent",
        start_time=now,
        end_time=now + timedelta(seconds=1),
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
    )
    parent_input = OperationInfo(
        **{
            **parent_fields,
            "status": OperationStatus.SUCCEEDED
            if replay_children
            else OperationStatus.STARTED,
            "is_replayed": False,
        },
        end_time=now + timedelta(seconds=2) if replay_children else None,
    )
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation,
                operations={"parent": parent_input, "child": child},
                updated_operations={"child": child},
            )
        )
        if not replay_children:
            plugin.on_operation_start(OperationStartInfo(**parent_fields))
        start = UserFunctionStartInfo(
            **parent_fields, is_replay_children=replay_children
        )
        plugin.on_user_function_start(start)
        actual_parent = plugin._get_span("parent").get_span_context()
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        plugin.on_operation_end(
            OperationEndInfo(
                **{
                    **parent_fields,
                    "status": OperationStatus.SUCCEEDED,
                    "is_replayed": replay_children,
                },
                end_time=now + timedelta(seconds=2),
            )
        )
        # The application skips this callback on replay. Its updated input
        # snapshot is the only completion delivery; no child End hook fires.
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
        )

        completions = [
            span
            for span in exporter.get_finished_spans()
            if span.attributes.get("durable.operation.id") == "child"
        ]
        assert len(completions) == 1
        assert completions[0].parent is not None
        assert completions[0].parent.span_id == actual_parent.span_id
        assert completions[0].context.trace_id == actual_parent.trace_id
        if view is InvocationOtelPlugin:
            parent_span = next(
                span
                for span in exporter.get_finished_spans()
                if span.context.span_id == actual_parent.span_id
            )
            assert parent_span.start_time <= completions[0].start_time
            assert completions[0].end_time <= parent_span.end_time
    finally:
        provider.shutdown()
