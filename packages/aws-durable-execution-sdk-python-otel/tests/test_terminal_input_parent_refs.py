"""Completion parentage prefers actual spans and falls back only at invocation End."""

import contextvars
import threading
from concurrent.futures import ThreadPoolExecutor
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
from opentelemetry import context as otel_context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)


def _info(cls, now, **changes):
    fields = dict(
        operation_id="b",
        operation_type=OperationType.CALLBACK,
        sub_type=None,
        name="b",
        parent_id="parent",
        start_time=now,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        end_time=now + timedelta(seconds=1),
    )
    fields.update(changes)
    return cls(**fields)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("parent_visible", [False, True])
def test_nonterminal_or_missing_parent_waits_for_its_actual_span(view, parent_visible):
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
        request_id="request",
        execution_arn="execution/parent",
        is_first_invocation=False,
        execution_start_time=now,
    )
    b = _info(OperationInfo, now)
    parent = _info(
        OperationInfo,
        now,
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        name="parent",
        parent_id=None,
        status=OperationStatus.STARTED,
        end_time=None,
    )
    operations = {"b": b}
    if parent_visible:
        operations["parent"] = parent

    def invoke():
        before = otel_context.get_current()
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation, operations=operations, updated_operations={"b": b}
            )
        )
        plugin.on_operation_end(_info(OperationEndInfo, now))
        assert not [
            s
            for s in exporter.get_finished_spans()
            if s.attributes.get("durable.operation.id") == "b"
        ]
        parent_fields = dict(
            operation_id="parent",
            operation_type=OperationType.CONTEXT,
            name="parent",
            parent_id=None,
            status=OperationStatus.STARTED,
            is_replayed=True,
            end_time=None,
        )
        plugin.on_operation_start(_info(OperationStartInfo, now, **parent_fields))
        start = _info(UserFunctionStartInfo, now, **parent_fields)
        plugin.on_user_function_start(start)
        actual_parent = plugin._get_span("parent").get_span_context()
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        plugin.on_operation_end(
            _info(
                OperationEndInfo,
                now,
                operation_id="parent",
                operation_type=OperationType.CONTEXT,
                name="parent",
                parent_id=None,
            )
        )
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        spans = [
            s
            for s in exporter.get_finished_spans()
            if s.attributes.get("durable.operation.id") == "b"
        ]
        assert len(spans) == 1 and spans[0].parent.span_id == actual_parent.span_id
        assert otel_context.get_current() == before
        assert not plugin._tracing_enabled and plugin._context_tokens == {}

    try:
        contextvars.Context().run(invoke)
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_unavailable_parent_exports_once_with_fallback_before_context_cleanup(
    view, caplog
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
        request_id="request",
        execution_arn="execution/unknown-parent",
        is_first_invocation=False,
        execution_start_time=now,
    )
    b = _info(OperationInfo, now)

    def invoke():
        before = otel_context.get_current()
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation, operations={"b": b}, updated_operations={"b": b}
            )
        )
        plugin.on_operation_end(_info(OperationEndInfo, now))
        plugin.on_operation_end(_info(OperationEndInfo, now))
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
        )
        children = [
            s
            for s in exporter.get_finished_spans()
            if s.attributes.get("durable.operation.id") == "b"
        ]
        assert len(children) == 1
        invocation_span = next(
            span for span in exporter.get_finished_spans() if span.name == "Invocation"
        )
        assert children[0].parent is not None
        assert children[0].context.trace_id == invocation_span.context.trace_id
        assert children[0].parent.span_id == (
            invocation_span.context.span_id
            if view is InvocationOtelPlugin
            else operation_id_to_span_id(invocation["execution_arn"], "parent")
        )
        exported = exporter.get_finished_spans()
        assert exported.index(children[0]) < exported.index(invocation_span)
        assert (
            len(
                [
                    record
                    for record in caplog.records
                    if "fallback parent" in record.message
                ]
            )
            == 1
        )
        assert (
            plugin._completions.pending == plugin._completions.children_by_parent == {}
        )
        assert otel_context.get_current() == before
        assert not plugin._tracing_enabled and plugin._context_tokens == {}

    try:
        contextvars.Context().run(invoke)
    finally:
        provider.shutdown()


def test_completed_context_remains_parent_for_late_execution_checkpoint():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    invocation = dict(
        request_id="request",
        execution_arn="execution/late-parent",
        is_first_invocation=True,
        execution_start_time=now,
    )
    parent_fields = dict(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        name="parent",
        parent_id=None,
        status=OperationStatus.STARTED,
        end_time=None,
    )
    before = otel_context.get_current()
    try:
        plugin.on_invocation_start(InvocationStartInfo(**invocation))
        plugin.on_operation_start(_info(OperationStartInfo, now, **parent_fields))
        start = _info(UserFunctionStartInfo, now, **parent_fields)
        plugin.on_user_function_start(start)
        plugin.on_operation_start(
            _info(
                OperationStartInfo, now, status=OperationStatus.STARTED, end_time=None
            )
        )
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        plugin.on_operation_end(
            _info(
                OperationEndInfo,
                now,
                operation_id="parent",
                operation_type=OperationType.CONTEXT,
                name="parent",
                parent_id=None,
            )
        )
        parents = [s for s in exporter.get_finished_spans() if s.name == "parent"]
        assert len(parents) == 1
        plugin.on_operation_end(_info(OperationEndInfo, now))
        children = [s for s in exporter.get_finished_spans() if s.name == "b"]
        assert (
            len(children) == 1
            and children[0].parent.span_id == parents[0].context.span_id
        )
        assert (
            len([s for s in exporter.get_finished_spans() if s.name == "parent"]) == 1
        )
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        assert not plugin._tracing_enabled and otel_context.get_current() == before
    finally:
        provider.shutdown()


def test_completed_input_parent_waits_for_current_invocation_segment():
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
        request_id="request",
        execution_arn="execution/replayed-parent",
        is_first_invocation=False,
        execution_start_time=now,
    )
    parent_fields = dict(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        name="parent",
        parent_id=None,
        is_replayed=True,
    )
    b = _info(OperationInfo, now)
    parent = _info(OperationInfo, now, **parent_fields)
    before = otel_context.get_current()
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                **invocation,
                operations={"parent": parent, "b": b},
                updated_operations={"b": b},
            )
        )
        plugin.on_operation_end(_info(OperationEndInfo, now))
        assert not [s for s in exporter.get_finished_spans() if s.name == "b"]
        start = _info(
            UserFunctionStartInfo, now, **parent_fields, is_replay_children=True
        )
        plugin.on_user_function_start(start)
        actual_parent = plugin._get_span("parent").get_span_context()
        children = [s for s in exporter.get_finished_spans() if s.name == "b"]
        assert (
            len(children) == 1 and children[0].parent.span_id == actual_parent.span_id
        )
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        plugin.on_operation_end(_info(OperationEndInfo, now, **parent_fields))
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.PENDING)
        )
        assert not plugin._tracing_enabled and otel_context.get_current() == before
        assert plugin._completed_input_parent_ids == set()
    finally:
        provider.shutdown()


def test_completed_parent_refs_cover_later_updates_without_guessing():
    from aws_durable_execution_sdk_python_otel._invocation_state import (
        completed_input_parent_ids,
    )

    now = datetime.now(UTC)
    parent = _info(
        OperationInfo,
        now,
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        name="parent",
        parent_id=None,
    )
    unfinished = _info(
        OperationInfo,
        now,
        operation_id="unfinished",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    step = _info(
        OperationInfo, now, operation_id="step", operation_type=OperationType.STEP
    )
    invocation = dict(
        request_id="request",
        execution_arn="execution/full-parent",
        is_first_invocation=False,
        execution_start_time=now,
    )
    assert completed_input_parent_ids(
        InvocationStartInfo(
            **invocation,
            operations={"parent": parent, "unfinished": unfinished, "step": step},
            updated_operations={},
        )
    ) == {"parent"}
    missing_parent_child = _info(OperationInfo, now)
    assert (
        completed_input_parent_ids(
            InvocationStartInfo(
                **invocation,
                operations={"b": missing_parent_child},
                updated_operations={"b": missing_parent_child},
            )
        )
        == set()
    )


def test_parent_registration_survives_concurrent_terminal_materialization(monkeypatch):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = ExecutionOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    now = datetime.now(UTC)
    invocation = dict(
        request_id="atomic-parent",
        execution_arn="execution/atomic-parent",
        is_first_invocation=True,
        execution_start_time=now,
    )
    parent_entered, allow_parent = threading.Event(), threading.Event()
    original_start = plugin._start_span

    def start_span(**kwargs):
        if kwargs["operation_id"] == "parent":
            parent_entered.set()
            assert allow_parent.wait(5)
        return original_start(**kwargs)

    monkeypatch.setattr(plugin, "_start_span", start_span)

    def invoke():
        before = otel_context.get_current()
        plugin.on_invocation_start(InvocationStartInfo(**invocation))
        parent_fields = dict(
            operation_id="parent",
            operation_type=OperationType.CONTEXT,
            name="parent",
            parent_id=None,
            status=OperationStatus.STARTED,
            end_time=None,
        )
        plugin.on_operation_start(_info(OperationStartInfo, now, **parent_fields))
        start = _info(UserFunctionStartInfo, now, **parent_fields)
        plugin.on_user_function_start(start)
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        parent_fields.update(
            status=OperationStatus.SUCCEEDED, end_time=now + timedelta(seconds=2)
        )
        parent_context, child_context = (
            contextvars.copy_context(),
            contextvars.copy_context(),
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            parent = pool.submit(
                parent_context.run,
                plugin.on_operation_end,
                _info(OperationEndInfo, now, **parent_fields),
            )
            try:
                assert parent_entered.wait(5)
                child = pool.submit(
                    child_context.run,
                    plugin.on_operation_end,
                    _info(OperationEndInfo, now),
                )
                child.result(timeout=5)
            finally:
                allow_parent.set()
            parent.result(timeout=5)
        close_error = None
        try:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )
        except ValueError as exc:
            close_error = exc
        spans = exporter.get_finished_spans()
        parents = [
            s for s in spans if s.attributes.get("durable.operation.id") == "parent"
        ]
        children = [s for s in spans if s.attributes.get("durable.operation.id") == "b"]
        assert len(parents) == len(children) == 1
        assert children[0].parent.span_id == parents[0].context.span_id
        assert children[0].context.trace_id == parents[0].context.trace_id
        assert close_error is None
        assert not plugin._tracing_enabled and otel_context.get_current() == before

    try:
        contextvars.Context().run(invoke)
    finally:
        provider.shutdown()
