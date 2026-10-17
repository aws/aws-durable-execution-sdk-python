# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exercise case 22's callback contexts through real concurrent work and resume."""

from __future__ import annotations

import importlib.util
import json
import sys
from collections import Counter
from contextlib import nullcontext
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from aws_durable_execution_sdk_python.execution import DurableExecutionInvocationInput
from aws_durable_execution_sdk_python.types import LambdaContext
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
CALLBACK_PARENTS = {
    "step": "otel-context-step attempt 1",
    "child": "otel-context-child",
    "child-step": "otel-context-child-step attempt 1",
    "child-restored": "otel-context-child",
    "parallel-a": "otel-context-branch-a",
    "parallel-step-a": "otel-context-branch-step-a attempt 1",
    "parallel-b": "otel-context-branch-b",
    "parallel-step-b": "otel-context-branch-step-b attempt 1",
    "map-0": "otel-context-iteration-0",
    "map-step-0": "otel-context-map-step-0 attempt 1",
    "map-1": "otel-context-iteration-1",
    "map-step-1": "otel-context-map-step-1 attempt 1",
}
HANDLER_COUNTS = {"handler": 2, "handler-restored": 2, "handler-after-resume": 1}


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize(
    "ambient", [False, True], ids=["no-ambient", "unrelated-ambient"]
)
def test_user_function_probes_keep_sdk_context_across_resume(
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    ambient: bool,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    before_context = context.get_current()
    common_spec = importlib.util.spec_from_file_location(
        "common", SRC_DIR / "common.py"
    )
    assert common_spec is not None and common_spec.loader is not None
    common = importlib.util.module_from_spec(common_spec)
    monkeypatch.setitem(sys.modules, "common", common)
    common_spec.loader.exec_module(common)
    monkeypatch.setattr(common, "otel_plugin", lambda: plugin)
    spec = importlib.util.spec_from_file_location(
        "otel_22_user_function_context", SRC_DIR / "otel_22_user_function_context.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    barriers: dict[str, Barrier] = {}
    for pair in (
        ("parallel-a", "parallel-b"),
        ("parallel-step-a", "parallel-step-b"),
        ("map-0", "map-1"),
        ("map-step-0", "map-step-1"),
    ):
        barrier = Barrier(2, timeout=5)
        barriers.update(dict.fromkeys(pair, barrier))
    original_probe = module.probe

    def synchronized_probe(label: str) -> None:
        callback_context = context.get_current()
        # Both sibling callbacks must be active before either observes its
        # parent. Synchronize only in the test; never supply a tracing context.
        if label in barriers:
            barriers[label].wait()
        original_probe(label)
        if label in barriers:
            barriers[label].wait()
        assert context.get_current() is callback_context

    monkeypatch.setattr(module, "probe", synchronized_probe)
    invocation_contexts: list[tuple[context.Context, context.Context]] = []

    def entry(
        event: DurableExecutionInvocationInput, lambda_context: LambdaContext
    ) -> dict[str, Any]:
        # Simulate host instrumentation outside the unmodified durable handler.
        # With no extracted parent, its unrelated trace must not become the
        # parent of any SDK operation or handler probe.
        host_context = context.get_current()
        scope = (
            provider.get_tracer("host-instrumentation").start_as_current_span(
                "ambient-invocation"
            )
            if ambient
            else nullcontext()
        )
        try:
            with scope:
                invocation_context = context.get_current()
                try:
                    return module.handler(event, lambda_context)
                finally:
                    invocation_contexts.append(
                        (invocation_context, context.get_current())
                    )
        finally:
            assert context.get_current() is host_context

    try:
        with DurableFunctionTestRunner(handler=entry, skip_time=False) as runner:
            arn = runner.run_async(
                input=json.dumps({"scenario": "user-function-context"}),
                execution_timeout=30,
            )
            result = runner.wait_for_result(arn, timeout=30)
            history = runner.get_execution_history(arn)
        assert result.status.value == "SUCCEEDED", result.error
        assert result.result is not None
        assert json.loads(result.result) == "context-complete"
        assert [
            event.event_type
            for event in history.events
            if event.event_type.startswith("Wait")
        ] == ["WaitStarted", "WaitSucceeded"]

        spans = exporter.get_finished_spans()
        invocations = [span for span in spans if span.name == "Invocation"]
        assert len(invocations) == 2
        assert [
            span.attributes["durable.invocation.status"]
            for span in invocations
            if span.attributes is not None
        ] == ["PENDING", "SUCCEEDED"]
        assert [
            span.attributes["durable.invocation.first"]
            for span in invocations
            if span.attributes is not None
        ] == [True, False]
        workflows = [span for span in spans if span.name == "Workflow"]
        assert len(workflows) == 1
        assert workflows[0].context is not None
        canonical_trace_id = workflows[0].context.trace_id
        assert {
            span.context.trace_id
            for span in spans
            if span.context is not None
            and span.attributes is not None
            and span.attributes.get("durable.execution.arn") == arn
        } == {canonical_trace_id}

        probes = [span for span in spans if span.name.startswith("conformance.")]
        # Count raw exports so replayed callback bodies and duplicate exports
        # fail even if they reuse a span ID.
        assert Counter(span.name for span in probes) == Counter(
            {
                f"conformance.{label}": count
                for label, count in (
                    {**dict.fromkeys(CALLBACK_PARENTS, 1), **HANDLER_COUNTS}
                ).items()
            }
        )
        spans_by_id = {
            (span.context.trace_id, span.context.span_id): span
            for span in spans
            if span.context is not None
        }
        handler_parent = (
            "Workflow" if plugin_type is ExecutionOtelPlugin else "Invocation"
        )
        for probe_span in probes:
            assert probe_span.context is not None
            assert probe_span.context.trace_id == canonical_trace_id
            assert probe_span.parent is not None
            assert probe_span.parent.trace_id == canonical_trace_id
            assert probe_span.attributes is not None
            assert "durable.execution.arn" not in probe_span.attributes
            label = str(probe_span.attributes["conformance.callback"])
            assert probe_span.name == f"conformance.{label}"
            parent = spans_by_id[(canonical_trace_id, probe_span.parent.span_id)]
            assert parent.name == CALLBACK_PARENTS.get(label, handler_parent)
            assert parent.attributes is not None
            assert parent.attributes["durable.execution.arn"] == arn

        for index, invocation in enumerate(invocations):
            assert invocation.start_time is not None and invocation.end_time is not None
            invocation_probes = [
                span
                for span in probes
                if span.start_time is not None
                and invocation.start_time <= span.start_time <= invocation.end_time
            ]
            expected = (
                {
                    **dict.fromkeys(CALLBACK_PARENTS, 1),
                    "handler": 1,
                    "handler-restored": 1,
                }
                if index == 0
                else dict.fromkeys(HANDLER_COUNTS, 1)
            )
            assert Counter(span.name for span in invocation_probes) == Counter(
                {f"conformance.{label}": count for label, count in expected.items()}
            )
            handler_probes = [
                span
                for span in invocation_probes
                if span.attributes is not None
                and span.attributes["conformance.callback"] in HANDLER_COUNTS
            ]
            expected_parent = (
                workflows[0] if plugin_type is ExecutionOtelPlugin else invocation
            )
            assert all(
                span.parent == expected_parent.context for span in handler_probes
            )

        ambient_spans = [span for span in spans if span.name == "ambient-invocation"]
        assert len(ambient_spans) == (2 if ambient else 0)
        for span in ambient_spans:
            assert span.context is not None
            assert span.context.trace_id != canonical_trace_id
        # Observe restoration on the runner's invocation threads, including
        # PENDING, rather than checking only the pytest caller's context.
        assert len(invocation_contexts) == 2
        assert all(before is after for before, after in invocation_contexts)
        assert context.get_current() is before_context
    finally:
        provider.shutdown()
