"""A shared plugin routes worker/checkpoint events to their real invocation."""

import json
import logging
import threading

import pytest
from aws_durable_execution_sdk_python import durable_execution, durable_step
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_same_handler_shared_plugin_keeps_two_execution_traces(
    view, monkeypatch, caplog
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
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
    ready = threading.Event()
    release = threading.Event()
    side_effects = []

    @durable_step
    def save(_context, label, stage):
        side_effects.append((label, stage))
        return label + stage

    def body(event, context):
        label = event["label"]
        first = context.step(save(label, "first"), name="first")
        if label == "a":
            ready.set()
            assert release.wait(5)
        second = context.step(save(label, "second"), name="second")
        return [first, second]

    handler = durable_execution(body, plugins=[plugin])
    try:
        with (
            DurableFunctionTestRunner(handler=handler, poll_interval=0.01) as ra,
            DurableFunctionTestRunner(handler=handler, poll_interval=0.01) as rb,
        ):
            a = ra.run_async(input='{"label":"a"}')
            assert ready.wait(5)
            try:
                b = rb.run_async(input='{"label":"b"}')
                result_b = rb.wait_for_result(b, timeout=5)
            finally:
                release.set()
            result_a = ra.wait_for_result(a, timeout=5)
        assert result_a.status.value == result_b.status.value == "SUCCEEDED"
        assert json.loads(result_a.result) == ["afirst", "asecond"]
        assert json.loads(result_b.result) == ["bfirst", "bsecond"]
        assert sorted(side_effects) == [
            ("a", "first"),
            ("a", "second"),
            ("b", "first"),
            ("b", "second"),
        ]
        spans = exporter.get_finished_spans()
        workflows = [s for s in spans if s.name == "Workflow"]
        assert len(workflows) == 2
        assert {s.attributes["durable.execution.arn"] for s in workflows} == {a, b}
        trace_for = {
            s.attributes["durable.execution.arn"]: s.context.trace_id for s in workflows
        }
        assert len(set(trace_for.values())) == 2
        for arn in (a, b):
            own = [s for s in spans if s.attributes.get("durable.execution.arn") == arn]
            assert all(s.context.trace_id == trace_for[arn] for s in own)
            steps = [
                s for s in own if s.attributes.get("durable.operation.type") == "STEP"
            ]
            assert len(steps) == 4
            assert len({s.context.span_id for s in steps}) == 4
            terminals = [
                s
                for s in steps
                if s.attributes.get("durable.operation.status") == "SUCCEEDED"
            ]
            assert len(terminals) == 2
            assert {s.attributes["durable.operation.name"] for s in terminals} == {
                "first",
                "second",
            }
            assert all(s.parent is not None for s in terminals)
            own_ids = {s.context.span_id for s in own}
            assert all(s.parent.span_id in own_ids for s in steps)
        assert not [
            r
            for r in caplog.records
            if r.levelno >= logging.ERROR and ("plugin" in r.name or "otel" in r.name)
        ]
    finally:
        release.set()
        provider.shutdown()
