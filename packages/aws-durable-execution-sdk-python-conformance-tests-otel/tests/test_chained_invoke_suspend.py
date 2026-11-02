"""The real case 27 child suspends and resumes before its caller completes."""

import importlib.util
import json
from pathlib import Path
import sys

import pytest
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


SRC = Path(__file__).resolve().parents[1] / "src"


@pytest.mark.parametrize(
    "view", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_case27_resumes_the_real_child_and_preserves_its_result(view, monkeypatch):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    monkeypatch.setenv("OTEL_INVOKE_TARGET_FUNCTION_NAME", "case27-child")
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    spec = importlib.util.spec_from_file_location("common", SRC / "common.py")
    common = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "common", common)
    spec.loader.exec_module(common)
    monkeypatch.setattr(
        common,
        "otel_plugin_factory",
        lambda: view(
            OtelPluginConfig(
                tracer_provider=provider,
                context_extractor=lambda _: None,
                enrich_logger=False,
            )
        ),
    )
    spec = importlib.util.spec_from_file_location(
        "otel_27_chained_invoke_suspend", SRC / "otel_27_chained_invoke_suspend.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    payload = {"scenario": "chained-invoke-suspend", "nested": {"value": 42}}
    try:
        with DurableFunctionTestRunner(
            handler=module.handler,
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=30,
        ) as runner:
            runner.register_durable_function("case27-child", module.target_handler)
            arn = runner.run_async(input=json.dumps(payload))
            result = runner.wait_for_result(arn, timeout=15)
            history = runner.get_execution_history(arn, include_execution_data=True)
            starts = [
                e for e in history.events if e.event_type == "ChainedInvokeStarted"
            ]
            ends = [
                e for e in history.events if e.event_type == "ChainedInvokeSucceeded"
            ]
            assert len(starts) == len(ends) == 1
            assert starts[0].operation_id == ends[0].operation_id
            child_arn = starts[0].chained_invoke_started_details.durable_execution_arn
            child_history = runner.get_execution_history(
                child_arn, include_execution_data=True
            )

        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == payload
        for execution_history in (history, child_history):
            assert (
                sum(
                    e.event_type == "InvocationCompleted"
                    for e in execution_history.events
                )
                == 2
            )
            assert (
                sum(
                    e.event_type == "ExecutionStarted" for e in execution_history.events
                )
                == 1
            )
        assert sum(e.event_type == "WaitStarted" for e in child_history.events) == 1
        assert sum(e.event_type == "WaitSucceeded" for e in child_history.events) == 1
        spans = exporter.get_finished_spans()
        for execution_arn in (arn, child_arn):
            invocations = sorted(
                [
                    s
                    for s in spans
                    if s.name == "Invocation"
                    and s.attributes.get("durable.execution.arn") == execution_arn
                ],
                key=lambda s: s.start_time,
            )
            assert [s.attributes["durable.invocation.status"] for s in invocations] == [
                "PENDING",
                "SUCCEEDED",
            ]
            assert [s.attributes["durable.invocation.first"] for s in invocations] == [
                True,
                False,
            ]
        waits = sorted(
            [s for s in spans if s.name == "otel-suspended-target-wait"],
            key=lambda s: s.start_time,
        )
        assert len(waits) == (2 if view is InvocationOtelPluginFactory else 1)
        assert len({s.attributes["durable.operation.id"] for s in waits}) == 1
        assert all(s.attributes["durable.execution.arn"] == child_arn for s in waits)
        if view is InvocationOtelPluginFactory:
            assert waits[-1].links[0].context.span_id == waits[0].context.span_id
        else:
            resumed = next(
                s
                for s in spans
                if s.name == "Invocation"
                and s.attributes.get("durable.execution.arn") == child_arn
                and not s.attributes["durable.invocation.first"]
            )
            assert waits[0].links[0].context.span_id == resumed.context.span_id
    finally:
        provider.shutdown()
