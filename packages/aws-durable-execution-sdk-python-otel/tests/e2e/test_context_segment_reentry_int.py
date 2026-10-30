"""A completed child can run again during one invocation's timed branch resume."""

import json
import logging
import threading

import pytest
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import (
    Duration,
    ParallelBranch,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


@pytest.mark.parametrize(
    "view", [InvocationOtelPluginFactory, ExecutionOtelPluginFactory]
)
def test_timed_branch_resume_ends_each_reentered_child_segment(
    view, monkeypatch, caplog
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    sibling_running = threading.Event()
    resumed = threading.Event()
    child_calls = []
    saved_calls = []
    invocations = []
    child_starts = []
    child_ends = []
    replay_exports = []

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            invocations.append(info.request_id)

        def on_user_function_start(self, info):
            if info.name == "large-child":
                child_starts.append(info)

        def on_operation_end(self, info):
            if info.name == "large-child":
                child_ends.append(info)
                if info.is_replayed:
                    replay_exports.append(
                        len(
                            [
                                span
                                for span in exporter.get_finished_spans()
                                if span.name == "large-child"
                            ]
                        )
                    )

    class ObserverFactory:
        def create_plugin(self, _info):
            return Observer()

    def saved(_step):
        saved_calls.append(1)
        return "saved"

    def large_child(context):
        child_calls.append(1)
        assert context.step(saved, name="saved-inside-child") == "saved"
        # This public large-result path checkpoints ReplayChildren rather than
        # storing the result. It reruns the child body on the timed resume.
        return "x" * (300 * 1024)

    def resuming_branch(context):
        assert sibling_running.wait(10)
        value = context.run_in_child_context(large_child, name="large-child")
        context.wait(Duration.from_seconds(1), name="resume-branch")
        resumed.set()
        return len(value)

    def running_sibling(_context):
        sibling_running.set()
        assert resumed.wait(10)
        return "sibling"

    def handler(_event, context):
        return context.parallel(
            [
                ParallelBranch(func=resuming_branch, name="resuming-branch"),
                ParallelBranch(func=running_sibling, name="running-sibling"),
            ],
            name="parallel-parent",
            config=ParallelConfig(max_concurrency=2),
        ).get_results()

    wrapped = durable_execution(
        handler,
        plugins=[
            view(
                OtelPluginConfig(
                    tracer_provider=provider,
                    context_extractor=lambda _: None,
                    enrich_logger=False,
                )
            ),
            ObserverFactory(),
        ],
    )
    try:
        with DurableFunctionTestRunner(
            handler=wrapped, skip_time=False, poll_interval=0.01, execution_timeout=20
        ) as runner:
            result = runner.run(input="{}")
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == [300 * 1024, "sibling"]
        assert len(invocations) == 1
        assert child_calls == [1, 1]
        assert saved_calls == [1]
        assert [info.is_replay_children for info in child_starts] == [False, True]
        assert any(not info.is_replayed for info in child_ends)
        assert any(info.is_replayed for info in child_ends)

        spans = exporter.get_finished_spans()
        children = [span for span in spans if span.name == "large-child"]
        expected_children = 2 if view is InvocationOtelPluginFactory else 1
        assert len(children) == expected_children
        assert replay_exports == [expected_children]
        assert all(
            span.attributes["durable.operation.status"] == "SUCCEEDED"
            and span.status.status_code is StatusCode.OK
            for span in children
        ), [(span.attributes, span.status.status_code) for span in children]
        parents = [span for span in spans if span.name == "resuming-branch"]
        assert len(parents) == 1
        for child in children:
            assert child.parent is not None
            assert child.parent.span_id == parents[0].context.span_id
            assert child.context.trace_id == parents[0].context.trace_id
            assert parents[0].start_time <= child.start_time < child.end_time
            assert child.end_time <= parents[0].end_time
        if len(children) == 2:
            assert children[0].context.span_id != children[1].context.span_id
            assert children[0].end_time <= children[1].start_time
        assert not [
            record
            for record in caplog.records
            if record.levelno >= logging.ERROR
            and ("plugin" in record.name or "otel" in record.name)
        ]
    finally:
        resumed.set()
        provider.shutdown()
