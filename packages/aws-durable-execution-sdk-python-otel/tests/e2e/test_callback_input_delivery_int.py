"""Input-only callback resumes export each terminal outcome once."""

import json
import logging
import threading
import time

import pytest
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize(
    "view,nested,page_bytes,fresh_executor,concurrent_completion",
    [
        (ExecutionOtelPlugin, False, None, False, False),
        (InvocationOtelPlugin, False, None, False, False),
        (ExecutionOtelPlugin, True, None, False, False),
        (InvocationOtelPlugin, True, None, False, False),
        (InvocationOtelPlugin, True, 1, False, False),
        (ExecutionOtelPlugin, False, None, True, False),
        (InvocationOtelPlugin, True, None, False, True),
    ],
    ids=[
        "execution",
        "invocation",
        "execution-nested",
        "invocation-nested",
        "paged",
        "fresh-executor",
        "concurrent",
    ],
)
def test_two_callbacks_do_not_repeat_terminal_spans_after_input_only_resume(
    view, nested, page_bytes, fresh_executor, concurrent_completion, caplog, monkeypatch
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    starts = []
    ends = []
    marker_calls = []
    second_end = threading.Event()
    release_end = threading.Event()

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            starts.append(info)

        def on_operation_end(self, info):
            if info.name in ("a", "b"):
                ends.append((len(starts), info.name, info.status.value))

        def on_invocation_end(self, info):
            if concurrent_completion and len(starts) == 2:
                second_end.set()
                assert release_end.wait(6)

    observer = Observe()

    def make_plugin():
        return view(
            OtelPluginConfig(
                tracer_provider=provider,
                enrich_logger=False,
                context_extractor=lambda _: None,
            )
        )

    @durable_step
    def marker(context, value):
        marker_calls.append(1)
        return value

    def callbacks(context):
        a = context.create_callback(
            name="a", config=CallbackConfig(serdes=JsonSerDes())
        )
        b = context.create_callback(
            name="b", config=CallbackConfig(serdes=JsonSerDes())
        )
        return [a.result(), b.result()]

    @durable_with_child_context
    def child(context):
        return callbacks(context)

    def body(event, context: DurableContext):
        result = (
            context.run_in_child_context(child(), name="group")
            if nested
            else callbacks(context)
        )
        return context.step(marker(result), name="after-b")

    wrapped = durable_execution(body, plugins=[make_plugin(), observer])

    def entrypoint(event, context):
        target = (
            durable_execution(body, plugins=[make_plugin(), observer])
            if fresh_executor
            else wrapped
        )
        return target(event, context)

    try:
        with DurableFunctionTestRunner(
            handler=entrypoint,
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=25,
            max_invocation_page_bytes=page_bytes,
        ) as runner:
            arn = runner.run_async(input="{}")
            a_id = runner.wait_for_callback(arn, name="a", timeout=5)
            b_id = runner.wait_for_callback(arn, name="b", timeout=5)
            runner.send_callback_success(a_id, result=b'"A"')
            if concurrent_completion:
                assert second_end.wait(5)
                runner.send_callback_success(b_id, result=b'"B"')
                release_end.set()
            else:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    events = runner.get_execution_history(
                        arn, include_execution_data=True
                    ).events
                    if (
                        sum(
                            event.event_type == "InvocationCompleted"
                            for event in events
                        )
                        >= 2
                    ):
                        break
                    time.sleep(0.01)
                else:
                    raise AssertionError("The second invocation did not suspend")
                runner.send_callback_success(b_id, result=b'"B"')
            result = runner.wait_for_result(arn, timeout=10)
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == ["A", "B"]
        assert len(starts) == 3
        assert marker_calls == [1]
        assert ends == [(2, "a", "SUCCEEDED"), (3, "b", "SUCCEEDED")]
        spans = exporter.get_finished_spans()
        callback_spans = [
            span
            for span in spans
            if (span.attributes or {}).get("durable.operation.type") == "CALLBACK"
        ]
        # Retain the nonterminal segments as well as the terminal segments;
        # filtering down to successful spans would hide the invocation story.
        expected = [("a", "SUCCEEDED"), ("b", "SUCCEEDED")]
        if view is InvocationOtelPlugin:
            expected = [
                ("b", "STARTED"),
                ("a", "STARTED"),
                ("a", "SUCCEEDED"),
                ("b", "STARTED"),
                ("b", "SUCCEEDED"),
            ]
        assert [
            (span.name, span.attributes["durable.operation.status"])
            for span in callback_spans
        ] == expected
        exported_ids = {span.context.span_id for span in spans}
        assert all(
            span.parent is not None and span.parent.span_id in exported_ids
            for span in callback_spans
        )
        assert not [
            record
            for record in caplog.records
            if record.levelno >= logging.ERROR
            and ("plugin" in record.name or "otel" in record.name)
        ]
    finally:
        release_end.set()
        provider.shutdown()
