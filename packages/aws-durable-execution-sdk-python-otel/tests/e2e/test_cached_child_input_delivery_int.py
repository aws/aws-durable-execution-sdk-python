"""Visible terminal input updates are delivered once with real retained children."""

import json
import logging
import os
from pathlib import Path
import time

import pytest
from aws_durable_execution_sdk_python import durable_execution, durable_step
from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize(
    ("view", "filesystem", "failed_delivery"),
    [
        (ExecutionOtelPlugin, False, False),
        (InvocationOtelPlugin, False, False),
        (ExecutionOtelPlugin, True, False),
        (InvocationOtelPlugin, True, False),
        (ExecutionOtelPlugin, False, True),
    ],
)
def test_retained_child_callback_notifies_before_input_ack(
    view, filesystem, failed_delivery, tmp_path, monkeypatch, caplog
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    store = (
        FileSystemExecutionStore.create(tmp_path / "store")
        if filesystem
        else InMemoryExecutionStore()
    )
    starts, ends, notifications, child_calls, marker_calls = [], [], [], [], []
    attempts = []
    saved_child_calls = []
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

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            starts.append(info)

        def on_invocation_end(self, info):
            ends.append(info)

        def on_operation_end(self, info):
            if info.name in ("a", "b"):
                notifications.append((len(starts), info))

    def saved_child(_step):
        saved_child_calls.append(1)
        return "saved"

    def child(context):
        child_calls.append(1)
        callback_id = context.create_callback(
            name="b", config=CallbackConfig(serdes=JsonSerDes())
        ).callback_id
        assert context.step(saved_child, name="saved-child") == "saved"
        # The real service hides descendants of a completed ordinary context.
        # A real large result makes this context use ReplayChildren, retaining
        # B's metadata without injecting input state or hook notifications.
        return {"id": callback_id, "large": "x" * (300 * 1024)}

    @durable_step
    def marker(_context, result):
        marker_calls.append(1)
        return result

    def handler(_event, context):
        a = context.create_callback(
            name="a", config=CallbackConfig(serdes=JsonSerDes())
        )
        b_id = context.run_in_child_context(child, name="cached-child")["id"]
        result = context.step(marker(a.result()), name="after-a")
        return {"a": result, "b_id": b_id}

    wrapped = durable_execution(handler, plugins=[Observe(), plugin])

    def entrypoint(event, context):
        attempts.append(1)
        if failed_delivery and len(attempts) == 2:
            raise RuntimeError("invocation failed before SDK input delivery")
        return wrapped(event, context)

    def wait_for_completed(runner, arn, count):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            events = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
            if (
                len(ends) >= count
                and sum(e.event_type == "InvocationCompleted" for e in events) >= count
            ):
                return events
            time.sleep(0.01)
        raise AssertionError("Expected invocation did not finish")

    try:
        with DurableFunctionTestRunner(
            handler=entrypoint,
            store=store,
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=25,
        ) as runner:
            arn = runner.run_async(input="{}")
            a_id = runner.wait_for_callback(arn, name="a", timeout=5)
            b_id = runner.wait_for_callback(arn, name="b", timeout=5)
            wait_for_completed(runner, arn, 1)
            runner.send_callback_success(b_id, result=b'"B"')
            # A pre-SDK failure is a physical attempt, not an SDK End hook.
            middle_history = wait_for_completed(runner, arn, 2)
            middle_state = store.load(arn)
            middle_updated_ids = list(middle_state.updated_operation_ids)
            runner.send_callback_success(a_id, result=b'"A"')
            result = runner.wait_for_result(arn, timeout=10)
            final_history = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == {"a": "A", "b_id": b_id}
        assert [i.status.value for i in ends] == ["PENDING", "PENDING", "SUCCEEDED"]
        assert child_calls == [1, 1, 1] and saved_child_calls == [1]
        assert marker_calls == [1]
        assert len(attempts) == (4 if failed_delivery else 3)
        b_info = next(
            info for info in starts[1].updated_operations.values() if info.name == "b"
        )
        parent = starts[1].operations[b_info.parent_id]
        assert parent.name == "cached-child" and parent.status.value == "SUCCEEDED"
        b_ends = [
            (invocation, info) for invocation, info in notifications if info.name == "b"
        ]
        terminal_spans = [
            s
            for s in exporter.get_finished_spans()
            if s.attributes.get("durable.operation.name") == "b"
            and s.attributes.get("durable.operation.status") == "SUCCEEDED"
        ]
        parent_spans = [
            s
            for s in exporter.get_finished_spans()
            if s.attributes.get("durable.operation.id") == b_info.parent_id
            and s.attributes.get("durable.operation.type") == "CONTEXT"
            and s.attributes.get("durable.operation.status") == "SUCCEEDED"
        ]
        hook_errors = [
            r.getMessage()
            for r in caplog.records
            if r.levelno >= logging.ERROR and ("plugin" in r.name or "otel" in r.name)
        ]
        proof_dir = os.environ.get("CACHED_CHILD_PROOF_DIR")
        if proof_dir:
            destination = Path(proof_dir)
            destination.mkdir(parents=True, exist_ok=True)
            (
                destination
                / f"{view.__name__}-{'fs' if filesystem else 'memory'}-failed{failed_delivery}.json"
            ).write_text(
                json.dumps(
                    {
                        "business_succeeded": True,
                        "result_b_matches_issued": True,
                        "invocation_statuses": [i.status.value for i in ends],
                        "child_calls": len(child_calls),
                        "saved_child_calls": len(saved_child_calls),
                        "retention_via_public_large_result": True,
                        "marker_calls": len(marker_calls),
                        "physical_attempts": len(attempts),
                        "failed_before_SDK": failed_delivery,
                        "input_updated_b": b_info.operation_id,
                        "input_parent_id": b_info.parent_id,
                        "parent_status": parent.status.value,
                        "middle_updated_ids": middle_updated_ids,
                        "b_notifications": [
                            (n, i.operation_id, i.parent_id, i.is_replayed)
                            for n, i in b_ends
                        ],
                        "b_terminal_spans": len(terminal_spans),
                        "hook_errors": hook_errors,
                        "parent_segment_span_ids": [
                            s.context.span_id for s in parent_spans
                        ],
                        "b_terminal_parent_ids": [
                            s.parent.span_id if s.parent else None
                            for s in terminal_spans
                        ],
                        "middle_history_events": [
                            (e.event_id, e.event_type, e.name) for e in middle_history
                        ],
                        "final_history_events": [
                            (e.event_id, e.event_type, e.name) for e in final_history
                        ],
                    },
                    indent=2,
                )
                + "\n"
            )
        assert len(b_ends) == 1
        assert b_ends[0][0] == 2
        assert b_ends[0][1].operation_id == b_info.operation_id
        assert b_ends[0][1].parent_id == b_info.parent_id
        assert not b_ends[0][1].is_replayed
        assert len(terminal_spans) == 1
        assert terminal_spans[0].parent is not None
        if view is ExecutionOtelPlugin:
            assert len(parent_spans) == 1
            assert terminal_spans[0].parent.span_id == parent_spans[0].context.span_id
        else:
            assert len(parent_spans) == 3
            assert len({span.context.span_id for span in parent_spans}) == 3
            invocations = [
                span
                for span in exporter.get_finished_spans()
                if span.name == "Invocation"
            ]
            assert len(invocations) == 3
            assert [span.parent.span_id for span in parent_spans] == [
                span.context.span_id for span in invocations
            ]
            assert terminal_spans[0].parent.span_id == parent_spans[1].context.span_id
        assert not hook_errors
    finally:
        provider.shutdown()
