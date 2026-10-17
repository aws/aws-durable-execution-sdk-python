"""Late callback completions keep the actual parent for their invocation."""

import json
import logging
import os
import queue
from pathlib import Path
import threading
import time

import pytest
from aws_durable_execution_sdk_python import durable_execution
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


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize(
    ("large_result", "resume_before_completion"),
    [(False, False), (True, False), (False, True)],
)
def test_late_callback_keeps_completed_or_replayed_parent(
    view, large_result, resume_before_completion, monkeypatch, caplog
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
    starts, ends, child_calls, saved_calls, root_calls = [], [], [], [], []
    root_step_started, release_root_step = threading.Event(), threading.Event()
    callback_ids = queue.Queue(maxsize=1)

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            starts.append(info)

        def on_invocation_end(self, info):
            ends.append(info)

    def saved(_step):
        saved_calls.append(1)
        return "saved"

    def child(ctx):
        child_calls.append(1)
        cb = ctx.create_callback(name="b", config=CallbackConfig(serdes=JsonSerDes()))
        assert ctx.step(saved, name="save-child") == "saved"
        return dict(
            b_id=cb.callback_id, large="x" * (300 * 1024) if large_result else "small"
        )

    def root_step(a_id, b_id):
        callback_ids.put((a_id, b_id))
        root_calls.append(1)
        root_step_started.set()
        assert release_root_step.wait(10)
        return "root-saved"

    def handler(_event, ctx):
        a = ctx.create_callback(name="a", config=CallbackConfig(serdes=JsonSerDes()))
        result = ctx.run_in_child_context(child, name="returned-child")
        if resume_before_completion:
            gate = ctx.create_callback(
                name="resume-gate", config=CallbackConfig(serdes=JsonSerDes())
            )
            assert gate.result() == "resume"
        if not large_result:
            assert (
                ctx.step(
                    lambda _: root_step(a.callback_id, result["b_id"]),
                    name="root-checkpoint",
                )
                == "root-saved"
            )
        return dict(a=a.result(), b_id=result["b_id"], length=len(result["large"]))

    wrapped = durable_execution(handler, plugins=[Observe(), plugin])

    def completed(runner, arn, count):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            history = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
            if (
                len(ends) >= count
                and sum(e.event_type == "InvocationCompleted" for e in history) >= count
            ):
                return history
            time.sleep(0.01)
        raise AssertionError("invocation completion not observed")

    def spans(name, status=None):
        return [
            s
            for s in exporter.get_finished_spans()
            if s.attributes.get("durable.operation.name") == name
            and (
                status is None or s.attributes.get("durable.operation.status") == status
            )
        ]

    try:
        with DurableFunctionTestRunner(
            handler=wrapped, skip_time=False, poll_interval=0.01, execution_timeout=35
        ) as runner:
            arn = runner.run_async(input="{}")
            if large_result:
                a_id = runner.wait_for_callback(arn, name="a", timeout=5)
                b_id = runner.wait_for_callback(arn, name="b", timeout=5)
                completed(runner, arn, 1)
            else:
                if resume_before_completion:
                    gate_id = runner.wait_for_callback(
                        arn, name="resume-gate", timeout=5
                    )
                    completed(runner, arn, 1)
                    runner.send_callback_success(gate_id, result=b'"resume"')
                # The runner wait_for_callback helper waits for suspension;
                # this case deliberately completes during the active root step.
                a_id, b_id = callback_ids.get(timeout=5)
                assert root_step_started.wait(5)
            runner.send_callback_success(b_id, result=b'"B"')
            release_root_step.set()
            completed(runner, arn, 2 if large_result or resume_before_completion else 1)
            middle_callbacks = spans("b", "SUCCEEDED")
            middle_parents = spans("returned-child", "SUCCEEDED")
            invocation_spans = [
                s for s in exporter.get_finished_spans() if s.name == "Invocation"
            ]
            runner.send_callback_success(a_id, result=b'"A"')
            result = runner.wait_for_result(arn, timeout=10)
            history = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
        expected_invocations = 3 if large_result or resume_before_completion else 2
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == dict(
            a="A", b_id=b_id, length=300 * 1024 if large_result else 5
        )
        assert len(starts) == expected_invocations and [
            x.status.value for x in ends
        ] == ["PENDING"] * (expected_invocations - 1) + ["SUCCEEDED"]
        assert saved_calls == [1] and root_calls == ([] if large_result else [1])
        assert len(child_calls) == (3 if large_result else 1)
        if resume_before_completion:
            assert all(
                info.name != "b" for info in starts[1].updated_operations.values()
            )
            # Service input prunes B even while it is STARTED. Its later
            # checkpoint response still carries the actual completion unchanged.
            assert all(info.name != "b" for info in starts[1].operations.values())
            completed_parent = next(
                info
                for info in starts[1].operations.values()
                if info.name == "returned-child"
            )
            assert completed_parent.status.value == "SUCCEEDED"
        hook_errors = [
            r.getMessage()
            for r in caplog.records
            if r.levelno >= logging.ERROR and ("plugin" in r.name or "otel" in r.name)
        ]
        proof = dict(
            view=view.__name__,
            large_result=large_result,
            resume_before_completion=resume_before_completion,
            statuses=[x.status.value for x in ends],
            saved_step_calls=len(saved_calls),
            root_step_calls=len(root_calls),
            child_calls=len(child_calls),
            middle_input_updated_names=[
                info.name for info in starts[1].updated_operations.values()
            ]
            if resume_before_completion
            else None,
            middle_callback_spans=[
                dict(span_id=s.context.span_id, parent_id=s.parent.span_id)
                for s in middle_callbacks
            ],
            middle_parent_spans=[
                dict(span_id=s.context.span_id, parent_id=s.parent.span_id)
                for s in middle_parents
            ],
            middle_invocation_ids=[s.context.span_id for s in invocation_spans],
            final_callback_spans=len(spans("b", "SUCCEEDED")),
            hook_errors=hook_errors,
            history=[(e.event_id, e.event_type, e.name) for e in history],
        )
        if os.environ.get("LATE_PARENT_PROOF_DIR"):
            out = Path(os.environ["LATE_PARENT_PROOF_DIR"])
            out.mkdir(parents=True, exist_ok=True)
            (
                out
                / f"{view.__name__}-large{large_result}-resume{resume_before_completion}.json"
            ).write_text(json.dumps(proof, indent=2) + "\n")
        assert len(middle_callbacks) == 1
        assert len(spans("b", "SUCCEEDED")) == 1
        if large_result and view is InvocationOtelPlugin:
            assert len(middle_parents) == 2
            assert (
                middle_parents[-1].parent.span_id
                == invocation_spans[-1].context.span_id
            )
            assert (
                middle_callbacks[0].parent.span_id == middle_parents[-1].context.span_id
            )
            assert (
                middle_callbacks[0].parent.span_id != middle_parents[0].context.span_id
            )
        else:
            assert (
                middle_callbacks[0].parent.span_id == middle_parents[0].context.span_id
            )
        assert not hook_errors
    finally:
        release_root_step.set()
        provider.shutdown()
