"""The real case11 handler starts one child, then replays its saved result."""

import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
from threading import Event
import time

import pytest
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC = Path(__file__).resolve().parents[1] / "src"


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_case11_uses_one_real_child_and_replays_after_barrier(
    view, monkeypatch, caplog
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    monkeypatch.setenv("OTEL_INVOKE_TARGET_FUNCTION_NAME", "case11-child")
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    common_spec = importlib.util.spec_from_file_location("common", SRC / "common.py")
    common = importlib.util.module_from_spec(common_spec)
    monkeypatch.setitem(sys.modules, "common", common)
    common_spec.loader.exec_module(common)
    # Configure the real bundled plugin/exporter; no propagation hook, model,
    # header, operation status or checkpoint response is injected.
    monkeypatch.setattr(
        common,
        "otel_plugin",
        lambda: view(OtelPluginConfig(tracer_provider=provider, enrich_logger=False)),
    )
    spec = importlib.util.spec_from_file_location(
        "otel_11_chained_invoke", SRC / "otel_11_chained_invoke.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    target_entered, admit_target = Event(), Event()
    target_calls = []
    payload = {"scenario": "chained-invoke", "value": {"preserved": [1, "same-result"]}}

    def target_entrypoint(event, context):
        # A harness admission gate makes the first caller suspension observable.
        # The unchanged production echo target still executes exactly once.
        target_calls.append(context.aws_request_id)
        target_entered.set()
        assert admit_target.wait(10)
        return module.target_handler(event, context)

    def wait_for_invocations(runner, arn, count):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            history = runner.get_execution_history(arn, include_execution_data=True)
            if (
                sum(e.event_type == "InvocationCompleted" for e in history.events)
                >= count
            ):
                return history
            time.sleep(0.01)
        raise AssertionError("Caller did not reach the expected suspension")

    try:
        with DurableFunctionTestRunner(
            handler=module.handler,
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=30,
        ) as runner:
            runner.register_durable_function("case11-child", target_entrypoint)
            arn = runner.run_async(input=json.dumps(payload))
            assert target_entered.wait(10)
            first = wait_for_invocations(runner, arn, 1)
            assert not any(
                e.event_type == "ChainedInvokeSucceeded" for e in first.events
            )
            admit_target.set()
            callback_id = runner.wait_for_callback(
                arn, name="otel-invoke-replay-barrier create callback id", timeout=10
            )
            middle = wait_for_invocations(runner, arn, 2)
            assert any(e.event_type == "ChainedInvokeSucceeded" for e in middle.events)
            assert not any(e.event_type == "CallbackSucceeded" for e in middle.events)
            runner.send_callback_success(callback_id, result=b'"resume"')
            result = runner.wait_for_result(arn, timeout=10)
            history = runner.get_execution_history(arn, include_execution_data=True)
            started = [
                e for e in history.events if e.event_type == "ChainedInvokeStarted"
            ]
            succeeded = [
                e for e in history.events if e.event_type == "ChainedInvokeSucceeded"
            ]
            assert len(started) == len(succeeded) == 1
            assert started[0].operation_id == succeeded[0].operation_id
            assert started[0].name == "otel-invoke"
            child_arn = started[0].chained_invoke_started_details.durable_execution_arn
            assert child_arn and child_arn != arn
            child_history = runner.get_execution_history(
                child_arn, include_execution_data=True
            )

        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == payload
        assert len(target_calls) == 1
        assert sum(e.event_type == "InvocationCompleted" for e in history.events) == 3
        assert (
            sum(e.event_type == "InvocationCompleted" for e in child_history.events)
            == 1
        )
        assert (
            sum(e.event_type == "ExecutionStarted" for e in child_history.events) == 1
        )
        assert (
            sum(e.event_type == "ExecutionSucceeded" for e in child_history.events) == 1
        )
        assert (
            sum(
                e.event_type == "StepSucceeded"
                and e.name == "otel-invoke-replay-barrier submitter"
                for e in history.events
            )
            == 1
        )
        spans = exporter.get_finished_spans()
        caller = sorted(
            [
                s
                for s in spans
                if s.name == "Invocation"
                and s.attributes.get("durable.execution.arn") == arn
            ],
            key=lambda s: s.start_time,
        )
        child = [
            s
            for s in spans
            if s.name == "Invocation"
            and s.attributes.get("durable.execution.arn") == child_arn
        ]
        assert [s.attributes["durable.invocation.status"] for s in caller] == [
            "PENDING",
            "PENDING",
            "SUCCEEDED",
        ]
        assert [s.attributes["durable.invocation.first"] for s in caller] == [
            True,
            False,
            False,
        ]
        assert (
            len(child) == 1
            and child[0].attributes["durable.invocation.status"] == "SUCCEEDED"
        )
        assert not [
            r
            for r in caplog.records
            if r.levelno >= logging.ERROR and ("plugin" in r.name or "otel" in r.name)
        ]
        output_dir = os.environ.get("CHAIN_CASE11_PROOF_DIR")
        if output_dir:
            destination = Path(output_dir)
            destination.mkdir(parents=True, exist_ok=True)
            proof = {
                "view": view.__name__,
                "result_unchanged": True,
                "target_calls": len(target_calls),
                "caller_statuses": [
                    s.attributes["durable.invocation.status"] for s in caller
                ],
                "caller_first_flags": [
                    s.attributes["durable.invocation.first"] for s in caller
                ],
                "caller_events": [
                    (e.event_id, e.event_type, e.operation_id, e.name)
                    for e in history.events
                ],
                "child_events": [
                    (e.event_id, e.event_type, e.operation_id, e.name)
                    for e in child_history.events
                ],
                "invoke_id": started[0].operation_id,
                "child_arn": child_arn,
                "caller_arn": arn,
                "local_scope": "public first START, actual registered echo child, completed-invoke replay; no real-service carrier/ancestry claim",
            }
            (destination / f"{view.__name__}.json").write_text(
                json.dumps(proof, indent=2) + "\n"
            )
    finally:
        admit_target.set()
        provider.shutdown()
