"""A caught durable failure remains an error span without invented diagnostics."""

import json
import threading

import pytest
from aws_durable_execution_sdk_python import durable_execution, durable_step
from aws_durable_execution_sdk_python.exceptions import CallbackError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("completion", ["initial", "resume"])
@pytest.mark.parametrize("outcome", ["empty-failure", "rich-failure", "success"])
def test_caught_callback_status_survives_two_later_replays(
    view, completion, outcome, monkeypatch
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
    created = threading.Event()
    completed = threading.Event()
    suspended = threading.Event()
    finished = threading.Condition()
    completed_invocations = []
    invocations = []
    target_ends = []
    side_effects = []
    first_exports = []
    caught = []
    callback_ids = []

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            invocations.append(info)

        def on_operation_end(self, info):
            if info.name == "target":
                target_ends.append((len(invocations), info))

        def on_invocation_end(self, info):
            if info.status is InvocationStatus.PENDING:
                suspended.set()
            with finished:
                completed_invocations.append(info)
                finished.notify_all()

    @durable_step
    def saved(_context, name, value):
        side_effects.append(name)
        if name == "observed":
            first_exports.extend(
                span for span in exporter.get_finished_spans() if span.name == "target"
            )
        return value

    def body(_event, context):
        callback = context.create_callback(name="target")
        context.step(saved("before", "saved"), name="before")
        if completion == "initial" and len(invocations) == 1:
            callback_ids.append(callback.callback_id)
            created.set()
            assert completed.wait(5), "Driver did not complete the callback"
        # A real checkpoint refreshes an external completion during the active
        # invocation; on resume this completed step is consumed from history.
        context.step(saved("refresh", "saved"), name="refresh")
        try:
            callback.result()
            result = "success"
        except CallbackError as error:
            result = type(error).__name__
            caught.append(result)
        result = context.step(saved("observed", result), name="observed")
        for name in ("barrier-1", "barrier-2"):
            context.create_callback(name=name).result()
        return result

    handler = durable_execution(body, plugins=[plugin, Observer()])
    error = (
        ErrorObject(
            message="supplied detail", type="External", data=None, stack_trace=None
        )
        if outcome == "rich-failure"
        else None
    )
    try:
        with DurableFunctionTestRunner(
            handler=handler, skip_time=False, poll_interval=0.01, execution_timeout=25
        ) as runner:
            arn = runner.run_async(input="{}")
            if completion == "initial":
                assert created.wait(5)
                callback_id = callback_ids[0]
            else:
                callback_id = runner.wait_for_callback(arn, name="target", timeout=5)
                assert suspended.wait(5)
            if outcome == "success":
                runner.send_callback_success(callback_id)
            else:
                runner.send_callback_failure(callback_id, error=error)
            completed.set()
            first_completion = 1 if completion == "initial" else 2
            for index, name in enumerate(("barrier-1", "barrier-2")):
                callback_id = runner.wait_for_callback(arn, name=name, timeout=5)
                with finished:
                    assert finished.wait_for(
                        lambda: len(completed_invocations) >= first_completion + index,
                        timeout=5,
                    )
                runner.send_callback_success(callback_id)
            result = runner.wait_for_result(arn, timeout=10)
        assert result.status.value == "SUCCEEDED"
        expected_result = "success" if outcome == "success" else "CallbackExternalError"
        assert json.loads(result.result) == expected_result
        assert side_effects == ["before", "refresh", "observed"]
        assert len(invocations) == (3 if completion == "initial" else 4)
        assert caught == ([] if outcome == "success" else [expected_result] * 3)
        assert len(target_ends) == 1
        invocation_number, info = target_ends[0]
        assert invocation_number == (1 if completion == "initial" else 2)
        assert info.status.value == ("SUCCEEDED" if outcome == "success" else "FAILED")
        assert info.error == error
        spans = [
            span for span in exporter.get_finished_spans() if span.name == "target"
        ]
        expected_statuses = [info.status.value]
        if view is InvocationOtelPlugin and completion == "resume":
            expected_statuses.insert(0, "STARTED")
        assert [
            span.attributes["durable.operation.status"] for span in spans
        ] == expected_statuses
        assert first_exports == spans
        terminal = spans[-1]
        assert terminal.status.status_code is (
            StatusCode.OK if outcome == "success" else StatusCode.ERROR
        )
        assert len(terminal.events) == (1 if error is not None else 0)
        if error is not None:
            assert terminal.events[0].attributes["exception.message"] == error.message
        workflow = next(
            s for s in exporter.get_finished_spans() if s.name == "Workflow"
        )
        assert workflow.status.status_code is StatusCode.OK
    finally:
        completed.set()
        provider.shutdown()
