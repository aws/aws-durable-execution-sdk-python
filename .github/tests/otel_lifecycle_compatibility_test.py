"""Exercise real installed version pairs through the public durable handler."""

from __future__ import annotations

import contextvars
from datetime import UTC, datetime
from importlib.metadata import version
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python import execution as core_execution
from aws_durable_execution_sdk_python.lambda_service import (
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from opentelemetry import baggage, context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from packaging.version import Version


class NoNetworkClient:
    def __getattr__(self, name: str):
        raise AssertionError(f"Unexpected service access: {name}")


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize("order", ["alone", "baggage-first", "baggage-last"])
@pytest.mark.parametrize("ambient_present", [False, True])
def test_installed_pair_preserves_context_and_documents_legacy_fallback(
    view, order, ambient_present
):
    legacy = os.environ.get("OTEL_COMPAT_LEGACY") == "1"
    assert Version(version("aws-durable-execution-sdk-python")) >= Version("2.1.0")
    assert version("aws-durable-execution-sdk-python-otel") == (
        "1.0.0" if legacy else "1.1.0"
    )
    assert "site-packages" in Path(core_execution.__file__).resolve().parts
    import aws_durable_execution_sdk_python_otel as installed_otel

    assert "site-packages" in Path(installed_otel.__file__).resolve().parts

    def run() -> None:
        provider = TracerProvider()
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("installed-lifecycle")
        phases = []
        worker_threads = []
        caller_thread = threading.get_ident()

        class BaggagePlugin(DurableInstrumentationPlugin):
            def on_invocation_start(self, info):
                self.token = context.attach(baggage.set_baggage("customer", "present"))
                phases.append("baggage-start")
                worker_threads.append(threading.get_ident())

            def on_invocation_end(self, info):
                context.detach(self.token)
                phases.append("baggage-end")
                worker_threads.append(threading.get_ident())

        plugin = view(
            OtelPluginConfig(
                tracer_provider=provider,
                enrich_logger=False,
                context_extractor=lambda _: None,
            )
        )
        plugins = {
            "alone": [plugin],
            "baggage-first": [BaggagePlugin(), plugin],
            "baggage-last": [plugin, BaggagePlugin()],
        }[order]
        body_parents = []

        @durable_execution(plugins=plugins, boto3_client=NoNetworkClient())
        def handler(event, durable_context):
            phases.append("body")
            worker_threads.append(threading.get_ident())
            assert baggage.get_baggage("incoming") == "keep"
            assert baggage.get_baggage("customer") == (
                None if order == "alone" else "present"
            )
            parent = trace.get_current_span().get_span_context()
            body_parents.append(parent)
            with tracer.start_as_current_span("customer-span"):
                pass
            return "ok"

        operation = Operation(
            operation_id="installed",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
            start_timestamp=datetime(2026, 10, 8, tzinfo=UTC),
            execution_details=ExecutionDetails(input_payload="{}"),
        )
        event = {
            "DurableExecutionArn": "test-arn/installed",
            "CheckpointToken": "token",
            "InitialExecutionState": {
                "Operations": [operation.to_json_dict()],
                "NextMarker": "",
            },
        }
        lambda_context = SimpleNamespace(
            aws_request_id="installed",
            client_context=None,
            identity=None,
            _epoch_deadline_time_in_ms=0,
            invoked_function_arn="test-arn",
            tenant_id=None,
        )
        ambient = tracer.start_span("host") if ambient_present else trace.INVALID_SPAN
        host = baggage.set_baggage(
            "incoming", "keep", trace.set_span_in_context(ambient)
        )
        token = context.attach(host)
        try:
            for _ in range(2):
                assert handler(event, lambda_context)["Status"] == "SUCCEEDED"
                assert context.get_current() is host
            assert caller_thread not in worker_threads
            assert phases == (
                ["body"] * 2
                if order == "alone"
                else ["baggage-start", "body", "baggage-end"] * 2
            )
            spans = exporter.get_finished_spans()
            if legacy and view is InvocationOtelPlugin:
                # The released plugin does not attach an Invocation fallback.
                assert body_parents == [ambient.get_span_context()] * 2
            else:
                name = "Invocation" if view is InvocationOtelPlugin else "Workflow"
                contexts = [span.context for span in spans if span.name == name]
                assert contexts
                assert all(
                    parent.is_valid and parent in contexts for parent in body_parents
                )
            users = [span for span in spans if span.name == "customer-span"]
            assert len(users) == 2
            assert [span.parent for span in users] == [
                parent if parent.is_valid else None for parent in body_parents
            ]
            assert plugin._context_tokens == {}
        finally:
            context.detach(token)
            ambient.end()
            provider.shutdown()

    contextvars.Context().run(run)


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
def test_installed_pair_preserves_nested_callback_terminal_on_resume(view):
    """A core-only upgrade must retain the released plugin's callback end."""
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestRunner,
    )
    import aws_durable_execution_sdk_python_testing as installed_testing

    import aws_durable_execution_sdk_python_otel as installed_otel

    assert "site-packages" in Path(installed_testing.__file__).resolve().parts
    assert "site-packages" in Path(core_execution.__file__).resolve().parts
    assert "site-packages" in Path(installed_otel.__file__).resolve().parts
    assert version("aws-durable-execution-sdk-python-otel") == (
        "1.0.0" if os.environ.get("OTEL_COMPAT_LEGACY") == "1" else "1.1.0"
    )
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )
    submitted = threading.Event()
    suspended = threading.Event()
    callback_ids = []
    updated_ids = []
    invocation_statuses = []
    side_effects = []

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            updated_ids.append(list(info.updated_operations))

        def on_invocation_end(self, info):
            invocation_statuses.append(info.status.value)
            if info.status.value == "PENDING":
                suspended.set()

    def submit(callback_id, _context):
        side_effects.append("submit")
        callback_ids.append(callback_id)
        submitted.set()

    def saved(_context):
        side_effects.append("saved")
        return "saved"

    def body(_event, context):
        context.step(saved, name="saved-before")
        return context.wait_for_callback(submit, name="await-external")

    handler = durable_execution(body, plugins=[plugin, Observer()])
    try:
        with DurableFunctionTestRunner(
            handler=handler, skip_time=False, poll_interval=0.01, execution_timeout=20
        ) as runner:
            arn = runner.run_async(input="{}")
            assert submitted.wait(8) and suspended.wait(8)
            runner.send_callback_success(callback_ids[0], result=b"done")
            result = runner.wait_for_result(arn, timeout=10)
        assert result.status.value == "SUCCEEDED"
        assert result.result == '"done"'
        assert invocation_statuses == ["PENDING", "SUCCEEDED"]
        assert side_effects == ["saved", "submit"]
        assert updated_ids[0] == [] and len(updated_ids[1]) == 1
        spans = exporter.get_finished_spans()
        callback_spans = [
            span for span in spans if span.name == "await-external create callback id"
        ]
        terminal = [
            span
            for span in callback_spans
            if span.attributes["durable.operation.status"] == "SUCCEEDED"
        ]
        assert len(terminal) == 1
        assert terminal[0].attributes["durable.operation.id"] == updated_ids[1][0]
        parent_spans = [span for span in spans if span.name == "await-external"]
        assert terminal[0].parent in [span.context for span in parent_spans]
        assert all(
            span.context.trace_id == terminal[0].context.trace_id
            for span in callback_spans
        )
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
def test_installed_pair_waits_for_completed_replaychildren_parent(view):
    """A real large child result retains and replays its callback's parent."""
    import json
    import time

    from aws_durable_execution_sdk_python.config import CallbackConfig
    from aws_durable_execution_sdk_python.serdes import JsonSerDes
    from aws_durable_execution_sdk_python_testing.runner import (
        DurableFunctionTestRunner,
    )
    import aws_durable_execution_sdk_python_otel as installed_otel
    import aws_durable_execution_sdk_python_testing as installed_testing

    for module in (core_execution, installed_otel, installed_testing):
        assert "site-packages" in Path(module.__file__).resolve().parts
    assert version("aws-durable-execution-sdk-python-otel") == (
        "1.0.0" if os.environ.get("OTEL_COMPAT_LEGACY") == "1" else "1.1.0"
    )
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )
    starts, ends, input_parents = [], [], []
    child_calls, saved_calls, marker_calls = [], [], []
    exports_at_marker = []

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            starts.append(info)

        def on_invocation_end(self, info):
            ends.append(info.status.value)

    def callback_spans():
        return [
            span
            for span in exporter.get_finished_spans()
            if span.name == "b"
            and span.attributes.get("durable.operation.status") == "SUCCEEDED"
        ]

    def saved(_context):
        saved_calls.append(1)
        return "saved"

    def child(context):
        child_calls.append(1)
        callback = context.create_callback(
            name="b", config=CallbackConfig(serdes=JsonSerDes())
        )
        assert context.step(saved, name="saved-child") == "saved"
        return {"id": callback.callback_id, "large": "x" * (300 * 1024)}

    def marker(_context):
        marker_calls.append(1)
        exports_at_marker.extend(callback_spans())
        return "marked"

    def body(_event, context):
        a = context.create_callback(
            name="a", config=CallbackConfig(serdes=JsonSerDes())
        )
        b_id = context.run_in_child_context(child, name="large-parent")["id"]
        result = a.result()
        assert context.step(marker, name="after-a") == "marked"
        return {"a": result, "b_id": b_id}

    wrapped = durable_execution(body, plugins=[Observer(), plugin])

    def entrypoint(event, context):
        input_parents.append(
            [
                (op.operation_id, op.status.value, op.context_details.replay_children)
                for op in event.initial_execution_state.operations
                if op.name == "large-parent" and op.context_details is not None
            ]
        )
        return wrapped(event, context)

    def completed(runner, arn, count):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            events = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
            if (
                len(ends) >= count
                and sum(e.event_type == "InvocationCompleted" for e in events) >= count
            ):
                return
            time.sleep(0.01)
        raise AssertionError("Invocation did not finish")

    try:
        with DurableFunctionTestRunner(
            handler=entrypoint,
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=25,
        ) as runner:
            arn = runner.run_async(input="{}")
            a_id = runner.wait_for_callback(arn, name="a", timeout=5)
            b_id = runner.wait_for_callback(arn, name="b", timeout=5)
            completed(runner, arn, 1)
            runner.send_callback_success(b_id, result=b'"B"')
            completed(runner, arn, 2)
            middle_exports = callback_spans()
            runner.send_callback_success(a_id, result=b'"A"')
            result = runner.wait_for_result(arn, timeout=10)
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == {"a": "A", "b_id": b_id}
        assert ends == ["PENDING", "PENDING", "SUCCEEDED"]
        assert child_calls == [1, 1, 1] and saved_calls == [1] and marker_calls == [1]
        b_info = next(
            info for info in starts[1].updated_operations.values() if info.name == "b"
        )
        assert input_parents[1] == [(b_info.parent_id, "SUCCEEDED", True)]
        terminal = callback_spans()
        assert len(middle_exports) == len(terminal) == 1
        assert exports_at_marker == middle_exports == terminal
        assert terminal[0].attributes["durable.operation.id"] == b_info.operation_id
        parents = [
            span
            for span in exporter.get_finished_spans()
            if span.attributes.get("durable.operation.id") == b_info.parent_id
            and span.attributes.get("durable.operation.status") == "SUCCEEDED"
        ]
        assert terminal[0].parent in [span.context for span in parents]
        assert all(
            span.context.trace_id == terminal[0].context.trace_id for span in parents
        )
        if view is InvocationOtelPlugin:
            invocations = [
                span
                for span in exporter.get_finished_spans()
                if span.name == "Invocation"
            ]
            assert len(parents) == len(invocations) == 3
            assert terminal[0].parent == parents[1].context
            assert parents[1].parent == invocations[1].context
        else:
            assert len(parents) == 1
    finally:
        provider.shutdown()
