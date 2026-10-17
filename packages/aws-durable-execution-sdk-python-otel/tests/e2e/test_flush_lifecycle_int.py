"""Only provider I/O may outlive the existing handler-worker End scope."""

import contextvars
import json
import threading
import time
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python.lambda_service import (
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
    _flush,
)


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_public_handler_finishes_while_one_provider_flush_is_blocked(view, monkeypatch):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 0.05)
    ambient = contextvars.ContextVar("flush-owner", default="caller")
    release = threading.Event()
    exporting = threading.Event()
    owners = []
    flush_calls = []
    exporter_contexts = []

    class Exporter(InMemorySpanExporter):
        def export(self, spans):
            exporting.set()
            exporter_contexts.append(ambient.get())
            assert release.wait(10)
            return super().export(spans)

    class Provider(TracerProvider):
        def force_flush(self, timeout_millis=30000):
            flush_calls.append(threading.get_ident())
            return super().force_flush(timeout_millis)

    class Owner(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            self.token = ambient.set("handler")
            self.thread = threading.get_ident()

        def on_invocation_end(self, info):
            assert ambient.get() == "handler"
            assert threading.get_ident() == self.thread
            # A successful token reset proves ownership of the actual Context,
            # rather than only comparing an OS thread ID that can be reused.
            ambient.reset(self.token)
            owners.append(self.thread)

    exporter = Exporter()
    provider = Provider()
    provider.add_span_processor(
        BatchSpanProcessor(exporter, schedule_delay_millis=60000)
    )
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )

    def body(_event, _context):
        assert ambient.get() == "handler"
        return "done"

    handler = durable_execution(body, plugins=[Owner(), plugin])

    def invoke():
        started = time.monotonic()
        with DurableFunctionTestRunner(handler=handler, poll_interval=0.01) as runner:
            result = runner.run(input="{}")
        assert time.monotonic() - started < 2
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == "done"
        assert ambient.get() == "caller"

    try:
        invoke()
        assert exporting.wait(1)
        invoke()
        assert len(owners) == 2
        assert len(flush_calls) == 1
        assert all(worker != flush_calls[0] for worker in owners)
        assert exporter_contexts == ["caller"]
    finally:
        release.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with _flush._lock:
                idle = _flush._active is None
            if idle:
                break
            time.sleep(0.001)
        assert idle
    try:
        # Actual completion restores admission; no accumulated late error or
        # stale invocation Context is delivered to this next request.
        monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
        invoke()
        assert len(flush_calls) == 2
        assert len(owners) == 3
        assert all(value == "caller" for value in exporter_contexts)
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("fatal", [False, True])
def test_public_fast_flush_failure_preserves_owner_cleanup_and_reuse(
    view, fatal, monkeypatch
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    binding = contextvars.ContextVar("flush-failure-owner", default="caller")
    ended = []
    error = SystemExit("provider fatal") if fatal else RuntimeError("provider ordinary")

    class Owner(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            self.token = binding.set("handler")
            self.thread = threading.get_ident()

        def on_invocation_end(self, info):
            assert threading.get_ident() == self.thread
            binding.reset(self.token)
            ended.append(self.thread)

    class Provider(TracerProvider):
        failure: BaseException | None = error

        def force_flush(self, timeout_millis=30000):
            if self.failure is not None:
                raise self.failure
            return super().force_flush(timeout_millis)

    provider = Provider()
    provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )

    def body(_event, _context):
        assert binding.get() == "handler"
        return "done"

    handler = durable_execution(body, plugins=[Owner(), plugin])
    operation = Operation(
        operation_id="flush-execution",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=datetime.now(UTC),
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    event = {
        "DurableExecutionArn": "test-arn/flush-execution",
        "CheckpointToken": "token",
        "InitialExecutionState": {
            "Operations": [operation.to_json_dict()],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }
    context = Mock()
    context.aws_request_id = "request"
    context.invoked_function_arn = "test-arn"
    context.client_context = None
    context.identity = None
    context.tenant_id = None
    context._epoch_deadline_time_in_ms = 0
    try:
        with patch("aws_durable_execution_sdk_python.execution.LambdaClient"):
            if fatal:
                with pytest.raises(SystemExit) as caught:
                    handler(event, context)
                assert caught.value is error
            else:
                assert handler(event, context) == {
                    "Status": "SUCCEEDED",
                    "Result": '"done"',
                }
            assert len(ended) == 1
            assert binding.get() == "caller"
            provider.failure = None
            assert handler(event, context) == {
                "Status": "SUCCEEDED",
                "Result": '"done"',
            }
            assert len(ended) == 2
            assert binding.get() == "caller"
    finally:
        provider.shutdown()
