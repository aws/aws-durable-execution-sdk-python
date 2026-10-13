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
