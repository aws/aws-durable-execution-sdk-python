"""Verify the factory migration boundary against a real installed OTel 1 wheel."""

from importlib.metadata import version
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python import execution as core_execution
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)
import aws_durable_execution_sdk_python_otel as installed_otel
from opentelemetry import context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


@pytest.mark.parametrize(
    ("view", "name"),
    [
        (ExecutionOtelPlugin, "otel-execution"),
        (InvocationOtelPlugin, "otel-invocation"),
    ],
)
@pytest.mark.parametrize("registration", ["explicit", "environment"])
def test_installed_legacy_registration_fails_before_invocation(
    monkeypatch: pytest.MonkeyPatch, view, name: str, registration: str
) -> None:
    assert version("aws-durable-execution-sdk-python") == "3.0.0"
    assert version("aws-durable-execution-sdk-python-otel") == "1.0.0"
    assert "site-packages" in Path(core_execution.__file__).resolve().parts
    assert "site-packages" in Path(installed_otel.__file__).resolve().parts
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    before = context.get_current()

    def body(_event, _context):
        raise AssertionError("An incompatible registration reached the handler")

    try:
        if registration == "explicit":
            plugin = view(
                OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
            )
            with pytest.raises(PluginLoadError, match="must be a plugin factory"):
                durable_execution(body, plugins=[plugin])
        else:
            monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", name)
            with pytest.raises(PluginLoadError) as caught:
                durable_execution(body)
            # The released entry point imports the removed v1 provider contract.
            # A failure for another reason must not pass this migration check.
            assert "Failed to load durable instrumentation plugin factory" in str(
                caught.value
            )
            assert "DurableInstrumentationPluginProvider" in str(caught.value)
        assert context.get_current() == before
        assert not exporter.get_finished_spans()
    finally:
        provider.shutdown()
