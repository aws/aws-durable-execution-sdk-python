"""Tests for application ownership of OpenTelemetry instrumentation."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


PluginType = type[ExecutionOtelPlugin] | type[InvocationOtelPlugin]


@pytest.mark.parametrize(
    "plugin_type",
    [ExecutionOtelPlugin, InvocationOtelPlugin],
)
def test_plugin_does_not_register_botocore_instrumentation(
    plugin_type: PluginType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = TracerProvider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    instrument_calls: list[dict[str, object]] = []

    class BotocoreInstrumentor:
        is_instrumented_by_opentelemetry = False

        def instrument(self, **kwargs: object) -> None:
            instrument_calls.append(kwargs)

    botocore_module = ModuleType("opentelemetry.instrumentation.botocore")
    setattr(botocore_module, "BotocoreInstrumentor", BotocoreInstrumentor)
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.instrumentation.botocore",
        botocore_module,
    )

    plugin_type(OtelPluginConfig(enrich_logger=False))

    assert instrument_calls == []
