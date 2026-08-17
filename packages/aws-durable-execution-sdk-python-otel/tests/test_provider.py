"""Tests for the shared TracerProvider factory (create_tracer_provider)."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from aws_durable_execution_sdk_python_otel.provider import create_tracer_provider


def test_explicit_provider_is_used():
    provider = TracerProvider()
    result = create_tracer_provider(OtelPluginConfig(tracer_provider=provider))
    assert result.tracer_provider is provider
    assert result.uses_global_provider is False


def test_unset_provider_uses_global_provider():
    result = create_tracer_provider(OtelPluginConfig())
    assert result.tracer_provider is trace.get_tracer_provider()
    assert result.uses_global_provider is True


def test_supplied_global_provider_remains_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = TracerProvider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)

    result = create_tracer_provider(OtelPluginConfig(tracer_provider=provider))

    assert result.tracer_provider is provider
    assert result.uses_global_provider is False
