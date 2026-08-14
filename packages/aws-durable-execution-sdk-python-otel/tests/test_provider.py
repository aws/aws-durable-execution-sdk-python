"""Tests for the shared TracerProvider factory (create_tracer_provider)."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
    ProviderSource,
)
from aws_durable_execution_sdk_python_otel.provider import create_tracer_provider


def test_explicit_provider_is_used():
    provider = TracerProvider()
    result = create_tracer_provider(
        OtelPluginConfig(
            provider_source=ProviderSource.EXPLICIT, tracer_provider=provider
        )
    )
    assert result.tracer_provider is provider
    assert result.source is ProviderSource.EXPLICIT


def test_global_source_returns_global_provider():
    result = create_tracer_provider(
        OtelPluginConfig(provider_source=ProviderSource.GLOBAL)
    )
    assert result.tracer_provider is trace.get_tracer_provider()
    assert result.source is ProviderSource.GLOBAL


def test_unset_config_defaults_to_global_provider():
    # The default: no provider_source given -> use the global provider.
    result = create_tracer_provider(OtelPluginConfig())
    assert result.source is ProviderSource.GLOBAL
    assert result.tracer_provider is trace.get_tracer_provider()


# ---------------------------------------------------------------------------
# Config validation (each source has the fields it needs)
# ---------------------------------------------------------------------------
def test_explicit_source_requires_tracer_provider():
    with pytest.raises(ValueError, match="requires a tracer_provider"):
        OtelPluginConfig(provider_source=ProviderSource.EXPLICIT)


def test_tracer_provider_without_explicit_source_raises():
    # Default source is GLOBAL; a stray tracer_provider would be ignored.
    with pytest.raises(ValueError, match="only valid with provider_source=EXPLICIT"):
        OtelPluginConfig(tracer_provider=TracerProvider())


def test_global_source_rejects_tracer_provider():
    with pytest.raises(ValueError, match="only valid with provider_source=EXPLICIT"):
        OtelPluginConfig(
            provider_source=ProviderSource.GLOBAL, tracer_provider=TracerProvider()
        )
