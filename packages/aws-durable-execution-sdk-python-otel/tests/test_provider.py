"""Tests for the shared TracerProvider factory (create_tracer_provider)."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, TraceIdRatioBased

from aws_durable_execution_sdk_python_otel.execution_plugin_config import (
    ExecutionOtelPluginConfig,
    ExporterConfig,
)
from aws_durable_execution_sdk_python_otel.provider import (
    SAMPLING_RATIO_ENV,
    _build_resource,
    _build_sampler,
    _resolve_endpoint,
    create_tracer_provider,
)


def test_explicit_provider_is_used_and_not_owned():
    provider = TracerProvider()
    result = create_tracer_provider(
        ExecutionOtelPluginConfig(tracer_provider=provider)
    )
    assert result.tracer_provider is provider
    assert result.owns_provider is False


def test_use_default_provider_returns_global_and_not_owned():
    result = create_tracer_provider(
        ExecutionOtelPluginConfig(use_default_tracer_provider=True)
    )
    assert result.tracer_provider is trace.get_tracer_provider()
    assert result.owns_provider is False


def test_default_use_global_flag_applies_when_unset():
    # InvocationOtelPlugin passes default_use_global=True so an unset config
    # resolves to the global provider (JS Req 26.3).
    result = create_tracer_provider(
        ExecutionOtelPluginConfig(), default_use_global=True
    )
    assert result.tracer_provider is trace.get_tracer_provider()
    assert result.owns_provider is False


def test_auto_configured_provider_is_owned_sdk_provider():
    result = create_tracer_provider(ExecutionOtelPluginConfig())
    assert result.owns_provider is True
    assert isinstance(result.tracer_provider, TracerProvider)


def test_explicit_provider_takes_precedence_over_use_default():
    provider = TracerProvider()
    result = create_tracer_provider(
        ExecutionOtelPluginConfig(
            tracer_provider=provider, use_default_tracer_provider=True
        )
    )
    assert result.tracer_provider is provider
    assert result.owns_provider is False


# ---------------------------------------------------------------------------
# Sampler resolution
# ---------------------------------------------------------------------------
def test_sampler_defaults_to_always_on(monkeypatch):
    monkeypatch.delenv(SAMPLING_RATIO_ENV, raising=False)
    assert _build_sampler() is ALWAYS_ON


def test_sampler_uses_ratio_when_valid(monkeypatch):
    monkeypatch.setenv(SAMPLING_RATIO_ENV, "0.25")
    assert isinstance(_build_sampler(), TraceIdRatioBased)


@pytest.mark.parametrize("bad", ["not-a-number", "1.5", "-0.1"])
def test_sampler_falls_back_to_always_on_for_invalid_ratio(monkeypatch, bad):
    monkeypatch.setenv(SAMPLING_RATIO_ENV, bad)
    assert _build_sampler() is ALWAYS_ON


# ---------------------------------------------------------------------------
# Resource + endpoint resolution
# ---------------------------------------------------------------------------
def test_resource_is_none_without_function_name(monkeypatch):
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    assert _build_resource() is None


def test_resource_populates_lambda_attributes(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "my-fn")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "7")
    resource = _build_resource()
    assert resource is not None
    attrs = resource.attributes
    assert attrs["service.name"] == "my-fn"
    assert attrs["faas.name"] == "my-fn"
    assert attrs["cloud.provider"] == "aws"
    assert attrs["cloud.platform"] == "aws_lambda"
    assert attrs["cloud.region"] == "us-west-2"
    assert attrs["faas.version"] == "7"


def test_resolve_endpoint_appends_signal_path(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    config = ExecutionOtelPluginConfig(
        exporter_config=ExporterConfig(endpoint="http://collector:4318")
    )
    assert _resolve_endpoint(config) == "http://collector:4318/v1/traces"


def test_resolve_endpoint_does_not_double_append(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    config = ExecutionOtelPluginConfig(
        exporter_config=ExporterConfig(endpoint="http://collector:4318/v1/traces")
    )
    assert _resolve_endpoint(config) == "http://collector:4318/v1/traces"
