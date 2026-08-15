"""Shared TracerProvider factory for the durable-execution OTel plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from opentelemetry import trace

from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


if TYPE_CHECKING:
    from opentelemetry.trace import TracerProvider


@dataclass(frozen=True)
class ProviderResult:
    """Result of provider resolution: the provider and how it was chosen."""

    tracer_provider: TracerProvider
    uses_global_provider: bool


def create_tracer_provider(config: OtelPluginConfig) -> ProviderResult:
    """Resolve the configured provider or the global provider.

    Whether ``tracer_provider`` was supplied is retained separately from the
    resolved object. An explicit provider may also be installed globally, but it
    still has application-owned instrumentation and initialization behavior.

    Args:
        config: Shared plugin configuration.

    Returns:
        A :class:`ProviderResult`.
    """
    if config.tracer_provider is not None:
        provider: TracerProvider = config.tracer_provider
        return ProviderResult(provider, uses_global_provider=False)

    return ProviderResult(
        trace.get_tracer_provider(),
        uses_global_provider=True,
    )
