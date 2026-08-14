"""Shared TracerProvider factory for the durable-execution OTel plugins.

Builds the tracer provider selected by the config's
:class:`~aws_durable_execution_sdk_python_otel.otel_plugin_config.ProviderSource`:

1. ``EXPLICIT``  - the config's ``tracer_provider`` is used as-is.
2. ``GLOBAL``    - the globally configured provider is used (e.g. ADOT layer).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from opentelemetry import trace

from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
    ProviderSource,
)


if TYPE_CHECKING:
    from opentelemetry.trace import TracerProvider


@dataclass
class ProviderResult:
    """Result of provider resolution: the provider and how it was chosen."""

    tracer_provider: TracerProvider
    source: ProviderSource


def create_tracer_provider(config: OtelPluginConfig) -> ProviderResult:
    """Resolve a TracerProvider from the config's :attr:`provider_source`.

    A straight switch on ``config.provider_source``; the chosen tier is reported
    back as :class:`ProviderSource` so callers make the instrumentation/flush
    decision off a single value:

    1. ``EXPLICIT``  -> ``config.tracer_provider`` used as-is
    2. ``GLOBAL``    -> the globally configured provider

    Args:
        config: Shared plugin configuration.

    Returns:
        A :class:`ProviderResult`.
    """
    source = config.provider_source

    if source is ProviderSource.EXPLICIT:
        # Explicit provider: use as-is, never wrap/modify. OtelPluginConfig
        # validation guarantees tracer_provider is set for EXPLICIT.
        assert config.tracer_provider is not None
        provider: TracerProvider = config.tracer_provider
    elif source is ProviderSource.GLOBAL:
        provider = trace.get_tracer_provider()
    else:  # pragma: no cover - exhaustive over ProviderSource
        raise ValueError(f"unknown provider_source: {source!r}")

    return ProviderResult(provider, source)
