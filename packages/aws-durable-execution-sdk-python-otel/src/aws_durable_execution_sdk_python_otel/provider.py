"""Shared TracerProvider factory for the durable-execution OTel plugins.

Implements the 3-tier provider resolution used by both plugins, reported as a
:class:`ProviderSource`:

1. An explicit ``tracer_provider`` in config is used as-is (``EXPLICIT``).
2. Otherwise, when ``use_default_tracer_provider`` is True, the globally
   configured provider is used (``GLOBAL``).
3. Otherwise a fully auto-configured SDK provider is created with an OTLP
   exporter, batch processor, sampler and Lambda resource attributes
   (``AUTO_OTLP``); this is the only tier the plugin owns/flushes.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from opentelemetry import propagate, trace
from opentelemetry.propagators.composite import CompositePropagator

from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    DEFAULT_OTLP_ENDPOINT,
    OtelPluginConfig,
)


if TYPE_CHECKING:
    from opentelemetry.sdk.trace import IdGenerator
    from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
    from opentelemetry.trace import TracerProvider


logger = logging.getLogger(__name__)

SAMPLING_RATIO_ENV = "OTEL_DURABLE_SAMPLING_RATIO"
OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"


class ProviderSource(Enum):
    """Which of the three resolution tiers produced the tracer provider."""

    EXPLICIT = "explicit"  # caller supplied config.tracer_provider
    GLOBAL = "global"  # use_default_tracer_provider -> trace.get_tracer_provider()
    AUTO_OTLP = "auto_otlp"  # default: plugin builds and owns an OTLP provider


@dataclass
class ProviderResult:
    """Result of provider resolution: the provider and how it was chosen."""

    tracer_provider: TracerProvider
    source: ProviderSource

    @property
    def owns_provider(self) -> bool:
        """True only when the plugin created (and therefore manages) the provider."""
        return self.source is ProviderSource.AUTO_OTLP


def _resolve_endpoint(config: OtelPluginConfig) -> str:
    """Resolve the OTLP traces endpoint (config -> env -> default).

    The Python OTLP/HTTP exporter uses an explicitly-passed ``endpoint`` verbatim
    (unlike the JS exporter, which appends ``/v1/traces``), so we append the
    signal path here when the caller supplied only a base URL.
    """
    base = (
        config.exporter_config.endpoint
        or os.environ.get(OTLP_ENDPOINT_ENV)
        or DEFAULT_OTLP_ENDPOINT
    )
    base = base.rstrip("/")
    if not base.endswith("/v1/traces"):
        base = f"{base}/v1/traces"
    return base


def _build_sampler():
    """Build the sampler from ``OTEL_DURABLE_SAMPLING_RATIO``.

    A valid ratio in ``[0, 1]`` yields a ``TraceIdRatioBased`` sampler; anything
    else falls back to ``ALWAYS_ON``. If constructing the ratio sampler raises,
    the error propagates and fails provider setup.
    """
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON, TraceIdRatioBased

    raw = os.environ.get(SAMPLING_RATIO_ENV)
    if raw is None:
        return ALWAYS_ON
    try:
        ratio = float(raw)
    except (TypeError, ValueError):
        return ALWAYS_ON
    if not (0.0 <= ratio <= 1.0):
        return ALWAYS_ON
    return TraceIdRatioBased(ratio)


def _build_resource():
    """Build a Lambda resource from AWS_* env vars."""
    from opentelemetry.sdk.resources import Resource

    function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    if not function_name:
        return None
    attributes: dict[str, str] = {
        "service.name": function_name,
        "faas.name": function_name,
        "cloud.provider": "aws",
        "cloud.platform": "aws_lambda",
    }
    region = os.environ.get("AWS_REGION")
    if region:
        attributes["cloud.region"] = region
    version = os.environ.get("AWS_LAMBDA_FUNCTION_VERSION")
    if version:
        attributes["faas.version"] = version
    return Resource.create(attributes)


def _default_propagators(config: OtelPluginConfig):
    """Return configured propagators or the X-Ray + W3C default."""
    if config.propagators is not None:
        return list(config.propagators)
    from opentelemetry.propagators.aws import AwsXRayPropagator
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    return [AwsXRayPropagator(), TraceContextTextMapPropagator()]


def _create_auto_provider(
    config: OtelPluginConfig,
    id_generator: IdGenerator | None,
) -> SdkTracerProvider:
    """Create a fully self-configured SDK TracerProvider with OTLP export."""
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    exporter = OTLPSpanExporter(
        endpoint=_resolve_endpoint(config),
        headers=config.exporter_config.headers or None,
    )
    provider_kwargs: dict = {"sampler": _build_sampler()}
    resource = _build_resource()
    if resource is not None:
        provider_kwargs["resource"] = resource
    if id_generator is not None:
        provider_kwargs["id_generator"] = id_generator

    provider = SdkTracerProvider(**provider_kwargs)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    composite = CompositePropagator(_default_propagators(config))
    propagate.set_global_textmap(composite)
    return provider


def create_tracer_provider(
    config: OtelPluginConfig,
    *,
    id_generator: IdGenerator | None = None,
) -> ProviderResult:
    """Resolve a TracerProvider using the shared 3-tier priority.

    The chosen tier is reported as :class:`ProviderSource` so callers make the
    instrumentation/flush decision once, off a single value:

    1. ``config.tracer_provider`` set        -> ``EXPLICIT``   (used as-is)
    2. ``config.use_default_tracer_provider`` -> ``GLOBAL``     (global provider)
    3. otherwise (default)                    -> ``AUTO_OTLP``  (plugin-owned OTLP)

    Args:
        config: Shared plugin configuration.
        id_generator: Deterministic ID generator injected into an
            auto-configured provider so cross-invocation trace stitching works.

    Returns:
        A :class:`ProviderResult`.
    """
    if config.tracer_provider is not None:
        # Explicit provider: use as-is, never wrap/modify.
        return ProviderResult(config.tracer_provider, ProviderSource.EXPLICIT)

    if config.use_default_tracer_provider:
        return ProviderResult(trace.get_tracer_provider(), ProviderSource.GLOBAL)

    return ProviderResult(
        _create_auto_provider(config, id_generator), ProviderSource.AUTO_OTLP
    )
