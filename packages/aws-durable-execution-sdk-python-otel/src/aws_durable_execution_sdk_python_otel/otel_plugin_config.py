"""Shared configuration for the durable-execution OpenTelemetry plugins.

Both :class:`ExecutionOtelPlugin` and :class:`InvocationOtelPlugin` accept a
single :class:`OtelPluginConfig`, so configuration options are
consistent and not duplicated across plugins.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Sequence


if TYPE_CHECKING:
    from opentelemetry.propagators.textmap import TextMapPropagator
    from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

    from aws_durable_execution_sdk_python_otel.context_extractors import (
        ContextExtractor,
    )


DEFAULT_INSTRUMENT_NAME = "aws-durable-execution-sdk-python"
DEFAULT_WORKFLOW_SPAN_NAME = "Workflow"
# OTLPSpanExporter appends /v1/traces itself, so the base endpoint must NOT
# include it (mirrors the JS fix in PR #729 that removed the duplicate path).
DEFAULT_OTLP_ENDPOINT = "http://localhost:4318"


class ProviderSource(Enum):
    """Which tracer-provider tier an :class:`OtelPluginConfig` selects.

    The single value that drives provider construction (``create_tracer_provider``)
    and the plugins' instrumentation, span-parenting and flush decisions.
    """

    EXPLICIT = "explicit"  # use config.tracer_provider as-is
    GLOBAL = "global"  # default: use the global provider (trace.get_tracer_provider())
    AUTO_OTLP = "auto_otlp"  # plugin builds and owns an OTLP provider


@dataclass
class ExporterConfig:
    """OTLP exporter configuration for the auto-configured TracerProvider."""

    endpoint: str | None = None
    headers: dict[str, str] | None = None


@dataclass
class OtelPluginConfig:
    """Canonical configuration shared by both OTel plugins.

    Fields relevant only to :class:`ExecutionOtelPlugin` (e.g. ``workflow_span_name``)
    are ignored without error by :class:`InvocationOtelPlugin`.

    Attributes:
        provider_source: Selects how the tracer provider is obtained
            (:class:`ProviderSource`). Defaults to ``GLOBAL`` (uses the globally
            configured provider, e.g. the ADOT Lambda layer, via
            ``trace.get_tracer_provider()``). ``AUTO_OTLP`` makes the plugin
            build and own an OTLP provider. ``EXPLICIT`` uses ``tracer_provider``
            as-is and skips instrumentation registration.
        tracer_provider: The provider used when ``provider_source`` is
            ``EXPLICIT``. Required in that case and must be left unset for
            ``GLOBAL`` / ``AUTO_OTLP``.
        context_extractor: Upstream trace-context extractor. Defaults to the
            X-Ray extractor when omitted.
        instrument_name: Instrumentation scope name.
        enable_http_instrumentation: Whether to register HTTP instrumentation
            when the plugin owns an auto-configured provider. Defaults to True.
        exporter_config: OTLP exporter settings for the auto-configured provider.
        propagators: Custom propagators for the auto-configured provider.
            Defaults to ``[AWSXRayPropagator, W3CTraceContextPropagator]``.
        workflow_span_name: Name of the Workflow root span (ExecutionOtelPlugin).
        enrich_logger: Install the root-logger OTel context filter.
    """

    provider_source: ProviderSource = ProviderSource.GLOBAL
    tracer_provider: SdkTracerProvider | None = None
    context_extractor: ContextExtractor | None = None
    instrument_name: str = DEFAULT_INSTRUMENT_NAME
    enable_http_instrumentation: bool = True
    exporter_config: ExporterConfig = field(default_factory=ExporterConfig)
    propagators: Sequence[TextMapPropagator] | None = None
    workflow_span_name: str = DEFAULT_WORKFLOW_SPAN_NAME
    enrich_logger: bool = True

    def __post_init__(self) -> None:
        """Validate that each provider source has the fields it requires.

        The config is fully driven by :attr:`provider_source`; ``tracer_provider``
        is the one source-specific field, so it must be present for ``EXPLICIT``
        and absent for ``GLOBAL`` / ``AUTO_OTLP`` (where it would be silently
        ignored).
        """
        if self.provider_source is ProviderSource.EXPLICIT:
            if self.tracer_provider is None:
                raise ValueError("provider_source=EXPLICIT requires a tracer_provider.")
        elif self.tracer_provider is not None:
            raise ValueError(
                "tracer_provider is only valid with provider_source=EXPLICIT; "
                f"got provider_source={self.provider_source.name}. Set "
                "ProviderSource.EXPLICIT, or drop tracer_provider for "
                "GLOBAL / AUTO_OTLP."
            )
