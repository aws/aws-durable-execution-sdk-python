"""Shared configuration for the durable-execution OpenTelemetry plugins.

Both :class:`ExecutionOtelPlugin` and :class:`InvocationOtelPlugin` accept a
single :class:`OtelPluginConfig`, so configuration options are
consistent and not duplicated across plugins.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
        tracer_provider: Explicit provider to use as-is. Highest priority; when
            set, the plugin does not own or modify it and skips instrumentation
            registration.
        use_default_tracer_provider: When True (and no explicit provider),
            resolve the globally configured provider via ``trace.get_tracer_provider()``.
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

    tracer_provider: SdkTracerProvider | None = None
    use_default_tracer_provider: bool | None = None
    context_extractor: ContextExtractor | None = None
    instrument_name: str = DEFAULT_INSTRUMENT_NAME
    enable_http_instrumentation: bool = True
    exporter_config: ExporterConfig = field(default_factory=ExporterConfig)
    propagators: Sequence[TextMapPropagator] | None = None
    workflow_span_name: str = DEFAULT_WORKFLOW_SPAN_NAME
    enrich_logger: bool = True
