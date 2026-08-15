"""Shared configuration for the durable-execution OpenTelemetry plugins.

Both :class:`ExecutionOtelPlugin` and :class:`InvocationOtelPlugin` accept a
single :class:`OtelPluginConfig`, so configuration options are
consistent and not duplicated across plugins.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

    from aws_durable_execution_sdk_python_otel.context_extractors import (
        ContextExtractor,
    )


DEFAULT_INSTRUMENT_NAME = "aws-durable-execution-sdk-python"
DEFAULT_WORKFLOW_SPAN_NAME = "Workflow"


@dataclass
class OtelPluginConfig:
    """Canonical configuration shared by both OTel plugins.

    Fields relevant only to :class:`ExecutionOtelPlugin` (e.g. ``workflow_span_name``)
    are ignored without error by :class:`InvocationOtelPlugin`.

    Attributes:
        tracer_provider: An application-owned provider to use as-is. When
            omitted, the globally configured provider is used (for example, the
            provider installed by the ADOT Lambda layer). Standalone
            instrumentation registration is skipped for an application-owned
            provider.
        context_extractor: Upstream trace-context extractor. Defaults to the
            X-Ray extractor when omitted.
        instrument_name: Instrumentation scope name.
        workflow_span_name: Name of the Workflow root span (ExecutionOtelPlugin).
        enrich_logger: Install the root-logger OTel context filter.
    """

    tracer_provider: SdkTracerProvider | None = None
    context_extractor: ContextExtractor | None = None
    instrument_name: str = DEFAULT_INSTRUMENT_NAME
    workflow_span_name: str = DEFAULT_WORKFLOW_SPAN_NAME
    enrich_logger: bool = True
