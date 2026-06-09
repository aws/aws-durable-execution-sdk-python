"""OTel-enriched logger for durable executions.

Provides a LoggerInterface wrapper that injects OpenTelemetry trace context
(trace_id, span_id, trace_sampled) into every log message's extra dict. This
enables log-trace correlation in observability backends without changing user code.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from opentelemetry.trace import TraceFlags


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import LoggerInterface

    from aws_durable_execution_sdk_python_otel.plugin import DurableExecutionOtelPlugin


class OtelEnrichedLogger:
    """LoggerInterface wrapper that injects OTel trace context into log extra fields.

    The span context is resolved by the plugin via get_current_span_context(),
    which returns the active operation span inside steps and the invocation span
    for top-level handler code.

    Injected fields:
        - otel.trace_id: 32-char hex trace identifier
        - otel.span_id: 16-char hex span identifier
        - otel.trace_sampled: boolean indicating if the trace is sampled

    Args:
        inner: The underlying logger to delegate to after enrichment.
        plugin: The OTel plugin instance that resolves the current span context.
    """

    def __init__(
        self, inner: LoggerInterface, plugin: DurableExecutionOtelPlugin
    ) -> None:
        self._inner = inner
        self._plugin = plugin

    def debug(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._inner.debug(msg, *args, extra=self._enrich(extra))

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._inner.info(msg, *args, extra=self._enrich(extra))

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._inner.warning(msg, *args, extra=self._enrich(extra))

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._inner.error(msg, *args, extra=self._enrich(extra))

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        self._inner.exception(msg, *args, extra=self._enrich(extra))

    def _enrich(self, extra: Mapping[str, object] | None) -> dict[str, object]:
        """Inject OTel trace context into the extra dict.

        trace_id, span_id, and trace_sampled come from the span context resolved
        by the plugin, so the values always match the exported spans.
        """
        enriched: dict[str, object] = dict(extra) if extra else {}

        span_context = self._plugin.get_current_span_context()
        if span_context and span_context.is_valid:
            enriched["otel.trace_id"] = format(span_context.trace_id, "032x")
            enriched["otel.span_id"] = format(span_context.span_id, "016x")
            enriched["otel.trace_sampled"] = bool(
                span_context.trace_flags & TraceFlags.SAMPLED
            )

        return enriched
