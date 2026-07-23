"""Root-logger filter that stamps OTel trace context onto every log record.

The filter attaches to a stdlib logging handler and enriches *every* record
that flows through it: direct ``logging.getLogger().info(...)`` calls,
child-logger records that propagate to root, and third-party library logs.

The span/trace identifiers are added as ``LogRecord`` attributes:

    - ``traceId``: 32-char hex trace identifier
    - ``spanId``: 16-char hex span identifier
    - ``otelTraceSampled``: boolean indicating if the trace is sampled

These attributes are only set when a valid span context is active. Records
emitted outside an active invocation (e.g. during Lambda teardown) pass through
unmodified, so any log formatter or schema must treat the fields as optional.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from opentelemetry.trace import TraceFlags


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_otel.plugin import InvocationOtelPlugin


class OtelContextLogFilter(logging.Filter):
    """Logging filter that injects the active OTel span context onto records.

    The filter is a pure reader of the plugin's current span context. It
    resolves the span at emit time, on the thread that emits the record, via
    ``plugin.get_current_span_context()``. That method returns the active
    operation span inside steps and child contexts (attached to the worker
    thread's OTel context) and falls back to the invocation span for top-level
    handler code.

    The filter never caches identifiers and always returns ``True`` so it never
    drops a record.

    Args:
        plugin: The OTel plugin instance that resolves the current span context.
    """

    def __init__(self, plugin: InvocationOtelPlugin) -> None:
        super().__init__()
        self._plugin = plugin

    def filter(self, record: logging.LogRecord) -> bool:
        """Stamp the active span context onto the record, then allow it through."""
        span_context = self._plugin.get_current_span_context()
        if span_context and span_context.is_valid:
            record.traceId = format(span_context.trace_id, "032x")
            record.spanId = format(span_context.span_id, "016x")
            record.otelTraceSampled = bool(
                span_context.trace_flags & TraceFlags.SAMPLED
            )
        return True


def install_log_filter(
    plugin: InvocationOtelPlugin,
    target_logger: logging.Logger | None = None,
) -> OtelContextLogFilter | None:
    """Attach an OtelContextLogFilter to a logger's handlers, idempotently.

    The filter is attached to each handler on ``target_logger`` (the root logger
    by default). Attaching to handlers rather than the logger itself ensures
    records propagated from child loggers are also enriched, since handler
    filters run for every record reaching the handler.

    This is safe to call on every invocation: if a handler already has an
    OtelContextLogFilter, it is left as-is, so warm Lambda reuse will not stack
    duplicate filters. A single shared filter instance is reused across all
    handlers.

    Args:
        plugin: The OTel plugin that resolves the current span context.
        target_logger: Logger whose handlers receive the filter. Defaults to the
            root logger, which in AWS Lambda is where runtime log handlers live.

    Returns:
        The filter instance attached to the handlers, or ``None`` if the target
        logger has no handlers to attach to.
    """
    logger = target_logger if target_logger is not None else logging.getLogger()

    context_filter: OtelContextLogFilter | None = None
    for handler in logger.handlers:
        existing = next(
            (f for f in handler.filters if isinstance(f, OtelContextLogFilter)),
            None,
        )
        if existing is not None:
            # Reuse the already-installed filter so a single instance is shared.
            context_filter = existing
            continue
        if context_filter is None:
            context_filter = OtelContextLogFilter(plugin)
        handler.addFilter(context_filter)

    return context_filter
