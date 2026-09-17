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

Resolving *which* invocation a record belongs to
-----------------------------------------------

A logging handler is process-global and outlives every invocation, while a
plugin instance serves exactly one invocation. Concurrent executions in one
environment (Lambda Managed Instances) therefore have several plugin instances
alive at once, all reachable from one installed filter. The filter must not hold
a single mutable reference to "the" plugin: whichever invocation started last
would win, and every other invocation's records would be stamped with its trace.

So the binding is per invocation, not per filter:

    - ``bind_invocation`` marks an invocation open and claims the calling
      thread/task for it, through a :class:`contextvars.ContextVar`. A record
      emitted on a claimed thread resolves to the invocation that claimed it,
      which is per-thread and per-task and so cannot be overwritten by a
      concurrent invocation.
    - ``unbind_invocation`` marks the invocation closed.
    - A record emitted on a thread no invocation has claimed resolves to the one
      open invocation, if exactly one is open. That covers the SDK's user-code
      worker threads: the invocation-start hook runs on the Lambda handler
      thread, the handler body runs on a pool thread the plugin has not been
      given control on yet, and Python does not propagate context into new
      threads.
    - If several invocations are open and the thread is unclaimed, the record is
      left unstamped. An unattributed record is a smaller defect than one
      attributed to another customer execution.

Reading the active span straight from the OTel context (as the Java plugin's
static ``MdcSpanEnricher`` does) is not sufficient here: the invocation span is
never attached to the OTel context, and the context of the handler thread -- the
only place the plugin attaches anything at invocation scope -- is not visible on
the worker thread that runs the handler body. Top-level records would silently
lose correlation. The plugin's ``get_current_span_context()`` still reads the
OTel context first, so records emitted inside a step or child context resolve to
the active operation span exactly as before.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import weakref
from typing import TYPE_CHECKING, Protocol

from opentelemetry.trace import TraceFlags


if TYPE_CHECKING:
    from opentelemetry.trace import SpanContext


class _SpanContextProvider(Protocol):
    """Structural type for any plugin that resolves the active span context.

    The log filter only needs this one capability, so it accepts any object
    that provides it (e.g. ``InvocationOtelPlugin`` or ``ExecutionOtelPlugin``)
    rather than a specific plugin class.
    """

    def get_current_span_context(self) -> SpanContext | None: ...


# Guards the open-invocation registry. Held for the length of a set membership
# test or a single mutation, never while a plugin is called.
_registry_lock = threading.Lock()

# Invocations that have started and not yet ended. Weak so that a plugin whose
# end hook never ran (a process torn down mid-invocation) cannot keep itself,
# and the spans it holds, alive for the life of the environment.
_open_invocations: weakref.WeakSet[_SpanContextProvider] = weakref.WeakSet()

# The invocation owning the current thread/task. Set by bind_invocation on every
# thread the owning plugin is given control on.
_current_invocation: contextvars.ContextVar[_SpanContextProvider | None] = (
    contextvars.ContextVar("durable_execution_otel_invocation", default=None)
)

# Serializes installation so two invocations starting at once cannot both find
# a handler filterless and both add a filter to it.
_install_lock = threading.Lock()


def bind_invocation(provider: _SpanContextProvider) -> None:
    """Mark ``provider``'s invocation open and claim this thread/task for it.

    Called by a plugin when it takes control on a thread: at invocation start on
    the Lambda handler thread, and again from the hooks that run on the threads
    executing user code. Idempotent, so a plugin can call it from every such
    hook without tracking which threads it has already claimed.

    Args:
        provider: The plugin serving the invocation that owns this thread.
    """
    with _registry_lock:
        _open_invocations.add(provider)
    _current_invocation.set(provider)


def unbind_invocation(provider: _SpanContextProvider) -> None:
    """Mark ``provider``'s invocation closed and release its claim on this thread.

    Claims made on other threads are not released here -- a context can only be
    reset from the thread that set it -- so :func:`_resolve_provider` also checks
    that a claim names a still-open invocation. That check is what keeps a
    pooled thread outliving its invocation from correlating a later record to a
    finished one.

    Args:
        provider: The plugin whose invocation has ended.
    """
    with _registry_lock:
        _open_invocations.discard(provider)
    if _current_invocation.get() is provider:
        _current_invocation.set(None)


def _resolve_provider() -> _SpanContextProvider | None:
    """Return the invocation to correlate a record emitted right here against."""
    claimed = _current_invocation.get()
    with _registry_lock:
        if claimed is not None and claimed in _open_invocations:
            return claimed
        if len(_open_invocations) == 1:
            return next(iter(_open_invocations))
    return None


class OtelContextLogFilter(logging.Filter):
    """Logging filter that injects the active OTel span context onto records.

    The filter holds no state: it resolves the invocation and the span at emit
    time, on the thread that emits the record, so one installed filter serves
    any number of concurrent invocations. Resolution is described in the module
    docstring; the span itself comes from that invocation's
    ``get_current_span_context()``, which returns the active operation span
    inside steps and child contexts and falls back to the invocation span for
    top-level handler code.

    The filter never caches identifiers and always returns ``True`` so it never
    drops a record.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Stamp the active span context onto the record, then allow it through."""
        provider = _resolve_provider()
        if provider is None:
            return True
        span_context = provider.get_current_span_context()
        if span_context and span_context.is_valid:
            record.traceId = format(span_context.trace_id, "032x")
            record.spanId = format(span_context.span_id, "016x")
            record.otelTraceSampled = bool(
                span_context.trace_flags & TraceFlags.SAMPLED
            )
        return True


def install_log_filter(
    target_logger: logging.Logger | None = None,
) -> OtelContextLogFilter | None:
    """Attach an OtelContextLogFilter to a logger's handlers, idempotently.

    The filter is attached to each handler on ``target_logger`` (the root logger
    by default). Attaching to handlers rather than the logger itself ensures
    records propagated from child loggers are also enriched, since handler
    filters run for every record reaching the handler.

    This is safe to call on every invocation, and from several at once: the
    check for an already-installed filter and the install that follows it happen
    under one lock, so concurrent first-time callers cannot stack duplicate
    filters on a handler. Installation carries no invocation identity -- see
    :func:`bind_invocation` for that -- so a warm environment reuses the
    filter installed by the first invocation as is.

    Args:
        target_logger: Logger whose handlers receive the filter. Defaults to the
            root logger, which in AWS Lambda is where runtime log handlers live.

    Returns:
        The filter instance attached to the handlers, or ``None`` if the target
        logger has no handlers to attach to.
    """
    logger = target_logger if target_logger is not None else logging.getLogger()

    with _install_lock:
        context_filter: OtelContextLogFilter | None = None
        for handler in logger.handlers:
            existing = next(
                (f for f in handler.filters if isinstance(f, OtelContextLogFilter)),
                None,
            )
            if existing is not None:
                # Reuse the already-installed filter so a single instance is
                # shared by every handler.
                context_filter = existing
                continue
            if context_filter is None:
                context_filter = OtelContextLogFilter()
            handler.addFilter(context_filter)

        return context_filter
