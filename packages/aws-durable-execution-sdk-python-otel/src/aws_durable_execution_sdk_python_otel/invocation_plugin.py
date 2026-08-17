"""OpenTelemetry instrumentation plugin for AWS Durable Execution SDK."""

from __future__ import annotations

import datetime
import logging
import threading
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import (
    InvocationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.trace import (
    Link,
    Span,
    SpanContext,
    SpanKind,
    StatusCode,
    TraceFlags,
    Tracer,
)

from aws_durable_execution_sdk_python_otel import context_scope
from aws_durable_execution_sdk_python_otel.context_extractors import (
    ContextExtractor,
    xray_context_extractor,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.log_filter import install_log_filter
from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.provider import create_tracer_provider
from aws_durable_execution_sdk_python_otel.instrumentations import (
    register_standalone_instrumentations,
)


logger = logging.getLogger(__name__)

_TERMINAL_INVOCATION_STATUSES = frozenset(
    {InvocationStatus.SUCCEEDED, InvocationStatus.FAILED}
)

_SpanAttributes = dict[str, str | bool | int]


def _to_otel_timestamp(dt: datetime.datetime | None) -> int | None:
    """Convert a datetime to OTel timestamp (nanoseconds since epoch), or None."""
    if dt is None:
        dt = datetime.datetime.now(datetime.UTC)
    return int(dt.timestamp() * 1_000_000_000)


class InvocationOtelPlugin(DurableInstrumentationPlugin):
    """OpenTelemetry instrumentation plugin for durable executions.

    The plugin creates spans for Lambda invocations, durable operations, and
    user-function attempts. Trace IDs are derived from the durable execution ARN
    and execution start time so each replay or resumed invocation contributes to
    the same trace.

    Operation IDs are converted into deterministic span IDs. The first observed
    span for an operation uses that deterministic ID; later continuation spans
    use newly generated span IDs and link back to the deterministic span ID so
    trace viewers can relate retries and cross-invocation completions to the
    original logical operation.

    Args:
        config: Shared plugin configuration (the same OtelPluginConfig accepted
            by ExecutionOtelPlugin). When omitted, defaults are used (X-Ray
            extractor, "Workflow" span name, log enrichment on). Like
            ExecutionOtelPlugin and the JS SDK plugins, the default
            ``provider_source`` is ``GLOBAL``: the plugin uses the globally
            configured tracer provider (e.g. the ADOT Lambda layer). Set
            ``provider_source=ProviderSource.AUTO_OTLP`` on the config to have
            the plugin build and own an auto-configured OTLP provider instead.
    """

    DEFAULT_INSTRUMENT_NAME = "aws-durable-execution-sdk-python"

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        """Initialize the plugin from a shared OtelPluginConfig.

        Accepts the same OtelPluginConfig as ExecutionOtelPlugin so both plugins
        share one configuration surface (context extractor, instrumentation
        name, provider selection, exporter/propagator settings, log
        enrichment). Like ExecutionOtelPlugin and the JS SDK plugins, the
        default ``provider_source`` is ``GLOBAL``: it uses the globally
        configured (e.g. ADOT) provider. Pass
        ``provider_source=ProviderSource.AUTO_OTLP`` to have the plugin build
        and own an auto-configured OTLP provider instead.

        The tracer provider is configured with this plugin's deterministic ID
        generator so spans for a durable execution share stable trace and
        logical operation identifiers.

        When ``enrich_logger`` is enabled (default), the plugin installs a
        logging filter that stamps the active OTel trace context onto every
        emitted log record.
        """
        self._config = config or OtelPluginConfig()
        self._context_extractor: ContextExtractor = (
            self._config.context_extractor or xray_context_extractor
        )
        self._workflow_span_name = self._config.workflow_span_name
        self._enrich_logger = self._config.enrich_logger

        # Like ExecutionOtelPlugin (and the JS SDK plugins), InvocationOtelPlugin
        # defaults to provider_source=GLOBAL (the globally configured, e.g. ADOT,
        # provider); set provider_source=ProviderSource.AUTO_OTLP on the config
        # to build and own an auto-configured OTLP provider instead.
        self._id_generator = DeterministicIdGenerator()
        result = create_tracer_provider(
            self._config,
            id_generator=self._id_generator,
        )
        self._provider = result.tracer_provider

        # Deterministic trace stitching requires the SDK TracerProvider, which
        # exposes id_generator/sampler. The API's default ProxyTracerProvider
        # (returned before an SDK provider is configured) does not. Rather than
        # fail the invocation over an observability concern, warn and continue:
        # the proxy's tracer is effectively a no-op (and auto-delegates if an SDK
        # provider is configured later). In a Lambda OTel/ADOT deployment the
        # layer configures a real SDK provider before the handler imports.
        if isinstance(self._provider, SdkTracerProvider):
            self._id_generator = DeterministicIdGenerator.install_on_provider(
                self._provider
            )
        else:
            logger.warning(
                "InvocationOtelPlugin expected an SDK TracerProvider "
                "(opentelemetry.sdk.trace.TracerProvider) but got %s. Spans will "
                "not use deterministic IDs. "
                "Ensure the OpenTelemetry SDK is configured (e.g. via the ADOT "
                "Lambda layer) or pass an explicit tracer_provider.",
                type(self._provider).__name__,
            )
        self._tracer: Tracer = self._provider.get_tracer(self._config.instrument_name)

        try:
            register_standalone_instrumentations(self._config, result)
        except Exception:
            logger.exception("Failed to register standalone instrumentations")

        # per invocation status:
        self._execution_arn = ""
        self._extracted_context: Context | None = None
        self._workflow_span: Span | None = None
        # Maps operation ID (None for root) to the active span.
        self._operation_spans: dict[str | None, Span] = {}
        self._operation_spans_lock = threading.RLock()
        # Bumped every invocation. context_scope uses it to discard scopes a
        # previous invocation left attached on a reused thread.
        self._epoch = 0

        if self._enrich_logger:
            # Install the root-logger filter so every log record is stamped with
            # the active span context. The Lambda runtime attaches its root
            # handler before the handler module is imported (and thus before the
            # plugin is constructed), so the handlers are available here.
            install_log_filter(self)

    def _set_span(self, operation_id: str | None, span: Span) -> None:
        """Register the active span for an operation ID."""
        with self._operation_spans_lock:
            self._operation_spans[operation_id] = span

    def _delete_span(self, operation_id: str | None) -> None:
        """Remove the active span for an operation ID if one is stored."""
        with self._operation_spans_lock:
            self._operation_spans.pop(operation_id, None)

    def _get_span(self, operation_id: str | None) -> Span | None:
        """Return the active span for an operation ID, if present."""
        with self._operation_spans_lock:
            return self._operation_spans.get(operation_id)

    @staticmethod
    def _attempt_span_key(info: UserFunctionStartInfo | UserFunctionEndInfo) -> str:
        """Return the registry key for a STEP attempt span."""
        return f"{info.operation_id}:attempt:{info.attempt or 1}"

    @classmethod
    def _scope_key(cls, info: UserFunctionStartInfo | UserFunctionEndInfo) -> str:
        """Return the context-scope key for a user-function hook pair.

        Mirrors the span registry key so the scope pushed by
        ``on_user_function_start`` is the one ``on_user_function_end`` pops.
        """
        if info.operation_type is OperationType.STEP:
            return cls._attempt_span_key(info)
        return info.operation_id

    def _scope_base_context(self) -> Context:
        """Return the context a new operation scope should be layered onto.

        For the outermost durable scope on a thread, the extracted upstream
        context is the base: user code runs on a worker whose context starts
        empty, so anything the context extractor supplied -- a remote parent,
        baggage -- would be dropped by using the current context there, and
        downstream propagation inside steps would lose it.

        For a nested scope the current context already carries that extracted
        context transitively, via the enclosing scope, so it is used as the base
        to preserve whatever ran in between (ambient spans, baggage added by user
        code). "Nested" is decided by reading ownership out of the context, not
        from thread-local state, so a scope entered on a thread that received a
        propagated operation context builds on it instead of discarding it.
        """
        if context_scope.owns_current(self):
            return context.get_current()
        # An extractor may deliberately return an empty Context to isolate the
        # operation from whatever is ambient. Context subclasses dict, so an empty
        # one is falsy -- test for None, or that intent silently inverts into
        # inheriting the worker's ambient baggage and suppression values.
        if self._extracted_context is not None:
            return self._extracted_context
        return context.get_current()

    def get_current_span_context(self) -> SpanContext | None:
        """Return the span context to use for log correlation.

        Resolution order:
        1. The span in the current OTel context, but only when this plugin
           established that context. Inside a step that is the active attempt span,
           and inside a child context the active context span. After a nested
           operation ends, its scope is detached and the enclosing child context
           span -- still attached -- becomes current again. Ownership travels with
           the context, so a propagated operation context still resolves here.
        2. The invocation span from the plugin registry. This covers top-level
           handler code (the invocation span is never attached to any thread's
           context) and code between top-level operations. Gating step 1 on an
           owned scope matters in GLOBAL (ADOT) mode: the ambient Lambda span is
           current on the handler thread and would otherwise be reported in place
           of the durable span.

        Returns:
            A valid SpanContext, or None if no span is active.
        """
        if context_scope.owns_current(self):
            span_context = trace.get_current_span().get_span_context()
            if span_context and span_context.is_valid:
                return span_context

        invocation_span = self._get_span(None)
        if invocation_span:
            invocation_context = invocation_span.get_span_context()
            if invocation_context and invocation_context.is_valid:
                return invocation_context

        return None

    # ------------------------------------------------------------------
    # Context resolution
    # ------------------------------------------------------------------
    def _resolve_parent_span(self, parent_id: str | None = None) -> Span:
        """Resolve the active parent span for a durable operation.

        ``parent_id`` is ``None`` for root-level durable operations beneath the
        invocation span. For child operations, the parent operation must already
        have an active span in the current invocation.

        Raises:
            ValueError: If the requested parent span is not active.
        """

        # Check if we already have a context for this parent
        existing_span = self._get_span(parent_id)
        if existing_span is not None:
            return existing_span

        raise ValueError("No parent span found")

    def _start_span(
        self,
        operation_id: str | None,
        name: str,
        attributes: _SpanAttributes,
        start_time: datetime.datetime | None = None,
        parent_span: Span | None = None,
        existed: bool = False,
        span_key: str | None = None,
        deterministic_span_id: bool = True,
    ) -> Span:
        """Start and store a span for an invocation or durable operation.

        Args:
            operation_id: Durable operation ID. ``None`` is used for the root
                invocation span.
            name: Span display name.
            attributes: Span attributes.
            start_time: Optional durable start timestamp.
            parent_span: Active parent span. When omitted, the extracted
                upstream context is used as the parent.
            existed: Whether the logical operation already had a previous span.
                Continuation spans link back to the deterministic span ID for
                the operation while using a fresh generated span ID.
            span_key: Optional registry key. Defaults to ``operation_id``.
            deterministic_span_id: Whether to use the deterministic operation
                span ID. Attempt spans set this to ``False`` so they can be
                separate children of the logical operation span.

        Returns:
            The started OpenTelemetry span.
        """
        logger.debug(
            "Starting OTel span: operation_id=%s, name=%s, parent_span=%s",
            operation_id,
            name,
            parent_span,
        )
        registry_key = span_key if span_key is not None else operation_id
        with self._operation_spans_lock:
            if not deterministic_span_id:
                links = []
                self._id_generator.set_next_span_id(None)
            elif existed:
                if not operation_id:
                    raise ValueError("operation id is required")
                span_id = operation_id_to_span_id(self._execution_arn, operation_id)
                links = [
                    Link(
                        context=SpanContext(
                            trace_id=self._id_generator.generate_trace_id(),
                            span_id=span_id,
                            is_remote=False,
                            trace_flags=TraceFlags(TraceFlags.SAMPLED),
                        )
                    )
                ]
                self._id_generator.set_next_span_id(None)
            else:
                links = []

                self._id_generator.set_next_span_id(
                    operation_id_to_span_id(self._execution_arn, operation_id)
                    if operation_id
                    else None
                )
            # Operation and attempt spans link to the execution-scoped Workflow
            # span (the invocation span itself, operation_id=None, does not).
            if self._workflow_span is not None and operation_id is not None:
                workflow_ctx = self._workflow_span.get_span_context()
                if workflow_ctx and workflow_ctx.is_valid:
                    links = [*links, Link(context=workflow_ctx)]
            if parent_span is None:
                # root span
                parent_context = self._extracted_context
            else:
                parent_context = trace.set_span_in_context(
                    parent_span, self._extracted_context
                )
            span = self._tracer.start_span(
                name=name,
                kind=SpanKind.INTERNAL,
                attributes=attributes,
                start_time=_to_otel_timestamp(start_time),
                context=parent_context,
                links=links,
            )
            self._operation_spans[registry_key] = span

        logger.debug("Started OTel span: %s", span)
        return span

    def _end_span(
        self, operation_id: str | None, end_timestamp: datetime.datetime | None = None
    ):
        """End and unregister the active span for an operation ID.

        Args:
            operation_id: Durable operation ID, or ``None`` for the invocation
                span.
            end_timestamp: Optional durable end timestamp to use as the span end
                time. When omitted, OpenTelemetry uses the current time.
        """
        logger.debug("Ending OTel span: operation_id=%s", operation_id)
        with self._operation_spans_lock:
            span = self._operation_spans.pop(operation_id, None)
        if span:
            # the span is not going to be populated if it has the same end_time and start_time
            end_time = _to_otel_timestamp(end_timestamp) if end_timestamp else None
            span.end(end_time=end_time)
            logger.debug("Ended OTel span: %s", span)

    # ------------------------------------------------------------------
    # Plugin lifecycle callbacks
    # ------------------------------------------------------------------
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        """Called at the start of each invocation. Creates the invocation span."""
        logger.debug("Durable invocation started: %s", info)
        self._epoch += 1
        self._execution_arn = info.execution_arn or ""
        self._extracted_context = self._context_extractor(info)
        self._id_generator.set_trace_id(self._execution_arn, info.execution_start_time)

        self._start_workflow_span(info)

        self._start_span(
            operation_id=None,
            name="Invocation",
            attributes=self._extract_attributes(info),
        )

    def _start_workflow_span(self, info: InvocationStartInfo) -> None:
        """Create the deterministic, execution-scoped Workflow root span.

        The Workflow span is a parentless root keyed to a deterministic span ID
        derived from the execution ARN, so every invocation of the same durable
        execution contributes to one Workflow span. It is exported once, on a
        terminal invocation. Operation and attempt spans link to it while
        remaining parented to the invocation span. It is created unconditionally
        -- InvocationOtelPlugin has no default/owned tracer-provider distinction,
        so the span is emitted whether the provider is the ambient (ADOT/global)
        one or an explicitly supplied one.
        """
        if not self._execution_arn:
            logger.warning("No execution ARN; skipping Workflow span creation")
            return
        self._id_generator.set_next_span_id(
            derive_workflow_span_id(self._execution_arn)
        )
        # Empty context => root span with no parent. _to_otel_timestamp falls
        # back to now() when execution_start_time is None.
        self._workflow_span = self._tracer.start_span(
            name=self._workflow_span_name,
            kind=SpanKind.INTERNAL,
            attributes={"durable.execution.arn": self._execution_arn},
            start_time=_to_otel_timestamp(info.execution_start_time),
            context=Context(),
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        """Called at the end of each invocation. Ends the invocation span and flushes."""
        logger.debug("Durable invocation ended: %s", info)
        # Spans are registered parent-first, so close pending spans in reverse
        # order to keep every child contained within its parent.
        with self._operation_spans_lock:
            operation_ids = list(reversed(self._operation_spans))
        for operation_id in operation_ids:
            if operation_id:
                self._end_span(operation_id)

        invocation_span = self._get_span(None)
        if invocation_span:
            invocation_span.set_attribute(
                "durable.invocation.status", info.status.value
            )
            # Span status mapping: SUCCEEDED/PENDING -> OK, FAILED -> ERROR,
            # RETRY -> UNSET. RETRY is left UNSET because the plugin interface
            # cannot tell whether the execution/workflow was STOPPED or
            # TIMED_OUT, so a RETRY invocation is not treated as a definitive
            # failure of the execution.
            if info.status in (InvocationStatus.SUCCEEDED, InvocationStatus.PENDING):
                invocation_span.set_status(StatusCode.OK)
            elif info.status is InvocationStatus.FAILED:
                invocation_span.set_status(
                    StatusCode.ERROR, info.error.message if info.error else ""
                )

        # end the invocation span
        self._end_span(None)

        # The Workflow span (execution view) is exported only on a terminal
        # status; on non-terminal statuses its reference is dropped without
        # ending it (so it is not exported yet). SUCCEEDED -> OK, FAILED -> ERROR;
        # RETRY/PENDING are non-terminal and leave it unexported.
        if self._workflow_span is not None:
            if info.status in _TERMINAL_INVOCATION_STATUSES:
                self._workflow_span.set_attribute(
                    "durable.execution.status",
                    info.status.value if info.status else "",
                )
                if info.status is InvocationStatus.FAILED:
                    self._workflow_span.set_status(
                        StatusCode.ERROR, info.error.message if info.error else ""
                    )
                elif info.status is InvocationStatus.SUCCEEDED:
                    self._workflow_span.set_status(StatusCode.OK)
                self._workflow_span.end()

        # Clear all per-invocation state to prevent leaks across warm Lambda reuses
        # Detach anything this plugin still holds on this thread so the handler
        # thread is left exactly as it was found. Scopes attached on the
        # per-invocation worker threads cannot be detached from here (a token is
        # only resettable in the context that created it); those threads are
        # destroyed with the invocation, and any scope a suspended operation left
        # behind is discarded by the epoch check on the next enter_scope.
        context_scope.unwind(self)
        self._execution_arn = ""
        self._extracted_context = None
        self._workflow_span = None
        with self._operation_spans_lock:
            self._operation_spans = {}

        # Flush before Lambda freeze
        if hasattr(self._provider, "force_flush"):
            self._provider.force_flush()

    def on_operation_start(self, info: OperationStartInfo) -> None:
        """Called when an operation begins. Creates a span for the operation."""
        logger.debug("Durable operation started: %s", info)
        if info.operation_type is OperationType.CONTEXT:
            # Context operations are tracked using on_user_function_start.
            return
        parent_span = self._resolve_parent_span(info.parent_id)
        attributes = self._extract_attributes(info)

        self._start_span(
            operation_id=info.operation_id,
            name=info.name or info.operation_id,
            attributes=attributes,
            start_time=datetime.datetime.now(datetime.UTC),
            parent_span=parent_span,
            existed=info.is_replayed,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """Called when an operation reaches a terminal durable status.

        Non-user-function operations are started by ``on_operation_start``. If
        an operation end is observed without a matching in-memory span, this
        invocation is completing an operation that began earlier, so a short
        continuation span is created and linked to the deterministic logical
        operation span before being ended.
        """
        logger.debug("Durable operation ended: %s", info)
        span = self._get_span(info.operation_id)
        if span is None:
            # the span was not started in the current invocation, so we need to
            # create a new one that links to the previous one
            parent_span = self._resolve_parent_span(info.parent_id)
            attributes = self._extract_attributes(info)
            span = self._start_span(
                operation_id=info.operation_id,
                name=info.name or info.operation_id,
                attributes=attributes,
                start_time=datetime.datetime.now(datetime.UTC),
                parent_span=parent_span,
                existed=True,
            )
        else:
            span.set_attributes(self._extract_attributes(info))

        if info.error:
            span.set_status(StatusCode.ERROR, info.error.message or "")
            span.record_exception(
                Exception(info.error.message or info.error.type or "Unknown error")
            )
        else:
            span.set_status(StatusCode.OK)

        self._end_span(info.operation_id)

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        """Called when a context or step operation starts user code.

        This callback runs inside the thread that executes user code so the
        started span can be attached to the OpenTelemetry context for any
        instrumentation used by that code. STEP attempts are emitted as child
        spans beneath the logical STEP operation span created by
        ``on_operation_start``.

        Args:
            info: Information about the operation attempt.
        """
        logger.debug("Durable user function started: %s", info)
        # Context and Step operations are tracked using on_user_function_start
        if info.operation_type not in [OperationType.CONTEXT, OperationType.STEP]:
            raise RuntimeError(
                "on_user_function_start should only be called for CONTEXT and STEP operations"
            )
        if info.operation_type is OperationType.STEP:
            parent_span = self._get_span(
                info.operation_id
            ) or self._resolve_parent_span(info.parent_id)
        else:
            parent_span = self._resolve_parent_span(info.parent_id)
        attributes = self._extract_attributes(info)
        span_name = info.name or info.operation_id
        if info.operation_type is OperationType.STEP:
            span_name = f"{span_name} attempt {info.attempt or 1}"
        span_key = (
            self._attempt_span_key(info)
            if info.operation_type is OperationType.STEP
            else info.operation_id
        )
        span = self._start_span(
            operation_id=info.operation_id,
            name=span_name,
            attributes=attributes,
            start_time=info.start_time,
            parent_span=parent_span,
            existed=info.attempt != 1 and info.operation_type is not OperationType.STEP,
            span_key=span_key,
            deterministic_span_id=info.operation_type is not OperationType.STEP,
        )
        # Attach on this worker thread so auto-instrumented calls made by the
        # user function become children of this span. The span's own parent was
        # chosen explicitly in _start_span; this only sets what is ambient while
        # the user function runs.
        context_scope.enter_scope(
            self,
            self._scope_key(info),
            # Built after enter_scope's cleanup so it cannot inherit values
            # from a scope that is about to be detached.
            lambda: trace.set_span_in_context(span, self._scope_base_context()),
            epoch=self._epoch,
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        """Called when a context or step operation finishes user code.

        STEP attempt spans are finalized here. CONTEXT spans stay open until
        ``on_operation_end`` supplies the authoritative durable status.

        Args:
            info: Information about the operation attempt.
        """
        logger.debug("Durable user function ended: %s", info)
        if info.operation_type not in [OperationType.CONTEXT, OperationType.STEP]:
            raise RuntimeError(
                "on_user_function_end should only be called for CONTEXT and STEP operations"
            )
        # Detach first, on the same thread that attached, so the context this
        # operation was entered from is restored exactly. Detaching (rather than
        # attaching the enclosing span again) is what keeps the scopes balanced:
        # code after a nested operation lands back on its parent context span,
        # still attached here, and a top-level operation lands back on the
        # thread's ambient context. Between-operation log records resolve through
        # get_current_span_context(), which falls back to the invocation span.
        context_scope.exit_scope(self, self._scope_key(info))
        # key = f"{info.operation_id}-{int(info.start_time.timestamp())}"
        span_key = (
            self._attempt_span_key(info)
            if info.operation_type is OperationType.STEP
            else info.operation_id
        )
        span = self._get_span(span_key)
        if not span:
            raise RuntimeError(
                "on_user_function_end called without matching on_user_function_start"
            )

        if info.operation_type is OperationType.STEP:
            span.set_attributes(self._extract_attributes(info))
            if info.outcome is UserFunctionOutcome.FAILED:
                span.set_status(
                    StatusCode.ERROR, info.error.message if info.error else ""
                )
                span.record_exception(
                    Exception(
                        (info.error.message or info.error.type)
                        if info.error
                        else "Unknown error"
                    )
                )
            else:
                span.set_status(StatusCode.OK)

            end_timestamp = info.end_time
            if end_timestamp is not None and end_timestamp == info.start_time:
                end_timestamp += datetime.timedelta(microseconds=1)
            self._end_span(span_key, end_timestamp)

    def _extract_attributes(self, info: Any) -> _SpanAttributes:
        """Extract durable execution fields as OpenTelemetry span attributes.

        Args:
            info: Invocation, operation, or user-function callback payload.

        Returns:
            A dictionary of durable execution attributes suitable for a span.
        """
        attributes: _SpanAttributes = {
            "durable.execution.arn": self._execution_arn,
        }

        if isinstance(info, InvocationStartInfo):
            attributes["durable.invocation.first"] = info.is_first_invocation
        if hasattr(info, "operation_id") and info.operation_id is not None:
            attributes["durable.operation.id"] = info.operation_id
        if hasattr(info, "operation_type") and info.operation_type is not None:
            attributes["durable.operation.type"] = info.operation_type.value
        if hasattr(info, "sub_type") and info.sub_type is not None:
            attributes["durable.operation.subtype"] = info.sub_type.value
        # STEP user-function spans represent attempts, not durable operations.
        if (
            not (
                isinstance(info, (UserFunctionStartInfo, UserFunctionEndInfo))
                and info.operation_type is OperationType.STEP
            )
            and hasattr(info, "status")
            and info.status is not None
        ):
            attributes["durable.operation.status"] = info.status.value
        if hasattr(info, "name") and info.name is not None:
            attributes["durable.operation.name"] = info.name
        # Per-attempt fields are meaningful for STEP (each attempt is retried)
        # but not for CONTEXT (a context is entered once per invocation, not
        # retried). Omit them on CONTEXT spans for cross-SDK consistency.
        if getattr(info, "operation_type", None) is not OperationType.CONTEXT:
            if hasattr(info, "attempt") and info.attempt is not None:
                attributes["durable.attempt.number"] = info.attempt
            if hasattr(info, "outcome") and info.outcome is not None:
                attributes["durable.attempt.outcome"] = info.outcome.value

        return attributes
