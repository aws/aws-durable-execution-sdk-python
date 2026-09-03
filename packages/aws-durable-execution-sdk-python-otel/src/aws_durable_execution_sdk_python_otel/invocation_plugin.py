"""OpenTelemetry instrumentation plugin for AWS Durable Execution SDK."""

from __future__ import annotations

import datetime
import logging
import threading
from typing import Any

from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStatus,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import Tracer as SdkTracer
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.trace import (
    Link,
    Span,
    SpanContext,
    SpanKind,
    StatusCode,
    Tracer,
    TraceState,
)

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ContextExtractor,
    ExtractedContext,
    _ensure_extracted_context,
    xray_context_extractor,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.durable_sampling import (
    DurableSampler,
    DurableSamplingIntent,
    is_sampled,
    resolve_sampling_result,
    store_sampling_intent,
)
from aws_durable_execution_sdk_python_otel.durable_parent_span import (
    DurableParentSpan,
)
from aws_durable_execution_sdk_python_otel.execution_trace_context import (
    ExecutionTraceContext,
    canonical_trace_id,
)
from aws_durable_execution_sdk_python_otel.log_filter import install_log_filter
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from aws_durable_execution_sdk_python_otel.provider import create_tracer_provider


logger = logging.getLogger(__name__)

_TERMINAL_INVOCATION_STATUSES = frozenset(
    {InvocationStatus.SUCCEEDED, InvocationStatus.FAILED}
)
_TIMESTAMP_STEP_NANOS = 1_000

_SpanAttributes = dict[str, str | bool | int]


def _to_otel_timestamp(dt: datetime.datetime | None) -> int:
    """Convert a datetime to OTel timestamp (nanoseconds since epoch)."""
    if dt is None:
        dt = datetime.datetime.now(datetime.UTC)
    return int(dt.timestamp() * 1_000_000_000)


class InvocationOtelPlugin(DurableInstrumentationPlugin):
    """OpenTelemetry instrumentation plugin for durable executions.

    The plugin creates spans for Lambda invocations, durable operations, and
    user-function attempts. Workflow and Invocation spans share one execution
    trace, parented to a propagated backend parent when present or a
    deterministic synthetic execution root otherwise. Invocation spans use the
    active ambient span only when it is already on that execution trace.
    Operation spans remain parented to the Invocation span and link to the
    Workflow span.

    Operation IDs are converted into deterministic span IDs. The first observed
    span for an operation uses that deterministic ID; later continuation spans
    use newly generated span IDs. Operation attributes and links to the Workflow
    span provide execution-scoped correlation across invocations.

    Args:
        config: Shared plugin configuration (the same OtelPluginConfig accepted
            by ExecutionOtelPlugin). When omitted, defaults are used (X-Ray
            extractor, "Workflow" span name, log enrichment on) and the plugin
            uses the globally configured tracer provider (for example, the
            provider installed by the ADOT Lambda layer).
    """

    DEFAULT_INSTRUMENT_NAME = "aws-durable-execution-sdk-python"

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        """Initialize the plugin from a shared OtelPluginConfig.

        Accepts the same OtelPluginConfig as ExecutionOtelPlugin so both plugins
        share one configuration surface (context extractor, instrumentation name,
        provider selection, and log enrichment). When no provider is supplied,
        the globally configured provider is used.

        The plugin tracer is configured with a scoped deterministic ID generator
        so durable spans share stable identifiers without changing unrelated
        instrumentation scopes on the same provider.

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

        result = create_tracer_provider(self._config)
        self._provider = result.tracer_provider
        self._uses_global_provider = result.uses_global_provider
        self._tracer: Tracer = self._provider.get_tracer(self._config.instrument_name)
        self._id_generator = DeterministicIdGenerator()
        self._sampling_delegate: Sampler | None = None
        self._bind_sdk_tracer()

        # per invocation status:
        self._execution_arn = ""
        self._execution_trace_id: int | None = None
        self._execution_start_time: datetime.datetime | None = None
        self._extracted_context: ExtractedContext | None = None
        self._execution_trace_context: ExecutionTraceContext | None = None
        self._sampling_intent: DurableSamplingIntent | None = None
        self._workflow_span: Span | None = None
        self._span_time_floor_ns: int | None = None
        # Maps operation ID (None for root) to the active span.
        self._operation_spans: dict[str | None, Span] = {}
        # Tokens returned by context.attach(), keyed by the span registry key,
        # paired with the thread that attached them. Every attach the plugin
        # owns is released through _detach_context so the plugin never leaves a
        # scope on the context stack.
        self._context_tokens: dict[str, tuple[int, object]] = {}
        self._operation_spans_lock = threading.RLock()
        self._tracing_enabled = False

        if self._enrich_logger:
            # Install the root-logger filter so every log record is stamped with
            # the active span context. The Lambda runtime attaches its root
            # handler before the handler module is imported (and thus before the
            # plugin is constructed), so the handlers are available here.
            install_log_filter(self)

    def _bind_sdk_tracer(self) -> bool:
        """Bind to an SDK tracer, retrying a deferred global provider."""
        self._sampling_delegate = None
        tracer = self._tracer
        if not isinstance(tracer, SdkTracer):
            if self._uses_global_provider:
                self._provider = trace.get_tracer_provider()
            tracer = self._provider.get_tracer(self._config.instrument_name)
            self._tracer = tracer
        if not isinstance(tracer, SdkTracer):
            return False

        self._id_generator = DeterministicIdGenerator.install_on_tracer(tracer)
        self._sampling_delegate = DurableSampler.install_on_tracer(tracer).delegate
        return True

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
    def _attempt_span_key(
        info: UserFunctionStartInfo | UserFunctionEndInfo,
    ) -> str:
        """Return the registry key for a STEP attempt span."""
        return f"{info.operation_id}:attempt:{info.attempt or 1}"

    @classmethod
    def _user_function_span_key(
        cls,
        info: UserFunctionStartInfo | UserFunctionEndInfo,
    ) -> str:
        """Return the registry key a user function's span and scope are stored under.

        STEP user functions are attempts, so each attempt gets its own key; a
        CONTEXT is entered once per invocation and uses the operation id.
        """
        if info.operation_type is OperationType.STEP:
            return cls._attempt_span_key(info)
        return info.operation_id

    # ------------------------------------------------------------------
    # Context scope helpers
    # ------------------------------------------------------------------
    def _attach_context(self, key: str, new_context: Context) -> None:
        """Attach a context and remember its token under ``key``.

        A token already stored under ``key`` means the previous scope for that
        operation was never released: its user function did not reach
        ``on_user_function_end``, because it suspended and a timed in-process
        resume re-entered the same operation. Release it before attaching, so
        the new token does not bury one that can never be detached -- otherwise
        releasing the new scope would restore the abandoned span and leave it
        current for later work on this thread.
        """
        with self._operation_spans_lock:
            if key in self._context_tokens:
                logger.debug(
                    "Releasing an unreleased context scope for %s before "
                    "re-attaching; its user function did not report an end.",
                    key,
                )
                self._detach_context(key)
            self._context_tokens[key] = (
                threading.get_ident(),
                context.attach(new_context),
            )

    def _detach_context(self, key: str) -> None:
        """Detach the context attached under ``key``, restoring its predecessor.

        A context token can only be reset on the thread that created it, so a
        token recorded on another thread is dropped instead of detached (OTel
        logs an error for a cross-thread reset). In practice the pairs always
        line up: user-function hooks run on the thread executing user code, and
        both the start and end hook for one attempt run on that same thread.
        """
        with self._operation_spans_lock:
            entry = self._context_tokens.pop(key, None)
        if entry is None:
            return
        thread_ident, token = entry
        if thread_ident == threading.get_ident():
            context.detach(token)  # type: ignore[arg-type]

    def _detach_remaining_contexts(self) -> None:
        """Release scopes still open, newest first, so nothing outlives the plugin.

        Reached when a lifecycle end hook never fires -- for example a user
        function that suspends, or a warm invocation that starts before the
        previous one was cleaned up.
        """
        with self._operation_spans_lock:
            keys = list(reversed(self._context_tokens))
        for key in keys:
            self._detach_context(key)

    def get_current_span_context(self) -> SpanContext | None:
        """Return the span context to use for log correlation.

        Resolution order:
        1. The same-trace span attached to the OTel thread-local context.
           Inside a step this is the active attempt span, and inside a child
           context this is the active context span (attached in
           on_user_function_start). Unrelated ambient spans are ignored so logs
           stay correlated to the durable execution trace.
        2. The invocation span from the plugin registry. This is the path used
           for top-level handler code: the invocation span is never attached to
           the worker thread's context, so the registry is the only way to
           resolve it. It also covers code between top-level operations, where
           detaching the operation scope restores a context with no durable
           span.

        Returns:
            A valid SpanContext, or None if no span is active.
        """
        span_context = trace.get_current_span().get_span_context()
        if (
            span_context
            and span_context.is_valid
            and span_context.trace_id == self._execution_trace_id
        ):
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

    def _invocation_parent_context(self) -> Context:
        """Return same-trace ambient context, else execution ancestor context."""
        execution_trace_context = self._execution_trace_context
        if execution_trace_context is None:
            return self._with_sampling(Context())

        ambient_span = trace.get_current_span()
        ambient_context = ambient_span.get_span_context()
        if (
            ambient_context.is_valid
            and ambient_context.trace_id == execution_trace_context.trace_id
        ):
            return self._with_sampling(
                trace.set_span_in_context(ambient_span, Context())
            )

        ancestor = DurableParentSpan(execution_trace_context.execution_ancestor)
        return self._with_sampling(trace.set_span_in_context(ancestor, Context()))

    def _with_sampling(self, parent_context: Context) -> Context:
        return store_sampling_intent(parent_context, self._sampling_intent)

    def _next_ordered_timestamp(
        self,
        timestamp: datetime.datetime | None = None,
    ) -> int:
        """Return an invocation-local timestamp that does not move backward."""
        candidate = _to_otel_timestamp(timestamp)
        with self._operation_spans_lock:
            floor = self._span_time_floor_ns
            if floor is not None and candidate <= floor:
                candidate = floor + _TIMESTAMP_STEP_NANOS
            self._span_time_floor_ns = candidate
        return candidate

    def _resolved_trace_state(self) -> TraceState:
        """Return the resolved sampling trace state, else the ancestor state.

        The sampling result preserves a same-trace ambient ``tracestate`` that
        the empty ancestor state would drop.
        """
        intent = self._sampling_intent
        if intent is not None and intent.result.trace_state is not None:
            return intent.result.trace_state
        if self._execution_trace_context is not None:
            return self._execution_trace_context.execution_ancestor.trace_state
        return TraceState()

    def _operation_link_context(self, operation_id: str) -> SpanContext | None:
        """Return the deterministic logical operation context for links."""
        execution_trace_context = self._execution_trace_context
        if execution_trace_context is None:
            return None
        return SpanContext(
            trace_id=execution_trace_context.trace_id,
            span_id=operation_id_to_span_id(self._execution_arn, operation_id),
            is_remote=False,
            trace_flags=execution_trace_context.trace_flags,
            trace_state=self._resolved_trace_state(),
        )

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
                ambient or upstream context is used as the parent.
            existed: Whether the logical operation already had a previous span.
                Continuation spans use a fresh generated span ID.
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
            links: list[Link]
            if not deterministic_span_id or existed:
                links = []
                span_id = None
            else:
                links = []
                span_id = (
                    operation_id_to_span_id(self._execution_arn, operation_id)
                    if operation_id
                    else None
                )
            if existed and operation_id is not None:
                operation_context = self._operation_link_context(operation_id)
                if operation_context is not None and operation_context.is_valid:
                    links = [*links, Link(context=operation_context)]
            # Operation and attempt spans link to the execution-scoped Workflow
            # span (the invocation span itself, operation_id=None, does not).
            if self._workflow_span is not None and operation_id is not None:
                workflow_ctx = self._workflow_span.get_span_context()
                if workflow_ctx and workflow_ctx.is_valid:
                    links = [*links, Link(context=workflow_ctx)]
            if parent_span is None:
                parent_context = self._invocation_parent_context()
            else:
                parent_context = self._with_sampling(
                    trace.set_span_in_context(parent_span, Context())
                )
            span_start_time = (
                self._next_ordered_timestamp(start_time)
                if operation_id is not None
                else _to_otel_timestamp(start_time)
            )
            with self._id_generator.use_ids(trace_id=None, span_id=span_id):
                span = self._tracer.start_span(
                    name=name,
                    kind=SpanKind.INTERNAL,
                    attributes=attributes,
                    start_time=span_start_time,
                    context=parent_context,
                    links=links,
                )
            self._operation_spans[registry_key] = span
            if operation_id is None:
                self._span_time_floor_ns = span_start_time

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
            end_time = self._next_ordered_timestamp(end_timestamp)
            span.end(end_time=end_time)
            logger.debug("Ended OTel span: %s", span)

    # ------------------------------------------------------------------
    # Plugin lifecycle callbacks
    # ------------------------------------------------------------------
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        """Called at the start of each invocation. Creates the invocation span."""
        logger.debug("Durable invocation started: %s", info)
        self._reset_state()
        if info.execution_start_time is None:
            logger.warning(
                "InvocationOtelPlugin requires InvocationStartInfo.execution_start_time "
                "to derive a deterministic trace ID; telemetry is disabled for this "
                "invocation."
            )
            return
        self._tracing_enabled = self._bind_sdk_tracer()
        if not self._tracing_enabled:
            logger.warning(
                "InvocationOtelPlugin expected an SDK Tracer at invocation start "
                "but got %s; telemetry is disabled for this invocation. Ensure "
                "the OpenTelemetry SDK is configured before invocation start.",
                type(self._tracer).__name__,
            )
            return

        self._execution_arn = info.execution_arn or ""
        if not self._execution_arn:
            logger.warning(
                "InvocationOtelPlugin requires InvocationStartInfo.execution_arn "
                "to derive a deterministic execution root; telemetry is disabled "
                "for this invocation."
            )
            self._tracing_enabled = False
            return
        self._execution_start_time = info.execution_start_time
        self._extracted_context = _ensure_extracted_context(
            self._context_extractor(info)
        )
        self._execution_trace_id = canonical_trace_id(
            extracted=self._extracted_context,
            execution_arn=self._execution_arn,
            execution_start_time=info.execution_start_time,
        )
        if self._sampling_delegate is None:
            logger.warning(
                "No sampler available; telemetry is disabled for this invocation."
            )
            self._tracing_enabled = False
            return
        sampling_result = resolve_sampling_result(
            extracted=self._extracted_context,
            ambient_span=trace.get_current_span(),
            canonical_trace_id=self._execution_trace_id,
            sampler=self._sampling_delegate,
            span_name=self._workflow_span_name,
            attributes={"durable.execution.arn": self._execution_arn},
        )
        self._sampling_intent = DurableSamplingIntent(sampling_result)
        self._execution_trace_context = ExecutionTraceContext.resolve(
            extracted=self._extracted_context,
            canonical_trace_id=self._execution_trace_id,
            execution_arn=self._execution_arn,
            root_sampled=lambda: is_sampled(sampling_result),
        )

        self._start_workflow_span(info)

        self._start_span(
            operation_id=None,
            name="Invocation",
            attributes=self._extract_attributes(info),
        )

    def _start_workflow_span(self, info: InvocationStartInfo) -> None:
        """Install a non-recording placeholder for the execution-scoped Workflow span.

        The Workflow span is keyed to a deterministic span ID derived from the
        execution ARN, so every invocation of the same durable execution
        contributes to one Workflow span. During each invocation the plugin only
        needs its deterministic SpanContext so operation and attempt spans can
        link to it while remaining parented to the invocation span; a
        non-recording placeholder fills that role so a non-terminal invocation
        never abandons a recording span. The recording span is created and ended
        once, on a terminal status, by :meth:`_export_workflow_span`.
        """
        if not self._execution_arn:
            logger.warning("No execution ARN; skipping Workflow span creation")
            return
        if self._execution_trace_context is None:
            return
        workflow_span_context = SpanContext(
            trace_id=self._execution_trace_context.trace_id,
            span_id=derive_workflow_span_id(self._execution_arn),
            is_remote=False,
            trace_flags=self._execution_trace_context.trace_flags,
            trace_state=self._resolved_trace_state(),
        )
        self._workflow_span = DurableParentSpan(
            workflow_span_context,
            start_time=self._execution_start_time,
        )

    def _export_workflow_span(self, info: InvocationEndInfo) -> None:
        """Create and end the recording Workflow span once, on a terminal status.

        Uses the same deterministic span ID as the placeholder and the shared
        execution ancestor as its parent, so the exported Workflow span stays on
        the execution trace and correlates with every operation span across all
        invocations. Anchored at the execution start time.
        """
        if not self._execution_arn or self._execution_trace_context is None:
            return
        parent_context = self._with_sampling(
            trace.set_span_in_context(
                DurableParentSpan(self._execution_trace_context.execution_ancestor),
                Context(),
            )
        )
        with self._id_generator.use_ids(
            trace_id=None,
            span_id=derive_workflow_span_id(self._execution_arn),
        ):
            workflow_span = self._tracer.start_span(
                name=self._workflow_span_name,
                kind=SpanKind.INTERNAL,
                attributes={
                    "durable.execution.arn": self._execution_arn,
                    "durable.execution.status": (
                        info.status.value if info.status else ""
                    ),
                },
                start_time=_to_otel_timestamp(self._execution_start_time),
                context=parent_context,
            )
        if info.status is InvocationStatus.FAILED:
            workflow_span.set_status(
                StatusCode.ERROR, info.error.message if info.error else ""
            )
        elif info.status is InvocationStatus.SUCCEEDED:
            workflow_span.set_status(StatusCode.OK)
        workflow_span.end()

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        """Called at the end of each invocation. Ends the invocation span and flushes."""
        logger.debug("Durable invocation ended: %s", info)
        if not self._tracing_enabled:
            self._reset_state()
            return

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

        # The Workflow span (execution view) is a non-recording placeholder
        # during the invocation, so only a terminal status materializes and ends
        # the recording span. SUCCEEDED -> OK, FAILED -> ERROR; RETRY/PENDING are
        # non-terminal and leave it unexported until a later terminal invocation.
        if info.status in _TERMINAL_INVOCATION_STATUSES:
            self._export_workflow_span(info)

        self._reset_state()

        # Flush before Lambda freeze
        if hasattr(self._provider, "force_flush"):
            self._provider.force_flush()

    def _reset_state(self) -> None:
        """Clear per-invocation state for warm Lambda environment reuse."""
        self._detach_remaining_contexts()
        self._execution_arn = ""
        self._execution_trace_id = None
        self._extracted_context = None
        self._execution_trace_context = None
        self._sampling_intent = None
        self._execution_start_time = None
        self._workflow_span = None
        self._span_time_floor_ns = None
        with self._operation_spans_lock:
            self._operation_spans = {}
        self._tracing_enabled = False

    def on_operation_start(self, info: OperationStartInfo) -> None:
        """Called when an operation begins. Creates a span for the operation."""
        logger.debug("Durable operation started: %s", info)
        if not self._tracing_enabled:
            return
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
        continuation span with a fresh span ID is created and ended.
        """
        logger.debug("Durable operation ended: %s", info)
        if not self._tracing_enabled:
            return
        span = self._get_span(info.operation_id)
        if span is None:
            # The operation started in a prior invocation. Create a new
            # correlated segment and link it to the deterministic logical
            # operation context shared across invocations.
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

        self._end_span(info.operation_id, info.end_time)

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
        if not self._tracing_enabled:
            return
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
        span_key = self._user_function_span_key(info)
        span_start_time = (
            datetime.datetime.now(datetime.UTC)
            if info.operation_type is OperationType.CONTEXT
            else info.start_time
        )
        span = self._start_span(
            operation_id=info.operation_id,
            name=span_name,
            attributes=attributes,
            start_time=span_start_time,
            parent_span=parent_span,
            existed=info.attempt != 1 and info.operation_type is not OperationType.STEP,
            span_key=span_key,
            deterministic_span_id=info.operation_type is not OperationType.STEP,
        )
        self._attach_context(
            span_key, trace.set_span_in_context(span, context.get_current())
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        """Called when a context or step operation finishes user code.

        STEP attempt spans are finalized here. CONTEXT spans stay open until
        ``on_operation_end`` supplies the authoritative durable status.

        Args:
            info: Information about the operation attempt.
        """
        logger.debug("Durable user function ended: %s", info)
        if not self._tracing_enabled:
            return
        if info.operation_type not in [OperationType.CONTEXT, OperationType.STEP]:
            raise RuntimeError(
                "on_user_function_end should only be called for CONTEXT and STEP operations"
            )
        span_key = self._user_function_span_key(info)
        span = self._get_span(span_key)
        if not span:
            raise RuntimeError(
                "on_user_function_end called without matching on_user_function_start"
            )

        if (
            info.operation_type is OperationType.STEP
            and info.outcome is not UserFunctionOutcome.INCOMPLETE
        ):
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
        # Restore the enclosing context by releasing the scope this user
        # function attached. Code that runs after this operation (e.g. between
        # steps in a child context) correlates to its enclosing operation
        # again -- the parent context span for a nested operation, and the
        # context active before the operation for a top-level one, where
        # get_current_span_context falls back to the invocation span.
        self._detach_context(span_key)

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

        if hasattr(info, "is_first_invocation"):
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
                hasattr(info, "is_replay_children")
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
