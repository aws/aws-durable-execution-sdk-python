"""Execution-view OpenTelemetry plugin for AWS Durable Executions.

The :class:`ExecutionOtelPlugin` produces the deterministic span hierarchy

    Workflow -> Invocation -> Operation -> Attempt

that stitches a single trace across every Lambda invocation of one durable
execution. The Workflow span is the root (created in an empty context so it
never has a parent) and is exported exactly once, when the execution reaches a
terminal status. Operations are parented under the Workflow span (or their
parent operation) and *linked* to the current Invocation span.

This is the Python adaptation of the JS ``ExecutionOtelPlugin`` from
aws-durable-execution-sdk-js#729. Because the Python plugin interface differs
from JS (there is no ``wrapInvocation``/``wrapChildContextFn``/``enrichLogContext``
- context is attached inside the synchronous ``on_user_function_*`` hooks and
log correlation is handled by :mod:`log_filter`), the hook wiring mirrors the
existing :class:`~aws_durable_execution_sdk_python_otel.plugin.InvocationOtelPlugin`.
"""

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
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import (
    Link,
    Span,
    SpanContext,
    StatusCode,
    TraceFlags,
    Tracer,
)

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ContextExtractor,
    xray_context_extractor,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
    derive_workflow_span_id,
    operation_id_to_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin_config import (
    ExecutionOtelPluginConfig,
)
from aws_durable_execution_sdk_python_otel.instrumentations import (
    register_standalone_instrumentations,
)
from aws_durable_execution_sdk_python_otel.log_filter import install_log_filter
from aws_durable_execution_sdk_python_otel.provider import create_tracer_provider


logger = logging.getLogger(__name__)

_TERMINAL_INVOCATION_STATUSES = frozenset(
    {InvocationStatus.SUCCEEDED, InvocationStatus.FAILED}
)

# Registry key for the invocation span (operations use their operation_id).
_INVOCATION_KEY = "__invocation__"


def _to_otel_timestamp(dt: datetime.datetime | None) -> int | None:
    """Convert a datetime to an OTel timestamp (ns since epoch), or None."""
    if dt is None:
        return None
    return int(dt.timestamp() * 1_000_000_000)


class ExecutionOtelPlugin(DurableInstrumentationPlugin):
    """OTel plugin that renders a durable execution as one Workflow-rooted trace.

    Args:
        config: Shared plugin configuration. When omitted, defaults are used
            (auto-configured provider, X-Ray extractor, "Workflow" root span).
    """

    def __init__(self, config: ExecutionOtelPluginConfig | None = None) -> None:
        self._config = config or ExecutionOtelPluginConfig()
        self._context_extractor: ContextExtractor = (
            self._config.context_extractor or xray_context_extractor
        )
        self._workflow_span_name = self._config.workflow_span_name
        self._use_default = bool(self._config.use_default_tracer_provider)

        self._id_generator = DeterministicIdGenerator()
        result = create_tracer_provider(
            self._config,
            id_generator=self._id_generator,
            default_use_global=False,
        )
        self._provider = result.tracer_provider
        self._owns_provider = result.owns_provider

        # Deterministic stitching requires an SDK provider exposing id_generator.
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

        if isinstance(self._provider, SdkTracerProvider):
            self._id_generator = DeterministicIdGenerator(
                fallback_id_generator=getattr(self._provider, "id_generator", None)
            )
            self._provider.id_generator = self._id_generator
        else:
            logger.warning(
                "ExecutionOtelPlugin expected an SDK TracerProvider but got %s; "
                "spans will not use deterministic IDs.",
                type(self._provider).__name__,
            )

        self._tracer: Tracer = self._provider.get_tracer(self._config.instrument_name)

        try:
            register_standalone_instrumentations(
                self._config,
                self._provider if self._owns_provider else None,
                owns_provider=self._owns_provider,
                use_default_tracer_provider=self._use_default,
            )
        except Exception:
            logger.exception("Failed to register standalone instrumentations")

        # Per-invocation state.
        self._execution_arn = ""
        self._extracted_context: Context | None = None
        self._saved_invocation_context: Context | None = None
        self._workflow_span: Span | None = None
        self._invocation_span: Span | None = None
        self._operation_spans: dict[str, Span] = {}
        self._lock = threading.RLock()
        self._is_cold_start = True

        if self._config.enrich_logger:
            install_log_filter(self)

    # ------------------------------------------------------------------
    # Span registry helpers
    # ------------------------------------------------------------------
    def _set_span(self, key: str, span: Span) -> None:
        with self._lock:
            self._operation_spans[key] = span

    def _get_span(self, key: str | None) -> Span | None:
        if key is None:
            return None
        with self._lock:
            return self._operation_spans.get(key)

    def _pop_span(self, key: str) -> Span | None:
        with self._lock:
            return self._operation_spans.pop(key, None)

    @staticmethod
    def _attempt_key(info: UserFunctionStartInfo | UserFunctionEndInfo) -> str:
        return f"{info.operation_id}:attempt:{info.attempt or 1}"

    def get_current_span_context(self) -> SpanContext | None:
        """Return the active span context for log correlation (see log_filter)."""
        span_context = trace.get_current_span().get_span_context()
        if span_context and span_context.is_valid:
            return span_context
        for candidate in (self._invocation_span, self._workflow_span):
            if candidate is not None:
                ctx = candidate.get_span_context()
                if ctx and ctx.is_valid:
                    return ctx
        return None

    # ------------------------------------------------------------------
    # Links
    # ------------------------------------------------------------------
    def _build_invocation_links(self) -> list[Link]:
        """Link operation/attempt spans to the invocation span (JS Req 19)."""
        if self._use_default and self._saved_invocation_context is not None:
            span = trace.get_current_span(self._saved_invocation_context)
            ctx = span.get_span_context()
            if ctx and ctx.is_valid:
                return [Link(context=ctx)]
            return []
        if not self._use_default and self._invocation_span is not None:
            ctx = self._invocation_span.get_span_context()
            if ctx and ctx.is_valid:
                return [Link(context=ctx)]
        return []

    def _resolve_parent(self, parent_id: str | None) -> Span | None:
        """Resolve the parent span: parent operation, else the Workflow span."""
        if parent_id is not None:
            existing = self._get_span(parent_id)
            if existing is not None:
                return existing
        return self._workflow_span

    # ------------------------------------------------------------------
    # Invocation lifecycle
    # ------------------------------------------------------------------
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        logger.debug("Durable invocation started: %s", info)
        self._execution_arn = info.execution_arn or ""
        self._extracted_context = self._context_extractor(info)
        self._id_generator.set_trace_id(
            self._execution_arn, info.execution_start_time
        )

        # Capture the ambient context for link-building in default mode (Req 20).
        if self._use_default:
            self._saved_invocation_context = otel_context.get_current()

        self._start_workflow_span(info)
        if not self._use_default:
            self._start_invocation_span(info)

        # Make the Workflow span the active span so auto-instrumented spans
        # created during the invocation become its children (JS Req 13).
        if self._workflow_span is not None:
            otel_context.attach(
                trace.set_span_in_context(
                    self._workflow_span, self._extracted_context
                )
            )

        self._is_cold_start = False

    def _start_workflow_span(self, info: InvocationStartInfo) -> None:
        if not self._execution_arn:
            logger.warning("No execution ARN; skipping Workflow span creation")
            return
        self._id_generator.set_next_span_id(
            derive_workflow_span_id(self._execution_arn)
        )
        start_time = _to_otel_timestamp(info.execution_start_time) or _to_otel_timestamp(
            datetime.datetime.now(datetime.UTC)
        )
        # Empty context => root span with no parent (JS Req 9.2).
        self._workflow_span = self._tracer.start_span(
            name=self._workflow_span_name,
            attributes={"durable.execution.arn": self._execution_arn},
            start_time=start_time,
            context=Context(),
        )

    def _start_invocation_span(self, info: InvocationStartInfo) -> None:
        if self._workflow_span is None:
            return
        parent_ctx = trace.set_span_in_context(
            self._workflow_span, self._extracted_context
        )
        self._id_generator.set_next_span_id(None)
        attributes: dict[str, Any] = {
            "durable.execution.arn": self._execution_arn,
            "faas.coldstart": self._is_cold_start,
            "cloud.provider": "aws",
            "cloud.platform": "aws_lambda",
        }
        if info.request_id:
            attributes["faas.invocation_id"] = info.request_id
        self._invocation_span = self._tracer.start_span(
            name="invocation",
            attributes=attributes,
            context=parent_ctx,
        )
        self._set_span(_INVOCATION_KEY, self._invocation_span)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        logger.debug("Durable invocation ended: %s", info)
        # End any still-open operation spans defensively.
        with self._lock:
            keys = [k for k in self._operation_spans if k != _INVOCATION_KEY]
        for key in keys:
            span = self._pop_span(key)
            if span is not None:
                span.end()

        # End the invocation span regardless of terminal status (JS Req 10.7).
        if self._invocation_span is not None:
            self._invocation_span.end()

        # The Workflow span is exported only on a terminal status; otherwise its
        # reference is dropped without ending it (JS Req 9.7-8).
        if self._workflow_span is not None:
            if info.status in _TERMINAL_INVOCATION_STATUSES:
                self._workflow_span.set_attribute(
                    "durable.execution.status",
                    info.status.value if info.status else "",
                )
                if info.error:
                    self._workflow_span.set_status(
                        StatusCode.ERROR, info.error.message or ""
                    )
                else:
                    self._workflow_span.set_status(StatusCode.OK)
                self._workflow_span.end()

        self._reset_state()

        if hasattr(self._provider, "force_flush"):
            try:
                self._provider.force_flush()
            except Exception:  # noqa: BLE001
                logger.exception("force_flush failed at invocation end")

    def _reset_state(self) -> None:
        self._execution_arn = ""
        self._extracted_context = None
        self._saved_invocation_context = None
        self._workflow_span = None
        self._invocation_span = None
        with self._lock:
            self._operation_spans = {}

    # ------------------------------------------------------------------
    # Operation lifecycle
    # ------------------------------------------------------------------
    def on_operation_start(self, info: OperationStartInfo) -> None:
        logger.debug("Durable operation started: %s", info)
        if info.operation_type is OperationType.CONTEXT:
            return  # tracked via on_user_function_start
        parent = self._resolve_parent(info.parent_id)
        self._start_operation_span(
            operation_id=info.operation_id,
            name=info.name or info.operation_id,
            info=info,
            parent=parent,
            start_time=info.start_time,
            existed=info.is_replayed,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        logger.debug("Durable operation ended: %s", info)
        if info.operation_type is OperationType.CONTEXT:
            return
        span = self._get_span(info.operation_id)
        if span is None:
            # Cross-invocation stitching: operation started in a prior
            # invocation. Create + immediately end a linked span (JS Req 15.3).
            parent = self._resolve_parent(info.parent_id)
            span = self._start_operation_span(
                operation_id=info.operation_id,
                name=info.name or info.operation_id,
                info=info,
                parent=parent,
                start_time=info.start_time,
                existed=True,
            )
        else:
            span.set_attributes(self._operation_attributes(info))

        if info.error:
            span.set_status(StatusCode.ERROR, info.error.message or "")
            span.record_exception(
                Exception(info.error.message or info.error.type or "Unknown error")
            )
        else:
            span.set_status(StatusCode.OK)

        end_time = info.end_time
        if end_time is not None and end_time == info.start_time:
            end_time += datetime.timedelta(microseconds=1)
        popped = self._pop_span(info.operation_id)
        if popped is not None:
            popped.end(end_time=_to_otel_timestamp(end_time))

    def _start_operation_span(
        self,
        *,
        operation_id: str,
        name: str,
        info: Any,
        parent: Span | None,
        start_time: datetime.datetime | None,
        existed: bool,
        span_key: str | None = None,
        deterministic: bool = True,
    ) -> Span:
        """Start a span for an operation/attempt and register it."""
        key = span_key if span_key is not None else operation_id
        with self._lock:
            links = self._build_invocation_links()
            if not deterministic:
                self._id_generator.set_next_span_id(None)
            elif existed:
                # Continuation span: keep a link back to the deterministic
                # logical-operation span so viewers can relate the stitched span.
                span_id = operation_id_to_span_id(self._execution_arn, operation_id)
                links.append(
                    Link(
                        context=SpanContext(
                            trace_id=self._id_generator.generate_trace_id(),
                            span_id=span_id,
                            is_remote=False,
                            trace_flags=TraceFlags(TraceFlags.SAMPLED),
                        )
                    )
                )
                self._id_generator.set_next_span_id(None)
            else:
                self._id_generator.set_next_span_id(
                    operation_id_to_span_id(self._execution_arn, operation_id)
                )

            if parent is None:
                parent_ctx = self._extracted_context or Context()
            else:
                parent_ctx = trace.set_span_in_context(
                    parent, self._extracted_context
                )
            span = self._tracer.start_span(
                name=name,
                attributes=self._operation_attributes(info),
                start_time=_to_otel_timestamp(start_time),
                context=parent_ctx,
                links=links,
            )
            self._operation_spans[key] = span
        return span

    # ------------------------------------------------------------------
    # User function (CONTEXT / STEP attempt) lifecycle
    # ------------------------------------------------------------------
    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        logger.debug("Durable user function started: %s", info)
        if info.operation_type not in (OperationType.CONTEXT, OperationType.STEP):
            raise RuntimeError(
                "on_user_function_start only supports CONTEXT and STEP operations"
            )
        if info.operation_type is OperationType.STEP:
            parent = self._get_span(info.operation_id) or self._resolve_parent(
                info.parent_id
            )
            name = f"{info.name or info.operation_id} attempt {info.attempt or 1}"
            span = self._start_operation_span(
                operation_id=info.operation_id,
                name=name,
                info=info,
                parent=parent,
                start_time=info.start_time,
                existed=False,
                span_key=self._attempt_key(info),
                deterministic=False,
            )
        else:  # CONTEXT
            parent = self._resolve_parent(info.parent_id)
            span = self._start_operation_span(
                operation_id=info.operation_id,
                name=info.name or info.operation_id,
                info=info,
                parent=parent,
                start_time=info.start_time,
                existed=False,
            )
        otel_context.attach(trace.set_span_in_context(span, self._extracted_context))

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        logger.debug("Durable user function ended: %s", info)
        if info.operation_type not in (OperationType.CONTEXT, OperationType.STEP):
            raise RuntimeError(
                "on_user_function_end only supports CONTEXT and STEP operations"
            )
        key = (
            self._attempt_key(info)
            if info.operation_type is OperationType.STEP
            else info.operation_id
        )
        span = self._get_span(key)
        if span is None:
            raise RuntimeError(
                "on_user_function_end without matching on_user_function_start"
            )
        span.set_attributes(self._operation_attributes(info))
        if info.outcome is UserFunctionOutcome.FAILED:
            span.set_status(StatusCode.ERROR, info.error.message if info.error else "")
            span.record_exception(
                Exception(
                    (info.error.message or info.error.type)
                    if info.error
                    else "Unknown error"
                )
            )
        else:
            span.set_status(StatusCode.OK)

        end_time = info.end_time
        if end_time is not None and end_time == info.start_time:
            end_time += datetime.timedelta(microseconds=1)
        popped = self._pop_span(key)
        if popped is not None:
            popped.end(end_time=_to_otel_timestamp(end_time))

        # Restore the enclosing span as active (parent op, else invocation/workflow).
        enclosing = (
            self._get_span(info.parent_id)
            or self._invocation_span
            or self._workflow_span
        )
        if enclosing is not None:
            otel_context.attach(
                trace.set_span_in_context(enclosing, self._extracted_context)
            )

    # ------------------------------------------------------------------
    # Attributes
    # ------------------------------------------------------------------
    def _operation_attributes(self, info: Any) -> dict[str, Any]:
        attributes: dict[str, Any] = {"durable.execution.arn": self._execution_arn}
        if getattr(info, "operation_id", None) is not None:
            attributes["durable.operation.id"] = info.operation_id
        if getattr(info, "operation_type", None) is not None:
            attributes["durable.operation.type"] = info.operation_type.value
        if getattr(info, "sub_type", None) is not None:
            attributes["durable.operation.subtype"] = info.sub_type.value
        if getattr(info, "status", None) is not None:
            attributes["durable.operation.status"] = info.status.value
        if getattr(info, "name", None) is not None:
            attributes["durable.operation.name"] = info.name
        if getattr(info, "operation_type", None) is not OperationType.CONTEXT:
            if getattr(info, "attempt", None) is not None:
                attributes["durable.operation.attempt"] = info.attempt
            if getattr(info, "outcome", None) is not None:
                attributes["durable.attempt.outcome"] = info.outcome.value
        return attributes
