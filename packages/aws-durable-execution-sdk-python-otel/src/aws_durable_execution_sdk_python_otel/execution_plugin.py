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
existing :class:`~aws_durable_execution_sdk_python_otel.invocation_plugin.InvocationOtelPlugin`.
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
    SpanKind,
    StatusCode,
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
from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
    ProviderSource,
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
            (globally configured provider, X-Ray extractor, "Workflow" root
            span).
    """

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self._config = config or OtelPluginConfig()
        self._context_extractor: ContextExtractor = (
            self._config.context_extractor or xray_context_extractor
        )
        self._workflow_span_name = self._config.workflow_span_name

        self._id_generator = DeterministicIdGenerator()
        result = create_tracer_provider(
            self._config,
            id_generator=self._id_generator,
        )
        self._provider = result.tracer_provider
        # GLOBAL (ADOT) mode parents the Invocation span to the ambient Lambda
        # invocation span instead of the Workflow span (see
        # _start_invocation_span).
        self._provider_source = result.source

        # Deterministic stitching requires an SDK provider exposing id_generator.
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

        if isinstance(self._provider, SdkTracerProvider):
            self._id_generator = DeterministicIdGenerator.install_on_provider(
                self._provider
            )
        else:
            logger.warning(
                "ExecutionOtelPlugin expected an SDK TracerProvider but got %s; "
                "spans will not use deterministic IDs.",
                type(self._provider).__name__,
            )

        self._tracer: Tracer = self._provider.get_tracer(self._config.instrument_name)

        try:
            register_standalone_instrumentations(self._config, result)
        except Exception:
            logger.exception("Failed to register standalone instrumentations")

        # Per-invocation state.
        self._execution_arn = ""
        self._extracted_context: Context | None = None
        self._workflow_span: Span | None = None
        self._invocation_span: Span | None = None
        self._operation_spans: dict[str, Span] = {}
        self._lock = threading.RLock()
        # Bumped every invocation. context_scope uses it to discard scopes a
        # previous invocation left attached on a reused thread.
        self._epoch = 0

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

    @classmethod
    def _scope_key(cls, info: UserFunctionStartInfo | UserFunctionEndInfo) -> str:
        """Return the context-scope key for a user-function hook pair.

        Mirrors the span registry key so the scope pushed by
        ``on_user_function_start`` is the one ``on_user_function_end`` pops.
        """
        if info.operation_type is OperationType.STEP:
            return cls._attempt_key(info)
        return info.operation_id

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
        """Link operation/attempt spans to the durable invocation span."""
        if self._invocation_span is not None:
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
        self._epoch += 1
        self._execution_arn = info.execution_arn or ""
        self._extracted_context = self._context_extractor(info)
        self._id_generator.set_trace_id(self._execution_arn, info.execution_start_time)

        self._start_workflow_span(info)
        # Create the Invocation span in both modes. In default-provider mode it
        # is parented to the ambient Lambda invocation span.
        self._start_invocation_span(info)

        # No context is attached here. Nothing on this thread needs it: user code
        # runs on a separate worker (ThreadPoolExecutor does not copy
        # contextvars), so an attach here would never reach it, while the Lambda
        # handler thread is reused across warm invocations -- an unpaired attach
        # would leak an ended span into the next execution, whose context
        # extractor and ambient-parent lookup would then adopt it and merge two
        # executions into one trace. The Workflow and Invocation spans are used
        # as explicit parents instead (see _resolve_parent), matching the Java
        # plugins, which never make either span current. Log correlation for this
        # thread resolves through get_current_span_context().

    def _start_workflow_span(self, info: InvocationStartInfo) -> None:
        if not self._execution_arn:
            logger.warning("No execution ARN; skipping Workflow span creation")
            return
        self._id_generator.set_next_span_id(
            derive_workflow_span_id(self._execution_arn)
        )
        start_time = _to_otel_timestamp(
            info.execution_start_time
        ) or _to_otel_timestamp(datetime.datetime.now(datetime.UTC))
        # Empty context => root span with no parent.
        self._workflow_span = self._tracer.start_span(
            name=self._workflow_span_name,
            kind=SpanKind.INTERNAL,
            attributes={"durable.execution.arn": self._execution_arn},
            start_time=start_time,
            context=Context(),
        )

    def _start_invocation_span(self, info: InvocationStartInfo) -> None:
        self._id_generator.set_next_span_id(None)
        attributes: dict[str, Any]
        if self._provider_source is ProviderSource.GLOBAL:
            # Default-provider mode: parent the Invocation span to the ambient
            # Lambda invocation span (from the ADOT layer or other
            # auto-instrumentation), which is still the active context here (the
            # Workflow span is created with an empty context and not yet
            # attached). Lambda semantic attributes belong to that ambient span,
            # so carry only durable correlation attributes here.
            parent_ctx = otel_context.get_current()
            attributes = {
                "durable.execution.arn": self._execution_arn,
                "durable.invocation.first": info.is_first_invocation,
            }
        else:
            if self._workflow_span is None:
                return
            parent_ctx = trace.set_span_in_context(
                self._workflow_span, self._extracted_context
            )
            attributes = {
                "durable.execution.arn": self._execution_arn,
                "durable.invocation.first": info.is_first_invocation,
            }
        self._invocation_span = self._tracer.start_span(
            name="Invocation",
            kind=SpanKind.INTERNAL,
            attributes=attributes,
            context=parent_ctx,
        )
        self._set_span(_INVOCATION_KEY, self._invocation_span)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        logger.debug("Durable invocation ended: %s", info)
        # Operation spans still open here belong to operations that suspended
        # (e.g. PENDING/RETRYING) rather than completed this invocation. They are
        # ended only by on_operation_end; drop the references without ending them
        # so they are not exported as if completed. _reset_state
        # clears the span map below.

        # End the invocation span regardless of terminal status. Record the
        # invocation status and map it to a span status:
        #   SUCCEEDED/PENDING -> OK  (this invocation did its work, whether it
        #                             completed the execution or cleanly suspended)
        #   FAILED            -> ERROR
        #   RETRY             -> UNSET
        # RETRY is left UNSET because the plugin interface cannot tell whether the
        # execution/workflow was STOPPED or TIMED_OUT: a RETRY invocation is not a
        # definitive failure of the execution, so we avoid marking the span ERROR.
        if self._invocation_span is not None:
            self._invocation_span.set_attribute(
                "durable.invocation.status",
                info.status.value if info.status else "",
            )
            if info.status in (InvocationStatus.SUCCEEDED, InvocationStatus.PENDING):
                self._invocation_span.set_status(StatusCode.OK)
            elif info.status is InvocationStatus.FAILED:
                self._invocation_span.set_status(
                    StatusCode.ERROR, info.error.message if info.error else ""
                )
            self._invocation_span.end()

        # The Workflow span (execution view) is exported only on a terminal
        # status; otherwise its reference is dropped without ending it. Its span
        # status reflects the execution outcome: SUCCEEDED -> OK, FAILED -> ERROR
        # (RETRY/PENDING are non-terminal and never reach here -> UNSET).
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

        self._reset_state()

        if hasattr(self._provider, "force_flush"):
            try:
                self._provider.force_flush()
            except Exception:  # noqa: BLE001
                logger.exception("force_flush failed at invocation end")

    def _reset_state(self) -> None:
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
        self._start_span(
            operation_id=info.operation_id,
            name=info.name or info.operation_id,
            info=info,
            parent=parent,
            start_time=info.start_time,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        logger.debug("Durable operation ended: %s", info)
        span = self._get_span(info.operation_id)
        if span is None:
            # Cross-invocation stitching: operation started in a prior
            # invocation. Create + immediately end a linked span.
            parent = self._resolve_parent(info.parent_id)
            span = self._start_span(
                operation_id=info.operation_id,
                name=info.name or info.operation_id,
                info=info,
                parent=parent,
                start_time=info.start_time,
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

    def _start_span(
        self,
        *,
        operation_id: str,
        name: str,
        info: Any,
        parent: Span | None,
        start_time: datetime.datetime | None,
        span_key: str | None = None,
        deterministic: bool = True,
    ) -> Span:
        """Start a span for an operation/attempt and register it."""
        key = span_key if span_key is not None else operation_id
        with self._lock:
            links = self._build_invocation_links()
            if deterministic:
                # Operation spans always use the deterministic logical-operation
                # span ID so a suspended-then-completed operation exports a
                # single span (on completion) with a stable ID across invocations.
                self._id_generator.set_next_span_id(
                    operation_id_to_span_id(self._execution_arn, operation_id)
                )
            else:
                self._id_generator.set_next_span_id(None)

            if parent is None:
                parent_ctx = self._extracted_context or Context()
            else:
                parent_ctx = trace.set_span_in_context(parent, self._extracted_context)
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
            span = self._start_span(
                operation_id=info.operation_id,
                name=name,
                info=info,
                parent=parent,
                start_time=info.start_time,
                span_key=self._attempt_key(info),
                deterministic=False,
            )
        else:  # CONTEXT
            parent = self._resolve_parent(info.parent_id)
            span = self._start_span(
                operation_id=info.operation_id,
                name=info.name or info.operation_id,
                info=info,
                parent=parent,
                start_time=info.start_time,
            )
        # Attach on this worker thread so auto-instrumented calls made by the
        # user function become children of this span. The scope is pushed onto
        # whatever is already current (rather than replacing it with
        # _extracted_context) so an ambient context on this thread survives; the
        # span's own parent was chosen explicitly in _start_span.
        context_scope.enter_scope(
            self,
            self._scope_key(info),
            trace.set_span_in_context(span, otel_context.get_current()),
            epoch=self._epoch,
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        logger.debug("Durable user function ended: %s", info)
        if info.operation_type not in (OperationType.CONTEXT, OperationType.STEP):
            raise RuntimeError(
                "on_user_function_end only supports CONTEXT and STEP operations"
            )
        # Detach first, on the same thread that attached, so the context this
        # operation was entered from is restored exactly. Detaching (rather than
        # attaching the enclosing span again) is what keeps the scopes balanced:
        # a nested operation lands back on its parent's still-attached scope, and
        # a top-level one lands back on the thread's ambient context.
        context_scope.exit_scope(self, self._scope_key(info))
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
        if info.operation_type is OperationType.STEP:
            span.set_attributes(self._operation_attributes(info))
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

            end_time = info.end_time
            if end_time is not None and end_time == info.start_time:
                end_time += datetime.timedelta(microseconds=1)
            popped = self._pop_span(key)
            if popped is not None:
                popped.end(end_time=_to_otel_timestamp(end_time))

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
        # STEP user-function spans represent attempts, not durable operations.
        if (
            not (
                isinstance(info, (UserFunctionStartInfo, UserFunctionEndInfo))
                and info.operation_type is OperationType.STEP
            )
            and getattr(info, "status", None) is not None
        ):
            attributes["durable.operation.status"] = info.status.value
        if getattr(info, "name", None) is not None:
            attributes["durable.operation.name"] = info.name
        if getattr(info, "operation_type", None) is not OperationType.CONTEXT:
            if getattr(info, "attempt", None) is not None:
                attributes["durable.attempt.number"] = info.attempt
            if getattr(info, "outcome", None) is not None:
                attributes["durable.attempt.outcome"] = info.outcome.value
        return attributes
