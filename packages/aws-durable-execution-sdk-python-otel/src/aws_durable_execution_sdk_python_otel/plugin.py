"""OpenTelemetry instrumentation plugin for AWS Durable Execution SDK."""

from __future__ import annotations

import datetime
import logging
import threading
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.lambda_service import OperationType
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
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
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
    operation_id_to_span_id,
)


if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


def _to_otel_timestamp(dt: datetime.datetime | None) -> int | None:
    """Convert a datetime to OTel timestamp (nanoseconds since epoch), or None."""
    if dt is None:
        dt = datetime.datetime.now(datetime.UTC)
    return int(dt.timestamp() * 1_000_000_000)


class DurableExecutionOtelPlugin(DurableInstrumentationPlugin):
    """OpenTelemetry instrumentation plugin for durable executions.

    The plugin creates spans for Lambda invocations, durable operations, and
    user-function attempts. Trace IDs are derived from the durable execution ARN
    and execution start time so each replay or resumed invocation contributes to
    the same trace.

    Operation IDs are converted into deterministic span IDs. The first observed
    span for an operation uses that deterministic ID; later continuation spans
    use newly generated span IDs and link back to the deterministic span ID so
    trace viewers can relate retries and replay-created terminal spans to the
    original logical operation.

    Args:
        trace_provider: OpenTelemetry tracer provider used to create spans.
        context_extractor: Optional extractor for upstream context. Defaults to
            AWS X-Ray header extraction.
        sampling_rate: Ratio used by ``TraceIdRatioBased`` sampling.
        instrument_name: Instrumentation scope name registered with the tracer.
    """

    DEFAULT_INSTRUMENT_NAME = "aws-durable-execution-sdk-python"

    def __init__(
        self,
        trace_provider: SdkTracerProvider,
        context_extractor: ContextExtractor | None = None,
        sampling_rate: float = 1.0,
        instrument_name: str = DEFAULT_INSTRUMENT_NAME,
    ) -> None:
        """Initialize the plugin with an OpenTelemetry tracer provider.

        The provided tracer provider is configured with this plugin's
        deterministic ID generator and sampling strategy so spans for a durable
        execution share stable trace and logical operation identifiers.
        """
        self._context_extractor: ContextExtractor = (
            context_extractor or xray_context_extractor
        )

        self._provider = trace_provider
        # A ProxyTracerProvider (the API default from trace.get_tracer_provider()
        # before an SDK provider is configured) has no id_generator; fall back to
        # None so DeterministicIdGenerator uses its own default generator.
        self._id_generator: DeterministicIdGenerator = DeterministicIdGenerator(
            fallback_id_generator=getattr(self._provider, "id_generator", None)
        )
        self._provider.id_generator = self._id_generator
        self._provider.sampler = TraceIdRatioBased(sampling_rate)
        self._tracer: Tracer = self._provider.get_tracer(instrument_name)

        # per invocation status:
        self._execution_arn = ""
        self._extracted_context: Context | None = None
        # Maps operation ID (None for root) to the active span.
        self._operation_spans: dict[str | None, Span] = {}
        self._operation_spans_lock = threading.RLock()

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
        attributes: dict[str, str],
        start_time: datetime.datetime | None = None,
        parent_span: Span | None = None,
        existed: bool = False,
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

        Returns:
            The started OpenTelemetry span.
        """
        logger.info(
            "starting a span: operation_id=%s, name=%s, parent_span=%s",
            operation_id,
            name,
            parent_span,
        )
        with self._operation_spans_lock:
            if existed:
                if not operation_id:
                    raise ValueError("operation id is required")
                span_id = operation_id_to_span_id(operation_id)
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
                    operation_id_to_span_id(operation_id) if operation_id else None
                )
            if parent_span is None:
                # root span
                parent_context = self._extracted_context
            else:
                parent_context = trace.set_span_in_context(
                    parent_span, self._extracted_context
                )
            span = self._tracer.start_span(
                name=name,
                attributes=attributes,
                start_time=_to_otel_timestamp(start_time),
                context=parent_context,
                links=links,
            )
            self._operation_spans[operation_id] = span

        logger.info("started a span: %s", span)
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
        logger.info("ending a span for operation: %s", operation_id)
        with self._operation_spans_lock:
            span = self._operation_spans.pop(operation_id, None)
        if span:
            # the span is not going to be populated if it has the same end_time and start_time
            end_time = _to_otel_timestamp(end_timestamp) if end_timestamp else None
            span.end(end_time=end_time)
            logger.info("ended otel span: %s", span)

    # ------------------------------------------------------------------
    # Plugin lifecycle callbacks
    # ------------------------------------------------------------------
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        """Called at the start of each invocation. Creates the invocation span."""
        logger.info("Invocation started: %s", info)
        self._execution_arn = info.execution_arn or ""
        self._extracted_context = self._context_extractor(info)
        self._id_generator.set_trace_id(self._execution_arn, info.start_time)

        self._start_span(
            operation_id=None,
            name=f"invocation",
            attributes=self._extract_attributes(info),
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        """Called at the end of each invocation. Ends the invocation span and flushes."""
        logger.info(f"Invocation ended: {info}")
        end_time = info.end_time
        # end all pending spans
        with self._operation_spans_lock:
            operation_ids = list(self._operation_spans.keys())
        for operation_id in operation_ids:
            if operation_id:
                self._end_span(operation_id, end_time)

        # end the invocation span
        self._end_span(None, end_time)

        # Clear all per-invocation state to prevent leaks across warm Lambda reuses
        self._execution_arn = ""
        self._extracted_context = None
        with self._operation_spans_lock:
            self._operation_spans = {}

        # Flush before Lambda freeze
        if hasattr(self._provider, "force_flush"):
            self._provider.force_flush()

    def on_operation_start(self, info: OperationStartInfo) -> None:
        """Called when an operation begins. Creates a span for the operation."""
        logger.info(f"Operation started: {info}")
        if info.operation_type in [OperationType.CONTEXT, OperationType.STEP]:
            # Context and Step operations are tracked using on_user_function_start
            return
        parent_span = self._resolve_parent_span(info.parent_id)
        attributes = self._extract_attributes(info)

        self._start_span(
            operation_id=info.operation_id,
            name=info.name or info.operation_id,
            attributes=attributes,
            start_time=info.start_time,
            parent_span=parent_span,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """Called when an operation reaches a terminal durable status.

        Non-user-function operations are started by ``on_operation_start``. If
        an operation end is observed without a matching in-memory span, this
        invocation is completing an operation that began earlier, so a short
        continuation span is created and linked to the deterministic logical
        operation span before being ended.
        """
        logger.info(f"Operation ended: {info}")
        if info.operation_type in [OperationType.CONTEXT, OperationType.STEP]:
            # Context and Step operations are tracked using on_user_function_end
            return
        span = self._get_span(info.operation_id)
        if not span:
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

        if info.error:
            span.set_status(StatusCode.ERROR, info.error.message or "")
            span.record_exception(
                Exception(info.error.message or info.error.type or "Unknown error")
            )
        else:
            span.set_status(StatusCode.OK)

        end_timestamp = info.end_time
        if end_timestamp is not None and end_timestamp == info.start_time:
            end_timestamp += datetime.timedelta(microseconds=1)
        self._end_span(info.operation_id, end_timestamp)

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        """Called when a context or step operation starts user code.

        This callback runs inside the thread that executes user code so the
        started span can be attached to the OpenTelemetry context for any
        instrumentation used by that code. Attempts after the first are emitted
        as continuation spans linked to the logical operation span.

        Args:
            info: Information about the operation attempt.
        """
        logger.info("User function started: %s", info)
        # Context and Step operations are tracked using on_user_function_start
        if info.operation_type not in [OperationType.CONTEXT, OperationType.STEP]:
            raise RuntimeError(
                "on_user_function_start should only be called for CONTEXT and STEP operations"
            )
        parent_span = self._resolve_parent_span(info.parent_id)
        attributes = self._extract_attributes(info)
        span = self._start_span(
            operation_id=info.operation_id,
            name=info.name or info.operation_id,
            attributes=attributes,
            start_time=info.start_time,
            parent_span=parent_span,
            existed=info.attempt != 1,
        )
        context.attach(trace.set_span_in_context(span, self._extracted_context))

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        """Called when a context or step operation finishes user code.

        This callback records the final attempt status, captures exceptions for
        failed attempts, and ends the span that was attached in
        ``on_user_function_start``.

        Args:
            info: Information about the operation attempt.
        """
        logger.info("User function ended: %s", info)
        if info.operation_type not in [OperationType.CONTEXT, OperationType.STEP]:
            raise RuntimeError(
                "on_user_function_end should only be called for CONTEXT and STEP operations"
            )
        # key = f"{info.operation_id}-{int(info.start_time.timestamp())}"
        span = self._get_span(info.operation_id)
        if not span:
            raise RuntimeError(
                "on_user_function_end called without matching on_user_function_start"
            )

        span.set_attributes(self._extract_attributes(info))
        if info.outcome is UserFunctionOutcome.FAILED:
            span.set_status(StatusCode.ERROR, info.error.message if info.error else "")
            span.record_exception(
                Exception(
                    (info.error.message or info.error.type)
                    if info.error
                    else "Unknown error"
                )
            )
        elif info.outcome is UserFunctionOutcome.SUCCEEDED:
            span.set_status(StatusCode.OK)
        else:
            # PENDING
            span.set_status(StatusCode.UNSET, "PENDING")

        end_timestamp = info.end_time
        if end_timestamp is not None and end_timestamp == info.start_time:
            end_timestamp += datetime.timedelta(microseconds=1)
        self._end_span(info.operation_id, end_timestamp)
        # We don't call context.detach because the next operation will override it anyway

    def _extract_attributes(self, info: Any) -> dict[str, str]:
        """Extract durable execution fields as OpenTelemetry span attributes.

        Args:
            info: Invocation, operation, or user-function callback payload.

        Returns:
            A dictionary of durable execution attributes suitable for a span.
        """
        attributes: dict[str, str] = {
            "durable.execution.arn": self._execution_arn,
        }

        if hasattr(info, "operation_id") and info.operation_id is not None:
            attributes["durable.operation.id"] = info.operation_id
        if hasattr(info, "operation_type") and info.operation_type is not None:
            attributes["durable.operation.type"] = info.operation_type.value
        if hasattr(info, "name") and info.name is not None:
            attributes["durable.operation.name"] = info.name
        if hasattr(info, "attempt") and info.attempt is not None:
            attributes["durable.attempt.number"] = info.attempt
        if hasattr(info, "outcome") and info.outcome is not None:
            attributes["durable.attempt.outcome"] = info.outcome.value

        return attributes
