"""OpenTelemetry instrumentation plugin for AWS Durable Execution SDK."""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.trace import (
    Tracer,
    StatusCode,
    SpanContext,
    NonRecordingSpan,
    Span,
    SpanKind,
    TracerProvider,
)

from aws_durable_execution_sdk_python.plugin import (
    DurableExecutionPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    AttemptEndInfo,
    AttemptStartInfo,
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


class DurableExecutionOtelPlugin(DurableExecutionPlugin):
    """OpenTelemetry instrumentation plugin for durable executions.

    Produces spans for invocations, operations, and attempts with deterministic
    span/trace IDs so that all invocations of the same durable execution appear
    in a single trace.
    """

    DEFAULT_INSTRUMENT_NAME = "aws-durable-execution-sdk-python"

    def __init__(
        self,
        trace_provider: TracerProvider,
        context_extractor: ContextExtractor | None = None,
        sampling_rate: float = 1.0,
        instrument_name: str = DEFAULT_INSTRUMENT_NAME,
    ) -> None:
        self._context_extractor: ContextExtractor = (
            context_extractor or xray_context_extractor
        )

        self._id_generator: DeterministicIdGenerator = DeterministicIdGenerator()

        self._provider = trace_provider
        self._provider.id_generator = self._id_generator
        self._provider.sampler = TraceIdRatioBased(sampling_rate)
        self._tracer: Tracer = self._provider.get_tracer(instrument_name)

        # per invocation status:
        self._execution_arn = ""
        self._extracted_context: Context | None = None
        # Maps operation ID (None for root) → span/context
        self._operation_spans: dict[str | None, Span] = {}

    def set_span(self, operation_id: str | None, span: Span) -> None:
        self._operation_spans[operation_id] = span

    def delete_span(self, operation_id: str | None) -> None:
        self._operation_spans.pop(operation_id, None)

    def get_span(self, operation_id: str | None) -> Span | None:
        return self._operation_spans.get(operation_id)

    # ------------------------------------------------------------------
    # Context resolution
    # ------------------------------------------------------------------
    def _resolve_parent_span(self, parent_id: str | None = None) -> Span:
        """Resolve the parent context for a given operation.

        Uses _resolve_parent_id to determine the logical parent, then looks up
        the corresponding Context. If the parent ID points to a known operation
        that has a stored context, that context is returned directly. Otherwise,
        a placeholder non-recording span is created with the correct span ID and
        registered so that child spans are properly nested in the trace.
        """

        # Check if we already have a context for this parent
        existing_span = self.get_span(parent_id)
        if existing_span is not None:
            return existing_span

        if parent_id is None:
            raise ValueError("No invocation span found")

        # Parent span is missing — create a placeholder non-recording span
        context = SpanContext(
            trace_id=self._id_generator.generate_trace_id(),
            span_id=operation_id_to_span_id(parent_id),
            is_remote=False,
        )
        placeholder_span = NonRecordingSpan(context)

        # adding the missing fields expected by AttributePropagatingSpanProcessor
        # https://github.com/aws-observability/aws-otel-python-instrumentation/blob/28fc34eb3f0a1e11d6328cdee55b7eaba501d2af/aws-opentelemetry-distro/src/amazon/opentelemetry/distro/attribute_propagating_span_processor.py#L109C12-L109C20
        # https://github.com/aws-observability/aws-otel-python-instrumentation/issues/743
        placeholder_span.kind = SpanKind.INTERNAL
        placeholder_span.attributes = {}
        self.set_span(parent_id, placeholder_span)
        logger.info("created a placeholder span: %s", placeholder_span)

        return placeholder_span

    def _start_span(
        self,
        operation_id: str | None,
        name: str,
        attributes: dict[str, str],
        start_time: datetime.datetime | None = None,
        parent_span: Span = None,
    ) -> Span:
        logger.info(
            "starting a span: operation_id=%s, name=%s, parent_span=%s",
            operation_id,
            name,
            parent_span,
        )
        self._id_generator.set_next_span_id(operation_id)
        if parent_span is not None:
            parent_context = trace.set_span_in_context(
                parent_span, self._extracted_context
            )
        else:
            parent_context = self._extracted_context
        span = self._tracer.start_span(
            name=name,
            attributes=attributes,
            start_time=_to_otel_timestamp(start_time),
            context=parent_context,
        )
        logger.info("started a span: %s", span)
        self.set_span(operation_id, span)
        return span

    def _end_span(
        self, operation_id: str | None, end_timestamp: datetime.datetime | None
    ):
        logger.info("ending a span for operation: %s", operation_id)
        span = self.get_span(operation_id)
        if span:
            # the span is not going to be populated if it has the same end_time and start_time
            span.end(end_time=_to_otel_timestamp(end_timestamp))
            logger.info("ended otel span: %s", span)
        self.delete_span(operation_id)

    # ------------------------------------------------------------------
    # Plugin lifecycle callbacks
    # ------------------------------------------------------------------
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        """Called at the start of each invocation. Creates the invocation span."""
        logger.info("Invocation started: %s", info)
        self._execution_arn = info.execution_arn or ""
        self._extracted_context = self._context_extractor(info)
        self._id_generator.set_trace_id(self._execution_arn, info.start_timestamp)

        self._start_span(
            operation_id=None,
            name="invocation",
            attributes=self._extract_attributes(info),
        )

    def on_operation_start(self, info: OperationStartInfo) -> None:
        """Called when an operation begins. Creates a span for the operation."""
        logger.info(f"Operation started: {info}")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """Called when an operation ends. Ends the span but keeps context available.

        The context is preserved so that children in later checkpoint batches
        can still resolve their parent span correctly.
        """
        logger.info(f"Operation ended: {info}")
        parent_span = self._resolve_parent_span(info.parent_id)

        span = self._start_span(
            operation_id=info.operation_id,
            name=info.name or info.operation_id,
            attributes=self._extract_attributes(info),
            start_time=info.start_timestamp,
            parent_span=parent_span,
        )

        if info.error:
            span.set_status(StatusCode.ERROR, info.error.message or "")
            span.record_exception(
                Exception(info.error.message or info.error.type or "Unknown error")
            )

        end_timestamp = info.end_timestamp
        if end_timestamp is not None and end_timestamp == info.start_timestamp:
            end_timestamp += datetime.timedelta(microseconds=1)
        self._end_span(info.operation_id, end_timestamp)

    def on_operation_attempt_start(self, info: AttemptStartInfo) -> None:
        """Called when an operation attempt begins. Creates a child span."""
        logger.info(f"Attempt started: {info}")

    def on_operation_attempt_end(self, info: AttemptEndInfo) -> None:
        """Called when an operation attempt ends. Ends the attempt span."""
        logger.info(f"Attempt ended: %s", info)
        key = f"{info.operation_id}-attempt-{info.attempt}"

        # Attempt spans nest under their operation span
        parent_span = self._resolve_parent_span(info.operation_id)

        span = self._start_span(
            operation_id=key,
            name=f"{info.name or info.operation_id}-attempt-{info.attempt}",
            attributes=self._extract_attributes(info),
            start_time=info.start_timestamp,
            parent_span=parent_span,
        )

        if info.error:
            span.set_status(StatusCode.ERROR, info.error.message or "")
            span.record_exception(
                Exception(info.error.message or info.error.type or "Unknown error")
            )

        self._end_span(key, info.end_timestamp)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        """Called at the end of each invocation. Ends the invocation span and flushes."""
        logger.info(f"Invocation ended: {info}")
        self._end_span(None, info.end_timestamp)

        # Clear all per-invocation state to prevent leaks across warm Lambda reuses
        self._execution_arn = ""
        self._extracted_context = None
        self._operation_spans = {}

        # Flush before Lambda freeze
        if hasattr(self._provider, "force_flush"):
            self._provider.force_flush()

    def _extract_attributes(self, info) -> dict[str, str]:
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
        if hasattr(info, "succeeded") and info.succeeded is not None:
            attributes["durable.attempt.outcome"] = (
                "succeeded" if info.succeeded else "failed"
            )

        return attributes
