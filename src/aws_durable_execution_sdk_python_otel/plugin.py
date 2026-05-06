"""OpenTelemetry instrumentation plugin for AWS Durable Execution SDK."""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING, Any

from opentelemetry import trace, context
from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.trace import (
    Tracer,
    StatusCode,
    SpanContext,
    Span,
    TracerProvider,
    Link,
    TraceFlags,
)

from aws_durable_execution_sdk_python.lambda_service import OperationType
from aws_durable_execution_sdk_python.plugin import (
    DurableExecutionPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    UserFunctionStartInfo,
    UserFunctionEndInfo,
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

        raise ValueError("No parent span found")

    def _start_span(
        self,
        operation_id: str | None,
        span_id: int | None,
        name: str,
        attributes: dict[str, str],
        start_time: datetime.datetime | None = None,
        parent_span: Span = None,
        links: list[Link] | None = None,
    ) -> Span:
        logger.info(
            "starting a span: operation_id=%s, name=%s, parent_span=%s",
            operation_id,
            name,
            parent_span,
        )
        self._id_generator.set_next_span_id(span_id)
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
        logger.info("started a span: %s", span)
        self.set_span(operation_id, span)
        return span

    def _end_span(
        self, operation_id: str | None, end_timestamp: datetime.datetime | None = None
    ):
        logger.info("ending a span for operation: %s", operation_id)
        span = self.get_span(operation_id)
        if span:
            # the span is not going to be populated if it has the same end_time and start_time
            span.end(
                end_time=_to_otel_timestamp(end_timestamp) if end_timestamp else None
            )
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
        self._id_generator.set_trace_id(self._execution_arn, info.start_time)
        current_time = int(datetime.datetime.now(datetime.UTC).timestamp())

        self._start_span(
            operation_id=None,
            span_id=None,
            name=f"invocation-{current_time}",
            attributes=self._extract_attributes(info),
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        """Called at the end of each invocation. Ends the invocation span and flushes."""
        logger.info(f"Invocation ended: {info}")
        end_time = info.end_timestamp
        # end all pending spans
        for operation_id in list(self._operation_spans.keys()):
            if operation_id:
                self._end_span(operation_id, end_time)

        # end the invocation span
        self._end_span(None, end_time)

        # Clear all per-invocation state to prevent leaks across warm Lambda reuses
        self._execution_arn = ""
        self._extracted_context = None
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
            span_id=operation_id_to_span_id(info.operation_id),
            name=info.name or info.operation_id,
            attributes=attributes,
            start_time=info.start_time,
            parent_span=parent_span,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """Called when an operation ends. Ends the span but keeps context available.

        The context is preserved so that children in later checkpoint batches
        can still resolve their parent span correctly.
        """
        logger.info(f"Operation ended: {info}")
        if info.operation_type in [OperationType.CONTEXT, OperationType.STEP]:
            # Context and Step operations are tracked using on_user_function_end
            return
        span = self.get_span(info.operation_id)
        span_id = operation_id_to_span_id(info.operation_id)
        if not span:
            # the span was not started in the current invocation, so we need to create a new one
            parent_span = self._resolve_parent_span(info.parent_id)
            attributes = self._extract_attributes(info)
            link_to_previous_span = Link(
                context=SpanContext(
                    trace_id=self._id_generator.generate_trace_id(),
                    span_id=span_id,
                    is_remote=False,
                    trace_flags=TraceFlags(TraceFlags.SAMPLED),
                ),
            )
            span = self._start_span(
                operation_id=info.operation_id,
                span_id=None,
                name=info.name or info.operation_id,
                attributes=attributes,
                start_time=datetime.datetime.now(datetime.UTC),
                parent_span=parent_span,
                links=[link_to_previous_span],
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
        """Called when an operation starts to execute user provided function. This is called within the thread that runs user provided function.

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
        if not info.is_replay:
            # create a key based on operation_id for the first span as a reference by continuation spans
            span_id = operation_id_to_span_id(info.operation_id)
            links = []
        else:
            span_id = None
            links = [
                Link(
                    context=SpanContext(
                        trace_id=self._id_generator.generate_trace_id(),
                        span_id=operation_id_to_span_id(info.operation_id),
                        is_remote=False,
                        trace_flags=TraceFlags(TraceFlags.SAMPLED),
                    ),
                )
            ]

        span = self._start_span(
            operation_id=info.operation_id,
            span_id=span_id,
            name=info.name or info.operation_id,
            attributes=attributes,
            start_time=info.start_time,
            parent_span=parent_span,
            links=links,
        )
        context.attach(trace.set_span_in_context(span, self._extracted_context))

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        """Called when an operation finishes executing user provided function. This is called within the thread that runs user provided function.

        Args:
            info: Information about the operation attempt.
        """
        logger.info("User function ended: %s", info)
        if info.operation_type not in [OperationType.CONTEXT, OperationType.STEP]:
            raise RuntimeError(
                "on_user_function_end should only be called for CONTEXT and STEP operations"
            )
        # key = f"{info.operation_id}-{int(info.start_time.timestamp())}"
        span = self.get_span(info.operation_id)
        if not span:
            raise RuntimeError(
                "on_user_function_end called without matching on_user_function_start"
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
        # We don't call context.detach because the next operation will override it anyway

    def _extract_attributes(self, info: Any) -> dict[str, str]:
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
