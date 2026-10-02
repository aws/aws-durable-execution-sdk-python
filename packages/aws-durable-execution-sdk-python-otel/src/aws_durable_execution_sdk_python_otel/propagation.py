"""Pure X-Ray propagation encoding for the SDK-owned plugin contract."""

from aws_durable_execution_sdk_python.plugin import PropagationMetadata
from opentelemetry.trace import SpanContext


def propagation_metadata(span_context: SpanContext) -> PropagationMetadata:
    """Encode an operation's context without creating a span or changing state."""
    trace_id = f"{span_context.trace_id:032x}"
    return PropagationMetadata(
        x_amzn_trace_id=(
            f"Root=1-{trace_id[:8]}-{trace_id[8:]};"
            f"Parent={span_context.span_id:016x};"
            f"Sampled={int(span_context.trace_flags.sampled)}"
        )
    )
