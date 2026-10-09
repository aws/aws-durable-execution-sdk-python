"""Pure X-Ray propagation encoding for the SDK-owned plugin contract."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from aws_durable_execution_sdk_python import plugin as core_plugin
from opentelemetry.trace import SpanContext


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.plugin import PropagationMetadata
else:
    PropagationMetadata = getattr(core_plugin, "PropagationMetadata", Any)


def propagation_metadata(span_context: SpanContext) -> PropagationMetadata | None:
    """Encode an operation's context without creating a span or changing state."""
    # Old supported cores do not expose the additive contract. Existing tracing
    # still works; only this new optional contribution is unavailable there.
    metadata_type = getattr(core_plugin, "PropagationMetadata", None)
    if metadata_type is None:
        return None
    trace_id = f"{span_context.trace_id:032x}"
    return metadata_type(
        x_amzn_trace_id=(
            f"Root=1-{trace_id[:8]}-{trace_id[8:]};"
            f"Parent={span_context.span_id:016x};"
            f"Sampled={int(span_context.trace_flags.sampled)}"
        )
    )
