"""Materialize the stable SDK-owned ancestor of a fallback execution trace."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import SamplingResult
from opentelemetry.trace import SpanContext, SpanKind, Tracer

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
)
from aws_durable_execution_sdk_python_otel.durable_sampling import (
    DurableSamplingIntent,
    store_sampling_intent,
)
from aws_durable_execution_sdk_python_otel.timestamps import to_otel_timestamp


@dataclass(frozen=True)
class ExecutionRoot:
    """A zero-duration anchor, reproducible in any invocation of an execution.

    Re-exporting the same anchor permits recovery after a failed flush without
    checkpointing telemetry state. The anchor carries stable SDK attributes;
    configured provider resources and processor enrichment remain unchanged.
    """

    execution_arn: str
    ancestor: SpanContext
    start_time: datetime

    def export(
        self,
        tracer: Tracer,
        id_generator: DeterministicIdGenerator,
        sampling_intent: DurableSamplingIntent,
    ) -> None:
        """Export a sampled local ancestor; never replace an external parent."""
        if self.ancestor.is_remote or not self.ancestor.trace_flags.sampled:
            return

        # Reuse only the resolved decision without resampling. This same-ID
        # anchor must not acquire variable sampler attributes or tracestate.
        # The regular tracer preserves configured resources and processors.
        root_attributes: dict[str, str | bool] = {
            "durable.execution.arn": self.execution_arn,
            "durable.execution.synthetic_root": True,
        }
        root_intent = DurableSamplingIntent(
            SamplingResult(sampling_intent.result.decision)
        )
        root_context = store_sampling_intent(Context(), root_intent)
        timestamp = to_otel_timestamp(self.start_time)
        with id_generator._use_ids_for_span(
            trace_id=self.ancestor.trace_id,
            span_id=self.ancestor.span_id,
        ):
            span = tracer.start_span(
                "DurableExecutionRoot",
                context=root_context,
                kind=SpanKind.INTERNAL,
                attributes=root_attributes,
                start_time=timestamp,
            )
        span.end(end_time=timestamp)
