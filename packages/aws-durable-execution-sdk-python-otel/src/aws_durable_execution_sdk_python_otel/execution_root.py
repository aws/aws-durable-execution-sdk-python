"""Materialize the stable SDK-owned ancestor of a fallback execution trace."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from opentelemetry.context import Context
from opentelemetry.trace import SpanContext, SpanKind, Tracer

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
)
from aws_durable_execution_sdk_python_otel.durable_sampling import (
    DurableSamplingIntent,
    store_sampling_intent,
)


@dataclass(frozen=True)
class ExecutionRoot:
    """A zero-duration anchor, reproducible in any invocation of an execution.

    Re-exporting the same anchor permits recovery after a failed flush without
    checkpointing telemetry state. Resources and sampler metadata remain owned
    by the configured OpenTelemetry provider; SDK-added fields remain stable.
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

        # Reuse the invocation's resolved decision and sampler metadata without
        # resampling. An empty parent context makes this an actual root, while
        # the regular tracer preserves configured resources and processors.
        root_context = store_sampling_intent(Context(), sampling_intent)
        timestamp = int(self.start_time.timestamp() * 1_000_000_000)
        with id_generator.use_ids(
            trace_id=self.ancestor.trace_id, span_id=self.ancestor.span_id
        ):
            span = tracer.start_span(
                "DurableExecutionRoot",
                context=root_context,
                kind=SpanKind.INTERNAL,
                attributes={
                    "durable.execution.arn": self.execution_arn,
                    "durable.execution.synthetic_root": True,
                },
                start_time=timestamp,
            )
        span.end(end_time=timestamp)
