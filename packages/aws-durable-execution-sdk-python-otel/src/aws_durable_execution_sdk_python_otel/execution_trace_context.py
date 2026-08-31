"""Execution trace ancestry for durable execution telemetry."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from opentelemetry.trace import SpanContext, TraceFlags, TraceState

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ExtractedContext,
    Sampling,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    _to_otel_trace_id,
    derive_execution_root_span_id,
)


@dataclass(frozen=True)
class ExecutionTraceContext:
    """Common ancestor for Workflow and Invocation spans."""

    execution_ancestor: SpanContext

    @property
    def trace_id(self) -> int:
        return self.execution_ancestor.trace_id

    @property
    def trace_flags(self) -> TraceFlags:
        return self.execution_ancestor.trace_flags

    @classmethod
    def resolve(
        cls,
        *,
        extracted: ExtractedContext | None,
        canonical_trace_id: int,
        execution_arn: str,
        root_sampled: Callable[[], bool],
    ) -> "ExecutionTraceContext":
        """Resolve the execution ancestor.

        A complete extracted remote parent is authoritative. Otherwise a
        deterministic synthetic root anchors all invocations of the execution on
        the same trace.
        """
        sampling = extracted.sampling if extracted is not None else Sampling.UNDECIDED
        trace_flags = _trace_flags(sampling, root_sampled)
        if extracted is not None and extracted.has_complete_remote_parent:
            return cls(
                SpanContext(
                    trace_id=canonical_trace_id,
                    span_id=extracted.parent_span_id or 0,
                    is_remote=True,
                    trace_flags=trace_flags,
                    trace_state=TraceState(),
                )
            )

        return cls(
            SpanContext(
                trace_id=canonical_trace_id,
                span_id=derive_execution_root_span_id(execution_arn),
                is_remote=False,
                trace_flags=trace_flags,
                trace_state=TraceState(),
            )
        )


def canonical_trace_id(
    *,
    extracted: ExtractedContext | None,
    execution_arn: str,
    execution_start_time: datetime,
) -> int:
    """Return the stable trace ID for this durable execution."""
    if extracted is not None and extracted.has_valid_trace_id:
        return extracted.trace_id or 0
    return _to_otel_trace_id(execution_arn, execution_start_time)


def _trace_flags(
    sampling: Sampling,
    root_sampled: Callable[[], bool],
) -> TraceFlags:
    if sampling is Sampling.SAMPLED:
        return TraceFlags(TraceFlags.SAMPLED)
    if sampling is Sampling.NOT_SAMPLED:
        return TraceFlags(TraceFlags.DEFAULT)
    return (
        TraceFlags(TraceFlags.SAMPLED)
        if root_sampled()
        else TraceFlags(TraceFlags.DEFAULT)
    )
