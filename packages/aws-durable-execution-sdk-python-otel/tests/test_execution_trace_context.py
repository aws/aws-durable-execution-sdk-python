"""Tests for execution trace ancestry resolution."""

from __future__ import annotations

from datetime import UTC, datetime

from opentelemetry.trace import TraceFlags

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ExtractedContext,
    Sampling,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    _to_otel_trace_id,
    derive_execution_root_span_id,
)
from aws_durable_execution_sdk_python_otel.execution_trace_context import (
    ExecutionTraceContext,
    canonical_trace_id,
)


# ---------------------------------------------------------------------------
# ExtractedContext validity properties
# ---------------------------------------------------------------------------
def test_extracted_context_validity_properties() -> None:
    complete: ExtractedContext = ExtractedContext(
        trace_id=int("5759e988bd862e3fe1be46a994272793", 16),
        parent_span_id=int("53995c3f42cd8ad8", 16),
    )
    assert complete.has_valid_trace_id is True
    assert complete.has_valid_parent_span_id is True
    assert complete.has_complete_remote_parent is True


def test_extracted_context_rejects_missing_and_zero_ids() -> None:
    missing: ExtractedContext = ExtractedContext(trace_id=None, parent_span_id=None)
    assert missing.has_valid_trace_id is False
    assert missing.has_valid_parent_span_id is False
    assert missing.has_complete_remote_parent is False

    zero: ExtractedContext = ExtractedContext(trace_id=0, parent_span_id=0)
    assert zero.has_valid_trace_id is False
    assert zero.has_valid_parent_span_id is False


def test_extracted_context_parent_alone_is_not_complete() -> None:
    parent_only: ExtractedContext = ExtractedContext(
        trace_id=None,
        parent_span_id=int("53995c3f42cd8ad8", 16),
    )
    assert parent_only.has_valid_parent_span_id is True
    assert parent_only.has_valid_trace_id is False
    assert parent_only.has_complete_remote_parent is False


EXECUTION_ARN: str = "test-arn/execution-trace-context"
START_TIME: datetime = datetime(2026, 8, 27, 5, 11, 47, tzinfo=UTC)
REMOTE_TRACE_ID: int = int("5759e988bd862e3fe1be46a994272793", 16)
REMOTE_PARENT_ID: int = int("53995c3f42cd8ad8", 16)


def _complete_remote(sampling: Sampling = Sampling.SAMPLED) -> ExtractedContext:
    return ExtractedContext(
        trace_id=REMOTE_TRACE_ID,
        parent_span_id=REMOTE_PARENT_ID,
        sampling=sampling,
    )


# ---------------------------------------------------------------------------
# ExecutionTraceContext.resolve
# ---------------------------------------------------------------------------
def test_resolve_uses_complete_remote_parent_as_ancestor() -> None:
    ctx: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=_complete_remote(),
        canonical_trace_id=REMOTE_TRACE_ID,
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: False,
    )

    ancestor = ctx.execution_ancestor
    assert ancestor.trace_id == REMOTE_TRACE_ID
    assert ancestor.span_id == REMOTE_PARENT_ID
    assert ancestor.is_remote is True
    assert ctx.trace_id == REMOTE_TRACE_ID


def test_resolve_falls_back_to_synthetic_root_without_remote_parent() -> None:
    trace_id: int = _to_otel_trace_id(EXECUTION_ARN, START_TIME)
    ctx: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=None,
        canonical_trace_id=trace_id,
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: True,
    )

    ancestor = ctx.execution_ancestor
    assert ancestor.trace_id == trace_id
    assert ancestor.span_id == derive_execution_root_span_id(EXECUTION_ARN)
    assert ancestor.is_remote is False


def test_resolve_uses_synthetic_root_when_parent_incomplete() -> None:
    incomplete: ExtractedContext = ExtractedContext(
        trace_id=REMOTE_TRACE_ID,
        parent_span_id=None,
        sampling=Sampling.UNDECIDED,
    )
    ctx: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=incomplete,
        canonical_trace_id=REMOTE_TRACE_ID,
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: False,
    )

    assert ctx.execution_ancestor.span_id == derive_execution_root_span_id(
        EXECUTION_ARN
    )
    assert ctx.execution_ancestor.is_remote is False


# ---------------------------------------------------------------------------
# ExecutionTraceContext.resolve: trace flags
# ---------------------------------------------------------------------------
def test_resolve_backend_sampled_sets_sampled_flag() -> None:
    ctx: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=_complete_remote(Sampling.SAMPLED),
        canonical_trace_id=REMOTE_TRACE_ID,
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: False,
    )

    assert bool(ctx.trace_flags & TraceFlags.SAMPLED) is True


def test_resolve_backend_not_sampled_clears_sampled_flag() -> None:
    ctx: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=_complete_remote(Sampling.NOT_SAMPLED),
        canonical_trace_id=REMOTE_TRACE_ID,
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: True,
    )

    assert bool(ctx.trace_flags & TraceFlags.SAMPLED) is False


def test_resolve_undecided_defers_to_root_sampled_callback() -> None:
    sampled: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=None,
        canonical_trace_id=_to_otel_trace_id(EXECUTION_ARN, START_TIME),
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: True,
    )
    dropped: ExecutionTraceContext = ExecutionTraceContext.resolve(
        extracted=None,
        canonical_trace_id=_to_otel_trace_id(EXECUTION_ARN, START_TIME),
        execution_arn=EXECUTION_ARN,
        root_sampled=lambda: False,
    )

    assert bool(sampled.trace_flags & TraceFlags.SAMPLED) is True
    assert bool(dropped.trace_flags & TraceFlags.SAMPLED) is False


# ---------------------------------------------------------------------------
# canonical_trace_id
# ---------------------------------------------------------------------------
def test_canonical_trace_id_prefers_valid_extracted_trace_id() -> None:
    result: int = canonical_trace_id(
        extracted=_complete_remote(),
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
    )

    assert result == REMOTE_TRACE_ID


def test_canonical_trace_id_falls_back_to_derived_id() -> None:
    result: int = canonical_trace_id(
        extracted=None,
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
    )

    assert result == _to_otel_trace_id(EXECUTION_ARN, START_TIME)


def test_canonical_trace_id_falls_back_when_extracted_trace_id_invalid() -> None:
    no_trace: ExtractedContext = ExtractedContext(
        trace_id=None,
        parent_span_id=REMOTE_PARENT_ID,
        sampling=Sampling.SAMPLED,
    )

    result: int = canonical_trace_id(
        extracted=no_trace,
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
    )

    assert result == _to_otel_trace_id(EXECUTION_ARN, START_TIME)
