"""Tests for durable execution sampling support."""

from __future__ import annotations

import gc
import weakref
from typing import Any, cast

import pytest
from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    Decision,
    Sampler,
    SamplingResult,
)
from opentelemetry.trace import (
    NonRecordingSpan,
    Span,
    SpanContext,
    SpanKind,
    TraceFlags,
    TraceState,
)

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ExtractedContext,
    Sampling,
)
from aws_durable_execution_sdk_python_otel.durable_sampling import (
    DurableSampler,
    DurableSamplingIntent,
    _delegate_accepts_trace_state,
    _type_accepts_trace_state,
    is_sampled,
    resolve_sampling_result,
    store_sampling_intent,
)


TRACE_ID: int = int("5759e988bd862e3fe1be46a994272793", 16)
SPAN_ID: int = int("53995c3f42cd8ad8", 16)


def _span_context(
    *,
    trace_id: int = TRACE_ID,
    sampled: bool = False,
    trace_state: TraceState | None = None,
) -> SpanContext:
    flags: TraceFlags = TraceFlags(
        TraceFlags.SAMPLED if sampled else TraceFlags.DEFAULT
    )
    return SpanContext(
        trace_id=trace_id,
        span_id=SPAN_ID,
        is_remote=False,
        trace_flags=flags,
        trace_state=trace_state if trace_state is not None else TraceState(),
    )


def _invalid_span() -> Span:
    return NonRecordingSpan(SpanContext(0, 0, is_remote=False))


class _RecordingSpan(NonRecordingSpan):
    """A span context wrapper that reports itself as recording."""

    def is_recording(self) -> bool:
        return True


def _extracted(sampling: Sampling) -> ExtractedContext:
    return ExtractedContext(
        trace_id=TRACE_ID,
        parent_span_id=SPAN_ID,
        sampling=sampling,
    )


# ---------------------------------------------------------------------------
# resolve_sampling_result: explicit backend decision wins
# ---------------------------------------------------------------------------
def test_backend_sampled_records_and_samples() -> None:
    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.SAMPLED),
        ambient_span=_invalid_span(),
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_OFF,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE


def test_backend_not_sampled_drops() -> None:
    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.NOT_SAMPLED),
        ambient_span=_invalid_span(),
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_ON,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.DROP


def test_backend_sampled_preserves_same_trace_ambient_trace_state() -> None:
    trace_state: TraceState = TraceState([("vendor", "opaque")])
    ambient: Span = NonRecordingSpan(_span_context(trace_state=trace_state))

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.SAMPLED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_OFF,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE
    assert result.trace_state == trace_state


def test_backend_not_sampled_preserves_same_trace_ambient_trace_state() -> None:
    trace_state: TraceState = TraceState([("vendor", "opaque")])
    ambient: Span = NonRecordingSpan(_span_context(trace_state=trace_state))

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.NOT_SAMPLED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_ON,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.DROP
    assert result.trace_state == trace_state


def test_backend_decision_ignores_different_trace_ambient_trace_state() -> None:
    other_trace_id: int = TRACE_ID ^ 0x1
    trace_state: TraceState = TraceState([("vendor", "opaque")])
    ambient: Span = NonRecordingSpan(
        _span_context(trace_id=other_trace_id, trace_state=trace_state)
    )

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.SAMPLED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_OFF,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE
    assert result.trace_state != trace_state


# ---------------------------------------------------------------------------
# resolve_sampling_result: same-trace ambient span decides when undecided
# ---------------------------------------------------------------------------
def test_undecided_uses_sampled_ambient_span_on_same_trace() -> None:
    trace_state: TraceState = TraceState([("vendor", "opaque")])
    ambient: Span = NonRecordingSpan(
        _span_context(sampled=True, trace_state=trace_state)
    )

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.UNDECIDED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_OFF,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE
    assert result.trace_state == trace_state


def test_undecided_uses_recording_ambient_span_on_same_trace() -> None:
    ambient: Span = _RecordingSpan(_span_context(sampled=False))

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.UNDECIDED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_ON,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_ONLY


def test_undecided_drops_non_recording_ambient_span_on_same_trace() -> None:
    ambient: Span = NonRecordingSpan(_span_context(sampled=False))

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.UNDECIDED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_ON,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.DROP


# ---------------------------------------------------------------------------
# resolve_sampling_result: falls back to the configured sampler otherwise
# ---------------------------------------------------------------------------
def test_undecided_delegates_to_sampler_when_no_extracted_context() -> None:
    result: SamplingResult = resolve_sampling_result(
        extracted=None,
        ambient_span=_invalid_span(),
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_ON,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE


def test_undecided_delegates_to_sampler_for_different_trace_ambient_span() -> None:
    other_trace_id: int = TRACE_ID ^ 0x1
    ambient: Span = NonRecordingSpan(
        _span_context(trace_id=other_trace_id, sampled=True)
    )

    result: SamplingResult = resolve_sampling_result(
        extracted=_extracted(Sampling.UNDECIDED),
        ambient_span=ambient,
        canonical_trace_id=TRACE_ID,
        sampler=ALWAYS_OFF,
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.DROP


# ---------------------------------------------------------------------------
# DurableSampler
# ---------------------------------------------------------------------------
def test_durable_sampler_honors_stored_intent_over_delegate() -> None:
    sampler: DurableSampler = DurableSampler(ALWAYS_OFF)
    intent: DurableSamplingIntent = DurableSamplingIntent(
        SamplingResult(Decision.RECORD_AND_SAMPLE)
    )
    parent_context: Context = store_sampling_intent(Context(), intent)

    result: SamplingResult = sampler.should_sample(parent_context, TRACE_ID, "span")

    assert result.decision is Decision.RECORD_AND_SAMPLE


def test_durable_sampler_merges_span_and_intent_attributes() -> None:
    sampler: DurableSampler = DurableSampler(ALWAYS_OFF)
    intent: DurableSamplingIntent = DurableSamplingIntent(
        SamplingResult(Decision.RECORD_AND_SAMPLE, attributes={"from": "intent"})
    )
    parent_context: Context = store_sampling_intent(Context(), intent)

    result: SamplingResult = sampler.should_sample(
        parent_context,
        TRACE_ID,
        "span",
        attributes={"from": "span", "span_only": "kept"},
    )

    assert result.attributes is not None
    # Intent wins on a shared key.
    assert result.attributes["from"] == "intent"
    # Span-only keys survive the merge.
    assert result.attributes["span_only"] == "kept"


def test_durable_sampler_intent_preserves_trace_state() -> None:
    trace_state: TraceState = TraceState([("vendor", "opaque")])
    sampler: DurableSampler = DurableSampler(ALWAYS_OFF)
    intent: DurableSamplingIntent = DurableSamplingIntent(
        SamplingResult(Decision.RECORD_AND_SAMPLE, trace_state=trace_state)
    )
    parent_context: Context = store_sampling_intent(Context(), intent)

    result: SamplingResult = sampler.should_sample(parent_context, TRACE_ID, "span")

    assert result.trace_state == trace_state


def test_durable_sampler_delegates_without_intent() -> None:
    sampler: DurableSampler = DurableSampler(ALWAYS_ON)

    result: SamplingResult = sampler.should_sample(Context(), TRACE_ID, "span")

    assert result.decision is Decision.RECORD_AND_SAMPLE


class _LegacySampler:
    """A pre-1.21 sampler whose should_sample signature ends at ``links``.

    Deliberately not a ``Sampler`` subclass: it simulates the OpenTelemetry
    SDK 1.20 signature, which predates the ``trace_state`` parameter.
    """

    def should_sample(
        self,
        parent_context: Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes: Any = None,
        links: Any = None,
    ) -> SamplingResult:
        return SamplingResult(Decision.RECORD_AND_SAMPLE)

    def get_description(self) -> str:
        return "LegacySampler"


def test_durable_sampler_delegates_to_pre_1_21_sampler_signature() -> None:
    sampler: DurableSampler = DurableSampler(cast(Sampler, _LegacySampler()))

    result: SamplingResult = sampler.should_sample(Context(), TRACE_ID, "span")

    assert result.decision is Decision.RECORD_AND_SAMPLE


def test_resolve_delegates_to_pre_1_21_sampler_signature() -> None:
    result: SamplingResult = resolve_sampling_result(
        extracted=None,
        ambient_span=_invalid_span(),
        canonical_trace_id=TRACE_ID,
        sampler=cast(Sampler, _LegacySampler()),
        span_name="Workflow",
        attributes={},
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE


class _RaisingSampler(Sampler):
    """A modern-signature sampler whose body raises TypeError."""

    def should_sample(
        self,
        parent_context: Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes: Any = None,
        links: Any = None,
        trace_state: Any = None,
    ) -> SamplingResult:
        raise TypeError("boom from sampler body")

    def get_description(self) -> str:
        return "RaisingSampler"


def test_durable_sampler_does_not_swallow_sampler_body_type_error() -> None:
    sampler: DurableSampler = DurableSampler(_RaisingSampler())

    with pytest.raises(TypeError, match="boom from sampler body"):
        sampler.should_sample(Context(), TRACE_ID, "span")


class _KwargsSampler:
    """A wrapper-style sampler that captures trace_state via **kwargs only.

    Deliberately not a ``Sampler`` subclass: it models a forwarding sampler
    whose ``should_sample`` accepts extra keyword arguments through ``**kwargs``.
    """

    def __init__(self) -> None:
        self.received_trace_state: TraceState | None = None

    def should_sample(
        self,
        parent_context: Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes: Any = None,
        links: Any = None,
        **kwargs: Any,
    ) -> SamplingResult:
        self.received_trace_state = kwargs.get("trace_state")
        return SamplingResult(Decision.RECORD_AND_SAMPLE)

    def get_description(self) -> str:
        return "KwargsSampler"


def test_durable_sampler_passes_trace_state_by_keyword_to_kwargs_delegate() -> None:
    trace_state: TraceState = TraceState([("vendor", "opaque")])
    delegate: _KwargsSampler = _KwargsSampler()
    sampler: DurableSampler = DurableSampler(cast(Sampler, delegate))

    result: SamplingResult = sampler.should_sample(
        Context(),
        TRACE_ID,
        "span",
        trace_state=trace_state,
    )

    assert result.decision is Decision.RECORD_AND_SAMPLE
    assert delegate.received_trace_state == trace_state


class _UninspectableSampler:
    """A sampler whose should_sample has no inspectable signature."""

    # A built-in with no inspectable signature stands in for should_sample.
    should_sample = iter


def test_type_accepts_trace_state_defaults_true_when_uninspectable() -> None:
    # When the signature cannot be inspected, assume the modern signature
    # rather than dropping trace_state.
    assert _type_accepts_trace_state(_UninspectableSampler) is True


def test_type_accepts_trace_state_defaults_true_without_should_sample() -> None:
    # A type with no should_sample attribute defaults to the modern signature.
    assert _type_accepts_trace_state(object) is True


def test_delegate_accepts_trace_state_keys_on_type_not_instance() -> None:
    first: _KwargsSampler = _KwargsSampler()
    second: _KwargsSampler = _KwargsSampler()

    assert _delegate_accepts_trace_state(cast(Sampler, first)) is True
    assert _delegate_accepts_trace_state(cast(Sampler, second)) is True


def test_type_accepts_trace_state_cache_does_not_retain_sampler_instances() -> None:
    delegate: _KwargsSampler = _KwargsSampler()
    ref: weakref.ref = weakref.ref(delegate)

    # Populate the (type-keyed) cache via the instance-facing wrapper.
    assert _delegate_accepts_trace_state(cast(Sampler, delegate)) is True

    del delegate
    gc.collect()

    # The cache keys on the type, so the instance must be collectable.
    assert ref() is None


def test_durable_sampler_description_wraps_delegate() -> None:
    sampler: DurableSampler = DurableSampler(ALWAYS_ON)

    assert (
        sampler.get_description() == f"DurableSampler{{{ALWAYS_ON.get_description()}}}"
    )


class _StubTracer:
    def __init__(self, sampler: Sampler) -> None:
        self.sampler: Sampler = sampler


def test_install_on_tracer_wraps_and_is_idempotent() -> None:
    tracer: Any = _StubTracer(ALWAYS_ON)

    first: DurableSampler = DurableSampler.install_on_tracer(tracer)
    assert isinstance(tracer.sampler, DurableSampler)
    assert first.delegate is ALWAYS_ON

    second: DurableSampler = DurableSampler.install_on_tracer(tracer)
    assert second is first


# ---------------------------------------------------------------------------
# store_sampling_intent / is_sampled
# ---------------------------------------------------------------------------
def test_store_sampling_intent_returns_context_unchanged_when_none() -> None:
    parent_context: Context = Context()

    assert store_sampling_intent(parent_context, None) is parent_context


def test_is_sampled_matches_record_and_sample_only() -> None:
    assert is_sampled(SamplingResult(Decision.RECORD_AND_SAMPLE)) is True
    assert is_sampled(SamplingResult(Decision.RECORD_ONLY)) is False
    assert is_sampled(SamplingResult(Decision.DROP)) is False
