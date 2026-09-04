"""Durable execution sampling support."""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry.context import Context
from opentelemetry.sdk.trace import Tracer as SdkTracer
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.trace import Span, SpanContext, SpanKind, TraceFlags

from aws_durable_execution_sdk_python_otel.context_extractors import (
    ExtractedContext,
    Sampling,
)


_DURABLE_SAMPLING_INTENT_KEY = otel_context.create_key(
    "aws_durable_execution_sampling_intent"
)


@dataclass(frozen=True)
class DurableSamplingIntent:
    """Sampling result to apply to each durable span in one invocation."""

    result: SamplingResult


class DurableSampler(Sampler):
    """Sampler that honors a durable sampling intent carried on parent context."""

    def __init__(self, delegate: Sampler) -> None:
        self.delegate = delegate

    @classmethod
    def install_on_tracer(cls, tracer: SdkTracer) -> "DurableSampler":
        current_sampler = tracer.sampler
        if isinstance(current_sampler, cls):
            return current_sampler
        sampler = cls(current_sampler)
        tracer.sampler = sampler
        return sampler

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
        intent = otel_context.get_value(_DURABLE_SAMPLING_INTENT_KEY, parent_context)
        if isinstance(intent, DurableSamplingIntent):
            merged_attributes = dict(attributes or {})
            merged_attributes.update(dict(intent.result.attributes or {}))
            return SamplingResult(
                intent.result.decision,
                attributes=merged_attributes,
                trace_state=intent.result.trace_state,
            )
        return _delegate_should_sample(
            self.delegate,
            parent_context,
            trace_id,
            name,
            kind,
            attributes,
            links,
            trace_state,
        )

    def get_description(self) -> str:
        return f"DurableSampler{{{self.delegate.get_description()}}}"


def store_sampling_intent(
    parent_context: Context,
    intent: DurableSamplingIntent | None,
) -> Context:
    """Attach a durable sampling intent to a span parent context."""
    if intent is None:
        return parent_context
    return otel_context.set_value(_DURABLE_SAMPLING_INTENT_KEY, intent, parent_context)


def resolve_sampling_result(
    *,
    extracted: ExtractedContext | None,
    ambient_span: Span,
    canonical_trace_id: int,
    sampler: Sampler,
    span_name: str,
    attributes: dict[str, Any],
) -> SamplingResult:
    """Resolve one sampling decision for all durable spans in an invocation.

    Trace state from a same-trace ambient span is preserved across every
    branch, so an explicit backend decision overrides only the sampling
    outcome, not vendor/W3C ``tracestate`` propagation.
    """
    ambient_context = ambient_span.get_span_context()
    on_canonical_trace = _is_same_trace(ambient_context, canonical_trace_id)
    ambient_trace_state = ambient_context.trace_state if on_canonical_trace else None

    sampling = extracted.sampling if extracted is not None else Sampling.UNDECIDED
    if sampling is Sampling.SAMPLED:
        return SamplingResult(
            Decision.RECORD_AND_SAMPLE,
            trace_state=ambient_trace_state,
        )
    if sampling is Sampling.NOT_SAMPLED:
        return SamplingResult(Decision.DROP, trace_state=ambient_trace_state)

    if on_canonical_trace:
        if bool(ambient_context.trace_flags & TraceFlags.SAMPLED):
            decision = Decision.RECORD_AND_SAMPLE
        elif ambient_span.is_recording():
            decision = Decision.RECORD_ONLY
        else:
            decision = Decision.DROP
        return SamplingResult(decision, trace_state=ambient_trace_state)

    return _delegate_should_sample(
        sampler,
        Context(),
        canonical_trace_id,
        span_name,
        SpanKind.INTERNAL,
        attributes,
        (),
        None,
    )


def is_sampled(result: SamplingResult) -> bool:
    return result.decision is Decision.RECORD_AND_SAMPLE


@functools.lru_cache(maxsize=None)
def _type_accepts_trace_state(sampler_type: type) -> bool:
    """Return whether a sampler type's ``should_sample`` accepts ``trace_state``.

    ``trace_state`` was added to ``Sampler.should_sample`` in OpenTelemetry SDK
    1.21. The package supports ``opentelemetry-sdk>=1.20.0``, whose samplers end
    at ``links``. A parameter probe (rather than a call-time ``try/except``)
    avoids masking a ``TypeError`` raised inside the sampler body and never
    invokes the sampler twice.

    Keyed on the sampler *type* rather than a bound method: the signature is a
    property of the class, and a type key holds no reference to sampler
    instances, so a warm process does not retain every sampler it has seen.
    """
    should_sample = getattr(sampler_type, "should_sample", None)
    if should_sample is None:
        return True
    try:
        parameters = inspect.signature(should_sample).parameters
    except (TypeError, ValueError):
        return True
    if "trace_state" in parameters:
        return True
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )


def _delegate_accepts_trace_state(sampler: Sampler) -> bool:
    """Return whether ``sampler`` accepts ``trace_state`` in ``should_sample``."""
    return _type_accepts_trace_state(type(sampler))


def _delegate_should_sample(
    sampler: Sampler,
    parent_context: Context | None,
    trace_id: int,
    name: str,
    kind: SpanKind | None,
    attributes: Any,
    links: Any,
    trace_state: Any,
) -> SamplingResult:
    """Call a delegate sampler using the signature its OTel version supports."""
    if _delegate_accepts_trace_state(sampler):
        return sampler.should_sample(
            parent_context,
            trace_id,
            name,
            kind,
            attributes,
            links,
            trace_state=trace_state,
        )
    return sampler.should_sample(
        parent_context,
        trace_id,
        name,
        kind,
        attributes,
        links,
    )


def _is_same_trace(span_context: SpanContext, trace_id: int) -> bool:
    return span_context.is_valid and span_context.trace_id == trace_id
