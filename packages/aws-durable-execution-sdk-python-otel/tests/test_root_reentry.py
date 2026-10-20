"""A synthetic anchor must not lend its ID or sampling to processor-created roots."""

from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    InvocationEndInfo,
    InvocationStatus,
)
from opentelemetry import context
from opentelemetry.context import Context
from opentelemetry.sdk.trace import (
    TracerProvider,
    SpanProcessor,
    Span,
    RandomIdGenerator,
)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ALWAYS_OFF, Sampler
from opentelemetry.trace import SpanContext, Tracer

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
    ExtractedContext,
    Sampling,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    derive_execution_root_span_id,
)


TRACE = 0x11111111111111111111111111111111
FALLBACK_TRACE = 0x22222222222222222222222222222222
ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:reentry:1/durable-execution/test/id"
)
START = datetime(2026, 10, 1, tzinfo=UTC)


class _Ids(RandomIdGenerator):
    def __init__(self) -> None:
        self.trace_calls = 0

    def generate_trace_id(self) -> int:
        self.trace_calls += 1
        return FALLBACK_TRACE + self.trace_calls


class _Reentry(SpanProcessor):
    def __init__(self) -> None:
        self.tracer: Tracer | None = None
        self.nested_context: SpanContext | None = None
        self.context_unchanged = False

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        if span.name != "DurableExecutionRoot" or self.nested_context is not None:
            return
        assert self.tracer is not None
        before = context.get_current()
        nested = self.tracer.start_span("independent-root", context=Context())
        self.nested_context = nested.get_span_context()
        self.context_unchanged = context.get_current() == before
        nested.end()


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize(
    "target", ["same-scope", "same-tracer", "other-scope", "other-provider"]
)
@pytest.mark.parametrize("sampler", [ALWAYS_OFF, ALWAYS_ON])
def test_processor_reentry_retains_independent_ids_and_sampling(
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    target: str,
    sampler: Sampler,
) -> None:
    own = TracerProvider(sampler=sampler, id_generator=_Ids())
    exporter = InMemorySpanExporter()
    own.add_span_processor(SimpleSpanProcessor(exporter))
    processor = _Reentry()
    own.add_span_processor(processor)
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=own,
            instrument_name="durable-reentry",
            enrich_logger=False,
            context_extractor=lambda _: ExtractedContext(TRACE, None, Sampling.SAMPLED),
        )
    )
    other = None
    nested_exporter = exporter
    if target == "same-scope":
        processor.tracer = own.get_tracer("durable-reentry")
    elif target == "same-tracer":
        processor.tracer = plugin._tracer
    elif target == "other-scope":
        processor.tracer = own.get_tracer("other")
    else:
        other = TracerProvider(sampler=sampler, id_generator=_Ids())
        nested_exporter = InMemorySpanExporter()
        other.add_span_processor(SimpleSpanProcessor(nested_exporter))
        processor.tracer = other.get_tracer("other")
    original = context.get_current()
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                request_id="request",
                execution_arn=ARN,
                execution_start_time=START,
                is_first_invocation=True,
            )
        )
        plugin.on_invocation_end(
            InvocationEndInfo(
                request_id="request",
                execution_arn=ARN,
                execution_start_time=START,
                is_first_invocation=True,
                status=InvocationStatus.PENDING,
            )
        )
        root = next(
            s for s in exporter.get_finished_spans() if s.name == "DurableExecutionRoot"
        )
        assert root.context is not None
        assert root.context.trace_id == TRACE
        assert root.context.span_id == derive_execution_root_span_id(ARN)
        assert root.context.trace_flags.sampled
        nested = processor.nested_context
        assert nested is not None
        assert nested.trace_id == FALLBACK_TRACE + 1
        assert nested.span_id != root.context.span_id
        assert nested.trace_flags.sampled is (sampler is ALWAYS_ON)
        exported = [
            s
            for s in nested_exporter.get_finished_spans()
            if s.name == "independent-root"
        ]
        assert bool(exported) is (sampler is ALWAYS_ON)
        if exported:
            assert exported[0].parent is None
        assert context.get_current() == original
        assert processor.context_unchanged
        assert processor.tracer is not None
        after = processor.tracer.start_span("after", context=Context())
        assert after.get_span_context().trace_id == FALLBACK_TRACE + 2
        after.end()
    finally:
        own.shutdown()
        if other is not None:
            other.shutdown()
