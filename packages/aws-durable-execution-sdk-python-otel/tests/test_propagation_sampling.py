"""Only authoritative operation decisions contribute propagation metadata."""

from datetime import UTC, datetime
from typing import Any

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationEndInfo,
    OperationStartInfo,
    OperationStatus,
    OperationSubType,
    OperationType,
    PropagationInput,
)
from opentelemetry import context
from opentelemetry.sdk.trace import Tracer as SdkTracer, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


START = datetime(2026, 10, 14, tzinfo=UTC)
ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:parent:1/durable-execution/test/id"
)


class _PerNameSampler(Sampler):
    def __init__(self, invocation_sampled: bool) -> None:
        self.invocation_sampled = invocation_sampled
        self.calls: list[str] = []

    def should_sample(
        self,
        parent_context: Any,
        trace_id: int,
        name: str,
        kind: Any = None,
        attributes: Any = None,
        links: Any = None,
        trace_state: Any = None,
    ) -> SamplingResult:
        self.calls.append(name)
        sampled = (
            not self.invocation_sampled
            if name == "invoke-target"
            else self.invocation_sampled
        )
        return SamplingResult(Decision.RECORD_AND_SAMPLE if sampled else Decision.DROP)

    def get_description(self) -> str:
        return "per-name"


@pytest.mark.parametrize("view", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("replaced", [False, True])
@pytest.mark.parametrize("invocation_sampled", [False, True])
def test_deferred_metadata_requires_known_sampling_policy(
    view: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    replaced: bool,
    invocation_sampled: bool,
) -> None:
    before = context.get_current()
    sampler = _PerNameSampler(invocation_sampled)
    provider = TracerProvider(sampler=sampler)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    info = PropagationInput(ARN, "invoke", "target:1")
    operation: dict[str, Any] = dict(
        operation_id="invoke",
        operation_type=OperationType.CHAINED_INVOKE,
        sub_type=OperationSubType.CHAINED_INVOKE,
        name="invoke-target",
        parent_id=None,
        start_time=START,
        is_replayed=False,
    )
    end = InvocationEndInfo(
        request_id="request",
        execution_arn=ARN,
        execution_start_time=START,
        is_first_invocation=True,
        status=InvocationStatus.SUCCEEDED,
    )
    try:
        plugin.on_invocation_start(
            InvocationStartInfo(
                request_id="request",
                execution_arn=ARN,
                execution_start_time=START,
                is_first_invocation=True,
            )
        )
        assert isinstance(plugin._tracer, SdkTracer)
        if replaced:
            plugin._tracer.sampler = sampler
        calls = list(sampler.calls)
        exported = exporter.get_finished_spans()
        early = plugin.provide_propagation_metadata(info)
        assert sampler.calls == calls and exporter.get_finished_spans() == exported
        assert (early is None) is replaced
        plugin.on_operation_start(
            OperationStartInfo(**operation, status=OperationStatus.STARTED)
        )
        calls = list(sampler.calls)
        metadata = plugin.provide_propagation_metadata(info)
        assert sampler.calls == calls
        if replaced and view is ExecutionOtelPlugin:
            assert metadata is None
        else:
            assert metadata is not None and metadata.x_amzn_trace_id is not None
            actual = plugin._get_span("invoke")
            assert actual is not None
            fields = dict(
                part.split("=", 1) for part in metadata.x_amzn_trace_id.split(";")
            )
            assert int(fields["Parent"], 16) == actual.get_span_context().span_id
            assert fields["Sampled"] == str(
                int(actual.get_span_context().trace_flags.sampled)
            )
        plugin.on_operation_end(
            OperationEndInfo(
                **operation, status=OperationStatus.SUCCEEDED, end_time=START
            )
        )
        emitted = [
            span
            for span in exporter.get_finished_spans()
            if span.name == "invoke-target"
        ]
        assert bool(emitted) is (
            not invocation_sampled if replaced else invocation_sampled
        )
    finally:
        plugin.on_invocation_end(end)
        provider.shutdown()
    assert context.get_current() == before
