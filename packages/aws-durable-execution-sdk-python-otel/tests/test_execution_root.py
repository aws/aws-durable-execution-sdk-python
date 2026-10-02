"""Fallback ancestry regressions against real OTel providers and exporters."""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)
from opentelemetry import context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    Decision,
    Sampler,
    SamplingResult,
)
from opentelemetry.trace import SpanKind, StatusCode, TraceState

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


START = datetime(2026, 10, 1, tzinfo=UTC)
ARN = "arn:aws:lambda:us-west-2:123456789012:function:root:1/durable-execution/test/id"
TRACE = 0x12345678901234567890123456789012
PluginType = type[ExecutionOtelPlugin] | type[InvocationOtelPlugin]


@pytest.fixture(params=[ExecutionOtelPlugin, InvocationOtelPlugin])
def plugin_type(request: pytest.FixtureRequest) -> PluginType:
    return request.param


@pytest.fixture(autouse=True)
def balanced_context() -> Iterator[None]:
    before = context.get_current()
    yield
    assert context.get_current() == before


def create_plugin(
    plugin_type: PluginType,
    extracted: ExtractedContext | None = None,
    sampler: Sampler = ALWAYS_ON,
    resource: Resource | None = None,
    batch: bool = False,
) -> tuple[
    ExecutionOtelPlugin | InvocationOtelPlugin, TracerProvider, InMemorySpanExporter
]:
    provider = TracerProvider(sampler=sampler, resource=resource)
    exporter = InMemorySpanExporter()
    processor = (
        BatchSpanProcessor(exporter, schedule_delay_millis=60000)
        if batch
        else SimpleSpanProcessor(exporter)
    )
    provider.add_span_processor(processor)
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: extracted,
            enrich_logger=False,
        )
    )
    return plugin, provider, exporter


def start_info(
    arn: str = ARN, first: bool = True, request: str = "request"
) -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id=request,
        execution_arn=arn,
        execution_start_time=START,
        is_first_invocation=first,
    )


def end_info(
    status: InvocationStatus,
    arn: str = ARN,
    first: bool = True,
    request: str = "request",
) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id=request,
        execution_arn=arn,
        execution_start_time=START,
        is_first_invocation=first,
        status=status,
    )


@pytest.mark.parametrize(
    "extracted",
    [
        None,
        ExtractedContext(0, 0),
        ExtractedContext(2**128, 2**64),
        ExtractedContext(TRACE, None),
        ExtractedContext(TRACE, 0),
    ],
)
@pytest.mark.parametrize("status", list(InvocationStatus))
def test_sampled_anchor_available_before_first_invocation_returns(
    plugin_type: PluginType,
    extracted: ExtractedContext | None,
    status: InvocationStatus,
) -> None:
    plugin, provider, exporter = create_plugin(plugin_type, extracted)
    try:
        plugin.on_invocation_start(start_info())
        roots = exporter.get_finished_spans()
        assert len(roots) == 1
        root = roots[0]
        assert root.name == "DurableExecutionRoot"
        assert root.context is not None
        assert root.context.span_id == derive_execution_root_span_id(ARN)
        assert root.parent is None
        assert (
            root.start_time == root.end_time == int(START.timestamp() * 1_000_000_000)
        )
        assert root.status.status_code is StatusCode.UNSET
        assert root.kind is SpanKind.INTERNAL
        assert root.attributes == {
            "durable.execution.arn": ARN,
            "durable.execution.synthetic_root": True,
        }
        plugin.on_invocation_end(end_info(status))
        spans = exporter.get_finished_spans()
        assert len([s for s in spans if s.name == "DurableExecutionRoot"]) == 1
        for span in spans:
            if span.name in ("Invocation", "Workflow"):
                assert span.parent is not None
                assert span.parent.span_id == root.context.span_id
                assert span.context is not None
                assert span.context.trace_id == root.context.trace_id
    finally:
        provider.shutdown()


@pytest.mark.parametrize("status", [InvocationStatus.PENDING, InvocationStatus.RETRY])
def test_nonterminal_end_flushes_anchor(
    plugin_type: PluginType, status: InvocationStatus
) -> None:
    plugin, provider, exporter = create_plugin(plugin_type, batch=True)
    try:
        plugin.on_invocation_start(start_info())
        plugin.on_invocation_end(end_info(status))
        assert {s.name for s in exporter.get_finished_spans()} == {
            "DurableExecutionRoot",
            "Invocation",
        }
    finally:
        provider.shutdown()


def test_retry_and_resume_keep_sdk_fields_and_provider_resources(
    plugin_type: PluginType,
) -> None:
    roots = []
    for index, status in enumerate(
        [InvocationStatus.RETRY, InvocationStatus.PENDING, InvocationStatus.FAILED]
    ):
        resource = Resource(
            {
                "service.name": "function",
                "faas.instance": f"sandbox-{index}",
                "process.pid": index,
            }
        )
        plugin, provider, exporter = create_plugin(plugin_type, resource=resource)
        try:
            # A first-invocation retry is still first; a later terminal resume is not.
            plugin.on_invocation_start(
                start_info(first=index < 2, request=f"request-{index}")
            )
            plugin.on_invocation_end(
                end_info(status, first=index < 2, request=f"request-{index}")
            )
            spans = exporter.get_finished_spans()
            root = next(s for s in spans if s.name == "DurableExecutionRoot")
            assert root.attributes is not None
            assert root.context is not None
            roots.append(
                (
                    root.context.trace_id,
                    root.context.span_id,
                    root.parent,
                    root.start_time,
                    root.end_time,
                    dict(root.attributes),
                    root.status.status_code,
                    root.kind,
                    root.events,
                    root.links,
                )
            )
            invocation = next(s for s in spans if s.name == "Invocation")
            assert invocation.resource == provider.resource
            assert invocation.resource.attributes["faas.instance"] == f"sandbox-{index}"
            assert root.resource == provider.resource
            assert root.resource.attributes["service.name"] == "function"
            assert root.resource.attributes["faas.instance"] == f"sandbox-{index}"
        finally:
            provider.shutdown()
    assert roots[0] == roots[1] == roots[2]


@pytest.mark.parametrize(
    ("extracted", "sampler", "expected"),
    [
        (ExtractedContext(TRACE, None, Sampling.NOT_SAMPLED), ALWAYS_ON, False),
        (ExtractedContext(TRACE, None, Sampling.SAMPLED), ALWAYS_OFF, True),
        (None, ALWAYS_OFF, False),
        (None, ALWAYS_ON, True),
        (ExtractedContext(TRACE, 42, Sampling.SAMPLED), ALWAYS_ON, False),
    ],
)
def test_sampling_and_external_parent_ownership(
    plugin_type: PluginType,
    extracted: ExtractedContext | None,
    sampler: Sampler,
    expected: bool,
) -> None:
    plugin, provider, exporter = create_plugin(plugin_type, extracted, sampler)
    try:
        plugin.on_invocation_start(start_info())
        plugin.on_invocation_end(end_info(InvocationStatus.SUCCEEDED))
        spans = exporter.get_finished_spans()
        assert bool([s for s in spans if s.name == "DurableExecutionRoot"]) is expected
        if extracted is not None and extracted.has_complete_remote_parent:
            assert all(s.parent is not None and s.parent.span_id == 42 for s in spans)
        elif not expected:
            assert not spans
    finally:
        provider.shutdown()


def test_shared_trace_retains_execution_scoped_root_identity(
    plugin_type: PluginType,
) -> None:
    plugin, provider, exporter = create_plugin(
        plugin_type, ExtractedContext(TRACE, None, Sampling.SAMPLED)
    )
    try:
        for arn in [ARN, ARN + "-other"]:
            plugin.on_invocation_start(start_info(arn=arn))
            plugin.on_invocation_end(end_info(InvocationStatus.SUCCEEDED, arn=arn))
        roots = [
            s for s in exporter.get_finished_spans() if s.name == "DurableExecutionRoot"
        ]
        assert len(roots) == 2
        assert all(s.context is not None and s.context.trace_id == TRACE for s in roots)
        assert {s.context.span_id for s in roots if s.context is not None} == {
            derive_execution_root_span_id(ARN),
            derive_execution_root_span_id(ARN + "-other"),
        }
    finally:
        provider.shutdown()


def test_later_sampled_invocation_recovers_unsampled_first(
    plugin_type: PluginType,
) -> None:
    plugin, provider, exporter = create_plugin(plugin_type, sampler=ALWAYS_OFF)
    try:
        plugin.on_invocation_start(start_info())
        plugin.on_invocation_end(end_info(InvocationStatus.PENDING))
        assert not exporter.get_finished_spans()
    finally:
        provider.shutdown()
    plugin, provider, exporter = create_plugin(plugin_type)
    try:
        plugin.on_invocation_start(start_info(first=False))
        plugin.on_invocation_end(end_info(InvocationStatus.SUCCEEDED, first=False))
        assert any(
            s.name == "DurableExecutionRoot" for s in exporter.get_finished_spans()
        )
    finally:
        provider.shutdown()


class _CountingSampler(Sampler):
    def __init__(self, decision: Decision) -> None:
        self.calls = 0
        self.decision = decision

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
        self.calls += 1
        return SamplingResult(
            self.decision,
            attributes={"sampler.call": self.calls},
            trace_state=TraceState([("vendor", str(self.calls))]),
        )

    def get_description(self) -> str:
        return "CountingSampler"


@pytest.mark.parametrize(
    "decision", [Decision.RECORD_AND_SAMPLE, Decision.RECORD_ONLY, Decision.DROP]
)
def test_root_preserves_resolved_sampling_without_resampling(
    plugin_type: PluginType,
    decision: Decision,
) -> None:
    sampler = _CountingSampler(decision)
    plugin, provider, exporter = create_plugin(plugin_type, sampler=sampler)
    try:
        plugin.on_invocation_start(start_info())
        plugin.on_invocation_end(end_info(InvocationStatus.PENDING))
        assert sampler.calls == 1
        roots = [
            s for s in exporter.get_finished_spans() if s.name == "DurableExecutionRoot"
        ]
        if decision is Decision.RECORD_AND_SAMPLE:
            assert len(roots) == 1
            root = roots[0]
            assert root.attributes is not None
            assert root.attributes["sampler.call"] == 1
            assert root.context is not None
            assert root.context.trace_state == TraceState([("vendor", "1")])
        else:
            assert not roots
    finally:
        provider.shutdown()
