"""Pure propagation production from both views and the actual operation context."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from aws_durable_execution_sdk_python import plugin as core_plugin
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    InvocationEndInfo,
    InvocationStatus,
    OperationStartInfo,
    OperationEndInfo,
    OperationStatus,
    OperationSubType,
    OperationType,
    PluginExecutor,
    PropagationInput,
)
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider, SpanProcessor, Span
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON, Sampler
from opentelemetry.trace import NoOpTracerProvider

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
    ExtractedContext,
    Sampling,
)
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    operation_id_to_span_id,
)


START = datetime(2026, 10, 2, tzinfo=UTC)
END = START + timedelta(seconds=1)
ARN = "arn:aws:lambda:us-west-2:123456789012:function:propagation:1/durable-execution/test/id"
TRACE_ID = 0x12345678901234567890123456789012
INPUT = PropagationInput(ARN, "invoke-1", "target:1")
PluginType = type[ExecutionOtelPlugin] | type[InvocationOtelPlugin]


class _Starts(SpanProcessor):
    def __init__(self) -> None:
        self.count = 0

    def on_start(
        self, span: Span, parent_context: context.Context | None = None
    ) -> None:
        self.count += 1


@pytest.fixture(params=[ExecutionOtelPlugin, InvocationOtelPlugin])
def plugin_type(request: pytest.FixtureRequest) -> PluginType:
    return request.param


@pytest.fixture(autouse=True)
def balanced_context() -> Iterator[None]:
    before = context.get_current()
    yield
    assert context.get_current() == before


def start_info(first: bool = True) -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id="request",
        execution_arn=ARN,
        execution_start_time=START,
        is_first_invocation=first,
    )


def end_info(
    status: InvocationStatus = InvocationStatus.SUCCEEDED,
) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id="request",
        execution_arn=ARN,
        execution_start_time=START,
        is_first_invocation=True,
        status=status,
    )


def start_operation(
    plugin: ExecutionOtelPlugin | InvocationOtelPlugin, replayed: bool = False
) -> None:
    plugin.on_operation_start(
        OperationStartInfo(
            operation_id=INPUT.operation_id,
            operation_type=OperationType.CHAINED_INVOKE,
            sub_type=OperationSubType.CHAINED_INVOKE,
            name="invoke-target",
            parent_id=None,
            start_time=START,
            is_replayed=replayed,
            status=OperationStatus.STARTED,
        )
    )


def end_operation(plugin: ExecutionOtelPlugin | InvocationOtelPlugin) -> None:
    plugin.on_operation_end(
        OperationEndInfo(
            operation_id=INPUT.operation_id,
            operation_type=OperationType.CHAINED_INVOKE,
            sub_type=OperationSubType.CHAINED_INVOKE,
            name="invoke-target",
            parent_id=None,
            start_time=START,
            end_time=END,
            is_replayed=False,
            status=OperationStatus.SUCCEEDED,
        )
    )


@pytest.mark.parametrize(
    ("extracted", "sampler", "sampled"),
    [
        (ExtractedContext(TRACE_ID, 42, Sampling.SAMPLED), ALWAYS_OFF, True),
        (ExtractedContext(TRACE_ID, 42, Sampling.NOT_SAMPLED), ALWAYS_ON, False),
        (None, ALWAYS_ON, True),
        (None, ALWAYS_OFF, False),
    ],
)
def test_collector_encodes_operation_parent_without_side_effects(
    plugin_type: PluginType,
    extracted: ExtractedContext | None,
    sampler: Sampler,
    sampled: bool,
) -> None:
    provider = TracerProvider(sampler=sampler)
    exporter = InMemorySpanExporter()
    starts = _Starts()
    provider.add_span_processor(starts)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: extracted,
            enrich_logger=False,
        )
    )
    collector = PluginExecutor([DurableInstrumentationPlugin(), plugin])
    assert collector.provide_propagation_metadata(INPUT).x_amzn_trace_id is None
    plugin.on_invocation_start(start_info())
    try:
        starts_before = starts.count
        current_before = trace.get_current_span()
        exported_before = exporter.get_finished_spans()
        metadata = collector.provide_propagation_metadata(INPUT)
        assert starts.count == starts_before
        assert trace.get_current_span() is current_before
        assert exporter.get_finished_spans() == exported_before
        assert metadata.x_amzn_trace_id is not None
        fields = dict(
            part.split("=", 1) for part in metadata.x_amzn_trace_id.split(";")
        )
        assert fields["Sampled"] == ("1" if sampled else "0")
        parent_id = int(fields["Parent"], 16)
        assert parent_id == operation_id_to_span_id(ARN, INPUT.operation_id)
        assert parent_id != 42
        if extracted is not None:
            assert fields["Root"] == "1-12345678-901234567890123456789012"
        start_operation(plugin)
        starts_before = starts.count
        # A different active ambient trace must not replace execution ownership.
        ambient_provider = TracerProvider()
        try:
            with ambient_provider.get_tracer("unrelated").start_as_current_span(
                "ambient", context=context.Context()
            ) as ambient:
                assert collector.provide_propagation_metadata(INPUT) == metadata
                assert trace.get_current_span() is ambient
        finally:
            ambient_provider.shutdown()
        assert starts.count == starts_before
        assert (
            collector.provide_propagation_metadata(
                PropagationInput("other", "invoke-1", "target")
            ).x_amzn_trace_id
            is None
        )
        end_operation(plugin)
        plugin.on_invocation_end(end_info())
        if sampled:
            operation = next(
                s for s in exporter.get_finished_spans() if s.name == "invoke-target"
            )
            assert operation.context is not None
            assert operation.context.span_id == parent_id
            trace_hex = f"{operation.context.trace_id:032x}"
            assert fields["Root"] == f"1-{trace_hex[:8]}-{trace_hex[8:]}"
        else:
            assert not exporter.get_finished_spans()
        assert collector.provide_propagation_metadata(INPUT).x_amzn_trace_id is None
    finally:
        plugin.on_invocation_end(end_info())
        provider.shutdown()


def test_invocation_continuation_uses_actual_fresh_operation_span() -> None:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    try:
        plugin.on_invocation_start(start_info())
        start_operation(plugin)
        first = plugin.provide_propagation_metadata(INPUT)
        plugin.on_invocation_end(end_info(InvocationStatus.PENDING))
        plugin.on_invocation_start(start_info(first=False))
        start_operation(plugin, replayed=True)
        continued = plugin.provide_propagation_metadata(INPUT)
        assert first is not None and continued is not None
        assert continued.x_amzn_trace_id is not None
        assert first.x_amzn_trace_id != continued.x_amzn_trace_id
        parent = int(
            dict(p.split("=", 1) for p in continued.x_amzn_trace_id.split(";"))[
                "Parent"
            ],
            16,
        )
        end_operation(plugin)
        plugin.on_invocation_end(end_info())
        operations = [
            s for s in exporter.get_finished_spans() if s.name == "invoke-target"
        ]
        assert operations[-1].context is not None
        assert operations[-1].context.span_id == parent
    finally:
        provider.shutdown()


def test_unbound_provider_does_not_produce_metadata(
    plugin_type: PluginType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: NoOpTracerProvider())
    plugin = plugin_type(
        OtelPluginConfig(
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    plugin.on_invocation_start(start_info())
    assert plugin.provide_propagation_metadata(INPUT) is None
    plugin.on_invocation_end(end_info())


def test_resume_preserves_logical_identity_and_rejects_old_execution(
    plugin_type: PluginType,
) -> None:
    provider = TracerProvider()
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    try:
        plugin.on_invocation_start(start_info())
        first = plugin.provide_propagation_metadata(INPUT)
        plugin.on_invocation_end(end_info(InvocationStatus.RETRY))
        plugin.on_invocation_start(start_info(first=False))
        assert plugin.provide_propagation_metadata(INPUT) == first
        plugin.on_invocation_end(end_info())
        other_start = InvocationStartInfo(
            request_id="other",
            execution_arn="other-execution",
            execution_start_time=START,
            is_first_invocation=True,
        )
        plugin.on_invocation_start(other_start)
        assert plugin.provide_propagation_metadata(INPUT) is None
        plugin.on_invocation_end(end_info())
    finally:
        provider.shutdown()


def test_failed_invocation_setup_cannot_supply_stale_metadata(
    plugin_type: PluginType,
) -> None:
    provider = TracerProvider()
    fail = False

    def extract(_info: InvocationStartInfo) -> ExtractedContext | None:
        if fail:
            raise ValueError("invalid extraction")
        return None

    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider, context_extractor=extract, enrich_logger=False
        )
    )
    try:
        plugin.on_invocation_start(start_info())
        assert plugin.provide_propagation_metadata(INPUT) is not None
        plugin.on_invocation_end(end_info())
        fail = True
        with pytest.raises(ValueError, match="invalid extraction"):
            plugin.on_invocation_start(start_info(first=False))
        assert plugin.provide_propagation_metadata(INPUT) is None
        plugin.on_invocation_end(end_info())
    finally:
        provider.shutdown()


def test_legacy_core_keeps_tracing_without_propagation_contract(
    plugin_type: PluginType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(core_plugin, "PropagationMetadata")
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    try:
        plugin.on_invocation_start(start_info())
        assert plugin.provide_propagation_metadata(INPUT) is None
        start_operation(plugin)
        end_operation(plugin)
        plugin.on_invocation_end(end_info())
        assert {"invoke-target", "Invocation", "Workflow"} <= {
            s.name for s in exporter.get_finished_spans()
        }
    finally:
        provider.shutdown()
