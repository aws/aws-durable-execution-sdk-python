"""Fallback anchor coverage through the decorator and local durable runner."""

import inspect
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.trace import TraceState

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


class _CaptureInvocationEnd(DurableInstrumentationPlugin):
    def __init__(self, exporter: InMemorySpanExporter) -> None:
        self.exporter = exporter
        self.snapshots: list[tuple[InvocationStatus, tuple[ReadableSpan, ...]]] = []

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.snapshots.append((info.status, tuple(self.exporter.get_finished_spans())))


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("outcome", ["success", "failure", "timeout"])
def test_fallback_anchor_survives_suspension(
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    outcome: str,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(
        BatchSpanProcessor(exporter, schedule_delay_millis=60000)
    )
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    observer = _CaptureInvocationEnd(exporter)
    completed_steps: list[str] = []
    before = context.get_current()

    def step(_step_context: Any) -> str:
        completed_steps.append("step")
        return "stored"

    def handler(_event: Any, durable: DurableContext) -> str:
        value = durable.step(step, name="completed-step")
        durable.wait(
            Duration.from_seconds(60 if outcome == "timeout" else 1), name="wait"
        )
        if outcome == "failure":
            raise ValueError("failed after resume")
        return value

    wrapped = durable_execution(handler, plugins=[plugin, observer])
    try:
        # Published runners use real time; newer workspace runners default to
        # skipping durable waits. Exercise timeout without advancing that wait.
        runner_options: dict[str, Any] = {"handler": wrapped}
        if "skip_time" in inspect.signature(DurableFunctionTestRunner).parameters:
            runner_options["skip_time"] = outcome != "timeout"
        with DurableFunctionTestRunner(**runner_options) as runner:
            arn = runner.run_async(
                input="{}", timeout=3 if outcome == "timeout" else 15
            )
            result = runner.wait_for_result(arn, timeout=10)
        assert completed_steps == ["step"]
        assert observer.snapshots[0][0] is InvocationStatus.PENDING
        first_spans = observer.snapshots[0][1]
        first_roots = [s for s in first_spans if s.name == "DurableExecutionRoot"]
        assert len(first_roots) == 1
        assert not any(s.name == "Workflow" for s in first_spans)
        roots = [
            s for s in exporter.get_finished_spans() if s.name == "DurableExecutionRoot"
        ]
        assert all(s.to_json() == first_roots[0].to_json() for s in roots)
        if outcome == "timeout":
            assert result.status.value == "FAILED"
            assert result.error is not None
            assert "timed out" in (result.error.message or "")
            assert len(observer.snapshots) == 1
            assert len(roots) == 1
            assert not any(s.name == "Workflow" for s in exporter.get_finished_spans())
        else:
            assert result.status.value == (
                "FAILED" if outcome == "failure" else "SUCCEEDED"
            )
            workflows = [
                s for s in exporter.get_finished_spans() if s.name == "Workflow"
            ]
            assert len(workflows) == 1
            assert workflows[0].parent is not None
            assert first_roots[0].context is not None
            assert workflows[0].parent.span_id == first_roots[0].context.span_id
        assert context.get_current() == before
    finally:
        provider.shutdown()


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_public_resume_keeps_anchor_stable_and_processor_sampling_independent(
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
) -> None:
    class Policy(Sampler):
        def __init__(self) -> None:
            self.resolutions: list[str] = []
            self.processor_calls: list[str] = []

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
            if name == "processor-probe":
                self.processor_calls.append(name)
                return SamplingResult(Decision.DROP)
            self.resolutions.append(name)
            number = len(self.resolutions)
            return SamplingResult(
                Decision.RECORD_AND_SAMPLE,
                attributes={"sampler.invocation": number},
                trace_state=TraceState([("vendor", str(number))]),
            )

        def get_description(self) -> str:
            return "public lifecycle sampling policy"

    policy = Policy()
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=policy)
    provider.add_span_processor(
        BatchSpanProcessor(exporter, schedule_delay_millis=60000)
    )
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    observed_probes: list[bool] = []

    class Processor(SpanProcessor):
        def on_start(self, span: Span, parent_context: Any = None) -> None:
            if span.name == "processor-probe":
                return
            with plugin._tracer.start_as_current_span(
                "processor-probe", context=parent_context
            ) as probe:
                observed_probes.append(probe.is_recording())

    provider.add_span_processor(Processor())
    observer = _CaptureInvocationEnd(exporter)
    completed_steps: list[str] = []
    before = context.get_current()

    def step(_step_context: Any) -> str:
        completed_steps.append("once")
        return "stored"

    def handler(_event: Any, durable: DurableContext) -> str:
        result = durable.step(step, name="completed-step")
        durable.wait(Duration.from_seconds(1), name="resume")
        return result

    wrapped = durable_execution(handler, plugins=[plugin, observer])
    try:
        with DurableFunctionTestRunner(handler=wrapped) as runner:
            arn = runner.run_async(input="{}", timeout=15)
            result = runner.wait_for_result(arn, timeout=10)
        assert result.status.value == "SUCCEEDED"
        assert completed_steps == ["once"]
        assert [status for status, _ in observer.snapshots] == [
            InvocationStatus.PENDING,
            InvocationStatus.SUCCEEDED,
        ]
        spans = exporter.get_finished_spans()
        roots = [span for span in spans if span.name == "DurableExecutionRoot"]
        invocations = [span for span in spans if span.name == "Invocation"]
        assert len(roots) == len(invocations) == len(policy.resolutions) == 2
        assert roots[0].to_json() == roots[1].to_json()
        assert roots[0].attributes == {
            "durable.execution.arn": arn,
            "durable.execution.synthetic_root": True,
        }
        assert (
            roots[0].context is not None
            and roots[0].context.trace_state == TraceState()
        )
        for number, invocation in enumerate(invocations, start=1):
            assert invocation.attributes is not None and invocation.context is not None
            assert invocation.attributes["sampler.invocation"] == number
            assert invocation.context.trace_state == TraceState(
                [("vendor", str(number))]
            )
        assert observed_probes and not any(observed_probes)
        assert len(policy.processor_calls) == len(observed_probes)
        assert not [span for span in spans if span.name == "processor-probe"]
        assert context.get_current() == before
    finally:
        provider.shutdown()
