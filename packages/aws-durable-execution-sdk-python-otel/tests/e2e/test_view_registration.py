"""Registration and decorator lifecycle tests for mutually exclusive OTel views."""

from collections.abc import Iterator
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.fixture
def telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    before = context.get_current()
    yield provider, exporter
    assert context.get_current() == before
    provider.shutdown()


def _handler(_event: Any, _context: DurableContext) -> str:
    return "unused"


@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("reverse", [False, True])
def test_competing_views_rejected_before_hooks(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    registration: str,
    reverse: bool,
) -> None:
    provider, exporter = telemetry
    classes = [ExecutionOtelPlugin, InvocationOtelPlugin]
    names = ["otel-execution", "otel-invocation"]
    if reverse:
        classes.reverse()
        names.reverse()
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    explicit: list[DurableInstrumentationPlugin] = []
    if registration == "explicit":
        explicit = [cls(config) for cls in classes]
    elif registration == "environment":
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", ",".join(names))
    else:
        explicit = [classes[0](config)]
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", names[1])
    before = context.get_current()
    with pytest.raises(PluginLoadError) as error:
        durable_execution(_handler, plugins=explicit)
    assert "ExecutionOtelPlugin" in str(error.value)
    assert "InvocationOtelPlugin" in str(error.value)
    assert "Keep only one" in str(error.value)
    assert context.get_current() == before
    assert not exporter.get_finished_spans()


class _Observer(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self.statuses: list[InvocationStatus] = []

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.statuses.append(info.status)


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("from_environment", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_one_view_and_unrelated_plugin_suspend_resume(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    from_environment: bool,
    fail: bool,
) -> None:
    provider, exporter = telemetry
    observer = _Observer()
    plugins: list[DurableInstrumentationPlugin] = [observer]
    if from_environment:
        name = (
            "otel-execution"
            if plugin_type is ExecutionOtelPlugin
            else "otel-invocation"
        )
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", name)
    else:
        plugins.append(
            plugin_type(OtelPluginConfig(tracer_provider=provider, enrich_logger=False))
        )
    calls: list[str] = []

    def work(_step_context: Any) -> str:
        calls.append("step")
        with provider.get_tracer("customer").start_as_current_span("customer"):
            return "saved"

    def handler(_event: Any, durable: DurableContext) -> str:
        result = durable.step(work, name="before-wait")
        durable.wait(Duration.from_seconds(1), name="wait")
        if fail:
            raise ValueError("terminal failure")
        return result

    wrapped = durable_execution(handler, plugins=plugins)
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        result = runner.run(input="{}", execution_timeout=15)
    assert result.status.value == ("FAILED" if fail else "SUCCEEDED")
    assert calls == ["step"]
    assert InvocationStatus.PENDING in observer.statuses
    spans = exporter.get_finished_spans()
    workflows = [span for span in spans if span.name == "Workflow"]
    invocations = [span for span in spans if span.name == "Invocation"]
    assert len(workflows) == 1
    assert len(invocations) == len(observer.statuses)
    assert workflows[0].status.status_code is (
        trace.StatusCode.ERROR if fail else trace.StatusCode.OK
    )
    customer = next(span for span in spans if span.name == "customer")
    assert customer.parent is not None
    assert any(
        span.context is not None and span.context.span_id == customer.parent.span_id
        for span in spans
    )


def test_no_otel_plugin_remains_valid(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    _, exporter = telemetry
    handler = durable_execution(_handler, plugins=[_Observer()])
    with DurableFunctionTestRunner(handler=handler) as runner:
        assert runner.run(input="{}", execution_timeout=15).status.value == "SUCCEEDED"
    assert not exporter.get_finished_spans()
