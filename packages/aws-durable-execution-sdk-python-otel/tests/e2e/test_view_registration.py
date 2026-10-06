"""Registration and decorator lifecycle tests for mutually exclusive OTel views."""

import logging
from types import SimpleNamespace
from collections.abc import Iterator
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginProvider,
    DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
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
@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("fail", [False, True])
def test_one_view_and_unrelated_plugin_suspend_resume(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    registration: str,
    fail: bool,
) -> None:
    provider, exporter = telemetry
    observer = _Observer()
    plugins: list[DurableInstrumentationPlugin] = [observer]
    if registration != "explicit":
        name = (
            "otel-execution"
            if plugin_type is ExecutionOtelPlugin
            else "otel-invocation"
        )
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", name)
    if registration != "environment":
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
        result = runner.run(input="{}", timeout=15)
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
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    assert not exporter.get_finished_spans()


class _RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("reverse", [False, True])
def test_rejected_configuration_leaves_no_stale_log_filter(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    registration: str,
    reverse: bool,
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    classes = [ExecutionOtelPlugin, InvocationOtelPlugin]
    names = ["otel-execution", "otel-invocation"]
    if reverse:
        classes.reverse()
        names.reverse()
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    explicit: list[DurableInstrumentationPlugin] = []
    if registration == "explicit":
        explicit = [cls(config) for cls in classes]
    elif registration == "environment":
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", ",".join(names))
    else:
        explicit = [classes[0](config)]
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", names[1])
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(_handler, plugins=explicit)
    assert handler.filters == []
    assert not exporter.get_finished_spans()

    def log_step(_step_context: Any) -> str:
        logging.getLogger("recovery").warning("correlated recovery record")
        return "ok"

    def recovered(_event: Any, durable: DurableContext) -> str:
        return durable.step(log_step, name="logged-step")

    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    wrapped = durable_execution(recovered, plugins=[classes[0](config)])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    record = next(r for r in handler.records if r.name == "recovery")
    attempt = next(
        s for s in exporter.get_finished_spans() if s.name == "logged-step attempt 1"
    )
    assert attempt.context is not None
    assert getattr(record, "traceId", None) == f"{attempt.context.trace_id:032x}"
    assert getattr(record, "spanId", None) == f"{attempt.context.span_id:016x}"
    assert len(handler.filters) == 1


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_rejection_preserves_previously_accepted_plugin_filter(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    accepted = plugin_type(config)
    assert len(handler.filters) == 1
    installed_filter = handler.filters[0]
    other_type = (
        InvocationOtelPlugin
        if plugin_type is ExecutionOtelPlugin
        else ExecutionOtelPlugin
    )
    valid = durable_execution(_handler, plugins=[accepted])
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(_handler, plugins=[accepted, other_type(config)])
    assert handler.filters == [installed_filter]
    with DurableFunctionTestRunner(handler=valid) as runner:
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    assert any(s.name == "Invocation" for s in exporter.get_finished_spans())


def test_execution_constructor_retains_ambient_log_correlation(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _ = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    with provider.get_tracer("startup").start_as_current_span("startup") as ambient:
        ExecutionOtelPlugin(
            OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
        )
        logging.getLogger("startup").warning("constructor correlation")
        record = handler.records[-1]
        assert (
            getattr(record, "traceId", None)
            == f"{ambient.get_span_context().trace_id:032x}"
        )
        assert (
            getattr(record, "spanId", None)
            == f"{ambient.get_span_context().span_id:016x}"
        )


def test_wrong_type_discovered_factory_releases_constructor_filter(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    selected = DurableInstrumentationPluginProvider(
        plugin_type=ExecutionOtelPlugin,
        factory=lambda: InvocationOtelPlugin(config),
        plugin_api_version=DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION,
    )
    entry = SimpleNamespace(
        name="wrong-view", value="test:wrong", dist=None, load=lambda: selected
    )
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        lambda **_: [entry],
    )
    monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", "wrong-view")
    with pytest.raises(
        PluginLoadError,
        match="returned.*InvocationOtelPlugin.*expected.*ExecutionOtelPlugin",
    ):
        durable_execution(_handler)
    assert handler.filters == []
    assert not exporter.get_finished_spans()


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("enrich_logger", [False, True])
def test_rejected_instance_can_be_accepted_without_losing_startup_enrichment(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    enrich_logger: bool,
) -> None:
    from aws_durable_execution_sdk_python.plugin import InvocationStartInfo

    provider, _ = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    plugin = plugin_type(
        OtelPluginConfig(tracer_provider=provider, enrich_logger=enrich_logger)
    )
    other_type = (
        InvocationOtelPlugin
        if plugin_type is ExecutionOtelPlugin
        else ExecutionOtelPlugin
    )
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(
            _handler,
            plugins=[
                plugin,
                other_type(
                    OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
                ),
            ],
        )
    assert handler.filters == []
    # Reuse the exact rejected instance, as a singleton provider can do.
    durable_execution(_handler, plugins=[plugin])
    assert len(handler.filters) == int(enrich_logger)
    durable_execution(_handler, plugins=[plugin])
    assert len(handler.filters) == int(enrich_logger)
    if enrich_logger:
        assert getattr(handler.filters[0], "_plugin") is plugin
    # Missing execution time exits before the invocation-time install path.
    plugin.on_invocation_start(
        InvocationStartInfo(
            execution_arn="test",
            request_id="request",
            is_first_invocation=True,
            execution_start_time=None,
        )
    )
    assert len(handler.filters) == int(enrich_logger)
    if plugin_type is ExecutionOtelPlugin and enrich_logger:
        with provider.get_tracer("startup").start_as_current_span("startup") as ambient:
            logging.getLogger("startup-recovery").warning("after re-registration")
            assert (
                getattr(handler.records[-1], "traceId")
                == f"{ambient.get_span_context().trace_id:032x}"
            )


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_legacy_otel_subclass_is_not_implicitly_opted_in(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
) -> None:
    provider, _ = telemetry
    calls: list[bool] = []
    legacy_type = type(
        "LegacyOtelSubclass",
        (plugin_type,),
        {
            "exclusive_group": 42,
            "on_registration_result": lambda self, accepted: calls.append(accepted),
        },
    )
    plugin = legacy_type(
        OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    )
    handler = durable_execution(_handler, plugins=[plugin])
    with DurableFunctionTestRunner(handler=handler) as runner:
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    assert calls == []
