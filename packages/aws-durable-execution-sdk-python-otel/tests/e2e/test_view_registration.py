"""Registration and decorator lifecycle tests for mutually exclusive OTel views."""

import logging
from types import SimpleNamespace
from collections.abc import Iterator
from typing import Any, cast

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.exceptions import PluginLoadError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStartInfo,
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
    ExecutionOtelPluginFactory,
    InvocationOtelPlugin,
    InvocationOtelPluginFactory,
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


class _InheritedExecutionFactory(ExecutionOtelPluginFactory):
    """OTel wrapper that inherits the built-in factory registration metadata."""

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        super().__init__(config)
        self.instances: list[DurableInstrumentationPlugin] = []

    def create_plugin(self, info: InvocationStartInfo) -> ExecutionOtelPlugin:
        plugin = super().create_plugin(info)
        self.instances.append(plugin)
        return plugin


class _InheritedInvocationFactory(InvocationOtelPluginFactory):
    """OTel wrapper that inherits the built-in factory registration metadata."""

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        super().__init__(config)
        self.instances: list[DurableInstrumentationPlugin] = []

    def create_plugin(self, info: InvocationStartInfo) -> InvocationOtelPlugin:
        plugin = super().create_plugin(info)
        self.instances.append(plugin)
        return plugin


class _ComposedExecutionFactory:
    """OTel composition wrapper that declares the registration contract."""

    __durable_registration_api__ = 1
    exclusive_group = "aws-durable-execution-otel-view"

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self._delegate = ExecutionOtelPluginFactory(config)
        self.instances: list[DurableInstrumentationPlugin] = []

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        plugin = self._delegate.create_plugin(info)
        self.instances.append(plugin)
        return plugin


class _ComposedInvocationFactory:
    """OTel composition wrapper that declares the registration contract."""

    __durable_registration_api__ = 1
    exclusive_group = "aws-durable-execution-otel-view"

    def __init__(self, config: OtelPluginConfig | None = None) -> None:
        self._delegate = InvocationOtelPluginFactory(config)
        self.instances: list[DurableInstrumentationPlugin] = []

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        plugin = self._delegate.create_plugin(info)
        self.instances.append(plugin)
        return plugin


def _wrapped_execution_factory(
    config: OtelPluginConfig, style: str
) -> DurableInstrumentationPluginFactory:
    if style == "inherit":
        return _InheritedExecutionFactory(config)
    return _ComposedExecutionFactory(config)


def _wrapped_invocation_factory(
    config: OtelPluginConfig, style: str
) -> DurableInstrumentationPluginFactory:
    if style == "inherit":
        return _InheritedInvocationFactory(config)
    return _ComposedInvocationFactory(config)


def _created_plugins(factory: DurableInstrumentationPluginFactory) -> list[object]:
    return list(getattr(factory, "instances", []))


@pytest.mark.parametrize("wrapped", ["execution", "invocation", "both"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("style", ["inherit", "compose"])
def test_wrapper_factory_cannot_bypass_view_exclusion(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    wrapped: str,
    reverse: bool,
    style: str,
) -> None:
    provider, exporter = telemetry
    created: list[str] = []
    handler_calls: list[str] = []
    original_execution_create = ExecutionOtelPluginFactory.create_plugin
    original_invocation_create = InvocationOtelPluginFactory.create_plugin

    def create_execution(
        self: ExecutionOtelPluginFactory, info: InvocationStartInfo
    ) -> DurableInstrumentationPlugin:
        created.append("execution")
        return original_execution_create(self, info)

    def create_invocation(
        self: InvocationOtelPluginFactory, info: InvocationStartInfo
    ) -> DurableInstrumentationPlugin:
        created.append("invocation")
        return original_invocation_create(self, info)

    monkeypatch.setattr(ExecutionOtelPluginFactory, "create_plugin", create_execution)
    monkeypatch.setattr(InvocationOtelPluginFactory, "create_plugin", create_invocation)

    def handler(_event: Any, _context: DurableContext) -> str:
        handler_calls.append("ran")
        return "unused"

    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    execution: DurableInstrumentationPluginFactory = (
        _wrapped_execution_factory(config, style)
        if wrapped in {"execution", "both"}
        else ExecutionOtelPluginFactory(config)
    )
    invocation: DurableInstrumentationPluginFactory = (
        _wrapped_invocation_factory(config, style)
        if wrapped in {"invocation", "both"}
        else InvocationOtelPluginFactory(config)
    )
    observer = _ObserverFactory()
    plugins: list[DurableInstrumentationPluginFactory] = [
        execution,
        invocation,
        observer,
    ]
    if reverse:
        plugins.reverse()

    with pytest.raises(PluginLoadError) as error:
        durable_execution(handler, plugins=plugins)
    message = str(error.value)
    assert type(execution).__qualname__ in message
    assert type(invocation).__qualname__ in message
    assert "mutually exclusive" in message
    assert "Keep only one" in message
    assert "aws-durable-execution-otel-view" in message
    assert created == []
    assert _created_plugins(execution) == []
    assert _created_plugins(invocation) == []
    assert observer.instances == []
    assert observer.statuses == []
    assert handler_calls == []
    assert not exporter.get_finished_spans()


@pytest.mark.parametrize(
    "wrapper",
    [
        _InheritedExecutionFactory,
        _InheritedInvocationFactory,
        _ComposedExecutionFactory,
        _ComposedInvocationFactory,
    ],
)
def test_single_wrapped_view_with_unrelated_plugin_is_accepted(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    wrapper: type[Any],
) -> None:
    provider, exporter = telemetry
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    factory = wrapper(config)
    observer = _ObserverFactory()
    handler_calls: list[str] = []

    def handler(_event: Any, _context: DurableContext) -> str:
        handler_calls.append("ran")
        return "unused"

    configured = durable_execution(handler, plugins=[factory, observer])
    with DurableFunctionTestRunner(handler=configured) as runner:
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    assert handler_calls == ["ran"]
    assert len(factory.instances) == 1
    assert observer.statuses == [InvocationStatus.SUCCEEDED]
    assert any(
        span.name in {"Workflow", "Invocation"}
        for span in exporter.get_finished_spans()
    )


@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("reverse", [False, True])
def test_competing_views_rejected_before_hooks(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    registration: str,
    reverse: bool,
) -> None:
    provider, exporter = telemetry
    classes: list[
        type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory]
    ] = [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
    names = ["otel-execution", "otel-invocation"]
    if reverse:
        classes.reverse()
        names.reverse()
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    explicit: list[DurableInstrumentationPluginFactory] = []
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
    assert "ExecutionOtelPluginFactory" in str(error.value)
    assert "InvocationOtelPluginFactory" in str(error.value)
    assert "Keep only one" in str(error.value)
    assert context.get_current() == before
    assert not exporter.get_finished_spans()


class _Observer(DurableInstrumentationPlugin):
    def __init__(self, statuses: list[InvocationStatus]) -> None:
        self.statuses = statuses

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.statuses.append(info.status)


class _ObserverFactory:
    def __init__(self) -> None:
        self.statuses: list[InvocationStatus] = []
        self.instances: list[_Observer] = []

    def create_plugin(self, _info: InvocationStartInfo) -> _Observer:
        plugin = _Observer(self.statuses)
        self.instances.append(plugin)
        return plugin


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("fail", [False, True])
def test_one_view_and_unrelated_plugin_suspend_resume(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    registration: str,
    fail: bool,
) -> None:
    provider, exporter = telemetry
    observer = _ObserverFactory()
    plugins: list[DurableInstrumentationPluginFactory] = [observer]
    name = (
        "otel-execution"
        if plugin_type is ExecutionOtelPluginFactory
        else "otel-invocation"
    )
    factory = plugin_type(
        OtelPluginConfig(tracer_provider=provider, enrich_logger=False)
    )
    if registration != "explicit":
        # The v2 factory API deduplicates the same factory object. Its result
        # type cannot be inspected without constructing an invocation plugin.
        entry = SimpleNamespace(
            name=name, value=f"test:{name}", dist=None, load=lambda: factory
        )
        monkeypatch.setattr(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            lambda **_: [entry],
        )
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", name)
    if registration != "environment":
        plugins.append(factory)
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
    assert len(observer.instances) == len(observer.statuses)
    assert len({id(plugin) for plugin in observer.instances}) == len(observer.instances)
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
    handler = durable_execution(_handler, plugins=[_ObserverFactory()])
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
    classes: list[
        type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory]
    ] = [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
    names = ["otel-execution", "otel-invocation"]
    if reverse:
        classes.reverse()
        names.reverse()
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    explicit: list[DurableInstrumentationPluginFactory] = []
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


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_rejection_preserves_previously_accepted_plugin_filter(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    accepted = plugin_type(config)
    assert handler.filters == []
    other_type = (
        InvocationOtelPluginFactory
        if plugin_type is ExecutionOtelPluginFactory
        else ExecutionOtelPluginFactory
    )
    valid = durable_execution(_handler, plugins=[accepted])
    with DurableFunctionTestRunner(handler=valid) as runner:
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    assert len(handler.filters) == 1
    installed_filter = handler.filters[0]
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(_handler, plugins=[accepted, other_type(config)])
    assert handler.filters == [installed_filter]
    with DurableFunctionTestRunner(handler=valid) as runner:
        assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
    assert (
        len([s for s in exporter.get_finished_spans() if s.name == "Invocation"]) == 2
    )


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_factory_registration_leaves_ambient_logging_untouched(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    with provider.get_tracer("startup").start_as_current_span("startup") as ambient:
        factory = plugin_type(
            OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
        )
        durable_execution(_handler, plugins=[factory])
        logging.getLogger("startup").warning("before invocation")
        assert handler.filters == []
        assert not hasattr(handler.records[-1], "traceId")
        assert trace.get_current_span() is ambient
        assert not exporter.get_finished_spans()


def test_invalid_discovered_registration_does_not_construct_a_plugin(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    factory_calls: list[str] = []

    # A bare callable is not a factory object in v2. Reject it at registration,
    # before it could allocate the old provider's constructor resources.
    def invalid_factory():
        factory_calls.append("constructed")
        raise AssertionError("Registration called an invalid factory")

    entry = SimpleNamespace(
        name="wrong-view", value="test:wrong", dist=None, load=lambda: invalid_factory
    )
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        lambda **_: [entry],
    )
    monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", "wrong-view")
    with pytest.raises(PluginLoadError, match="must.*plugin factory"):
        durable_execution(_handler)
    assert factory_calls == []
    assert handler.filters == []
    assert not exporter.get_finished_spans()


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("enrich_logger", [False, True])
def test_rejected_factory_can_be_accepted_with_fresh_invocation_plugins(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    enrich_logger: bool,
) -> None:
    provider, exporter = telemetry
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    instances: list[DurableInstrumentationPlugin] = []

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        plugin = plugin_type.create_plugin(self, info)
        instances.append(plugin)
        return plugin

    recording_type = type(
        "RecordingFactory", (plugin_type,), {"create_plugin": create_plugin}
    )
    factory = recording_type(
        OtelPluginConfig(tracer_provider=provider, enrich_logger=enrich_logger)
    )
    other_type = (
        InvocationOtelPluginFactory
        if plugin_type is ExecutionOtelPluginFactory
        else ExecutionOtelPluginFactory
    )
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(_handler, plugins=[factory, other_type()])
    assert handler.filters == []
    assert instances == []
    wrapped = durable_execution(_handler, plugins=[factory])
    assert instances == []
    for _ in range(2):
        with DurableFunctionTestRunner(handler=wrapped) as runner:
            assert runner.run(input="{}", timeout=15).status.value == "SUCCEEDED"
        assert len(handler.filters) == int(enrich_logger)
    assert len(instances) == 2
    assert instances[0] is not instances[1]
    assert (
        len([s for s in exporter.get_finished_spans() if s.name == "Invocation"]) == 2
    )


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
def test_legacy_otel_subclass_keeps_same_named_helpers_inert(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
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


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("legacy_attributes", [False, True])
def test_subclass_inherits_view_exclusion_and_rejection_cleanup(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    registration: str,
    legacy_attributes: bool,
) -> None:
    provider, exporter = telemetry
    calls: list[bool] = []
    subclass = type(
        "CustomOtelView",
        (plugin_type,),
        {
            "exclusive_group": 42,
            "on_registration_result": lambda self, accepted: calls.append(accepted),
        }
        if legacy_attributes
        else {},
    )
    other_type = (
        InvocationOtelPluginFactory
        if plugin_type is ExecutionOtelPluginFactory
        else ExecutionOtelPluginFactory
    )
    handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [handler])
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    factory_calls: list[str] = []

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        factory_calls.append("constructed")
        return plugin_type.create_plugin(self, info)

    monkeypatch.setattr(subclass, "create_plugin", create_plugin)
    selected = subclass(config)
    entry = SimpleNamespace(
        name="custom-view", value="test:custom", dist=None, load=lambda: selected
    )
    explicit: list[DurableInstrumentationPluginFactory]
    if registration == "environment":
        monkeypatch.setattr(
            "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
            lambda **_: [entry],
        )
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", "custom-view")
        explicit = [other_type(config)]
    elif registration == "mixed":
        monkeypatch.setenv(
            "DURABLE_EXECUTION_PLUGINS",
            "otel-invocation"
            if other_type is InvocationOtelPluginFactory
            else "otel-execution",
        )
        explicit = [subclass(config)]
    else:
        explicit = [subclass(config), other_type(config)]

    assert handler.filters == []
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(_handler, plugins=explicit)
    assert handler.filters == []
    assert calls == []
    assert factory_calls == []
    assert not exporter.get_finished_spans()


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("registration", ["explicit", "environment", "mixed"])
@pytest.mark.parametrize("inherited_group", [False, True])
def test_redeclared_view_group_keeps_inherited_and_own_exclusion(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    registration: str,
    inherited_group: bool,
) -> None:
    class OwnGroupPeer:
        __durable_registration_api__ = 1
        exclusive_group = "custom-otel-view"

        def create_plugin(
            self, _info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            return _Observer([])

    subclass = type(
        "CustomOtelView",
        (plugin_type,),
        {
            "__durable_registration_api__": 1,
            "exclusive_group": "custom-otel-view",
        },
    )
    opposite = (
        InvocationOtelPluginFactory
        if plugin_type is ExecutionOtelPluginFactory
        else ExecutionOtelPluginFactory
    )
    provider, exporter = telemetry
    log_handler = _RecordingHandler()
    monkeypatch.setattr(logging.getLogger(), "handlers", [log_handler])
    config = OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    factory_calls: list[str] = []

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        factory_calls.append("custom")
        return plugin_type.create_plugin(self, info)

    monkeypatch.setattr(subclass, "create_plugin", create_plugin)
    custom = subclass(config)
    peer = opposite(config) if inherited_group else OwnGroupPeer()
    original_peer_create = peer.create_plugin

    def create_peer(info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        factory_calls.append("peer")
        return original_peer_create(info)

    monkeypatch.setattr(peer, "create_plugin", create_peer)
    entries = [
        SimpleNamespace(
            name=name,
            value=f"test:{name}",
            dist=None,
            load=lambda factory=factory: factory,
        )
        for name, factory in [("custom", custom), ("peer", peer)]
    ]
    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.plugin_discovery.metadata.entry_points",
        lambda **_: entries,
    )
    if registration == "environment":
        explicit: list[DurableInstrumentationPluginFactory] = []
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", "custom,peer")
    elif registration == "mixed":
        explicit = [custom]
        monkeypatch.setenv("DURABLE_EXECUTION_PLUGINS", "peer")
    else:
        explicit = [custom, peer]
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(_handler, plugins=explicit)
    assert factory_calls == []
    assert log_handler.filters == []
    assert not exporter.get_finished_spans()


@pytest.mark.parametrize(
    "plugin_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("method_kind", ["static", "class"])
def test_factory_class_retains_inherited_view_exclusion(
    telemetry: tuple[TracerProvider, InMemorySpanExporter],
    plugin_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    method_kind: str,
) -> None:
    _, exporter = telemetry
    calls: list[str] = []

    def create(*_args):
        calls.append("constructed")
        raise AssertionError("Conflicting factories must not run")

    factory = type(
        "ClassFactory",
        (plugin_type,),
        {
            "create_plugin": staticmethod(create)
            if method_kind == "static"
            else classmethod(create)
        },
    )
    opposite = (
        InvocationOtelPluginFactory
        if plugin_type is ExecutionOtelPluginFactory
        else ExecutionOtelPluginFactory
    )
    with pytest.raises(PluginLoadError, match="mutually exclusive"):
        durable_execution(
            _handler,
            plugins=[cast(DurableInstrumentationPluginFactory, factory), opposite()],
        )
    assert calls == []
    assert not exporter.get_finished_spans()


def test_execution_constructor_does_not_claim_ambient_logs_before_start(
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
        # Core 3 registration owns factories, and the shared filter resolves a
        # live invocation claim. Merely constructing a plugin cannot claim an
        # ambient span before Start or attribute another invocation's records.
        assert len(handler.filters) == 1
        assert trace.get_current_span() is ambient
        assert ambient.get_span_context().is_valid
        assert not hasattr(record, "traceId")
        assert not hasattr(record, "spanId")
