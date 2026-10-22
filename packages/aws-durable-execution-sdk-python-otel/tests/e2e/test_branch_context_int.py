"""Public SDK branch workers inherit instrumented bindings without sharing them."""

from __future__ import annotations

import contextvars
import json
import threading
from typing import Any, Generic, TypeVar
from collections.abc import Callable

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration, MapConfig, ParallelConfig
from aws_durable_execution_sdk_python.concurrency.models import BatchResult
from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import baggage, context, trace
from opentelemetry.sdk.trace import TracerProvider

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


_PluginT = TypeVar("_PluginT", bound=DurableInstrumentationPlugin)


class _RecordingFactory(Generic[_PluginT]):
    """Build fresh invocation plugins and retain them for teardown assertions."""

    def __init__(self, build: Callable[[], _PluginT]) -> None:
        self.build = build
        self.instances: list[_PluginT] = []

    def create_plugin(self, info: InvocationStartInfo) -> _PluginT:
        plugin = self.build()
        self.instances.append(plugin)
        return plugin


@pytest.mark.parametrize(
    ("view", "failed_start"),
    [
        (None, False),
        (InvocationOtelPlugin, False),
        (ExecutionOtelPlugin, False),
        (InvocationOtelPlugin, True),
        (ExecutionOtelPlugin, True),
    ],
)
@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("resume", [False, True])
def test_public_branch_context_propagation_and_isolation(
    monkeypatch: pytest.MonkeyPatch,
    view: type[InvocationOtelPlugin] | type[ExecutionOtelPlugin] | None,
    failed_start: bool,
    caplog: pytest.LogCaptureFixture,
    kind: str,
    concurrency: int,
    resume: bool,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    marker = contextvars.ContextVar("branch-invocation-marker", default="unset")
    expected_binding = (
        "unset" if view is None else "partial" if failed_start else "plugin"
    )
    expected_baggage = (
        None if view is None else "partial" if failed_start else "present"
    )
    observations: list[tuple[int, str, Any, bool]] = []
    bodies: list[int] = []
    visits = {0: 0, 1: 0}
    statuses: list[InvocationStatus] = []
    starts: list[int] = []
    completed_ends: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2) if concurrency == 2 else None
    provider = TracerProvider()
    host = context.get_current()

    class Bind(DurableInstrumentationPlugin):
        def on_invocation_start(self, info: Any) -> None:
            starts.append(threading.get_ident())
            self.token = marker.set("plugin")
            self.bag = context.attach(baggage.set_baggage("tenant", "present"))

        def on_invocation_end(self, info: Any) -> None:
            context.detach(self.bag)
            marker.reset(self.token)
            # Record completion only after both tokens were reset in their owner.
            completed_ends.append(threading.get_ident())
            statuses.append(info.status)

    class Broken(DurableInstrumentationPlugin):
        def on_invocation_start(self, _info: Any) -> None:
            marker.set("partial")
            context.attach(baggage.set_baggage("tenant", "partial"))
            raise ValueError("expected failed Start")

    plugins: list[DurableInstrumentationPluginFactory] = []
    otel_factory = None
    if view is not None:
        plugins.append(_RecordingFactory(Bind))
        if failed_start:
            plugins.append(_RecordingFactory(Broken))
        otel_factory = _RecordingFactory(
            lambda: view(
                OtelPluginConfig(
                    tracer_provider=provider,
                    enrich_logger=False,
                    context_extractor=lambda _: None,
                )
            )
        )
        plugins.append(otel_factory)

    def branch(child: DurableContext, index: int) -> str:
        with lock:
            visits[index] += 1
            first_visit = visits[index] == 1
            observations.append(
                (
                    index,
                    marker.get(),
                    baggage.get_baggage("tenant"),
                    trace.get_current_span().get_span_context().is_valid,
                )
            )
        if view is not None:
            # Deliberately leave a binding behind: another logical branch or
            # resume on this pool thread must still start with its parent's copy.
            marker.set(f"branch-{index}")
        if barrier is not None and first_visit:
            barrier.wait(timeout=10)

        def step(_step: Any) -> str:
            with lock:
                bodies.append(index)
            return f"saved-{index}"

        saved = child.step(step, name="save")
        return saved

    def handler(_event: Any, durable: DurableContext) -> list[str]:
        assert marker.get() == expected_binding
        token = marker.set("handler" if view is None else expected_binding)
        try:
            result: BatchResult[str] = (
                durable.map(
                    [0, 1],
                    lambda child, item, index, items: branch(child, index),
                    name="mapped",
                    config=MapConfig(max_concurrency=concurrency),
                )
                if kind == "map"
                else durable.parallel(
                    [lambda child: branch(child, 0), lambda child: branch(child, 1)],
                    name="parallel",
                    config=ParallelConfig(max_concurrency=concurrency),
                )
            )
            assert marker.get() == ("handler" if view is None else expected_binding)
            if resume:
                durable.wait(Duration.from_seconds(1), name="resume")
            return result.get_results()
        finally:
            marker.reset(token)

    wrapped = durable_execution(handler, plugins=plugins)
    try:
        with DurableFunctionTestRunner(handler=wrapped) as runner:
            result = runner.run(input="{}", timeout=30)
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == ["saved-0", "saved-1"]
        assert sorted(bodies) == [0, 1]
        assert {item[0] for item in observations} == {0, 1}
        assert all(
            item[1:] == (expected_binding, expected_baggage, view is not None)
            for item in observations
        )
        # The final wait replays the completed batch without rerunning branches.
        assert visits == {0: 1, 1: 1}
        if view is not None and resume:
            assert InvocationStatus.PENDING in statuses
        assert starts == completed_ends
        if view is not None:
            assert starts and statuses[-1] is InvocationStatus.SUCCEEDED
            assert otel_factory is not None
            assert otel_factory.instances
            assert all(
                plugin._context_tokens == {} for plugin in otel_factory.instances
            )
        assert not any(
            r.exc_info and r.name == "opentelemetry.context" for r in caplog.records
        )
        assert marker.get() == "unset"
        assert context.get_current() == host
    finally:
        provider.shutdown()
