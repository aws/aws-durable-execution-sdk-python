"""Registered Start bindings reach independently resumed public branch workers."""

from __future__ import annotations

import contextvars
import json
import threading
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration, MapConfig, ParallelConfig
from aws_durable_execution_sdk_python.concurrency.models import BatchResult
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("failed_start", [False, True])
def test_partial_branch_resume_uses_fresh_successful_start_bindings(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    kind: str,
    concurrency: int,
    failed_start: bool,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    marker = contextvars.ContextVar("resumed-branch", default="host")
    partial = contextvars.ContextVar[str]("partial-start")
    observations: list[tuple[int, str]] = []
    visits = {0: 0, 1: 0}
    bodies: list[int] = []
    statuses: list[InvocationStatus] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2) if concurrency == 2 else None

    class Bind(DurableInstrumentationPlugin):
        token: contextvars.Token[str] | None = None

        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            self.token = marker.set("plugin")

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            statuses.append(info.status)
            assert self.token is not None
            marker.reset(self.token)
            self.token = None

    class Broken(DurableInstrumentationPlugin):
        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            marker.set("failed")
            partial.set("failed")
            raise ValueError("expected failed Start")

    plugin = Bind()
    plugins: list[DurableInstrumentationPlugin] = [plugin]
    if failed_start:
        plugins.append(Broken())

    def branch(child: DurableContext, index: int) -> str:
        with lock:
            visits[index] += 1
            first_visit = visits[index] == 1
            observations.append((index, marker.get()))
        with pytest.raises(LookupError):
            partial.get()
        marker.set(f"branch-{index}")
        if barrier is not None and first_visit:
            barrier.wait(timeout=10)

        def step(_step: Any) -> str:
            with lock:
                bodies.append(index)
            return f"saved-{index}"

        saved = child.step(step, name="save")
        child.wait(Duration.from_seconds(index + 1), name="branch-wait")
        return saved

    def handler(_event: Any, durable: DurableContext) -> list[str]:
        assert marker.get() == "plugin"
        result: BatchResult[str]
        if kind == "map":
            result = durable.map(
                [0, 1],
                lambda child, item, index, items: branch(child, index),
                name="mapped",
                config=MapConfig(max_concurrency=concurrency),
            )
        else:
            result = durable.parallel(
                [lambda child: branch(child, 0), lambda child: branch(child, 1)],
                name="parallel",
                config=ParallelConfig(max_concurrency=concurrency),
            )
        assert marker.get() == "plugin"
        durable.wait(Duration.from_seconds(1), name="after-batch")
        return result.get_results()

    wrapped = durable_execution(handler, plugins=plugins)
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        result = runner.run(input="{}", timeout=30)
    assert result.status.value == "SUCCEEDED"
    assert json.loads(result.result) == ["saved-0", "saved-1"]
    assert sorted(bodies) == [0, 1]
    assert all(count >= 2 for count in visits.values())
    assert all(binding == "plugin" for _, binding in observations)
    assert InvocationStatus.PENDING in statuses
    assert statuses[-1] is InvocationStatus.SUCCEEDED
    assert marker.get() == "host" and plugin.token is None
    with pytest.raises(LookupError):
        partial.get()
    errors = [
        record
        for record in caplog.records
        if record.exc_info and record.name == "aws_durable_execution_sdk_python.plugin"
    ]
    assert len(errors) == (len(statuses) if failed_start else 0)
    assert all(
        record.exc_info is not None
        and str(record.exc_info[1]) == "expected failed Start"
        for record in errors
    )
