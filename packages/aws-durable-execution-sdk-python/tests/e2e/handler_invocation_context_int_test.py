"""Invocation context isolation across real suspension and replay."""

from __future__ import annotations

import contextvars
import json
from typing import Any

import pytest
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
)


@pytest.mark.parametrize("fail_after_resume", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_invocation_plugins_restore_host_context_across_resume(
    monkeypatch: pytest.MonkeyPatch, fail_after_resume: bool, reverse: bool
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    marker = contextvars.ContextVar("invocation-host", default="host")
    boundaries: list[tuple[str, str]] = []
    body_calls: list[str] = []

    class ScopePlugin(DurableInstrumentationPlugin):
        def __init__(self, name: str) -> None:
            self.name = name
            self.token: contextvars.Token[str] | None = None

        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            self.token = marker.set(self.name)

        def on_invocation_end(self, _info: InvocationEndInfo) -> None:
            assert self.token is not None
            marker.reset(self.token)
            self.token = None

    names = ["first", "second"]
    if reverse:
        names.reverse()

    def body(_event: Any, context: DurableContext) -> str:
        assert marker.get() == names[-1]

        def step(_step_context: Any) -> str:
            body_calls.append("step")
            return "saved"

        saved = context.step(step, name="before-wait")
        context.wait(Duration.from_seconds(1), name="resume")
        assert marker.get() == names[-1]
        if fail_after_resume:
            raise ValueError("failure after resume")
        return saved

    durable_handler = durable_execution(
        body, plugins=[ScopePlugin(name) for name in names]
    )

    def host(event: Any, context: Any) -> Any:
        before = marker.get()
        try:
            return durable_handler(event, context)
        finally:
            boundaries.append((before, marker.get()))

    with DurableFunctionTestRunner(handler=host) as runner:
        result = runner.run(input="{}", timeout=15)
    assert result.status.value == ("FAILED" if fail_after_resume else "SUCCEEDED")
    if not fail_after_resume:
        assert json.loads(result.result) == "saved"
    assert body_calls == ["step"]
    assert boundaries == [("host", "host"), ("host", "host")]
