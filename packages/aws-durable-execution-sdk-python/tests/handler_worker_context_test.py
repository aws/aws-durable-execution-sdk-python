"""Focused handler-dispatch tests with a mock service and real worker thread."""

from __future__ import annotations

import contextvars
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import InvocationError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)


@pytest.mark.parametrize("outcome", ["success", "failure", "retry"])
def test_handler_worker_preserves_context_and_restores_its_caller(
    monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    marker = contextvars.ContextVar("handler-worker-context", default="worker-empty")
    seen: list[str] = []
    worker_boundaries: list[tuple[str, str]] = []
    statuses: list[InvocationStatus] = []

    class ClaimPlugin(DurableInstrumentationPlugin):
        token: contextvars.Token[str] | None = None

        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            self.token = marker.set("invocation-start")

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            statuses.append(info.status)
            assert self.token is not None
            marker.reset(self.token)
            self.token = None

    def body(_event: Any, _context: DurableContext) -> str:
        seen.append(marker.get())
        marker.set("worker-mutation")
        if outcome == "failure":
            raise ValueError("handler failure")
        if outcome == "retry":
            raise InvocationError("handler retry")
        return "ok"

    class ObservingExecutor(ThreadPoolExecutor):
        def submit(
            self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
        ) -> Future[Any]:
            if fn is not body and body not in args:
                return super().submit(fn, *args, **kwargs)

            def observe() -> Any:
                before = marker.get()
                try:
                    return fn(*args, **kwargs)
                finally:
                    # This runs outside Context.run, on the actual SDK worker.
                    worker_boundaries.append((before, marker.get()))

            return super().submit(observe)

    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.execution.ThreadPoolExecutor",
        ObservingExecutor,
    )
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    client = Mock()
    handler = durable_execution(body, boto3_client=client, plugins=[ClaimPlugin()])
    event = {
        "DurableExecutionArn": "test-arn/handler-context",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "handler-context",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                }
            ],
            "NextMarker": "",
        },
    }
    lambda_context = Mock()
    lambda_context.aws_request_id = "context-request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 0
    lambda_context.invoked_function_arn = "test-arn"
    lambda_context.tenant_id = None
    token = marker.set("caller")
    try:
        if outcome == "retry":
            with pytest.raises(InvocationError, match="handler retry"):
                handler(event, lambda_context)
        else:
            result = handler(event, lambda_context)
            assert result["Status"] == (
                "SUCCEEDED" if outcome == "success" else "FAILED"
            )
        assert marker.get() == "caller"
    finally:
        marker.reset(token)
    assert seen == ["invocation-start"]
    assert len(worker_boundaries) == 1
    worker_before, worker_after = worker_boundaries[0]
    assert worker_after == worker_before
    assert worker_after != "worker-mutation"
    assert statuses == [
        {
            "success": InvocationStatus.SUCCEEDED,
            "failure": InvocationStatus.FAILED,
            "retry": InvocationStatus.RETRY,
        }[outcome]
    ]
    client.checkpoint_durable_execution.assert_not_called()


@pytest.mark.parametrize("failure", [None, "body", "enter", "exit"])
def test_optional_handler_scopes_are_balanced_and_cannot_change_outcome(
    failure: str | None,
) -> None:
    from contextlib import contextmanager
    from datetime import UTC, datetime
    from collections.abc import Iterator
    from aws_durable_execution_sdk_python.plugin import PluginExecutor

    marker = contextvars.ContextVar("handler-scope", default="caller")
    events: list[str] = []

    class ScopePlugin(DurableInstrumentationPlugin):
        def __init__(self, name: str):
            self.name = name

        @contextmanager
        def handler_context(self, info: InvocationStartInfo) -> Iterator[None]:
            assert info.execution_arn == "handler-scope"
            events.append("enter-" + self.name)
            if self.name == "inner" and failure == "enter":
                raise ValueError("plugin entry failure")
            token = marker.set(self.name)
            try:
                yield
            finally:
                marker.reset(token)
                events.append("exit-" + self.name)
                if self.name == "inner" and failure == "exit":
                    raise ValueError("plugin cleanup failure")

    executor = PluginExecutor([ScopePlugin("outer"), ScopePlugin("inner")])

    def body() -> str:
        assert marker.get() == ("outer" if failure == "enter" else "inner")
        events.append("body")
        if failure in ("body", "exit"):
            raise RuntimeError("original handler failure")
        return "ok"

    with executor.run():
        executor.on_invocation_start("handler-scope", True, datetime.now(UTC), None)
        if failure in ("body", "exit"):
            with pytest.raises(RuntimeError, match="original handler failure"):
                executor.run_handler(body)
        else:
            assert executor.run_handler(body) == "ok"
    assert marker.get() == "caller"
    assert events == ["enter-outer", "enter-inner", "body"] + (
        ["exit-outer"] if failure == "enter" else ["exit-inner", "exit-outer"]
    )


def test_handler_accepts_plugin_without_optional_scope() -> None:
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from typing import cast
    from aws_durable_execution_sdk_python.plugin import PluginExecutor

    legacy = cast(
        DurableInstrumentationPlugin,
        SimpleNamespace(on_invocation_start=lambda info: None),
    )
    executor = PluginExecutor([legacy])
    with executor.run():
        executor.on_invocation_start("legacy", True, datetime.now(UTC), None)
        assert executor.run_handler(lambda: "unchanged") == "unchanged"
