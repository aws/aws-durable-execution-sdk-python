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
            if fn is not body and not (args and args[0] is body):
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
