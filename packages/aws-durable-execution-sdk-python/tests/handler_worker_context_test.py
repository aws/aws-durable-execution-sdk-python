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
@pytest.mark.parametrize("plugin_mode", ["none", "healthy", "partial-failure"])
def test_handler_worker_preserves_context_and_restores_its_caller(
    monkeypatch: pytest.MonkeyPatch, outcome: str, plugin_mode: str
) -> None:
    marker = contextvars.ContextVar("handler-worker-context", default="worker-empty")
    seen: list[str] = []
    worker_boundaries: list[tuple[str, str]] = []
    statuses: list[InvocationStatus] = []

    class ClaimPlugin(DurableInstrumentationPlugin):
        token: contextvars.Token[str] | None = None

        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            self.token = marker.set("invocation-start")
            if plugin_mode == "partial-failure":
                raise ValueError("partial plugin setup")

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
    handler = durable_execution(
        body,
        boto3_client=client,
        plugins=[ClaimPlugin()] if plugin_mode != "none" else [],
    )
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
    assert len(worker_boundaries) == 1
    worker_before, worker_after = worker_boundaries[0]
    if plugin_mode != "none":
        assert seen == [
            "caller" if plugin_mode == "partial-failure" else "invocation-start"
        ]
        assert worker_after == worker_before
        assert worker_after != "worker-mutation"
        assert statuses == [
            {
                "success": InvocationStatus.SUCCEEDED,
                "failure": InvocationStatus.FAILED,
                "retry": InvocationStatus.RETRY,
            }[outcome]
        ]
    else:
        # No plugin means the original direct worker call: caller bindings are
        # absent, and mutations belong to the worker's own context.
        assert seen == [worker_before]
        assert seen != ["caller"]
        assert worker_after == "worker-mutation"
        assert statuses == []
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


@pytest.mark.parametrize("outcome", ["SUCCEEDED", "PENDING", "FAILED", "retry"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("hook_failure", [None, "start", "end"])
def test_invocation_context_scopes_do_not_escape_to_host(
    outcome: str, reverse: bool, hook_failure: str | None
) -> None:
    """Legacy hook order must not leave an already-ended plugin scope current."""
    from datetime import UTC, datetime
    import threading
    from aws_durable_execution_sdk_python.plugin import PluginExecutor

    marker = contextvars.ContextVar("invocation-scope", default="host")
    events: list[tuple[str, str, str, int]] = []
    caller_thread = threading.get_ident()

    class ScopePlugin(DurableInstrumentationPlugin):
        def __init__(self, name: str) -> None:
            self.name = name
            self.token: contextvars.Token[str] | None = None

        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            events.append(("start", self.name, marker.get(), threading.get_ident()))
            self.token = marker.set(self.name)
            if self.name == names[0] and hook_failure == "start":
                raise ValueError("plugin initialization failed")

        def on_invocation_end(self, _info: InvocationEndInfo) -> None:
            events.append(("end", self.name, marker.get(), threading.get_ident()))
            if self.name == names[0] and hook_failure == "end":
                raise ValueError("plugin finalization failed")
            assert self.token is not None
            marker.reset(self.token)
            self.token = None

    names = ["first", "second"]
    if reverse:
        names.reverse()
    executor = PluginExecutor([ScopePlugin(name) for name in names])
    handler_failure = InvocationError("retry")
    expected_output = {"Status": outcome}

    @executor.handle_durable_output
    def invoke(_event: Any, _context: Any) -> dict[str, str]:
        executor.on_invocation_start("test", True, datetime.now(UTC), None)
        assert executor.run_handler(marker.get) == names[-1]
        if outcome == "retry":
            raise handler_failure
        return expected_output

    token = marker.set("incoming")
    try:
        for _ in range(2):
            if outcome == "retry":
                with pytest.raises(InvocationError, match="retry") as caught:
                    invoke({}, None)
                assert caught.value is handler_failure
            else:
                assert invoke({}, None) is expected_output
            assert marker.get() == "incoming"
    finally:
        marker.reset(token)
    assert [(kind, name) for kind, name, _, _ in events] == [
        (kind, name) for _ in range(2) for kind in ("start", "end") for name in names
    ]
    assert all(thread == caller_thread for _, _, _, thread in events)
    assert [
        value for kind, name, value, _ in events if kind == "start" and name == names[0]
    ] == ["incoming", "incoming"]


def test_no_plugin_invocation_keeps_original_caller_context_semantics() -> None:
    from aws_durable_execution_sdk_python.plugin import PluginExecutor

    marker = contextvars.ContextVar("no-plugin-caller", default="host")
    executor = PluginExecutor([])

    @executor.handle_durable_output
    def invoke(_event: Any, _context: Any) -> dict[str, str]:
        marker.set("caller-side-change")
        return {"Status": "SUCCEEDED"}

    token = marker.set("incoming")
    try:
        invoke({}, None)
        assert marker.get() == "caller-side-change"
    finally:
        marker.reset(token)


@pytest.mark.parametrize("stage", ["start", "factory", "enter"])
@pytest.mark.parametrize("bad_first", [False, True])
@pytest.mark.parametrize("outcome", ["SUCCEEDED", "PENDING", "FAILED", "retry"])
def test_failed_plugin_setup_discards_partial_context_bindings(
    stage: str, bad_first: bool, outcome: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Keep healthy bindings, unset bindings, hook order, and token ownership."""
    from contextlib import nullcontext
    from datetime import UTC, datetime
    from typing import ContextManager
    from aws_durable_execution_sdk_python.plugin import PluginExecutor

    marker = contextvars.ContextVar("partial-setup", default="default")
    new_binding = contextvars.ContextVar[str]("partial-setup-no-default")
    events: list[tuple[str, str]] = []
    cleanup: list[str] = []
    healthy_inputs: list[tuple[str, str | None]] = []

    class Scope:
        def __init__(self, plugin: SetupPlugin) -> None:
            self.plugin = plugin

        def __enter__(self) -> None:
            self.plugin.bind("enter")

        def __exit__(self, *_args: Any) -> None:
            self.plugin.reset("exit")

    class SetupPlugin(DurableInstrumentationPlugin):
        def __init__(self, name: str) -> None:
            self.name = name
            self.token: contextvars.Token[str] | None = None

        def bind(self, where: str) -> None:
            events.append((where, self.name))
            if self.name == "healthy":
                healthy_inputs.append((marker.get(), new_binding.get(None)))
            self.token = marker.set(self.name)
            if self.name == "bad":
                new_binding.set("partial")
                raise ValueError("partial plugin setup")

        def reset(self, where: str) -> None:
            events.append((where, self.name))
            assert self.token is not None
            marker.reset(self.token)
            self.token = None
            cleanup.append(self.name)

        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            if stage == "start":
                self.bind("start")

        def on_invocation_end(self, _info: InvocationEndInfo) -> None:
            if stage == "start":
                self.reset("end")

        def handler_context(self, _info: InvocationStartInfo) -> ContextManager[None]:
            if stage == "start":
                return nullcontext()
            if self.name == "bad" and stage == "factory":
                self.bind("factory")
            return Scope(self)

    names = ["bad", "healthy"] if bad_first else ["healthy", "bad"]
    executor = PluginExecutor([SetupPlugin(name) for name in names])
    output = {"Status": outcome}
    failure = InvocationError("original retry")
    handler_failure = RuntimeError("original handler error")

    def body() -> dict[str, str]:
        assert marker.get() == "healthy"
        with pytest.raises(LookupError):
            new_binding.get()
        if outcome == "retry":
            raise failure
        if outcome == "FAILED":
            raise handler_failure
        return output

    @executor.handle_durable_output
    def invoke(_event: Any, _context: Any) -> dict[str, str]:
        executor.on_invocation_start("partial-setup", True, datetime.now(UTC), None)
        try:
            return executor.run_handler(body)
        except RuntimeError as error:
            assert error is handler_failure
            return output

    token = marker.set("incoming")
    try:
        for _ in range(2):
            if outcome == "retry":
                with pytest.raises(InvocationError) as caught:
                    invoke({}, None)
                assert caught.value is failure
            else:
                assert invoke({}, None) is output
            assert marker.get() == "incoming"
            with pytest.raises(LookupError):
                new_binding.get()
    finally:
        marker.reset(token)
    assert healthy_inputs == [("incoming", None)] * 2
    assert cleanup == (names if stage == "start" else ["healthy"]) * 2
    setup = "start" if stage == "start" else "enter"
    expected_setup = [
        ("factory" if name == "bad" and stage == "factory" else setup, name)
        for name in names
    ]
    expected_cleanup = (
        [("end", name) for name in names] if stage == "start" else [("exit", "healthy")]
    )
    assert events == (expected_setup + expected_cleanup) * 2
    # A token reset in the wrong Context is caught by the SDK, so explicitly
    # check diagnostics and successful cleanup rather than relying on raises.
    errors = [
        record.exc_info for record in caplog.records if record.exc_info is not None
    ]
    assert len(errors) == 2
    assert all(str(error[1]) == "partial plugin setup" for error in errors)
