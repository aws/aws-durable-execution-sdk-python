"""Real public invocation-worker lifecycle and ContextVar ownership controls."""

from __future__ import annotations

import contextvars
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    CheckpointError,
    CheckpointErrorCategory,
    InvocationError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInputWithClient,
    InitialExecutionState,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    DurableServiceClient,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.state import ExecutionState


def invocation(client: Any = None) -> tuple[Any, Any]:
    client = client or Mock(spec=DurableServiceClient)
    client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="next", new_execution_state=CheckpointUpdatedExecutionState()
    )
    event = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="test-arn/worker-lifecycle",
        checkpoint_token="initial",
        initial_execution_state=InitialExecutionState(
            operations=[
                Operation(
                    operation_id="execution",
                    operation_type=OperationType.EXECUTION,
                    status=OperationStatus.STARTED,
                    execution_details=ExecutionDetails(input_payload="{}"),
                )
            ],
            next_marker="",
        ),
        service_client=client,
    )
    context = Mock()
    context.aws_request_id = "worker-request"
    context.client_context = context.identity = context.invoked_function_arn = None
    context._epoch_deadline_time_in_ms = 0
    context.tenant_id = None
    return event, context


@pytest.mark.parametrize("outcome", ["success", "failure", "retry", "pending"])
@pytest.mark.parametrize("mode", ["none", "healthy", "failed-start", "failed-end"])
def test_public_worker_lifecycle_owns_context_and_outcome(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    outcome: str,
    mode: str,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    marker = contextvars.ContextVar("worker-owner", default="fresh-worker")
    phases: list[tuple[str, int, str]] = []
    boundary: list[tuple[str, int, str, str]] = []
    retry = InvocationError("original retry")
    fail = ValueError("original body failure")
    statuses: list[InvocationStatus] = []
    caller_thread = threading.get_ident()

    class Observer(ThreadPoolExecutor):
        def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
            role = (
                "checkpoint"
                if getattr(fn, "__name__", "") == "checkpoint_batches_forever"
                else "invocation"
            )

            def observed() -> Any:
                before = marker.get()
                try:
                    return fn(*args, **kwargs)
                finally:
                    boundary.append((role, threading.get_ident(), before, marker.get()))

            return super().submit(observed)

    monkeypatch.setattr(
        "aws_durable_execution_sdk_python.execution.ThreadPoolExecutor", Observer
    )

    class Plugin(DurableInstrumentationPlugin):
        token: Any = None

        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            phases.append(("start", threading.get_ident(), marker.get()))
            self.token = marker.set("start-binding")
            if mode == "failed-start":
                raise ValueError("startup failure")

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            phases.append(("end", threading.get_ident(), marker.get()))
            statuses.append(info.status)
            assert self.token is not None
            marker.reset(self.token)
            self.token = None
            if mode == "failed-end":
                raise ValueError("end failure")

    def body(_event: Any, _context: DurableContext) -> str:
        phases.append(("body", threading.get_ident(), marker.get()))
        marker.set("body-binding")
        try:
            if outcome == "failure":
                raise fail
            if outcome == "retry":
                raise retry
            if outcome == "pending":
                raise SuspendExecution("test suspension")
            return "ok"
        finally:
            phases.append(("finally", threading.get_ident(), marker.get()))

    handler = durable_execution(body, plugins=[] if mode == "none" else [Plugin()])
    event, context = invocation()
    token = marker.set("host")
    try:
        for _ in range(2):
            if outcome == "retry":
                with pytest.raises(InvocationError) as caught:
                    handler(event, context)
                assert caught.value is retry
            else:
                result = handler(event, context)
                assert (
                    result["Status"]
                    == {
                        "success": "SUCCEEDED",
                        "failure": "FAILED",
                        "pending": "PENDING",
                    }[outcome]
                )
                if outcome == "failure":
                    assert result["Error"]["ErrorMessage"] == "original body failure"
            assert marker.get() == "host"
    finally:
        marker.reset(token)
    per_call = 2 if mode == "none" else 4
    for offset in range(0, len(phases), per_call):
        batch = phases[offset : offset + per_call]
        assert [p[0] for p in batch] == (
            ["body", "finally"]
            if mode == "none"
            else ["start", "body", "finally", "end"]
        )
        assert len({p[1] for p in batch}) == 1
        assert batch[0][1] != caller_thread
    bodies = [p for p in phases if p[0] == "body"]
    expected = (
        "fresh-worker"
        if mode == "none"
        else "host"
        if mode == "failed-start"
        else "start-binding"
    )
    assert [p[2] for p in bodies] == [expected, expected]
    for role, tid, before, after in boundary:
        if role == "invocation":
            assert after == ("body-binding" if mode == "none" else before)
    if mode != "none":
        assert (
            statuses
            == [
                {
                    "success": InvocationStatus.SUCCEEDED,
                    "failure": InvocationStatus.FAILED,
                    "retry": InvocationStatus.RETRY,
                    "pending": InvocationStatus.PENDING,
                }[outcome]
            ]
            * 2
        )
    assert "different Context" not in caplog.text


@pytest.mark.parametrize("bad_first", [False, True])
@pytest.mark.parametrize("outcome", ["success", "failure", "retry", "pending"])
def test_failed_start_preserves_clean_bindings_and_original_token_owners(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    bad_first: bool,
    outcome: str,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    marker = contextvars.ContextVar("failed-start-owner", default="host")
    added = contextvars.ContextVar[str]("failed-start-added")
    events: list[tuple[str, str, int]] = []
    cleanup_contexts: list[str] = []
    original_close = ExecutionState.close

    def close(state: ExecutionState) -> None:
        cleanup_contexts.append(marker.get())
        with pytest.raises(LookupError):
            added.get()
        original_close(state)

    monkeypatch.setattr(ExecutionState, "close", close)

    class Plugin(DurableInstrumentationPlugin):
        def __init__(self, name: str) -> None:
            self.name = name
            self.token: contextvars.Token[str] | None = None
            self.added: contextvars.Token[str] | None = None

        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            events.append(("start", self.name, threading.get_ident()))
            self.token = marker.set(self.name)
            if self.name == "bad":
                self.added = added.set("partial")
                raise ValueError("partial startup")

        def on_invocation_end(self, _info: InvocationEndInfo) -> None:
            events.append(("end", self.name, threading.get_ident()))
            assert self.token is not None
            marker.reset(self.token)
            if self.added is not None:
                added.reset(self.added)

    failure = InvocationError("retry")

    def body(_event: Any, _context: DurableContext) -> str:
        events.append(("body", "handler", threading.get_ident()))
        assert marker.get() == "healthy"
        with pytest.raises(LookupError):
            added.get()
        if outcome == "retry":
            raise failure
        if outcome == "failure":
            raise ValueError("body")
        if outcome == "pending":
            raise SuspendExecution("test suspension")
        return "ok"

    names = ["bad", "healthy"] if bad_first else ["healthy", "bad"]
    handler = durable_execution(body, plugins=[Plugin(name) for name in names])
    event, context = invocation()
    for _ in range(2):
        if outcome == "retry":
            with pytest.raises(InvocationError) as caught:
                handler(event, context)
            assert caught.value is failure
        else:
            assert (
                handler(event, context)["Status"]
                == {"success": "SUCCEEDED", "failure": "FAILED", "pending": "PENDING"}[
                    outcome
                ]
            )
        assert marker.get() == "host"
        with pytest.raises(LookupError):
            added.get()
    assert cleanup_contexts == ["healthy"] * 2
    for offset in (0, 5):
        batch = events[offset : offset + 5]
        assert [(x[0], x[1]) for x in batch] == [("start", n) for n in names] + [
            ("body", "handler")
        ] + [("end", n) for n in names]
        assert len({x[2] for x in batch}) == 1
    errors = [
        r.exc_info
        for r in caplog.records
        if r.exc_info and r.name == "aws_durable_execution_sdk_python.plugin"
    ]
    assert len(errors) == 2 and all(str(e[1]) == "partial startup" for e in errors)


@pytest.mark.parametrize("result_kind", ["normal", "bad-json", "large", "large-error"])
def test_worker_prepares_output_joins_branches_waits_checkpoint_then_ends(
    monkeypatch: pytest.MonkeyPatch,
    result_kind: str,
) -> None:
    import aws_durable_execution_sdk_python.execution as execution

    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    events: list[tuple[str, int]] = []
    started = threading.Event()
    checkpoint_started = threading.Event()
    closing = threading.Event()
    branch_done = threading.Event()
    real_checkpoint = ExecutionState.checkpoint_batches_forever
    real_close = ExecutionState.close
    real_stop = ExecutionState.stop_checkpointing
    real_dumps = execution.json.dumps
    result: Any = {
        "normal": {"ok": True},
        "bad-json": object(),
        "large": "x" * 256,
        "large-error": None,
    }[result_kind]

    def record(name: str) -> None:
        events.append((name, threading.get_ident()))

    def checkpoint(state: ExecutionState) -> None:
        assert started.is_set()
        record("checkpoint-start")
        checkpoint_started.set()
        try:
            real_checkpoint(state)
        finally:
            record("checkpoint-end")

    def stop(state: ExecutionState) -> None:
        assert branch_done.is_set()
        record("checkpoint-stop")
        real_stop(state)

    def close(state: ExecutionState) -> None:
        record("close")
        closing.set()
        real_close(state)

    def dumps(value: Any, *args: Any, **kwargs: Any) -> str:
        if value is result or (
            isinstance(value, dict) and value.get("Status") == "FAILED"
        ):
            record("serialize")
        return real_dumps(value, *args, **kwargs)

    monkeypatch.setattr(ExecutionState, "checkpoint_batches_forever", checkpoint)
    monkeypatch.setattr(ExecutionState, "stop_checkpointing", stop)
    monkeypatch.setattr(ExecutionState, "close", close)
    monkeypatch.setattr(execution.json, "dumps", dumps)
    if result_kind in ("large", "large-error"):
        monkeypatch.setattr(execution, "LAMBDA_RESPONSE_SIZE_LIMIT", 64)

    class Plugin(DurableInstrumentationPlugin):
        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            record("start")
            assert not checkpoint_started.is_set()
            started.set()

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            record("end")
            assert branch_done.is_set()
            assert any(x[0] == "checkpoint-end" for x in events)
            assert info.status is (
                InvocationStatus.FAILED
                if result_kind in ("bad-json", "large-error")
                else InvocationStatus.SUCCEEDED
            )

    def body(_event: Any, ctx: DurableContext) -> Any:
        assert checkpoint_started.wait(5)
        record("body")
        pool = ThreadPoolExecutor(max_workers=1)
        ctx.state.register_branch_pool(pool)

        def late_branch() -> None:
            assert closing.wait(5)
            assert not ctx.state._checkpointing_stopped.is_set()
            ctx.state.create_checkpoint(
                OperationUpdate.create_execution_succeed(payload='"branch"'),
                is_sync=True,
            )
            record("branch-done")
            branch_done.set()

        pool.submit(late_branch)
        try:
            if result_kind == "large-error":
                raise ValueError("x" * 256)
            return result
        finally:
            record("finally")

    handler = durable_execution(body, plugins=[Plugin()])
    event, context = invocation()
    output = handler(event, context)
    record("caller")
    names = [x[0] for x in events]
    assert (
        names.index("start")
        < names.index("checkpoint-start")
        < names.index("body")
        < names.index("finally")
        < names.index("serialize")
        < names.index("close")
    )
    assert (
        names.index("branch-done")
        < names.index("checkpoint-stop")
        < names.index("checkpoint-end")
        < names.index("end")
        < names.index("caller")
    )
    worker_events = {
        tid
        for name, tid in events
        if name
        in {"start", "body", "finally", "serialize", "close", "checkpoint-stop", "end"}
    }
    assert len(worker_events) == 1 and threading.get_ident() not in worker_events
    assert output["Status"] == (
        "FAILED" if result_kind in ("bad-json", "large-error") else "SUCCEEDED"
    )


@pytest.mark.parametrize("shape", ["method", "property", "dynamic"])
def test_removed_handler_scope_api_is_not_inspected(
    monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    calls = []

    def legacy(*_args: Any) -> Any:
        calls.append("unexpected")
        raise AssertionError("Removed API called")

    choices: dict[str, dict[str, Any]] = {
        "method": {"handler_context": legacy},
        "property": {"handler_context": property(legacy)},
        "dynamic": {
            "__getattr__": lambda self, name: legacy()
            if name == "handler_context"
            else (_ for _ in ()).throw(AttributeError(name))
        },
    }
    plugin = type("LegacyPlugin", (DurableInstrumentationPlugin,), choices[shape])()
    handler = durable_execution(lambda _e, _c: "ok", plugins=[plugin])
    event, context = invocation()
    assert handler(event, context)["Status"] == "SUCCEEDED"
    assert calls == []


@pytest.mark.parametrize(
    "checkpoint_path", ["step-start", "large-result", "large-error"]
)
@pytest.mark.parametrize("retryable", [False, True])
def test_checkpoint_failure_reports_prepared_outcome_after_worker_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    checkpoint_path: str,
    retryable: bool,
) -> None:
    import aws_durable_execution_sdk_python.execution as execution

    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    if checkpoint_path != "step-start":
        monkeypatch.setattr(execution, "LAMBDA_RESPONSE_SIZE_LIMIT", 128)
    marker = contextvars.ContextVar("checkpoint-worker", default="host")
    failure = CheckpointError(
        "actual checkpoint failure",
        error_category=(
            CheckpointErrorCategory.INVOCATION
            if retryable
            else CheckpointErrorCategory.EXECUTION
        ),
    )
    events: list[tuple[str, int]] = []
    ends: list[tuple[InvocationEndInfo, str]] = []
    real_checkpoint = ExecutionState.checkpoint_batches_forever
    real_close = ExecutionState.close

    def record(name: str) -> None:
        events.append((name, threading.get_ident()))

    def checkpoint(state: ExecutionState) -> None:
        record("checkpoint-start")
        try:
            real_checkpoint(state)
        finally:
            record("checkpoint-end")

    def close(state: ExecutionState) -> None:
        record("close")
        assert marker.get() == "plugin"
        real_close(state)
        record("closed")

    def service_checkpoint(*_args: Any, **_kwargs: Any) -> Any:
        record("service-failure")
        raise failure

    monkeypatch.setattr(ExecutionState, "checkpoint_batches_forever", checkpoint)
    monkeypatch.setattr(ExecutionState, "close", close)

    class Plugin(DurableInstrumentationPlugin):
        token: contextvars.Token[str] | None = None

        def on_invocation_start(self, _info: InvocationStartInfo) -> None:
            record("start")
            self.token = marker.set("plugin")

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            record("end")
            ends.append((info, marker.get()))
            assert self.token is not None
            marker.reset(self.token)
            self.token = None

    def body(_event: Any, ctx: DurableContext) -> str:
        record("body")
        try:
            if checkpoint_path == "step-start":
                ctx.step(lambda _step: "ok", name="failed-checkpoint")
            if checkpoint_path == "large-error":
                raise ValueError("x" * 256)
            return "x" * 256
        finally:
            record("finally")

    plugin = Plugin()
    handler = durable_execution(body, plugins=[plugin])
    client = Mock(spec=DurableServiceClient)
    client.checkpoint.side_effect = service_checkpoint
    event, context = invocation(client)
    for _ in range(2):
        events.clear()
        ends.clear()
        if retryable:
            with pytest.raises(CheckpointError) as caught:
                handler(event, context)
            assert caught.value is failure
        else:
            output = handler(event, context)
            assert output["Status"] == "FAILED"
            assert output["Error"]["ErrorMessage"] == str(failure)
            assert output["Error"]["ErrorType"].endswith(".CheckpointError")
        record("caller")
        assert marker.get() == "host" and plugin.token is None
        assert len(ends) == 1
        info, active = ends[0]
        assert active == "plugin"
        assert info.status is (
            InvocationStatus.RETRY if retryable else InvocationStatus.FAILED
        )
        assert info.error is not None and info.error.message == str(failure)
        assert info.error.type is not None and info.error.type.endswith(
            ".CheckpointError"
        )
        names = [name for name, _tid in events]
        assert (
            names.index("start")
            < names.index("checkpoint-start")
            < names.index("service-failure")
        )
        assert (
            names.index("body")
            < names.index("finally")
            < names.index("close")
            < names.index("closed")
            < names.index("end")
            < names.index("caller")
        )
        assert (
            names.index("service-failure")
            < names.index("checkpoint-end")
            < names.index("end")
        )
        workers = {
            tid
            for name, tid in events
            if name in {"start", "body", "finally", "close", "closed", "end"}
        }
        assert len(workers) == 1 and threading.get_ident() not in workers
    assert not any(
        record.exc_info and record.name == "aws_durable_execution_sdk_python.plugin"
        for record in caplog.records
    )
