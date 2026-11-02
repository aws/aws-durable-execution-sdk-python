"""The operations each invocation input lists as updated.

Until an invocation succeeds, the input lists every operation. After that, it
lists every operation that changed after the state the last successful
invocation observed. A failed invocation does not count, so its retry lists
the same changes again, including the ones the failed invocation made.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.exceptions import InvocationError
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
    OperationType,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes

from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)

EXECUTION = "<execution>"


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Executor, "RETRY_BACKOFF_SECONDS", 0)


@pytest.fixture(params=["memory", "filesystem"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> ExecutionStore | None:
    if request.param == "filesystem":
        return FileSystemExecutionStore.create(tmp_path)
    return None


def _listed(info: InvocationStartInfo) -> set[str]:
    return {
        EXECUTION
        if operation.operation_type is OperationType.EXECUTION
        else operation.name or operation.operation_id
        for operation in info.updated_operations.values()
    }


def _run(
    body: Any,
    store: ExecutionStore | None,
    fail_before_sdk_on_attempt: int | None = None,
) -> list[set[str]]:
    """Run ``body`` and return what each invocation input listed as updated.

    ``fail_before_sdk_on_attempt`` makes that Lambda attempt fail before the
    SDK receives its input, so that attempt records no invocation start.
    """
    starts: list[InvocationStartInfo] = []
    attempts: list[int] = []

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            starts.append(info)

    class ObserverFactory:
        def create_plugin(self, _info: InvocationStartInfo) -> Observer:
            return Observer()

    durable_handler = durable_execution(body, plugins=[ObserverFactory()])

    def entrypoint(event: Any, context: Any) -> Any:
        attempts.append(1)
        if len(attempts) == fail_before_sdk_on_attempt:
            msg = "the invocation failed before the SDK received its input"
            raise RuntimeError(msg)
        return durable_handler(event, context)

    with DurableFunctionTestRunner(
        handler=entrypoint,
        store=store,
        poll_interval=0.01,
        execution_timeout=30,
    ) as runner:
        arn = runner.run_async(input="{}")
        result = runner.wait_for_result(arn, timeout=25)
    assert result.status is InvocationStatus.SUCCEEDED, result
    return [_listed(info) for info in starts]


def test_retry_after_failed_first_invocation_lists_execution_and_step(
    store: ExecutionStore | None,
) -> None:
    attempts: list[int] = []

    def body(_event: Any, context: DurableContext) -> str:
        attempts.append(1)
        context.step(lambda _c: "a", name="A")
        if len(attempts) == 1:
            msg = "fail once"
            raise InvocationError(msg)
        return "done"

    assert _run(body, store) == [{EXECUTION}, {EXECUTION, "A"}]


def test_resume_after_successful_invocation_lists_only_the_wait(
    store: ExecutionStore | None,
) -> None:
    def body(_event: Any, context: DurableContext) -> str:
        context.step(lambda _c: "a", name="A")
        context.wait(Duration.from_seconds(1), name="W")
        return "done"

    assert _run(body, store) == [{EXECUTION}, {"W"}]


def test_retry_after_failed_resume_lists_its_changes_again(
    store: ExecutionStore | None,
) -> None:
    attempts: list[int] = []

    def body(_event: Any, context: DurableContext) -> str:
        attempts.append(1)
        context.step(lambda _c: "a", name="A")
        context.wait(Duration.from_seconds(1), name="W")
        context.step(lambda _c: "b", name="B")
        if len(attempts) == 2:
            msg = "fail the resume once"
            raise InvocationError(msg)
        return "done"

    assert _run(body, store) == [{EXECUTION}, {"W"}, {"W", "B"}]


def test_failed_delivery_lists_the_wait_again(store: ExecutionStore | None) -> None:
    steps: list[str] = []

    def record(name: str) -> str:
        steps.append(name)
        return name

    def body(_event: Any, context: DurableContext) -> str:
        context.step(lambda _c: record("A"), name="A")
        context.wait(Duration.from_seconds(1), name="W")
        context.step(lambda _c: record("B"), name="B")
        return "done"

    assert _run(body, store, fail_before_sdk_on_attempt=2) == [{EXECUTION}, {"W"}]
    assert steps == ["A", "B"]


def test_callback_consumed_mid_invocation_is_not_listed_again() -> None:
    """A callback that completes while the handler runs reaches it in a
    checkpoint response. The next invocation does not list it again, so the
    callback ends once."""
    starts: list[InvocationStartInfo] = []
    ends: list[OperationEndInfo] = []
    callback_ids: list[str] = []
    callback_created = threading.Event()
    callback_sent = threading.Event()

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            starts.append(info)

        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.name == "target":
                ends.append(info)

    class ObserverFactory:
        def create_plugin(self, _info: InvocationStartInfo) -> Observer:
            return Observer()

    def blocking_step(_context: Any) -> str:
        assert callback_sent.wait(timeout=10)
        return "stepped"

    def body(_event: Any, context: DurableContext) -> str:
        callback = context.create_callback(
            name="target",
            config=CallbackConfig(
                timeout=Duration.from_seconds(60), serdes=JsonSerDes()
            ),
        )
        callback_ids.append(callback.callback_id)
        callback_created.set()
        context.step(blocking_step, name="blocker")
        value = callback.result()
        context.wait(Duration.from_seconds(1), name="pause")
        return str(value)

    with DurableFunctionTestRunner(
        handler=durable_execution(body, plugins=[ObserverFactory()]),
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=30,
    ) as runner:
        arn = runner.run_async(input="{}")
        assert callback_created.wait(timeout=10)
        runner.send_callback_success(callback_ids[0], result=json.dumps("cb").encode())
        callback_sent.set()
        result = runner.wait_for_result(arn, timeout=20)

    assert result.status is InvocationStatus.SUCCEEDED
    assert [_listed(info) for info in starts] == [{EXECUTION}, {"pause"}]
    assert len(ends) == 1
    assert ends[0].is_replayed is False
