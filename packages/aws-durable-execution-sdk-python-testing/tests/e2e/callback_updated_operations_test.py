"""Real callback completions are delivered once before resumed user code."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    Duration,
    WaitForCallbackConfig,
)
from aws_durable_execution_sdk_python.exceptions import CallbackError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
    UserFunctionStartInfo,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.types import WaitForCallbackContext

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)


class CallbackObserver(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self.invocations: list[InvocationStartInfo] = []
        self.ends: list[OperationEndInfo] = []
        self.order: list[str] = []

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self.invocations.append(info)

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.name == "target":
            self.ends.append(info)
            self.order.append("target-end")

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if info.name == "observed":
            self.order.append("observed-start")


def _submit(_callback_id: str, _context: WaitForCallbackContext) -> None:
    return None


def _suspended_callback(runner: DurableFunctionTestRunner, arn: str, name: str) -> str:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        events = runner.get_execution_history(arn, include_execution_data=True).events
        starts = [
            event
            for event in events
            if event.event_type == "CallbackStarted" and event.name == name
        ]
        if starts and any(
            event.event_type == "InvocationCompleted"
            and event.event_id > starts[0].event_id
            for event in events
        ):
            details = starts[0].callback_started_details
            assert details is not None
            assert details.callback_id is not None
            return details.callback_id
        time.sleep(0.01)
    raise AssertionError(f"Callback {name} did not suspend")


@pytest.mark.parametrize("outcome", ["success", "failure", "timeout"])
@pytest.mark.parametrize("filesystem", [False, True])
def test_callback_update_is_consumed_before_user_code(
    outcome: str, filesystem: bool, tmp_path: Path
) -> None:
    observer = CallbackObserver()
    marker_calls: list[str] = []

    @durable_step
    def observed(_context: StepContext, value: str) -> str:
        marker_calls.append(value)
        return value

    def handler(_event: Any, context: DurableContext) -> str:
        config = CallbackConfig(
            timeout=Duration.from_seconds(1 if outcome == "timeout" else 30),
            serdes=JsonSerDes(),
        )
        try:
            target = context.create_callback(name="target", config=config).result()
        except CallbackError:
            target = outcome
        assert isinstance(target, str)
        saved = context.step(observed(target), name="observed")
        callback_config = WaitForCallbackConfig(serdes=JsonSerDes())
        one = context.wait_for_callback(_submit, name="one", config=callback_config)
        two = context.wait_for_callback(_submit, name="two", config=callback_config)
        return "/".join((saved, one, two))

    wrapped = durable_execution(handler, plugins=[observer])
    store = FileSystemExecutionStore.create(tmp_path) if filesystem else None
    with DurableFunctionTestRunner(
        handler=wrapped,
        store=store,
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=25,
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = _suspended_callback(runner, arn, "target")
        if outcome == "success":
            runner.send_callback_success(
                callback_id, result=json.dumps("target").encode()
            )
        elif outcome == "failure":
            runner.send_callback_failure(
                callback_id, error=ErrorObject.from_message("explicit callback failure")
            )
        # The timeout case uses the runner's real scheduled callback deadline.
        for name in ["one", "two"]:
            callback_id = _suspended_callback(runner, arn, name + " create callback id")
            runner.send_callback_success(callback_id, result=json.dumps(name).encode())
        result = runner.wait_for_result(arn, timeout=10)

    expected = "target" if outcome == "success" else outcome
    assert result.status.value == "SUCCEEDED"
    assert result.result is not None
    assert json.loads(result.result) == expected + "/one/two"
    assert marker_calls == [expected]
    assert len(observer.invocations) == 4
    assert len(observer.ends) == 1
    terminal = observer.ends[0]
    assert (
        terminal.status.value
        == {"success": "SUCCEEDED", "failure": "FAILED", "timeout": "TIMED_OUT"}[
            outcome
        ]
    )
    assert terminal.is_replayed is False
    assert set(observer.invocations[1].updated_operations) == {terminal.operation_id}
    assert all(
        terminal.operation_id not in invocation.updated_operations
        for invocation in observer.invocations[2:]
    )
    assert observer.order == ["target-end", "observed-start"]
    if outcome == "failure":
        assert terminal.error is not None
        assert terminal.error.message == "explicit callback failure"
