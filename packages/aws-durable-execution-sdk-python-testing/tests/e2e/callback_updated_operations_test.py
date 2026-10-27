"""The first externally completed callback is visible before dependent code."""

import json
import time
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.exceptions import CallbackError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)


def _suspended_callback(runner: DurableFunctionTestRunner, arn: str) -> str:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        events = runner.get_execution_history(arn, include_execution_data=True).events
        starts = [
            event
            for event in events
            if event.event_type == "CallbackStarted" and event.name == "target"
        ]
        if starts and any(
            event.event_type == "InvocationCompleted"
            and event.event_id > starts[0].event_id
            for event in events
        ):
            details = starts[0].callback_started_details
            assert details is not None and details.callback_id is not None
            return details.callback_id
        time.sleep(0.01)
    raise AssertionError("The target callback did not suspend")


@pytest.mark.parametrize("outcome", ["success", "failure", "timeout"])
@pytest.mark.parametrize("filesystem", [False, True])
def test_callback_first_completion_precedes_dependent_step(
    outcome: str, filesystem: bool, tmp_path: Path
) -> None:
    invocations: list[InvocationStartInfo] = []
    ends: list[OperationEndInfo] = []
    saved_calls: list[str] = []
    observed_calls: list[str] = []
    completions_before_step: list[OperationEndInfo] = []

    class Observer(DurableInstrumentationPlugin):
        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            invocations.append(info)

        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.name == "target":
                ends.append(info)

    class ObserverFactory:
        def create_plugin(self, _info: InvocationStartInfo) -> Observer:
            return Observer()

    def saved_step(_context: Any) -> str:
        saved_calls.append("saved")
        return "saved"

    def observed_step(_context: Any, value: str) -> str:
        completions_before_step.extend(ends)
        observed_calls.append(value)
        return value

    def body(_event: Any, context: DurableContext) -> str:
        saved = context.step(saved_step, name="saved")
        config = CallbackConfig(
            timeout=Duration.from_seconds(1 if outcome == "timeout" else 30),
            serdes=JsonSerDes(),
        )
        try:
            value = context.create_callback(name="target", config=config).result()
        except CallbackError:
            value = outcome
        assert isinstance(value, str)
        observed = context.step(
            lambda step_context: observed_step(step_context, value), name="observed"
        )
        return saved + "/" + observed

    with DurableFunctionTestRunner(
        handler=durable_execution(body, plugins=[ObserverFactory()]),
        store=FileSystemExecutionStore.create(tmp_path) if filesystem else None,
        skip_time=False,
        poll_interval=0.01,
        execution_timeout=20,
    ) as runner:
        arn = runner.run_async(input="{}")
        callback_id = _suspended_callback(runner, arn)
        if outcome == "success":
            runner.send_callback_success(
                callback_id, result=json.dumps("target").encode()
            )
        elif outcome == "failure":
            runner.send_callback_failure(
                callback_id, error=ErrorObject.from_message("explicit failure")
            )
        # Timeout exercises the scheduled public-runner completion path.
        result = runner.wait_for_result(arn, timeout=10)

    expected = "target" if outcome == "success" else outcome
    assert result.status.value == "SUCCEEDED"
    assert (
        result.result is not None and json.loads(result.result) == "saved/" + expected
    )
    assert saved_calls == ["saved"]
    assert observed_calls == [expected]
    assert len(invocations) == 2
    assert completions_before_step, "The terminal hook must precede dependent code"
    terminal = completions_before_step[0]
    assert terminal.is_replayed is False
    assert (
        terminal.status.value
        == {"success": "SUCCEEDED", "failure": "FAILED", "timeout": "TIMED_OUT"}[
            outcome
        ]
    )
    snapshot = invocations[1].updated_operations[terminal.operation_id]
    assert snapshot.status is terminal.status
    assert snapshot.result == terminal.result
    assert snapshot.error == terminal.error
    if outcome == "success":
        assert terminal.result == json.dumps("target")
        assert terminal.error is None
    elif outcome == "failure":
        assert terminal.error is not None
        assert terminal.error.message == "explicit failure"
    else:
        assert terminal.error is not None
