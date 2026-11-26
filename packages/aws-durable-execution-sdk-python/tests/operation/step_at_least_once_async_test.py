"""Tests for StepSemantics.AT_LEAST_ONCE_ASYNC.

The handlers run through durable_execution with a fake service client, so the
real checkpoint batcher thread sends every checkpoint.
"""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import Duration, StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    BackgroundThreadError,
    StepError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInputWithClient,
    InitialExecutionState,
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdNamespace
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    DurableServiceClient,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StepDetails,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    OperationEndInfo,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from tests.test_helpers import plugin_factory

ASYNC = StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC)

_STATUS_BY_ACTION = {
    OperationAction.START: OperationStatus.STARTED,
    OperationAction.SUCCEED: OperationStatus.SUCCEEDED,
    OperationAction.FAIL: OperationStatus.FAILED,
    OperationAction.RETRY: OperationStatus.PENDING,
}


class _SlowClient:
    """Answer each checkpoint after a delay, and record what was answered.

    ``delays`` maps an action to the seconds a batch that carries it waits
    before the answer. ``fail_on`` names an action whose batch raises instead.
    """

    def __init__(
        self,
        delays: dict[OperationAction, float] | None = None,
        fail_on: OperationAction | None = None,
    ) -> None:
        self.delays = delays or {}
        self.fail_on = fail_on
        self.answered: list[OperationAction] = []
        self.lock = threading.Lock()
        self.mock = Mock(spec=DurableServiceClient)
        self.mock.checkpoint.side_effect = self._checkpoint

    def _checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None = None,
    ) -> CheckpointOutput:
        actions = [update.action for update in updates]
        delay = max((self.delays.get(action, 0.0) for action in actions), default=0)
        time.sleep(delay)
        if self.fail_on in actions:
            msg = "checkpoint failed"
            raise RuntimeError(msg)
        with self.lock:
            self.answered.extend(actions)
        return CheckpointOutput(
            checkpoint_token=f"{checkpoint_token}-next",
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=[
                    Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=_STATUS_BY_ACTION[update.action],
                        name=update.name,
                        step_details=StepDetails(attempt=1, result=update.payload),
                    )
                    for update in updates
                    if update.operation_type is OperationType.STEP
                ]
            ),
        )

    def was_answered(self, action: OperationAction) -> bool:
        with self.lock:
            return action in self.answered


def _invoke(handler, client: _SlowClient, operations: list[Operation] | None = None):
    execution = Operation(
        operation_id="exec1",
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        execution_details=ExecutionDetails(input_payload="{}"),
    )
    invocation_input = DurableExecutionInvocationInputWithClient(
        durable_execution_arn="arn:test:execution/exec1",
        checkpoint_token="token",  # noqa: S106
        initial_execution_state=InitialExecutionState(
            operations=[execution, *(operations or [])], next_marker=""
        ),
        service_client=client.mock,
    )
    lambda_context = Mock()
    lambda_context.aws_request_id = "request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 1_000_000  # noqa: SLF001
    lambda_context.invoked_function_arn = None
    lambda_context.tenant_id = None
    return handler(invocation_input, lambda_context)


def _no_retry(_error: Exception, _attempt: int) -> RetryDecision:
    return RetryDecision.no_retry()


def _retry(_error: Exception, _attempt: int) -> RetryDecision:
    return RetryDecision.retry(Duration.from_seconds(1))


def _raise(_context: Any) -> None:
    msg = "boom"
    raise ValueError(msg)


def test_async_step_returns_before_succeed_is_answered():
    client = _SlowClient({OperationAction.SUCCEED: 0.3})
    seen: dict[str, bool] = {}

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        result = context.step(lambda _: "value", name="s", config=ASYNC)
        seen["answered"] = client.was_answered(OperationAction.SUCCEED)
        return result

    output = _invoke(handler, client)

    assert seen["answered"] is False
    assert output["Status"] == InvocationStatus.SUCCEEDED.value
    assert client.was_answered(OperationAction.SUCCEED)


@pytest.mark.parametrize(
    "config",
    [None, StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE)],
)
def test_at_least_once_step_still_waits_for_succeed(config):
    client = _SlowClient({OperationAction.SUCCEED: 0.1})
    seen: dict[str, bool] = {}

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        result = context.step(lambda _: "value", name="s", config=config)
        seen["answered"] = client.was_answered(OperationAction.SUCCEED)
        return result

    output = _invoke(handler, client)

    assert seen["answered"] is True
    assert output["Status"] == InvocationStatus.SUCCEEDED.value


def test_async_step_waits_for_fail():
    client = _SlowClient({OperationAction.FAIL: 0.1})
    seen: dict[str, bool] = {}

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> None:
        config = StepConfig(
            retry_strategy=_no_retry,
            step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC,
        )
        try:
            context.step(_raise, name="s", config=config)
        except StepError:
            seen["answered"] = client.was_answered(OperationAction.FAIL)
            raise

    output = _invoke(handler, client)

    assert seen["answered"] is True
    assert output["Status"] == InvocationStatus.FAILED.value


def test_async_step_waits_for_retry():
    client = _SlowClient({OperationAction.RETRY: 0.1})
    seen: dict[str, bool] = {}

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> None:
        config = StepConfig(
            retry_strategy=_retry,
            step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC,
        )
        try:
            context.step(_raise, name="s", config=config)
        except SuspendExecution:
            seen["answered"] = client.was_answered(OperationAction.RETRY)
            raise

    output = _invoke(handler, client)

    assert seen["answered"] is True
    assert output["Status"] == InvocationStatus.PENDING.value


def test_started_async_step_runs_again_on_replay():
    """An async step whose SUCCEED was not saved runs again, like AT_LEAST_ONCE."""
    client = _SlowClient()
    calls: list[int] = []

    def run(_context: Any) -> str:
        calls.append(1)
        return "again"

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(run, name="s", config=ASYNC)

    started = Operation(
        operation_id=OperationIdNamespace().create_id_for_step(1),
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        name="s",
        sub_type=OperationSubType.STEP.value,
        step_details=StepDetails(attempt=0),
    )

    output = _invoke(handler, client, [started])

    assert calls == [1]
    assert output["Status"] == InvocationStatus.SUCCEEDED.value
    assert client.answered == [OperationAction.SUCCEED]


def test_succeed_queued_at_handler_end_is_saved_before_succeeded():
    """The batcher sends a SUCCEED that is still queued when the handler returns.

    The START batch is slow, so the SUCCEED is still in the queue when the
    handler returns and the batcher is told to stop.
    """
    client = _SlowClient({OperationAction.START: 0.3})

    def slow(_context: Any) -> str:
        # Longer than the batcher's empty-queue wait, so START goes alone.
        time.sleep(0.15)
        return "value"

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(slow, name="s", config=ASYNC)

    output = _invoke(handler, client)

    assert output["Status"] == InvocationStatus.SUCCEEDED.value
    assert client.answered == [OperationAction.START, OperationAction.SUCCEED]


def test_failed_succeed_batch_does_not_report_succeeded():
    client = _SlowClient(
        {OperationAction.SUCCEED: 0.1}, fail_on=OperationAction.SUCCEED
    )

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(lambda _: "value", name="s", config=ASYNC)

    with pytest.raises(BackgroundThreadError) as raised:
        _invoke(handler, client)
    assert str(raised.value.source_exception) == "checkpoint failed"


def test_plugin_sees_async_step_end_before_invocation_end():
    class Recorder(DurableInstrumentationPlugin):
        def __init__(self) -> None:
            self.events: list[Any] = []

        def on_operation_end(self, info: OperationEndInfo) -> None:
            self.events.append(info)

        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            self.events.append("invocation_end")

    recorder = Recorder()
    client = _SlowClient({OperationAction.SUCCEED: 0.2})

    @durable_execution(plugins=[plugin_factory(recorder)])
    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(lambda _: "value", name="s", config=ASYNC)

    output = _invoke(handler, client)

    assert output["Status"] == InvocationStatus.SUCCEEDED.value
    step_end, last = recorder.events
    assert isinstance(step_end, OperationEndInfo)
    assert step_end.status is OperationStatus.SUCCEEDED
    assert step_end.result == '"value"'
    assert last == "invocation_end"
