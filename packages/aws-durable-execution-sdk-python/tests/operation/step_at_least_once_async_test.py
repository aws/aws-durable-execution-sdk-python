"""Tests for StepSemantics.AT_LEAST_ONCE_ASYNC.

The handlers run through durable_execution with a fake service client, so the
real checkpoint batcher thread sends every checkpoint.
"""

from __future__ import annotations

import datetime
import threading
import time
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import Duration, StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    CheckpointError,
    DurableApiErrorCategory,
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
    WaitDetails,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    OperationEndInfo,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import PassThroughSerDes
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
        error: Exception | None = None,
    ) -> None:
        self.delays = delays or {}
        self.fail_on = fail_on
        self.error = error or RuntimeError("checkpoint failed")
        self.answered: list[OperationAction] = []
        self.answered_names: set[tuple[OperationAction, str | None]] = set()
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
            raise self.error
        with self.lock:
            self.answered.extend(actions)
            self.answered_names.update((u.action, u.name) for u in updates)
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

    def was_answered(self, action: OperationAction, name: str | None = None) -> bool:
        with self.lock:
            if name is not None:
                return (action, name) in self.answered_names
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

    with pytest.raises(RuntimeError, match="checkpoint failed"):
        _invoke(handler, client)


def _bytes_step_config(semantics: StepSemantics) -> StepConfig:
    # PassThroughSerDes keeps the bytes in the update, so sizing the update for
    # the batch fails in json.dumps.
    return StepConfig(serdes=PassThroughSerDes(), step_semantics=semantics)


def test_unsizable_async_succeed_does_not_report_succeeded():
    """The batcher cannot size the SUCCEED, so it is never saved. The handler
    ignores the step result and returns. The invocation raises the error, so
    Lambda retries it, instead of answering SUCCEEDED."""
    client = _SlowClient()
    config = _bytes_step_config(StepSemantics.AT_LEAST_ONCE_ASYNC)

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        context.step(lambda _: b"value", name="s", config=config)
        return "done"

    with pytest.raises(TypeError, match="bytes"):
        _invoke(handler, client)
    assert not client.was_answered(OperationAction.SUCCEED)


def test_unsizable_sync_succeed_raises_instead_of_hanging():
    """A sync SUCCEED that the batcher cannot size wakes its waiter with the
    error."""
    client = _SlowClient()
    config = _bytes_step_config(StepSemantics.AT_LEAST_ONCE)

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        context.step(lambda _: b"value", name="s", config=config)
        return "done"

    outcome: dict[str, BaseException] = {}

    def run() -> None:
        try:
            _invoke(handler, client)
        except BaseException as e:  # noqa: BLE001
            outcome["error"] = e

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=10)

    assert not thread.is_alive(), "sync step waiter hung"
    assert isinstance(outcome.get("error"), TypeError)
    assert not client.was_answered(OperationAction.SUCCEED)


def _failing_succeed(category: DurableApiErrorCategory) -> _SlowClient:
    return _SlowClient(
        {OperationAction.SUCCEED: 0.1},
        fail_on=OperationAction.SUCCEED,
        error=CheckpointError("checkpoint failed", error_category=category),
    )


def _step_then_raise(_event: Any, context: DurableContext) -> None:
    context.step(lambda _: "value", name="s", config=ASYNC)
    msg = "handler failed"
    raise ValueError(msg)


def test_handler_error_after_unsaved_succeed_retries_the_invocation():
    """The handler raises while the SUCCEED is queued, and the SUCCEED then
    fails with an invocation-scope error. The invocation raises, so Lambda
    retries it, instead of answering FAILED without the step's result."""
    client = _failing_succeed(DurableApiErrorCategory.INVOCATION)

    with pytest.raises(CheckpointError, match="checkpoint failed"):
        _invoke(durable_execution(_step_then_raise), client)


def test_handler_error_after_execution_scope_failure_answers_failed():
    """An execution-scope checkpoint failure, such as a validation error,
    still answers FAILED with the handler's error."""
    client = _failing_succeed(DurableApiErrorCategory.EXECUTION)

    output = _invoke(durable_execution(_step_then_raise), client)

    assert output["Status"] == InvocationStatus.FAILED.value
    assert output["Error"]["ErrorMessage"] == "handler failed"


def test_result_after_execution_scope_failure_answers_failed():
    """A handler that returns after an execution-scope SUCCEED failure answers
    FAILED with the checkpoint error, as when the handler sees the failure."""
    client = _failing_succeed(DurableApiErrorCategory.EXECUTION)

    @durable_execution
    def handler(_event: Any, context: DurableContext) -> str:
        return context.step(lambda _: "value", name="s", config=ASYNC)

    output = _invoke(handler, client)

    assert output["Status"] == InvocationStatus.FAILED.value
    assert output["Error"]["ErrorMessage"] == "checkpoint failed"


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


def test_default_step_semantics_reaches_every_context():
    """default_step_semantics applies at the root, in child contexts and in map
    items. A step's own step_semantics wins."""
    client = _SlowClient({OperationAction.SUCCEED: 0.2})
    saved_at_return: dict[str, bool] = {}

    def run_step(context: DurableContext, name: str, config=None) -> str:
        result = context.step(lambda _: name, name=name, config=config)
        saved_at_return[name] = client.was_answered(OperationAction.SUCCEED, name)
        return result

    @durable_execution(default_step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC)
    def handler(_event: Any, context: DurableContext) -> list[str]:
        results = [run_step(context, "root")]
        results.append(
            context.run_in_child_context(
                lambda child: run_step(child, "child"), name="group"
            )
        )
        results.extend(
            context.map(
                ["a", "b"],
                lambda item_context, item, _i, _items: run_step(
                    item_context, f"item-{item}"
                ),
                name="items",
            ).get_results()
        )
        results.append(
            run_step(
                context,
                "own",
                StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE),
            )
        )
        return results

    output = _invoke(handler, client)

    assert output["Status"] == InvocationStatus.SUCCEEDED.value
    assert saved_at_return == {
        "root": False,
        "child": False,
        "item-a": False,
        "item-b": False,
        "own": True,
    }
    for name in saved_at_return:
        assert client.was_answered(OperationAction.SUCCEED, name)


def _pending_wait_operation() -> Operation:
    """A wait that started in an earlier invocation and ends in an hour."""
    return Operation(
        operation_id=OperationIdNamespace().create_id_for_step(2),
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
        name="w",
        sub_type="Wait",
        wait_details=WaitDetails(
            scheduled_end_timestamp=datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(hours=1)
        ),
    )


def _step_then_pending_wait(_event: Any, context: DurableContext) -> str:
    context.step(lambda _: "v", name="s", config=ASYNC)
    context.wait(Duration.from_seconds(3600), name="w")
    return "done"


def test_suspend_after_execution_scope_failure_answers_failed():
    """The handler suspends while the async SUCCEED is queued, and it fails for good."""
    client = _failing_succeed(DurableApiErrorCategory.EXECUTION)

    out = _invoke(
        durable_execution(_step_then_pending_wait), client, [_pending_wait_operation()]
    )

    assert out["Status"] == InvocationStatus.FAILED.value


def test_suspend_after_invocation_scope_failure_retries_the_invocation():
    """The handler suspends while the async SUCCEED is queued, and it fails for now."""
    client = _failing_succeed(DurableApiErrorCategory.INVOCATION)

    with pytest.raises(CheckpointError):
        _invoke(
            durable_execution(_step_then_pending_wait),
            client,
            [_pending_wait_operation()],
        )
