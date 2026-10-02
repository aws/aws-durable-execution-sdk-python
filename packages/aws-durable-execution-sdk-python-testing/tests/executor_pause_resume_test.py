"""Unit tests for pause/resume on the Executor and its checkpoint storage.

Covers the ``paused`` flag on ``Execution``, round-tripped through
``to_json_dict``/``from_json_dict``, and the checkpoint path, which omits
the token while paused but still registers the updates. Follows the
harness in ``executor_checkpoint_test.py``.
"""

from __future__ import annotations

from unittest.mock import Mock

from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.executor import Executor, InvocationState
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.token import CheckpointToken


def _make_start_input() -> StartDurableExecutionInput:
    return StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )


def _make_executor_with_started_execution() -> tuple[
    Executor, InMemoryExecutionStore, Execution, str
]:
    store = InMemoryExecutionStore()
    scheduler = Mock()
    invoker = Mock()
    checkpoint_processor = Mock()

    execution = Execution.new(_make_start_input())
    execution.start()
    store.save(execution)

    executor = Executor(store, scheduler, invoker, checkpoint_processor)
    executor._set_invocation_gate(  # noqa: SLF001
        execution.durable_execution_arn, InvocationState.INVOKING
    )

    initial_token = CheckpointToken(
        execution_arn=execution.durable_execution_arn,
        token_sequence=0,
    ).to_str()
    return executor, store, execution, initial_token


def _step_start_update(op_id: str, name: str | None = None) -> OperationUpdate:
    return OperationUpdate(
        operation_id=op_id,
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name=name or op_id,
    )


# region: the paused flag round-trips through the store


def test_execution_paused_flag_defaults_false_and_round_trips():
    execution = Execution.new(_make_start_input())
    assert execution.paused is False

    execution.paused = True
    restored = Execution.from_json_dict(execution.to_json_dict())
    assert restored.paused is True


# endregion
# region: a checkpoint while paused omits the token but keeps the update


def test_checkpoint_while_paused_omits_token_but_registers_update():
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    execution.paused = True
    store.save(execution)

    response = executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("step-A")],
    )

    assert response.checkpoint_token is None
    assert [op.operation_id for op in response.new_execution_state.operations] == [
        "step-A"
    ]

    reloaded = store.load(execution.durable_execution_arn)
    assert any(
        op.operation_id == "step-A" for op in reloaded.get_navigable_operations()
    )
    assert reloaded.deferred_invocation is True


def test_checkpoint_while_not_paused_still_returns_token():
    executor, _store, execution, token_0 = _make_executor_with_started_execution()

    response = executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=token_0,
        updates=[],
    )

    assert response.checkpoint_token is not None


# endregion
# region: pause_execution / resume_execution are idempotent and a no-op once complete


def test_pause_and_resume_are_idempotent():
    executor, store, execution, _token_0 = _make_executor_with_started_execution()
    # pause_execution() waits for the invocation gate to clear; the shared
    # helper leaves it INVOKING to let the checkpoint tests run, so release
    # it here to model no invocation in flight.
    executor._set_invocation_gate(  # noqa: SLF001
        execution.durable_execution_arn, InvocationState.PRE_INVOKE
    )

    executor.pause_execution(execution.durable_execution_arn)
    executor.pause_execution(execution.durable_execution_arn)
    assert store.load(execution.durable_execution_arn).paused is True

    executor.resume_execution(execution.durable_execution_arn)
    executor.resume_execution(execution.durable_execution_arn)
    assert store.load(execution.durable_execution_arn).paused is False


def test_pause_is_a_no_op_once_the_execution_has_finished():
    executor, store, execution, _token_0 = _make_executor_with_started_execution()
    executor.complete_execution(execution.durable_execution_arn, result='"done"')

    executor.pause_execution(execution.durable_execution_arn)

    assert store.load(execution.durable_execution_arn).paused is False


def test_resume_is_a_no_op_when_not_paused():
    executor, store, execution, _token_0 = _make_executor_with_started_execution()

    executor.resume_execution(execution.durable_execution_arn)

    reloaded = store.load(execution.durable_execution_arn)
    assert reloaded.paused is False
    assert reloaded.deferred_invocation is False


# endregion
