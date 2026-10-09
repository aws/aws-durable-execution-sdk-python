"""Unit tests for pause/resume on the Executor and its checkpoint storage.

Covers the pause state on ``Execution``, round-tripped through
``to_json_dict``/``from_json_dict``, and the checkpoint path, which omits
the token while paused but still registers the updates. Follows the
harness in ``executor_checkpoint_test.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution, PauseState
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


@pytest.mark.parametrize(
    ("transitions", "expected_state"),
    [
        ((), PauseState.NOT_PAUSED),
        ((Execution.pause,), PauseState.PAUSED),
        ((Execution.pause, Execution.pause), PauseState.PAUSED),
        ((Execution.defer_invocation,), PauseState.NOT_PAUSED),
        (
            (Execution.pause, Execution.defer_invocation),
            PauseState.PAUSED_INVOCATION_DEFERRED,
        ),
        (
            (Execution.pause, Execution.defer_invocation, Execution.pause),
            PauseState.PAUSED_INVOCATION_DEFERRED,
        ),
        (
            (Execution.pause, Execution.defer_invocation, Execution.defer_invocation),
            PauseState.PAUSED_INVOCATION_DEFERRED,
        ),
    ],
)
def test_pause_state_transitions_round_trip(
    transitions: tuple[Callable[[Execution], None], ...], expected_state: PauseState
) -> None:
    execution = Execution.new(_make_start_input())
    for transition in transitions:
        transition(execution)

    data = execution.to_json_dict()
    assert data["PauseState"] == expected_state.value

    restored = Execution.from_json_dict(data)
    deferred = expected_state is PauseState.PAUSED_INVOCATION_DEFERRED
    for candidate in (execution, restored):
        assert candidate.pause_state is expected_state
        assert candidate.is_paused is (expected_state is not PauseState.NOT_PAUSED)
        assert candidate.has_deferred_invocation is deferred

        assert candidate.resume() is deferred

        assert candidate.pause_state is PauseState.NOT_PAUSED
        assert candidate.is_paused is False
        assert candidate.has_deferred_invocation is False

        assert candidate.resume() is False

        candidate.defer_invocation()
        assert candidate.pause_state is PauseState.NOT_PAUSED

        candidate.pause()
        assert candidate.pause_state is PauseState.PAUSED


def test_execution_without_saved_pause_state_loads_as_not_paused() -> None:
    data = Execution.new(_make_start_input()).to_json_dict()
    del data["PauseState"]

    restored = Execution.from_json_dict(data)

    assert restored.pause_state is PauseState.NOT_PAUSED
    assert restored.is_paused is False
    assert restored.resume() is False


def test_invalid_saved_pause_state_is_rejected() -> None:
    data = Execution.new(_make_start_input()).to_json_dict()
    data["PauseState"] = "INVALID"

    with pytest.raises(ValueError, match="is not a valid PauseState"):
        Execution.from_json_dict(data)


@pytest.mark.parametrize(
    "attribute", ["pause_state", "is_paused", "has_deferred_invocation"]
)
def test_pause_state_properties_are_read_only(attribute: str) -> None:
    execution = Execution.new(_make_start_input())

    with pytest.raises(AttributeError):
        setattr(execution, attribute, PauseState.PAUSED)

    assert execution.pause_state is PauseState.NOT_PAUSED


@pytest.mark.parametrize("with_updates", [True, False])
def test_checkpoint_while_paused_omits_token_and_state_but_registers_update(
    with_updates: bool,
) -> None:
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    previous_sequence = execution.token_sequence
    execution.pause()
    store.save(execution)
    updates = [_step_start_update("step-A")] if with_updates else []
    expected_operation_ids = ["step-A"] if with_updates else []

    response = executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=token_0,
        updates=updates,
    )

    assert response.checkpoint_token is None
    assert response.new_execution_state is not None
    assert response.new_execution_state.operations == []

    reloaded = store.load(execution.durable_execution_arn)
    assert reloaded.handler_seen_seq == 0
    assert reloaded.token_sequence == previous_sequence + 1
    assert [
        op.operation_id
        for op in reloaded.get_navigable_operations()
        if op.operation_type is OperationType.STEP
    ] == expected_operation_ids
    assert reloaded.pause_state is PauseState.PAUSED_INVOCATION_DEFERRED


def test_checkpoint_while_not_paused_still_returns_token():
    executor, _, execution, token_0 = _make_executor_with_started_execution()

    response = executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=token_0,
        updates=[],
    )

    assert response.checkpoint_token is not None


def test_begin_invocation_while_paused_defers_without_claiming_gate():
    executor, store, execution, _ = _make_executor_with_started_execution()
    executor._set_invocation_gate(  # noqa: SLF001
        execution.durable_execution_arn, InvocationState.PRE_INVOKE
    )
    execution.pause()
    store.save(execution)

    result = executor._begin_invocation(execution.durable_execution_arn)  # noqa: SLF001

    assert result is None
    assert (
        executor._invocation_gate(execution.durable_execution_arn)  # noqa: SLF001
        is InvocationState.PRE_INVOKE
    )
    assert (
        store.load(execution.durable_execution_arn).pause_state
        is PauseState.PAUSED_INVOCATION_DEFERRED
    )
    executor._invoker.create_invocation_input.assert_not_called()  # noqa: SLF001


def test_pause_and_resume_are_idempotent():
    executor, store, execution, _ = _make_executor_with_started_execution()
    # pause_execution() waits for the invocation gate to clear; the shared
    # helper leaves it INVOKING to let the checkpoint tests run, so release
    # it here to model no invocation in flight.
    executor._set_invocation_gate(  # noqa: SLF001
        execution.durable_execution_arn, InvocationState.PRE_INVOKE
    )

    executor.pause_execution(execution.durable_execution_arn)
    executor.pause_execution(execution.durable_execution_arn)
    assert store.load(execution.durable_execution_arn).is_paused is True

    executor.resume_execution(execution.durable_execution_arn)
    executor.resume_execution(execution.durable_execution_arn)
    assert store.load(execution.durable_execution_arn).is_paused is False
    # No invocation scheduled in between pause and resume, so the scheduler should receive no calls
    executor._scheduler.call_later.assert_not_called()  # noqa: SLF001


def test_pause_is_a_no_op_once_the_execution_has_finished():
    executor, store, execution, _ = _make_executor_with_started_execution()
    executor.complete_execution(execution.durable_execution_arn, result='"done"')

    executor.pause_execution(execution.durable_execution_arn)

    assert store.load(execution.durable_execution_arn).is_paused is False


def test_resume_is_a_no_op_when_not_paused():
    executor, store, execution, _ = _make_executor_with_started_execution()

    executor.resume_execution(execution.durable_execution_arn)

    reloaded = store.load(execution.durable_execution_arn)
    assert reloaded.is_paused is False
    assert reloaded.pause_state is PauseState.NOT_PAUSED


def test_checkpoint_with_the_withheld_invocations_token_is_rejected():
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    execution.pause()
    store.save(execution)
    executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("step-A")],
    )

    with pytest.raises(InvalidParameterValueException) as exc_info:
        executor.checkpoint_execution(
            execution_arn=execution.durable_execution_arn,
            checkpoint_token=token_0,
            updates=[_step_start_update("step-B")],
        )
    assert str(exc_info.value) == "Invalid checkpoint token"


@pytest.mark.parametrize("retry_client_token", ["c1", None, "different-client-token"])
def test_paused_checkpoint_retry_is_rejected_even_after_resume(
    retry_client_token: str | None,
) -> None:
    executor, store, execution, inbound = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn
    execution.pause()
    store.save(execution)

    first = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=inbound,
        updates=[],
        client_token="c1",
    )
    assert first.checkpoint_token is None

    executor.resume_execution(arn)
    assert store.load(arn).is_paused is False
    before_retry = deepcopy(store.load(arn).to_json_dict())

    with pytest.raises(
        InvalidParameterValueException, match="^Invalid checkpoint token$"
    ):
        executor.checkpoint_execution(
            execution_arn=arn,
            checkpoint_token=inbound,
            updates=[],
            client_token=retry_client_token,
        )

    assert store.load(arn).to_json_dict() == before_retry


def test_paused_checkpoint_does_not_create_idempotency_record() -> None:
    executor, store, execution, inbound = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    assert execution.last_checkpoint is None

    execution.pause()
    store.save(execution)
    executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=inbound,
        updates=[],
        client_token="c1",
    )

    assert store.load(arn).last_checkpoint is None


def test_invoke_execution_while_paused_still_schedules_with_its_delay():
    executor, store, execution, _ = _make_executor_with_started_execution()
    execution.pause()
    store.save(execution)

    executor._invoke_execution(execution.durable_execution_arn, delay=7)  # noqa: SLF001

    executor._scheduler.call_later.assert_called_once()  # noqa: SLF001
    assert executor._scheduler.call_later.call_args.kwargs["delay"] == 7  # noqa: SLF001
    assert store.load(execution.durable_execution_arn).pause_state is PauseState.PAUSED


def test_paused_pending_is_accepted_only_when_a_token_was_withheld():
    executor, store, execution, _ = _make_executor_with_started_execution()
    execution.pause()
    store.save(execution)
    pending = DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)

    with pytest.raises(InvalidParameterValueException) as exc_info:
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            execution.durable_execution_arn, pending, execution, execution.seq_counter
        )
    assert str(exc_info.value) == (
        "Cannot return PENDING status with no pending operations."
    )

    response = executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=execution.get_new_checkpoint_token(),
        updates=[],
    )

    assert response.checkpoint_token is None
    assert execution.pause_state is PauseState.PAUSED_INVOCATION_DEFERRED
    executor._validate_invocation_response_and_store(  # noqa: SLF001
        execution.durable_execution_arn, pending, execution, execution.seq_counter
    )

    executor.resume_execution(execution.durable_execution_arn)

    with pytest.raises(InvalidParameterValueException, match="no pending operations"):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            execution.durable_execution_arn, pending, execution, execution.seq_counter
        )


def test_resume_schedules_deferred_invocation_once_before_it_starts() -> None:
    executor, store, execution, _ = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn
    executor._set_invocation_gate(arn, InvocationState.PRE_INVOKE)  # noqa: SLF001
    execution.pause()
    store.save(execution)
    executor._begin_invocation(arn)  # noqa: SLF001
    assert execution.pause_state is PauseState.PAUSED_INVOCATION_DEFERRED

    executor.pause_execution(arn)
    executor.resume_execution(arn)
    executor.resume_execution(arn)

    scheduler = executor._scheduler  # noqa: SLF001
    invoker = executor._invoker  # noqa: SLF001
    assert isinstance(scheduler, Mock)
    assert isinstance(invoker, Mock)
    scheduler.call_later.assert_called_once()
    assert store.load(arn).pause_state is PauseState.NOT_PAUSED
    assert executor._invocation_gate(arn) is InvocationState.PRE_INVOKE  # noqa: SLF001
    invoker.create_invocation_input.assert_not_called()


def test_pause_and_resume_raise_for_an_unknown_execution() -> None:
    executor, _, _, _ = _make_executor_with_started_execution()

    for call in (executor.pause_execution, executor.resume_execution):
        with pytest.raises(ResourceNotFoundException) as exc_info:
            call("arn:unknown")
        assert str(exc_info.value) == "Durable Execution does not exist"

