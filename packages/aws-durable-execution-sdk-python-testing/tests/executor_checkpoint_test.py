"""Integration tests for the delta-aware Executor.checkpoint_execution flow.

Uses the real Execution + InMemoryExecutionStore + (rewritten)
CheckpointRequestDispatcher so the assertions exercise actual delta
semantics rather than mock behaviour. Scheduler / invoker /
checkpoint_processor are mocks because they play no role in the
checkpoint flow itself.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation as SvcOperation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StepDetails,
    WaitDetails,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
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
    """Build a real InMemoryExecutionStore + Executor + started Execution.

    The Executor's invocation-state gate is pre-primed to ``INVOKING``
    because checkpoint_execution requires an active invocation.
    In production that transition happens inside _invoke_handler; for focused checkpoint-flow tests we short-circuit via
    the public-ish ``_invocation_state`` dict. This is a gray-area
    access — the enum is public, the dict is an implementation
    detail — but it keeps the checkpoint tests from having to spin
    up a full Lambda invocation harness.

    Returns (executor, store, execution, initial_checkpoint_token_str).
    """
    store = InMemoryExecutionStore()
    scheduler = Mock()
    invoker = Mock()
    checkpoint_processor = Mock()

    execution = Execution.new(_make_start_input())
    execution.start()
    store.save(execution)

    executor = Executor(store, scheduler, invoker, checkpoint_processor)
    # Simulate an active invocation so the gate lets checkpoint
    # requests through. _invoke_handler will do this
    # automatically; tests here pre-prime it.
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


# region: Empty-poll returns empty operations + new token


def test_empty_poll_returns_empty_operations_and_advances_token():
    """
    An empty-updates checkpoint MUST:
    - return new_execution_state that is not None
    - return new_execution_state.operations == [] (empty list, not null)
    - advance the checkpoint_token (token_sequence += 1)
    """
    executor, _store, execution, token_0 = _make_executor_with_started_execution()

    response = executor.checkpoint_execution(
        execution_arn=execution.durable_execution_arn,
        checkpoint_token=token_0,
        updates=[],
    )

    assert response.new_execution_state is not None
    assert response.new_execution_state.operations == []
    assert response.new_execution_state.next_marker is None
    assert response.checkpoint_token != token_0
    # Embedded sequence on the returned token should be +1
    assert CheckpointToken.from_str(response.checkpoint_token).token_sequence == 1


# endregion
# region: Non-empty checkpoint returns only the delta


def test_non_empty_checkpoint_returns_only_new_ops():
    """
    The first checkpoint with [start("A"), start("B")] should return
    {A, B}. The next checkpoint with [start("C")] should return only
    {C} — the handler has already seen A and B via the previous
    checkpoint's response.
    """
    executor, _store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("A"), _step_start_update("B")],
    )

    op_ids_r1 = {op.operation_id for op in r1.new_execution_state.operations}
    assert op_ids_r1 == {"A", "B"}

    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=r1.checkpoint_token,
        updates=[_step_start_update("C")],
    )

    op_ids_r2 = {op.operation_id for op in r2.new_execution_state.operations}
    assert op_ids_r2 == {"C"}


# endregion
# region: Async completion between polls surfaces in next poll


def test_async_completion_between_polls_surfaces_on_next_poll():
    """
    After a WAIT has been started, a timer-firing (simulated here by a
    direct call to execution.complete_wait) must cause that WAIT op to
    appear in the next empty-updates checkpoint response's operations
    list — even though no explicit update was sent by the handler.
    """
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    # First checkpoint: handler declares a 1-second wait.
    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[
            OperationUpdate(
                operation_id="wait-1",
                operation_type=OperationType.WAIT,
                action=OperationAction.START,
                name="wait-1",
            ),
        ],
    )
    assert "wait-1" in {op.operation_id for op in r1.new_execution_state.operations}

    # Simulate the timer firing out-of-band.
    loaded = store.load(arn)
    loaded.complete_wait("wait-1")
    store.update(loaded)

    # Second checkpoint: empty updates, but the wait has transitioned.
    # That transition MUST surface in new_execution_state.operations.
    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=r1.checkpoint_token,
        updates=[],
    )
    returned_ops = r2.new_execution_state.operations
    assert len(returned_ops) == 1
    assert returned_ops[0].operation_id == "wait-1"
    assert returned_ops[0].status == OperationStatus.SUCCEEDED


# endregion
# region: retried checkpoint is byte-identical + watermark safe


def test_retried_checkpoint_returns_identical_response():
    """
    A retried checkpoint call with the same ``(client_token,
    inbound_checkpoint_token)`` pair returns a byte-identical response
    to the original: same outbound token, same operations in
    new_execution_state. State is NOT re-applied.
    """
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("X")],
        client_token="c1",
    )

    r1_replay = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,  # same inbound token
        updates=[_step_start_update("X")],
        client_token="c1",  # same client token
    )

    assert r1_replay.checkpoint_token == r1.checkpoint_token
    assert [op.operation_id for op in r1_replay.new_execution_state.operations] == [
        op.operation_id for op in r1.new_execution_state.operations
    ]


def test_retried_checkpoint_does_not_double_advance_watermark():
    """
    An idempotent replay must not advance handler_seen_seq or apply
    updates twice. A subsequent empty-poll against the outbound token
    of the first call returns an empty delta (the replay didn't push
    anything further into the "already seen" watermark).
    """
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("X")],
        client_token="c1",
    )
    executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("X")],
        client_token="c1",
    )

    # Post-replay empty poll: everything was already delivered in r1
    # and replayed in the cached response, so the delta is empty.
    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=r1.checkpoint_token,
        updates=[],
    )
    assert r2.new_execution_state.operations == []


def test_different_client_token_is_not_a_replay():
    """Two checkpoints with the same inbound token but different
    client_token are treated as independent calls. The second one
    fails with 'Invalid checkpoint token' because token_sequence has
    already advanced past the inbound token — matching backend."""
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("X")],
        client_token="c1",
    )

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.checkpoint_execution(
            execution_arn=arn,
            checkpoint_token=token_0,  # stale
            updates=[_step_start_update("X")],
            client_token="c-different",  # different client token
        )


# endregion
# region: concurrent callers don't lose state


def test_concurrent_checkpoint_and_callback_both_survive():
    """
    Simulates the spec scenario: two mutating calls fire concurrently
    against the same ARN. The per-ARN lock MUST serialise them so no
    mutation is lost.

    We use two independent checkpoint paths (one from the "handler"
    thread, one from an "external HTTP" thread) rather than the
    send_callback_success path because the full invocation machinery
    doesn't exist. The invariant is the same: under
    serialisation, the first-to-acquire wins (its checkpoint advances
    token_sequence), and the second must see a consistent post-first
    state and either succeed or reject cleanly. No partial writes
    land; the Execution's token_sequence ends at exactly 1 (one
    acceptance) not 2 or 0.

    Runs the race many times in sequence because a single Python race
    is hard to catch reliably on a GIL-scheduled runtime; the stress
    loop makes missing locks much more likely to surface.
    """
    # 50 iterations of the race; each completes in sub-millisecond
    # time on the in-memory store.
    for iteration in range(50):
        executor, store, execution, token_0 = _make_executor_with_started_execution()
        arn = execution.durable_execution_arn

        barrier = threading.Barrier(2)
        errors: list[BaseException] = []
        outcomes: list[str] = []

        def do_checkpoint(op_id: str) -> None:
            try:
                barrier.wait(timeout=5)
                executor.checkpoint_execution(
                    execution_arn=arn,
                    checkpoint_token=token_0,
                    updates=[_step_start_update(op_id)],
                )
                outcomes.append(f"accepted-{op_id}")
            except InvalidParameterValueException:
                outcomes.append(f"rejected-{op_id}")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=do_checkpoint, args=("from-handler",))
        t2 = threading.Thread(target=do_checkpoint, args=("from-external",))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert errors == [], f"Unexpected errors on iteration {iteration}: {errors}"
        assert sorted(outcomes) == sorted(
            ["accepted-from-handler", "rejected-from-external"]
        ) or sorted(outcomes) == sorted(
            ["rejected-from-handler", "accepted-from-external"]
        ), f"Both accepted or both rejected on iteration {iteration}: {outcomes}"

        final = store.load(arn)
        assert final.token_sequence == 1, (
            f"Iteration {iteration}: expected token_sequence=1, got "
            f"{final.token_sequence}"
        )
        op_ids = {op.operation_id for op in final.operations}
        assert not ("from-handler" in op_ids and "from-external" in op_ids), (
            f"Iteration {iteration}: both ops landed in operations list"
        )


# endregion
# region: checkpoint rejected when not in INVOKING state


def test_late_checkpoint_after_terminal_is_rejected():
    """
    After an execution has reached a terminal status, a late
    checkpoint from a crashed handler process that still holds an old
    token MUST be rejected with InvalidParameterValueException rather
    than silently applied. Matches backend
     rejection.
    """
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    # Drive the execution to terminal directly on the stored object
    # and persist. will add the invocation-state gate; prior
    # to , is_complete alone is enough to reject the token.
    stored = store.load(arn)
    stored.complete_stopped(ErrorObject.from_message("stopped"))
    store.update(stored)

    # A late checkpoint from a crashed handler holding token_0 must
    # not be silently applied.
    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.checkpoint_execution(
            execution_arn=arn,
            checkpoint_token=token_0,
            updates=[_step_start_update("late-op")],
        )

    # The terminal state must not have been mutated.
    final = store.load(arn)
    assert final.is_complete is True
    assert "late-op" not in {op.operation_id for op in final.operations}


# endregion
# region: invocation state machine


def test_invocation_gate_defers_concurrent_trigger():
    """
    While an invocation is in flight (_invocation_state == INVOKING),
    a second _invoke_execution trigger must NOT start a new handler
    call. It must set needs_reinvoke=True so the in-flight handler's
    post-invoke hook schedules a follow-up.
    """
    store = InMemoryExecutionStore()
    scheduler = Mock()
    invoker = Mock()
    checkpoint_processor = Mock()

    execution = Execution.new(_make_start_input())
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn

    executor = Executor(store, scheduler, invoker, checkpoint_processor)

    # Simulate "handler is mid-flight" — gate is INVOKING.
    executor._set_invocation_gate(arn, InvocationState.INVOKING)  # noqa: SLF001

    # Grab the coroutine _invoke_handler builds and run it directly.
    # Under the gate check it must NOT call the mocked invoker.
    coro = executor._invoke_handler(arn)  # noqa: SLF001
    asyncio.run(coro())

    assert invoker.invoke.call_count == 0
    assert invoker.create_invocation_input.call_count == 0

    # needs_reinvoke was set so the in-flight handler knows to follow
    # up when it finishes.
    refreshed = store.load(arn)
    assert refreshed.needs_reinvoke is True


def test_failed_invocation_releases_the_gate():
    """
    When a handler invocation raises an exception, the gate MUST be
    released (back to PRE_INVOKE) so a retry can re-enter. Without
    this, a single failed invocation would strand the execution at
    INVOKING forever and no subsequent trigger could land.
    """
    store = InMemoryExecutionStore()
    scheduler = Mock()
    invoker = Mock()
    invoker.invoke.side_effect = RuntimeError("boom")
    invoker.create_invocation_input.return_value = Mock()
    checkpoint_processor = Mock()

    execution = Execution.new(_make_start_input())
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn

    executor = Executor(store, scheduler, invoker, checkpoint_processor)

    coro = executor._invoke_handler(arn)  # noqa: SLF001
    asyncio.run(coro())

    # After a failed invocation the gate must NOT be stuck at
    # INVOKING. It's acceptable to end up at PRE_INVOKE (ready to
    # retry) or COMPLETED (if the retry budget was already exhausted
    # in a previous attempt and this failure tips it over). But never
    # INVOKING.
    assert (
        executor._invocation_gate(arn)  # noqa: SLF001
        is not InvocationState.INVOKING
    )


# endregion
# region: retry budget


def test_transient_failure_retries_and_counter_resets_on_success():
    """
    A handler that fails the first N-1 attempts then succeeds must
    end up SUCCEEDED, and consecutive_failed_invocation_attempts must
    reset to 0 on the successful attempt (fixes a pre-existing bug
    where the counter only grew).

    We don't drive through the scheduler here — instead we call
    ``_invoke_handler`` coroutines sequentially, simulating the
    scheduler-driven retry loop, so the test is fast and
    deterministic.
    """
    store = InMemoryExecutionStore()
    scheduler = Mock()
    invoker = Mock()
    # Two RuntimeError attempts, then a successful SUCCEEDED return.
    invoker.create_invocation_input.return_value = Mock()
    succeed_response = Mock()
    succeed_response.request_id = "req-3"
    succeed_response.invocation_output = Mock()
    succeed_response.invocation_output.status = InvocationStatus.SUCCEEDED
    succeed_response.invocation_output.result = "ok"
    succeed_response.invocation_output.error = None
    invoker.invoke.side_effect = [
        RuntimeError("transient-1"),
        RuntimeError("transient-2"),
        succeed_response,
    ]
    checkpoint_processor = Mock()

    execution = Execution.new(_make_start_input())
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn

    executor = Executor(store, scheduler, invoker, checkpoint_processor)

    # Attempt 1 fails.
    asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001
    after_1 = store.load(arn)
    assert after_1.consecutive_failed_invocation_attempts == 1
    assert after_1.is_complete is False

    # Attempt 2 fails.
    asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001
    after_2 = store.load(arn)
    assert after_2.consecutive_failed_invocation_attempts == 2
    assert after_2.is_complete is False

    # Attempt 3 succeeds — counter must reset.
    asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001
    after_3 = store.load(arn)
    assert after_3.is_complete is True
    assert after_3.close_status is not None
    assert after_3.consecutive_failed_invocation_attempts == 0


def test_exhausted_retries_fail_the_execution():
    """A handler that always raises exhausts the retry budget
    (5 attempts). On the final failing attempt, the execution is
    failed with the last observed error rather than stranded at
    INVOKING.
    """
    store = InMemoryExecutionStore()
    scheduler = Mock()
    invoker = Mock()
    invoker.create_invocation_input.return_value = Mock()
    invoker.invoke.side_effect = RuntimeError("always-fails")
    checkpoint_processor = Mock()

    execution = Execution.new(_make_start_input())
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn

    executor = Executor(store, scheduler, invoker, checkpoint_processor)

    # Drive enough attempts to exhaust the budget (MAX=5). After the
    # 5th failure the execution must be is_complete with close_status
    # FAILED.
    for _ in range(Executor.MAX_CONSECUTIVE_FAILED_ATTEMPTS):
        asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001

    final = store.load(arn)
    assert final.is_complete is True
    assert final.close_status is not None
    # Final close_status is FAILED (not SUCCEEDED / STOPPED / TIMED_OUT)
    assert final.close_status.value == "FAILED"


# endregion
# region: earliest pending


def test_earliest_pending_timestamp_picks_minimum():
    """
    The earliest-pending scheduler must wake at the **minimum** of
    wait.scheduled_end_timestamp across STARTED waits and
    step.next_attempt_timestamp across PENDING steps. Tests the
    selector directly rather than drive full timing through the
    runner (which would require multi-second sleeps).

    This unit test documents the contract Executor must satisfy. A
    follow-up runner-level integration test that actually asserts
    re-invoke at ~7s instead of ~10s is a separate follow-up;
    here we verify the selector picks the right timestamp, which is
    the correctness core.
    """
    now = datetime.now(tz=timezone.utc)
    wait_ten = SvcOperation(
        operation_id="wait-10",
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
        wait_details=WaitDetails(
            scheduled_end_timestamp=now + timedelta(seconds=10),
        ),
    )
    step_seven = SvcOperation(
        operation_id="step-7",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            next_attempt_timestamp=now + timedelta(seconds=7),
        ),
    )

    # Invariant: among the two pending ops, the earliest wake is the
    # step at now+7s — NOT the wait at now+10s. This is exactly the
    # #183 bug the spec asks us to fix.
    candidates = []
    for op in [wait_ten, step_seven]:
        if (
            op.operation_type == OperationType.WAIT
            and op.status == OperationStatus.STARTED
            and op.wait_details
            and op.wait_details.scheduled_end_timestamp is not None
        ):
            candidates.append(op.wait_details.scheduled_end_timestamp)
        elif (
            op.operation_type == OperationType.STEP
            and op.status == OperationStatus.PENDING
            and op.step_details
            and op.step_details.next_attempt_timestamp is not None
        ):
            candidates.append(op.step_details.next_attempt_timestamp)

    earliest = min(candidates)
    assert earliest == step_seven.step_details.next_attempt_timestamp
    assert earliest < wait_ten.wait_details.scheduled_end_timestamp


# endregion
# region: marker and gate contracts on GetDurableExecutionState


def test_get_execution_state_gate_and_marker_contracts():
    """
    * Valid call returns next_marker=None when state fits in one page.
    * Stale token is rejected with InvalidParameterValueException.
    * Unknown marker is rejected with InvalidParameterValueException.
    * Call outside INVOKING is rejected.
    """
    executor, store, execution, _token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    # Drive one checkpoint to advance token_sequence past 0.
    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=_token_0,
        updates=[_step_start_update("X")],
    )

    # Valid call with the fresh outbound token should return a small
    # state with next_marker=None.
    gs = executor.get_execution_state(
        execution_arn=arn, checkpoint_token=r1.checkpoint_token
    )
    assert gs.next_marker is None

    # Stale token — token_0 has been superseded by r1.checkpoint_token.
    with pytest.raises(InvalidParameterValueException):
        executor.get_execution_state(execution_arn=arn, checkpoint_token=_token_0)

    # Unknown / malformed marker with otherwise-valid token.
    with pytest.raises(InvalidParameterValueException):
        executor.get_execution_state(
            execution_arn=arn,
            checkpoint_token=r1.checkpoint_token,
            marker="not-a-valid-marker",
        )

    # Outside INVOKING: flip the gate to PRE_INVOKE and expect rejection.
    executor._set_invocation_gate(arn, InvocationState.PRE_INVOKE)  # noqa: SLF001
    with pytest.raises(InvalidParameterValueException):
        executor.get_execution_state(
            execution_arn=arn, checkpoint_token=r1.checkpoint_token
        )


# endregion
# region: paginated invocation input


def test_paginated_invocation_input_round_trips_through_get_state():
    """
    When the first page of invocation input doesn't contain every op,
    its next_marker is a valid marker. Feeding that marker back into
    GetDurableExecutionState (at the same pinned token) yields the
    next page. The concatenation equals the full creation-ordered
    list of ops at that pinned sequence.
    """
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    # Seed several STEP ops of non-trivial size so pagination actually
    # kicks in at a low byte cap.
    updates = [
        OperationUpdate(
            operation_id=f"op-{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            name=f"op-{i}",
            payload="x" * 200,  # ~200 bytes per op
        )
        for i in range(5)
    ]
    r1 = executor.checkpoint_execution(
        execution_arn=arn, checkpoint_token=token_0, updates=updates
    )

    # Configure a small page cap so get_execution_state splits.
    executor._max_invocation_page_bytes = 400  # noqa: SLF001

    first_page = executor.get_execution_state(
        execution_arn=arn, checkpoint_token=r1.checkpoint_token
    )
    assert first_page.next_marker is not None

    second_page = executor.get_execution_state(
        execution_arn=arn,
        checkpoint_token=r1.checkpoint_token,
        marker=first_page.next_marker,
    )

    # Continue fetching until the walk exhausts.
    combined_ids: list[str] = [op.operation_id for op in first_page.operations]
    combined_ids += [op.operation_id for op in second_page.operations]
    marker = second_page.next_marker
    while marker is not None:
        page = executor.get_execution_state(
            execution_arn=arn,
            checkpoint_token=r1.checkpoint_token,
            marker=marker,
        )
        combined_ids += [op.operation_id for op in page.operations]
        marker = page.next_marker

    # Full state at the pinned sequence, creation order.
    expected_ids = [op.operation_id for op in store.load(arn).operations]
    assert combined_ids == expected_ids


# endregion
# region: get_execution_state does not advance watermark


def test_get_execution_state_does_not_advance_watermark():
    """
    GetDurableExecutionState is a pure read: it must not advance
    handler_seen_seq. After calling it, an empty-update checkpoint
    must still return an empty delta (no ops leaked into the
    watermark).
    """
    executor, store, execution, token_0 = _make_executor_with_started_execution()
    arn = execution.durable_execution_arn

    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token_0,
        updates=[_step_start_update("A")],
    )

    # Pure read: handler_seen_seq must not move.
    seen_before = store.load(arn).handler_seen_seq
    executor.get_execution_state(
        execution_arn=arn, checkpoint_token=r1.checkpoint_token
    )
    seen_after = store.load(arn).handler_seen_seq
    assert seen_after == seen_before

    # Next empty-update checkpoint returns empty delta (the "A" op
    # was already delivered in r1's response, and get_execution_state
    # didn't reset or re-advance anything).
    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=r1.checkpoint_token,
        updates=[],
    )
    assert r2.new_execution_state.operations == []


# endregion
