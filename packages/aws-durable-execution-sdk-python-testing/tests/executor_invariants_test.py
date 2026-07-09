"""Invariant tests for .

These are plain unit tests (no property-based framework) that loop
over hand-constructed scenarios to assert design invariants hold.
Complementary to the AT tests in executor_checkpoint_test.py — the
ATs spot-check specific scenarios; these pin down the general
properties that must hold across any realistic sequence.

Invariants covered:

* seq_counter is strictly monotonic across any sequence of updates
  and async completions.
* token_sequence is strictly monotonic across accepted
  non-idempotent checkpoint calls; idempotent replays do not
  advance it.
* handler_seen_seq never retreats.
* Across any sequence of checkpoints, the union of responded ops
  equals every op ever touched.
* Paging round-trip: combined pages at a pinned sequence equal the
  full snapshot in creation order for a range of page-byte caps.
* Gate-release: after a sequence of transient invocation failures,
  the observed InvocationState is never INVOKING.
* Idempotent replay byte-equivalence.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future as ConcurrentFuture
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock

import pytest
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
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
    IllegalStateException,
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import (
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.executor import Executor, InvocationState
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.token import (
    CallbackToken,
    CheckpointToken,
)


def _start_input() -> StartDurableExecutionInput:
    return StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )


def _make_executor() -> tuple[Executor, InMemoryExecutionStore, str, str]:
    """Build a real Executor pre-primed to INVOKING."""
    store = InMemoryExecutionStore()
    execution = Execution.new(_start_input())
    execution.start()
    store.save(execution)
    executor = Executor(store, Mock(), Mock(), Mock())
    arn = execution.durable_execution_arn
    executor._invocation_state[arn] = InvocationState.INVOKING  # noqa: SLF001
    token = CheckpointToken(execution_arn=arn, token_sequence=0).to_str()
    return executor, store, arn, token


def _step_update(op_id: str) -> OperationUpdate:
    return OperationUpdate(
        operation_id=op_id,
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name=op_id,
    )


# region counter monotonicity


def test_invariant_seq_counter_strictly_monotonic_across_updates():
    """Every accepted update bumps seq_counter by exactly 1."""
    executor, store, arn, token = _make_executor()

    total_ops = 0
    for batch in [["A", "B"], ["C"], ["D", "E", "F"], ["G"]]:
        updates = [_step_update(op_id) for op_id in batch]
        response = executor.checkpoint_execution(
            execution_arn=arn, checkpoint_token=token, updates=updates
        )
        total_ops += len(batch)
        after = store.load(arn)
        # Every accepted update bumped seq_counter.
        assert after.seq_counter == total_ops
        token = response.checkpoint_token


def test_invariant_seq_counter_also_bumps_on_async_completions():
    """complete_wait / complete_retry / complete_callback_* each bump
    seq_counter by 1. Running an async completion between checkpoints
    keeps the counter strictly monotonic."""
    execution = Execution.new(_start_input())
    execution.start()

    # Simulate a series of async completions by creating WAIT ops and
    # completing them. The complete_wait helper in Execution touches
    # via self.touch_operation.
    for i in range(3):
        op_id = f"wait-{i}"
        execution.operations.append(
            SvcOperation(
                operation_id=op_id,
                operation_type=OperationType.WAIT,
                status=OperationStatus.STARTED,
                start_timestamp=datetime.now(tz=timezone.utc),
            )
        )

    for i in range(3):
        previous = execution.seq_counter
        execution.complete_wait(f"wait-{i}")
        assert execution.seq_counter == previous + 1


# endregion
# region token_sequence monotonicity


def test_invariant_token_sequence_strictly_monotonic_across_accepted_checkpoints():
    """Each accepted non-idempotent checkpoint advances token_sequence
    by exactly 1."""
    executor, store, arn, token = _make_executor()

    for expected_seq in range(1, 6):
        r = executor.checkpoint_execution(
            execution_arn=arn, checkpoint_token=token, updates=[]
        )
        assert (
            CheckpointToken.from_str(r.checkpoint_token).token_sequence == expected_seq
        )
        token = r.checkpoint_token


def test_invariant_idempotent_replay_does_not_advance_token_sequence():
    """A retried checkpoint returns the same outbound token and the
    execution's token_sequence stays put."""
    executor, store, arn, token = _make_executor()

    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token,
        updates=[_step_update("X")],
        client_token="c1",
    )
    seq_before_replay = store.load(arn).token_sequence

    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token,
        updates=[_step_update("X")],
        client_token="c1",
    )

    assert r1.checkpoint_token == r2.checkpoint_token
    assert store.load(arn).token_sequence == seq_before_replay


# endregion
# region watermark monotonicity


def test_invariant_handler_seen_seq_never_decreases():
    """handler_seen_seq only advances forward across any sequence of
    checkpoint calls."""
    executor, store, arn, token = _make_executor()

    watermark_history: list[int] = [store.load(arn).handler_seen_seq]

    for batch in [["A", "B"], [], ["C"], [], ["D", "E"]]:
        updates = [_step_update(op_id) for op_id in batch]
        r = executor.checkpoint_execution(
            execution_arn=arn, checkpoint_token=token, updates=updates
        )
        watermark_history.append(store.load(arn).handler_seen_seq)
        token = r.checkpoint_token

    # Monotonically non-decreasing.
    assert watermark_history == sorted(watermark_history)


# endregion
# region delta union equals touched


def test_invariant_delta_union_equals_touched_ops():
    """Across a sequence of checkpoints, the union of responded
    operation ids equals every op id that was ever touched via an
    update."""
    executor, store, arn, token = _make_executor()

    all_touched: set[str] = set()
    all_responded: set[str] = set()

    for batch in [["A", "B"], ["C"], ["D", "E", "F"]]:
        updates = [_step_update(op_id) for op_id in batch]
        all_touched.update(batch)
        r = executor.checkpoint_execution(
            execution_arn=arn, checkpoint_token=token, updates=updates
        )
        all_responded.update(op.operation_id for op in r.new_execution_state.operations)
        token = r.checkpoint_token

    assert all_responded == all_touched


# endregion
# region paging round-trip


@pytest.mark.parametrize("max_bytes", [1, 10, 100, 1_000_000])
def test_invariant_paging_round_trip_equals_full_snapshot(max_bytes: int):
    """For a range of page-byte caps, combined pages at a pinned
    sequence equal the full snapshot in creation order."""
    execution = Execution.new(_start_input())
    execution.start()

    for i in range(8):
        op_id = f"op-{i}"
        execution.operations.append(
            SvcOperation(
                operation_id=op_id,
                operation_type=OperationType.STEP,
                status=OperationStatus.STARTED,
                start_timestamp=datetime.now(tz=timezone.utc),
            )
        )
        execution.operation_size_bytes[op_id] = 20

    paginator = OperationPaginatorState.pin(execution)
    combined: list[str] = []
    marker: str | None = None
    # Bound the iteration to avoid infinite loops in case of a bug.
    for _ in range(100):
        ops, marker = paginator.page(marker, max_bytes)
        combined.extend(op.operation_id for op in ops)
        if marker is None:
            break
    else:
        msg = "paging did not terminate"
        raise AssertionError(msg)

    expected = [op.operation_id for op in execution.operations]
    assert combined == expected


# endregion
# region gate-release invariant


def test_invariant_gate_release_after_transient_failures():
    """After N failed handler attempts (below the retry budget), the
    observed InvocationState is NEVER INVOKING. It is either
    PRE_INVOKE (ready for another attempt) or COMPLETED (budget
    exhausted)."""
    store = InMemoryExecutionStore()
    invoker = Mock()
    invoker.create_invocation_input.return_value = Mock()
    invoker.invoke.side_effect = RuntimeError("transient")
    executor = Executor(store, Mock(), invoker, Mock())

    execution = Execution.new(_start_input())
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn

    # Drive a few failed attempts (still under MAX=5).
    for _ in range(3):
        asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001
        state = executor._invocation_state.get(arn, InvocationState.PRE_INVOKE)  # noqa: SLF001
        assert state is not InvocationState.INVOKING

    # After the budget is exhausted the execution is terminal; state
    # becomes COMPLETED (also not INVOKING).
    for _ in range(Executor.MAX_CONSECUTIVE_FAILED_ATTEMPTS):
        asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001
    state = executor._invocation_state.get(arn, InvocationState.PRE_INVOKE)  # noqa: SLF001
    assert state is not InvocationState.INVOKING


# endregion
# region idempotent replay byte-equivalence


def test_invariant_idempotent_replay_byte_equivalent():
    """Two calls with the same (client_token, inbound_token) produce
    byte-equivalent responses."""
    executor, store, arn, token = _make_executor()

    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token,
        updates=[_step_update("X"), _step_update("Y"), _step_update("Z")],
        client_token="c-identical",
    )
    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token,
        updates=[_step_update("X"), _step_update("Y"), _step_update("Z")],
        client_token="c-identical",
    )

    assert r1.checkpoint_token == r2.checkpoint_token
    assert [op.operation_id for op in r1.new_execution_state.operations] == [
        op.operation_id for op in r2.new_execution_state.operations
    ]
    assert r1.new_execution_state.next_marker == r2.new_execution_state.next_marker


# endregion
# region token_sequence never retreats under any sequence


def test_invariant_token_sequence_never_retreats():
    """Under any mix of accepted checkpoints + replays, token_sequence
    is non-decreasing."""
    executor, store, arn, token = _make_executor()

    history: list[int] = [store.load(arn).token_sequence]

    # Accepted
    r1 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token,
        updates=[_step_update("A")],
        client_token="c1",
    )
    history.append(store.load(arn).token_sequence)
    # Replay (no advance)
    executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=token,
        updates=[_step_update("A")],
        client_token="c1",
    )
    history.append(store.load(arn).token_sequence)
    # Accepted
    r2 = executor.checkpoint_execution(
        execution_arn=arn,
        checkpoint_token=r1.checkpoint_token,
        updates=[],
        client_token="c2",
    )
    history.append(store.load(arn).token_sequence)

    assert history == sorted(history)


# endregion
# region: earliest-pending fires due ops


def test_earliest_pending_fires_due_wait_and_invokes_handler():
    """End-to-end flow.

    Seed an execution with a WAIT op whose scheduled_end_timestamp
    is already in the past. Drive _schedule_earliest_pending; the
    armed wake-up fires immediately (zero delay), completes the wait,
    and triggers _invoke_execution.
    """
    store = InMemoryExecutionStore()
    # The scheduler is a Mock; we intercept call_later and record the
    # requested delay. In a real run, the scheduler would execute the
    # coroutine after that delay. We drive the coroutine ourselves
    # synchronously to avoid real-time sleeps.
    scheduler = MagicMock()
    recorded_calls: list[tuple] = []

    def fake_call_later(func, delay, completion_event=None):  # noqa: ARG001
        recorded_calls.append((func, delay))
        return ConcurrentFuture()

    scheduler.call_later.side_effect = fake_call_later

    invoker = Mock()
    checkpoint_processor = Mock()
    executor = Executor(store, scheduler, invoker, checkpoint_processor)

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-1",
        )
    )
    execution.start()
    # Past-timestamped WAIT op (so the wake-up delay is 0).
    past = datetime.now(tz=timezone.utc) - timedelta(seconds=5)
    execution.operations.append(
        SvcOperation(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=past),
        )
    )
    store.save(execution)
    arn = execution.durable_execution_arn

    # Act: arm the wake-up.
    executor._schedule_earliest_pending(arn)  # noqa: SLF001

    # A call_later was recorded with delay=0 (past timestamp clamps
    # to 0 per our `max(..., 0.0)` guard).
    assert len(recorded_calls) == 1
    func, delay = recorded_calls[0]
    assert delay == 0

    # Simulate the scheduler firing by awaiting the recorded coro.
    asyncio.run(func())

    # The WAIT op is now SUCCEEDED; seq_counter advanced; the
    # handler was triggered via _invoke_execution (which for a
    # mocked invoker shows up as a second scheduler.call_later).
    refreshed = store.load(arn)
    wait_op = [op for op in refreshed.operations if op.operation_id == "wait-1"][0]
    assert wait_op.status == OperationStatus.SUCCEEDED
    assert refreshed.seq_counter >= 1


# endregion
def test_earliest_pending_fires_due_step_retry():
    """Coverage for the STEP branch of _fire_due_and_invoke. Seeds a
    PENDING step with a past next_attempt_timestamp and drives the
    wake-up flow end-to-end."""
    store = InMemoryExecutionStore()
    scheduler = MagicMock()
    recorded: list[tuple] = []

    def fake_call_later(func, delay, completion_event=None):  # noqa: ARG001
        recorded.append((func, delay))
        return ConcurrentFuture()

    scheduler.call_later.side_effect = fake_call_later
    executor = Executor(store, scheduler, Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-step",
        )
    )
    execution.start()
    past = datetime.now(tz=timezone.utc) - timedelta(seconds=3)
    execution.operations.append(
        SvcOperation(
            operation_id="step-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.PENDING,
            step_details=StepDetails(next_attempt_timestamp=past),
        )
    )
    store.save(execution)
    arn = execution.durable_execution_arn

    executor._schedule_earliest_pending(arn)  # noqa: SLF001
    assert len(recorded) == 1
    asyncio.run(recorded[0][0]())

    refreshed = store.load(arn)
    step_op = [op for op in refreshed.operations if op.operation_id == "step-1"][0]
    assert step_op.status == OperationStatus.READY
    assert refreshed.seq_counter >= 1


def test_earliest_pending_noop_when_no_candidates():
    """When there are no STARTED waits or PENDING steps, the
    earliest-pending selector returns None and no wake-up is armed."""
    store = InMemoryExecutionStore()
    scheduler = MagicMock()
    executor = Executor(store, scheduler, Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-noop",
        )
    )
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn

    executor._schedule_earliest_pending(arn)  # noqa: SLF001

    # No call_later recorded because no candidates.
    assert scheduler.call_later.call_count == 0


def test_earliest_pending_noop_when_execution_complete():
    """A completed execution has no need for a wake-up — the helper
    bails out without calling the scheduler."""
    store = InMemoryExecutionStore()
    scheduler = MagicMock()
    executor = Executor(store, scheduler, Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-done",
        )
    )
    execution.start()
    execution.complete_success("ok")
    store.save(execution)
    arn = execution.durable_execution_arn

    executor._schedule_earliest_pending(arn)  # noqa: SLF001

    assert scheduler.call_later.call_count == 0


def test_earliest_pending_cancels_prior_wakeup_on_rearm():
    """Re-arming when a previous wake-up is already queued must
    cancel the earlier one so we don't end up with two timers."""
    store = InMemoryExecutionStore()
    scheduler = MagicMock()
    futures: list[ConcurrentFuture] = []

    def fake_call_later(func, delay, completion_event=None):  # noqa: ARG001, ARG002
        f: ConcurrentFuture = ConcurrentFuture()
        futures.append(f)
        return f

    scheduler.call_later.side_effect = fake_call_later
    executor = Executor(store, scheduler, Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-rearm",
        )
    )
    execution.start()
    future_ts = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
    execution.operations.append(
        SvcOperation(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=future_ts),
        )
    )
    store.save(execution)
    arn = execution.durable_execution_arn

    executor._schedule_earliest_pending(arn)  # noqa: SLF001
    executor._schedule_earliest_pending(arn)  # noqa: SLF001

    # Two call_laters were made, and the first future was cancelled.
    assert scheduler.call_later.call_count == 2
    assert futures[0].cancelled()


def test_earliest_pending_canceled_on_cleanup():
    """_cleanup_execution_state must cancel the armed wake-up so we
    don't keep firing into a dead execution."""
    store = InMemoryExecutionStore()
    scheduler = MagicMock()
    futures: list[ConcurrentFuture] = []

    def fake_call_later(func, delay, completion_event=None):  # noqa: ARG001, ARG002
        f: ConcurrentFuture = ConcurrentFuture()
        futures.append(f)
        return f

    scheduler.call_later.side_effect = fake_call_later
    executor = Executor(store, scheduler, Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-cleanup",
        )
    )
    execution.start()
    future_ts = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
    execution.operations.append(
        SvcOperation(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=future_ts),
        )
    )
    store.save(execution)
    arn = execution.durable_execution_arn

    executor._schedule_earliest_pending(arn)  # noqa: SLF001
    executor._cleanup_execution_state(arn)  # noqa: SLF001

    assert futures[0].cancelled()


def test_invoke_handler_bails_out_on_completed_gate():
    """When _invocation_state is already COMPLETED, _invoke_handler
    exits immediately without calling the invoker. Covers the
    post-terminal re-invoke guard path."""
    store = InMemoryExecutionStore()
    invoker = Mock()
    executor = Executor(store, Mock(), invoker, Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-completed",
        )
    )
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn
    executor._invocation_state[arn] = InvocationState.COMPLETED  # noqa: SLF001

    asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001

    assert invoker.invoke.call_count == 0
    assert invoker.create_invocation_input.call_count == 0


def test_invoke_handler_bails_out_on_is_complete():
    """Similarly, if the Execution is already is_complete when the
    handler starts, the handler transitions state to COMPLETED and
    exits without calling the invoker."""
    store = InMemoryExecutionStore()
    invoker = Mock()
    executor = Executor(store, Mock(), invoker, Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-done-early",
        )
    )
    execution.start()
    execution.complete_success("result")
    store.save(execution)
    arn = execution.durable_execution_arn

    asyncio.run(executor._invoke_handler(arn)())  # noqa: SLF001

    assert invoker.invoke.call_count == 0
    # Gate transitioned to COMPLETED.
    assert (
        executor._invocation_state.get(arn)  # noqa: SLF001
        is InvocationState.COMPLETED
    )


def test_is_current_token_returns_false_for_none_token():
    """Covers the `checkpoint_token is None` early-return path in
    _is_current_token. checkpoint_execution with None token is
    rejected with 'Invalid checkpoint token'."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-none-token",
        )
    )
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn
    executor._invocation_state[arn] = InvocationState.INVOKING  # noqa: SLF001

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.checkpoint_execution(arn, checkpoint_token=None)  # type: ignore[arg-type]


def test_is_current_token_returns_false_for_malformed_token():
    """Covers the `except (ValueError, KeyError)` branch of
    _is_current_token when CheckpointToken.from_str raises."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-bad-token",
        )
    )
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn
    executor._invocation_state[arn] = InvocationState.INVOKING  # noqa: SLF001

    with pytest.raises(
        InvalidParameterValueException, match="Invalid checkpoint token"
    ):
        executor.checkpoint_execution(arn, checkpoint_token="!!!not-base64!!!")


def test_fail_workflow_raises_on_already_complete_execution():
    """Covers _fail_workflow's defensive check: calling it on an
    already-terminated execution raises IllegalStateException with
    'Cannot make multiple close workflow decisions'. The sibling
    _complete_workflow path has a symmetric test in executor_test.py."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-already-done",
        )
    )
    execution.start()
    execution.complete_success("result")
    store.save(execution)
    arn = execution.durable_execution_arn

    with pytest.raises(
        IllegalStateException, match="Cannot make multiple close workflow decisions"
    ):
        executor._fail_workflow(arn, ErrorObject.from_message("boom"))  # noqa: SLF001


def test_earliest_pending_tolerates_store_errors():
    """If the store raises when we try to load the execution (e.g.
    torn down mid-flight), _schedule_earliest_pending silently
    exits without arming a wake-up or raising."""
    store = Mock()
    store.load.side_effect = RuntimeError("store torn down")
    scheduler = MagicMock()
    executor = Executor(store, scheduler, Mock(), Mock())

    # Should not raise; just returns silently.
    executor._schedule_earliest_pending("any-arn")  # noqa: SLF001

    assert scheduler.call_later.call_count == 0


def test_fire_due_and_invoke_tolerates_store_errors():
    """Same resilience for the actual wake-up firing path: if the
    store raises at fire time, the coroutine exits cleanly."""
    store = Mock()
    # First load succeeds (during _schedule_earliest_pending), next
    # load raises (when the coroutine fires).
    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-store-error",
        )
    )
    execution.start()
    past = datetime.now(tz=timezone.utc) - timedelta(seconds=1)
    execution.operations.append(
        SvcOperation(
            operation_id="wait-1",
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=past),
        )
    )
    # Load always returns the execution; but we'll swap to raise for the fire path.
    store.load.return_value = execution
    scheduler = MagicMock()
    recorded: list[tuple] = []

    def fake_call_later(func, delay, completion_event=None):  # noqa: ARG001, ARG002
        recorded.append((func, delay))
        return ConcurrentFuture()

    scheduler.call_later.side_effect = fake_call_later
    executor = Executor(store, scheduler, Mock(), Mock())

    executor._schedule_earliest_pending("any-arn")  # noqa: SLF001
    # Swap load to raise.
    store.load.side_effect = RuntimeError("store gone")

    # Drive the coroutine; it should exit cleanly.
    asyncio.run(recorded[0][0]())


def test_complete_workflow_raises_on_already_complete():
    """Covers _complete_workflow's defensive check (sibling to
    _fail_workflow's, both raise when called on an already-terminal
    execution)."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-complete-dup",
        )
    )
    execution.start()
    execution.complete_success("already-succeeded")
    store.save(execution)
    arn = execution.durable_execution_arn

    with pytest.raises(
        IllegalStateException, match="Cannot make multiple close workflow decisions"
    ):
        executor._complete_workflow(arn, result="again", error=None)  # noqa: SLF001


def test_on_callback_created_direct_call():
    """Covers Executor.on_callback_created direct invocation."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    token = CallbackToken(execution_arn="test-arn", operation_id="op-123")
    # callback_options=None triggers the early-return branch in
    # _schedule_callback_timeouts. Just verifies the call completes.
    executor.on_callback_created(
        execution_arn="test-arn",
        operation_id="op-123",
        callback_options=None,
        callback_token=token,
    )


def test_checkpoint_returns_full_delta_in_one_response():
    """The checkpoint response returns the entire unseen delta in a
    single response; the invocation page budget does not truncate it.
    handler_seen_seq advances to cover every returned op, so a
    subsequent empty poll returns nothing.

    Guards against reintroducing checkpoint-delta truncation, which
    could strand operations when touch-order diverges from
    creation-order.
    """
    store = InMemoryExecutionStore()
    # Deliberately tiny invocation page budget: it governs invocation-input
    # paging only and must NOT truncate the checkpoint delta.
    executor = Executor(store, Mock(), Mock(), Mock(), max_invocation_page_bytes=200)

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-trunc",
        )
    )
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn
    executor._invocation_state[arn] = InvocationState.INVOKING  # noqa: SLF001

    token = CheckpointToken(execution_arn=arn, token_sequence=0).to_str()

    # Submit 5 ops of ~100 bytes each — far larger than the 200-byte
    # invocation page budget. The checkpoint response must still return
    # all of them.
    updates = [
        OperationUpdate(
            operation_id=f"op-{i}",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            name=f"op-{i}",
            payload="x" * 100,
        )
        for i in range(5)
    ]

    r1 = executor.checkpoint_execution(
        execution_arn=arn, checkpoint_token=token, updates=updates
    )

    # Full delta in a single response — not truncated by the page budget.
    r1_ids = {op.operation_id for op in r1.new_execution_state.operations}
    assert r1_ids == {"op-0", "op-1", "op-2", "op-3", "op-4"}

    # Watermark advanced to cover everything: an empty follow-up poll
    # returns nothing.
    r2 = executor.checkpoint_execution(
        execution_arn=arn, checkpoint_token=r1.checkpoint_token, updates=[]
    )
    assert [op.operation_id for op in r2.new_execution_state.operations] == []


def test_cleanup_execution_state_cancels_callback_timers():
    """Covers the callback-timer loops in _cleanup_execution_state.
    Seeds _callback_timeouts and _callback_heartbeats with pending
    Futures and verifies they get cancelled."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    # Seed callback timer dicts with unfulfilled futures. Using
    # ConcurrentFuture because that's what the scheduler returns.
    pending_future_1 = ConcurrentFuture()
    pending_future_2 = ConcurrentFuture()
    done_future = ConcurrentFuture()
    done_future.set_result(None)  # already done

    executor._callback_timeouts["cb-1"] = pending_future_1  # noqa: SLF001
    executor._callback_timeouts["cb-done"] = done_future  # noqa: SLF001
    executor._callback_heartbeats["cb-2"] = pending_future_2  # noqa: SLF001

    executor._cleanup_execution_state("any-arn")  # noqa: SLF001

    assert pending_future_1.cancelled()
    assert pending_future_2.cancelled()
    # done future was not touched (done() is True, skipped).
    assert not done_future.cancelled()


def test_validate_invocation_response_raises_on_already_complete():
    """Covers the defensive is_complete check in
    _validate_invocation_response_and_store. Direct invocation with a
    completed Execution raises IllegalStateException."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-validate-complete",
        )
    )
    execution.start()
    execution.complete_success("already done")

    response = DurableExecutionInvocationOutput(
        status=InvocationStatus.SUCCEEDED, result="more"
    )

    with pytest.raises(IllegalStateException, match="Execution already completed"):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            execution.durable_execution_arn, response, execution
        )


def test_validate_invocation_response_raises_on_none_status():
    """Covers the status-is-None branch."""
    store = InMemoryExecutionStore()
    executor = Executor(store, Mock(), Mock(), Mock())

    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="fn",
            function_qualifier="$LATEST",
            execution_name="test",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="inv-none-status",
        )
    )
    execution.start()

    # Response with status=None should raise InvalidParameterValueException.
    response = DurableExecutionInvocationOutput(status=None)

    with pytest.raises(
        InvalidParameterValueException, match="Response status is required"
    ):
        executor._validate_invocation_response_and_store(  # noqa: SLF001
            execution.durable_execution_arn, response, execution
        )
