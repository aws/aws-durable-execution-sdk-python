"""Property-based test: Logger suppression follows owning context.

**Validates: Requirements 6.1, 6.2, 6.3**

Property 7: For any Logger instance attached to a DurableContext, the Logger
suppresses output (i.e., `_should_log()` returns False) if and only if the
owning context's `is_replaying()` returns True.
"""

import logging
from threading import Lock
from unittest.mock import Mock

from hypothesis import given, settings
from hypothesis import strategies as st

from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.logger import Logger, LogInfo
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus


# --- Strategies ---

# Terminal statuses that count as "completed"
COMPLETED_STATUSES = [
    OperationStatus.SUCCEEDED,
    OperationStatus.FAILED,
    OperationStatus.CANCELLED,
    OperationStatus.STOPPED,
    OperationStatus.TIMED_OUT,
]

# Non-terminal statuses
NON_COMPLETED_STATUSES = [
    OperationStatus.STARTED,
    OperationStatus.PENDING,
    OperationStatus.READY,
]

ALL_STATUSES = COMPLETED_STATUSES + NON_COMPLETED_STATUSES

# Non-EXECUTION operation types
NON_EXECUTION_TYPES = [
    OperationType.STEP,
    OperationType.WAIT,
    OperationType.CALLBACK,
    OperationType.CHAINED_INVOKE,
    OperationType.CONTEXT,
]

ALL_OPERATION_TYPES = NON_EXECUTION_TYPES + [OperationType.EXECUTION]

# Strategy for operation IDs
operation_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P")),
    min_size=1,
    max_size=30,
)

# Strategy for parent_id values
parent_id_strategy = st.one_of(st.none(), operation_id_strategy)

# Strategy for replay status
replay_status_strategy = st.sampled_from([ReplayStatus.REPLAY, ReplayStatus.NEW])


def _create_mock_state(operations: dict[str, Operation] | None = None) -> ExecutionState:
    """Create a mock ExecutionState with the given operations and a real lock."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state.operations = operations or {}
    mock_state._operations_lock = Lock()
    return mock_state


def _create_context_with_replay_status(
    replay_status: ReplayStatus,
    parent_id: str | None = None,
    operations: dict[str, Operation] | None = None,
) -> DurableContext:
    """Create a DurableContext with the given replay status."""
    mock_state = _create_mock_state(operations)
    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )
    return DurableContext(
        state=mock_state,
        execution_context=execution_context,
        parent_id=parent_id,
        replay_status=replay_status,
    )


@st.composite
def context_with_logger_scenario(draw):
    """Generate a DurableContext with a Logger attached, in any replay status.

    Returns a tuple of (context, replay_status) where the context has a Logger
    attached via the standard construction path.
    """
    replay_status = draw(replay_status_strategy)
    parent_id = draw(parent_id_strategy)

    # Generate some arbitrary operations in the state
    ops_list = draw(
        st.lists(
            st.tuples(
                operation_id_strategy,
                st.sampled_from(ALL_OPERATION_TYPES),
                st.sampled_from(ALL_STATUSES),
                parent_id_strategy,
            ),
            min_size=0,
            max_size=10,
            unique_by=lambda x: x[0],
        )
    )

    operations: dict[str, Operation] = {}
    for op_id, op_type, status, pid in ops_list:
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=status,
            parent_id=pid,
        )

    context = _create_context_with_replay_status(
        replay_status=replay_status,
        parent_id=parent_id,
        operations=operations,
    )

    return context, replay_status


@st.composite
def context_that_transitions_scenario(draw):
    """Generate a DurableContext in REPLAY that can transition to NEW after visiting ops.

    Returns a tuple of (context, scoped_completed_op_ids) where visiting all
    scoped_completed_op_ids will cause the context to transition from REPLAY to NEW.
    """
    parent_id = draw(parent_id_strategy)

    # Generate completed non-EXECUTION operations scoped to this context
    num_scoped = draw(st.integers(min_value=1, max_value=5))
    operations: dict[str, Operation] = {}
    scoped_op_ids: list[str] = []

    for _ in range(num_scoped):
        op_id = draw(operation_id_strategy.filter(lambda x, ops=operations: x not in ops))
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=draw(st.sampled_from(NON_EXECUTION_TYPES)),
            status=draw(st.sampled_from(COMPLETED_STATUSES)),
            parent_id=parent_id,
        )
        scoped_op_ids.append(op_id)

    # Optionally add unrelated operations
    num_unrelated = draw(st.integers(min_value=0, max_value=5))
    for _ in range(num_unrelated):
        op_id = draw(operation_id_strategy.filter(lambda x, ops=operations: x not in ops))
        # Use a different parent_id so they don't affect this context
        other_parent_id = draw(
            operation_id_strategy.filter(lambda x: x != parent_id)
        )
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=draw(st.sampled_from(ALL_OPERATION_TYPES)),
            status=draw(st.sampled_from(ALL_STATUSES)),
            parent_id=other_parent_id,
        )

    context = _create_context_with_replay_status(
        replay_status=ReplayStatus.REPLAY,
        parent_id=parent_id,
        operations=operations,
    )

    return context, scoped_op_ids


# --- Property 7 Tests ---


@given(data=context_with_logger_scenario())
@settings(max_examples=200)
def test_logger_suppresses_when_context_is_replaying(data):
    """Property 7: Logger suppresses output when owning context is replaying.

    **Validates: Requirements 6.1, 6.2, 6.3**

    For any Logger instance attached to a DurableContext in REPLAY status,
    `_should_log()` returns False (output is suppressed).
    """
    context, replay_status = data

    if replay_status is not ReplayStatus.REPLAY:
        return  # Only test REPLAY case here

    # The Logger is attached to the context via standard construction
    assert context.logger._should_log() is False, (
        f"Logger should suppress output (return False from _should_log()) "
        f"when owning context is in REPLAY status, but got True"
    )


@given(data=context_with_logger_scenario())
@settings(max_examples=200)
def test_logger_emits_when_context_is_new(data):
    """Property 7: Logger emits output when owning context is in NEW status.

    **Validates: Requirements 6.1, 6.2, 6.3**

    For any Logger instance attached to a DurableContext in NEW status,
    `_should_log()` returns True (output is emitted).
    """
    context, replay_status = data

    if replay_status is not ReplayStatus.NEW:
        return  # Only test NEW case here

    # The Logger is attached to the context via standard construction
    assert context.logger._should_log() is True, (
        f"Logger should emit output (return True from _should_log()) "
        f"when owning context is in NEW status, but got False"
    )


@given(data=context_with_logger_scenario())
@settings(max_examples=200)
def test_logger_suppression_iff_context_is_replaying(data):
    """Property 7 (biconditional): Logger suppresses iff owning context is replaying.

    **Validates: Requirements 6.1, 6.2, 6.3**

    For any Logger instance attached to a DurableContext, the Logger suppresses
    output (i.e., `_should_log()` returns False) if and only if the owning
    context's `is_replaying()` returns True.
    """
    context, _ = data

    # The biconditional: _should_log() == (not is_replaying())
    assert context.logger._should_log() == (not context.is_replaying()), (
        f"Logger suppression should follow owning context's replay status. "
        f"context.is_replaying()={context.is_replaying()}, "
        f"logger._should_log()={context.logger._should_log()}. "
        f"Expected _should_log() == (not is_replaying())"
    )


@given(data=context_that_transitions_scenario())
@settings(max_examples=200)
def test_logger_suppression_follows_context_transition(data):
    """Property 7 (transition): Logger suppression tracks context replay transitions.

    **Validates: Requirements 6.1, 6.2, 6.3**

    When a DurableContext transitions from REPLAY to NEW (by visiting all its
    scoped completed operations), the attached Logger's `_should_log()` transitions
    from False to True accordingly.
    """
    context, scoped_op_ids = data

    # Before transition: context is REPLAY, logger should suppress
    assert context.is_replaying() is True
    assert context.logger._should_log() is False, (
        "Logger should suppress output before context transitions from REPLAY"
    )

    # Visit all scoped completed operations to trigger transition
    for op_id in scoped_op_ids:
        context.track_replay(op_id)

    # After transition: context is NEW, logger should emit
    assert context.is_replaying() is False
    assert context.logger._should_log() is True, (
        "Logger should emit output after context transitions to NEW"
    )
