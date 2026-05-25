"""Property-based tests for per-context replay status.

These tests validate correctness properties from the design document
for the per-context replay status feature.
"""

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
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus


# --- Strategies ---

# Strategy for generating valid operation IDs (non-empty strings)
operation_id_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P")),
    min_size=1,
    max_size=30,
)

# Strategy for terminal operation statuses
terminal_status_st = st.sampled_from(
    [
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.STOPPED,
        OperationStatus.TIMED_OUT,
    ]
)

# Strategy for non-EXECUTION operation types (types that count for replay)
non_execution_type_st = st.sampled_from(
    [
        OperationType.STEP,
        OperationType.WAIT,
        OperationType.CALLBACK,
        OperationType.CHAINED_INVOKE,
        OperationType.CONTEXT,
    ]
)


def _scoped_operation_st():
    """Strategy for generating a single scoped operation (id, type, status) tuple."""
    return st.tuples(operation_id_st, non_execution_type_st, terminal_status_st)


def _create_mock_state_with_operations(
    operations: dict[str, Operation],
) -> ExecutionState:
    """Create a mock ExecutionState with the given operations dict and a real lock."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state.operations = operations
    mock_state._operations_lock = Lock()
    return mock_state


def _create_context_in_replay(
    state: ExecutionState, parent_id: str | None = None
) -> DurableContext:
    """Create a DurableContext in REPLAY status."""
    execution_context = ExecutionContext(
        durable_execution_arn=state.durable_execution_arn
    )
    return DurableContext(
        state=state,
        execution_context=execution_context,
        parent_id=parent_id,
        replay_status=ReplayStatus.REPLAY,
    )


# --- Property 2: Replay-to-NEW transition upon full visitation ---


@given(
    scoped_ops=st.lists(_scoped_operation_st(), min_size=1, max_size=20, unique_by=lambda x: x[0]),
    parent_id=st.one_of(st.none(), operation_id_st),
)
@settings(max_examples=200)
def test_replay_to_new_transition_upon_full_visitation(
    scoped_ops: list[tuple[str, OperationType, OperationStatus]],
    parent_id: str | None,
):
    """Property 2: Replay-to-NEW transition upon full visitation.

    **Validates: Requirements 3.3, 7.3**

    For any DurableContext in REPLAY status with N completed operations scoped
    to it (N >= 1), once `track_replay` has been called with all N operation IDs,
    the context's `is_replaying()` returns False (status is NEW).
    """
    # Build operations scoped to this context (parent_id matches)
    operations: dict[str, Operation] = {}
    op_ids: list[str] = []
    for op_id, op_type, status in scoped_ops:
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=status,
            parent_id=parent_id,
        )
        op_ids.append(op_id)

    state = _create_mock_state_with_operations(operations)
    context = _create_context_in_replay(state, parent_id=parent_id)

    # Verify context starts in REPLAY
    assert context.is_replaying() is True

    # Call track_replay with all N operation IDs
    for op_id in op_ids:
        context.track_replay(op_id)

    # After visiting all scoped completed operations, context must be NEW
    assert context.is_replaying() is False, (
        f"Context should have transitioned to NEW after visiting all {len(op_ids)} "
        f"scoped completed operations, but is_replaying() still returns True"
    )


@given(
    scoped_ops=st.lists(_scoped_operation_st(), min_size=1, max_size=20, unique_by=lambda x: x[0]),
    parent_id=st.one_of(st.none(), operation_id_st),
    extra_visit_ids=st.lists(operation_id_st, min_size=1, max_size=10),
)
@settings(max_examples=200)
def test_replay_to_new_transition_with_extra_visits(
    scoped_ops: list[tuple[str, OperationType, OperationStatus]],
    parent_id: str | None,
    extra_visit_ids: list[str],
):
    """Property 2 (extended): Transition still occurs even with extra track_replay calls.

    **Validates: Requirements 3.3, 7.3**

    Calling track_replay with additional IDs beyond the scoped operations
    should not prevent the transition from REPLAY to NEW once all scoped
    operations have been visited.
    """
    # Build operations scoped to this context
    operations: dict[str, Operation] = {}
    op_ids: list[str] = []
    for op_id, op_type, status in scoped_ops:
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=status,
            parent_id=parent_id,
        )
        op_ids.append(op_id)

    state = _create_mock_state_with_operations(operations)
    context = _create_context_in_replay(state, parent_id=parent_id)

    # Visit extra IDs first (these are not in the scoped operations)
    for extra_id in extra_visit_ids:
        context.track_replay(extra_id)

    # Now visit all scoped operation IDs
    for op_id in op_ids:
        context.track_replay(op_id)

    # After visiting all scoped completed operations, context must be NEW
    assert context.is_replaying() is False, (
        f"Context should have transitioned to NEW after visiting all {len(op_ids)} "
        f"scoped completed operations (plus extras), but is_replaying() still returns True"
    )


@given(
    parent_id=st.one_of(st.none(), operation_id_st),
    visit_id=operation_id_st,
)
@settings(max_examples=100)
def test_replay_to_new_transition_zero_operations(
    parent_id: str | None,
    visit_id: str,
):
    """Property 2 (edge case N=0): Empty scoped operations transitions on first track_replay.

    **Validates: Requirements 3.3, 7.3**

    If a context in REPLAY status has zero completed operations scoped to it,
    the first track_replay call should transition it to NEW (since the empty
    set is trivially a subset of any visited set).
    """
    # No operations in state
    state = _create_mock_state_with_operations({})
    context = _create_context_in_replay(state, parent_id=parent_id)

    assert context.is_replaying() is True

    # Any track_replay call should trigger transition since empty set is subset of anything
    context.track_replay(visit_id)

    assert context.is_replaying() is False, (
        "Context with zero scoped completed operations should transition to NEW "
        "on the first track_replay call"
    )
