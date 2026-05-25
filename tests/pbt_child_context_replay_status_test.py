"""Property-based test: Child context initial replay status.

**Validates: Requirements 4.1, 4.2, 4.3**

Property 5: For any child DurableContext created via `create_child_context`,
the child's initial replay status is REPLAY if and only if at least one
completed non-EXECUTION operation exists in ExecutionState with `parent_id`
matching the child's scope. Otherwise, the initial status is NEW.
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

# Strategy for parent_id values (None represents root context)
parent_id_strategy = st.one_of(st.none(), st.text(min_size=1, max_size=20))


def _create_parent_context_with_operations(
    operations_dict: dict[str, Operation],
    parent_parent_id: str | None = None,
) -> DurableContext:
    """Create a parent DurableContext with a mock ExecutionState containing the given operations."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state.operations = operations_dict
    mock_state._operations_lock = Lock()

    execution_context = ExecutionContext(
        durable_execution_arn=mock_state.durable_execution_arn
    )
    return DurableContext(
        state=mock_state,
        execution_context=execution_context,
        parent_id=parent_parent_id,
        replay_status=ReplayStatus.NEW,
    )


@st.composite
def child_context_scenario_with_completed_ops(draw):
    """Generate a scenario where the child's scope has at least one completed non-EXECUTION operation.

    Returns a tuple of (operations_dict, parent_parent_id, child_operation_id)
    where the child's parent_id (== child_operation_id for non-virtual) has
    at least one completed non-EXECUTION operation.
    """
    # The parent context's parent_id
    parent_parent_id = draw(parent_id_strategy)

    # The operation_id used to create the child (becomes child's parent_id for non-virtual)
    child_operation_id = draw(operation_id_strategy)

    # Generate at least one completed non-EXECUTION operation scoped to the child
    num_child_completed = draw(st.integers(min_value=1, max_value=5))
    operations: dict[str, Operation] = {}

    for i in range(num_child_completed):
        op_id = draw(operation_id_strategy.filter(lambda x, ops=operations: x not in ops))
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=draw(st.sampled_from(NON_EXECUTION_TYPES)),
            status=draw(st.sampled_from(COMPLETED_STATUSES)),
            parent_id=child_operation_id,
        )

    # Optionally add some unrelated operations (different parent_id, EXECUTION type, etc.)
    num_unrelated = draw(st.integers(min_value=0, max_value=5))
    for i in range(num_unrelated):
        op_id = draw(operation_id_strategy.filter(lambda x, ops=operations: x not in ops))
        # Mix of different parent_ids, types, and statuses
        op_parent_id = draw(
            st.one_of(
                st.just(parent_parent_id),
                st.text(min_size=1, max_size=20),
                st.none(),
            )
        )
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=draw(st.sampled_from(ALL_OPERATION_TYPES)),
            status=draw(st.sampled_from(ALL_STATUSES)),
            parent_id=op_parent_id,
        )

    return operations, parent_parent_id, child_operation_id


@st.composite
def child_context_scenario_without_completed_ops(draw):
    """Generate a scenario where the child's scope has NO completed non-EXECUTION operations.

    The child's scope may have:
    - EXECUTION operations (which don't count)
    - Non-completed operations (STARTED, PENDING, READY)
    - No operations at all

    Returns a tuple of (operations_dict, parent_parent_id, child_operation_id).
    """
    # The parent context's parent_id
    parent_parent_id = draw(parent_id_strategy)

    # The operation_id used to create the child (becomes child's parent_id for non-virtual)
    child_operation_id = draw(operation_id_strategy)

    operations: dict[str, Operation] = {}

    # Optionally add operations scoped to the child that DON'T qualify
    # (either EXECUTION type or non-terminal status)
    num_non_qualifying = draw(st.integers(min_value=0, max_value=5))
    for i in range(num_non_qualifying):
        op_id = draw(operation_id_strategy.filter(lambda x, ops=operations: x not in ops))
        # Either EXECUTION type (any status) or non-EXECUTION with non-terminal status
        is_execution = draw(st.booleans())
        if is_execution:
            operations[op_id] = Operation(
                operation_id=op_id,
                operation_type=OperationType.EXECUTION,
                status=draw(st.sampled_from(ALL_STATUSES)),
                parent_id=child_operation_id,
            )
        else:
            operations[op_id] = Operation(
                operation_id=op_id,
                operation_type=draw(st.sampled_from(NON_EXECUTION_TYPES)),
                status=draw(st.sampled_from(NON_COMPLETED_STATUSES)),
                parent_id=child_operation_id,
            )

    # Optionally add operations with different parent_ids (these shouldn't affect the child)
    num_unrelated = draw(st.integers(min_value=0, max_value=5))
    for i in range(num_unrelated):
        op_id = draw(operation_id_strategy.filter(lambda x, ops=operations: x not in ops))
        # Use a parent_id that is NOT the child_operation_id
        op_parent_id = draw(
            st.one_of(
                st.just(parent_parent_id),
                st.text(min_size=1, max_size=20).filter(lambda x: x != child_operation_id),
                st.none(),
            )
        )
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=draw(st.sampled_from(ALL_OPERATION_TYPES)),
            status=draw(st.sampled_from(ALL_STATUSES)),
            parent_id=op_parent_id,
        )

    return operations, parent_parent_id, child_operation_id


@st.composite
def child_context_scenario_any(draw):
    """Generate any scenario for child context creation.

    Returns a tuple of (operations_dict, parent_parent_id, child_operation_id, is_virtual).
    """
    parent_parent_id = draw(parent_id_strategy)
    child_operation_id = draw(operation_id_strategy)
    is_virtual = draw(st.booleans())

    # Generate a mix of operations with various parent_ids, types, and statuses
    all_possible_parent_ids = [parent_parent_id, child_operation_id, None]
    extra_parent_ids = draw(st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=3))
    all_possible_parent_ids.extend(extra_parent_ids)

    operations_list = draw(
        st.lists(
            st.tuples(
                operation_id_strategy,
                st.sampled_from(all_possible_parent_ids),
                st.sampled_from(ALL_STATUSES),
                st.sampled_from(ALL_OPERATION_TYPES),
            ),
            min_size=0,
            max_size=15,
            unique_by=lambda x: x[0],
        )
    )

    operations: dict[str, Operation] = {}
    for op_id, op_parent_id, op_status, op_type in operations_list:
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=op_status,
            parent_id=op_parent_id,
        )

    return operations, parent_parent_id, child_operation_id, is_virtual


# --- Property Tests ---


@given(data=child_context_scenario_with_completed_ops())
@settings(max_examples=200)
def test_child_starts_in_replay_when_completed_ops_exist(data):
    """Property 5: Child context starts in REPLAY when completed non-EXECUTION ops exist.

    **Validates: Requirements 4.1, 4.2, 4.3**

    For any child DurableContext created via `create_child_context`, if at least
    one completed non-EXECUTION operation exists in ExecutionState with `parent_id`
    matching the child's scope, the child's initial replay status is REPLAY.
    """
    operations, parent_parent_id, child_operation_id = data

    parent_context = _create_parent_context_with_operations(
        operations, parent_parent_id
    )
    child_context = parent_context.create_child_context(
        operation_id=child_operation_id, is_virtual=False
    )

    assert child_context.is_replaying() is True, (
        f"Child context should start in REPLAY when completed non-EXECUTION "
        f"operations exist for its scope (parent_id={child_operation_id!r}), "
        f"but is_replaying() returned False"
    )


@given(data=child_context_scenario_without_completed_ops())
@settings(max_examples=200)
def test_child_starts_in_new_when_no_completed_ops_exist(data):
    """Property 5: Child context starts in NEW when no completed non-EXECUTION ops exist.

    **Validates: Requirements 4.1, 4.2, 4.3**

    For any child DurableContext created via `create_child_context`, if no
    completed non-EXECUTION operation exists in ExecutionState with `parent_id`
    matching the child's scope, the child's initial replay status is NEW.
    """
    operations, parent_parent_id, child_operation_id = data

    parent_context = _create_parent_context_with_operations(
        operations, parent_parent_id
    )
    child_context = parent_context.create_child_context(
        operation_id=child_operation_id, is_virtual=False
    )

    assert child_context.is_replaying() is False, (
        f"Child context should start in NEW when no completed non-EXECUTION "
        f"operations exist for its scope (parent_id={child_operation_id!r}), "
        f"but is_replaying() returned True"
    )


@given(data=child_context_scenario_any())
@settings(max_examples=200)
def test_child_replay_status_iff_completed_non_execution_ops_exist(data):
    """Property 5: Child replay status is REPLAY iff completed non-EXECUTION ops exist.

    **Validates: Requirements 4.1, 4.2, 4.3**

    For any child DurableContext created via `create_child_context`, the child's
    initial replay status is REPLAY if and only if at least one completed
    non-EXECUTION operation exists in ExecutionState with `parent_id` matching
    the child's scope. Otherwise, the initial status is NEW.

    This test covers both virtual and non-virtual child contexts.
    """
    operations, parent_parent_id, child_operation_id, is_virtual = data

    parent_context = _create_parent_context_with_operations(
        operations, parent_parent_id
    )
    child_context = parent_context.create_child_context(
        operation_id=child_operation_id, is_virtual=is_virtual
    )

    # Determine the child's effective parent_id based on virtual flag
    # For virtual: child_parent_id == parent's _parent_id (parent_parent_id)
    # For non-virtual: child_parent_id == child_operation_id
    effective_child_parent_id = parent_parent_id if is_virtual else child_operation_id

    # Compute expected: does at least one completed non-EXECUTION op exist
    # with parent_id matching the child's scope?
    completed_statuses = {
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.STOPPED,
        OperationStatus.TIMED_OUT,
    }

    has_scoped_completed = any(
        op.parent_id == effective_child_parent_id
        and op.operation_type is not OperationType.EXECUTION
        and op.status in completed_statuses
        for op in operations.values()
    )

    expected_replaying = has_scoped_completed

    assert child_context.is_replaying() is expected_replaying, (
        f"Child context replay status mismatch. "
        f"Expected is_replaying()={expected_replaying} "
        f"(has_scoped_completed={has_scoped_completed}, "
        f"effective_child_parent_id={effective_child_parent_id!r}, "
        f"is_virtual={is_virtual}), "
        f"but got is_replaying()={child_context.is_replaying()}"
    )


@given(data=child_context_scenario_any())
@settings(max_examples=200)
def test_child_context_transitions_independently(data):
    """Property 5 (independence): Child context determines its own REPLAY-to-NEW transition.

    **Validates: Requirements 4.1, 4.2, 4.3**

    A child context that starts in REPLAY can independently transition to NEW
    by visiting all its scoped completed operations, regardless of the parent's
    replay status.
    """
    operations, parent_parent_id, child_operation_id, is_virtual = data

    parent_context = _create_parent_context_with_operations(
        operations, parent_parent_id
    )
    child_context = parent_context.create_child_context(
        operation_id=child_operation_id, is_virtual=is_virtual
    )

    # If child starts in REPLAY, verify it can transition to NEW independently
    if child_context.is_replaying():
        # Determine the child's effective parent_id
        effective_child_parent_id = parent_parent_id if is_virtual else child_operation_id

        # Find all completed non-EXECUTION ops scoped to the child
        completed_statuses = {
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
            OperationStatus.TIMED_OUT,
        }

        scoped_completed_ids = [
            op_id
            for op_id, op in operations.items()
            if op.parent_id == effective_child_parent_id
            and op.operation_type is not OperationType.EXECUTION
            and op.status in completed_statuses
        ]

        # Visit all scoped completed operations
        for op_id in scoped_completed_ids:
            child_context.track_replay(op_id)

        # After visiting all, child should transition to NEW
        assert child_context.is_replaying() is False, (
            f"Child context should transition to NEW after visiting all "
            f"{len(scoped_completed_ids)} scoped completed operations, "
            f"but is_replaying() still returns True"
        )
