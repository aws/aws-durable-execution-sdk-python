"""Property-based test: Root context initial replay status.

**Validates: Requirements 5.1, 5.2**

Property 6: For any ExecutionState, the root DurableContext's initial replay status
is REPLAY if and only if at least one non-EXECUTION operation exists in the state.
Otherwise, the initial status is NEW.
"""

from threading import Lock
from unittest.mock import Mock

from hypothesis import given, settings
from hypothesis import strategies as st

from aws_durable_execution_sdk_python.execution import _determine_initial_replay_status
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus


# --- Strategies ---

# All operation statuses
ALL_STATUSES = [
    OperationStatus.SUCCEEDED,
    OperationStatus.FAILED,
    OperationStatus.CANCELLED,
    OperationStatus.STOPPED,
    OperationStatus.TIMED_OUT,
    OperationStatus.STARTED,
    OperationStatus.PENDING,
    OperationStatus.READY,
]

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
operation_id_st = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P")),
    min_size=1,
    max_size=30,
)

# Strategy for parent_id values
parent_id_st = st.one_of(st.none(), operation_id_st)


def _create_mock_state(operations: dict[str, Operation]) -> ExecutionState:
    """Create a mock ExecutionState with the given operations and a real lock."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_state.operations = operations
    mock_state._operations_lock = Lock()
    return mock_state


@st.composite
def operations_with_at_least_one_non_execution(draw):
    """Generate operations where at least one is non-EXECUTION type."""
    # Generate at least one non-EXECUTION operation
    non_exec_ops = draw(
        st.lists(
            st.tuples(
                operation_id_st,
                st.sampled_from(NON_EXECUTION_TYPES),
                st.sampled_from(ALL_STATUSES),
                parent_id_st,
            ),
            min_size=1,
            max_size=10,
            unique_by=lambda x: x[0],
        )
    )

    # Optionally add some EXECUTION operations
    exec_ops = draw(
        st.lists(
            st.tuples(
                operation_id_st,
                st.just(OperationType.EXECUTION),
                st.sampled_from(ALL_STATUSES),
                parent_id_st,
            ),
            min_size=0,
            max_size=5,
            unique_by=lambda x: x[0],
        )
    )

    all_ops = non_exec_ops + exec_ops
    operations: dict[str, Operation] = {}
    for op_id, op_type, status, pid in all_ops:
        if op_id not in operations:
            operations[op_id] = Operation(
                operation_id=op_id,
                operation_type=op_type,
                status=status,
                parent_id=pid,
            )

    return operations


@st.composite
def operations_only_execution_type(draw):
    """Generate operations where all are EXECUTION type (or empty)."""
    ops = draw(
        st.lists(
            st.tuples(
                operation_id_st,
                st.just(OperationType.EXECUTION),
                st.sampled_from(ALL_STATUSES),
                parent_id_st,
            ),
            min_size=0,
            max_size=10,
            unique_by=lambda x: x[0],
        )
    )

    operations: dict[str, Operation] = {}
    for op_id, op_type, status, pid in ops:
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=status,
            parent_id=pid,
        )

    return operations


@st.composite
def arbitrary_operations(draw):
    """Generate an arbitrary mix of operations (any types, any statuses)."""
    ops = draw(
        st.lists(
            st.tuples(
                operation_id_st,
                st.sampled_from(ALL_OPERATION_TYPES),
                st.sampled_from(ALL_STATUSES),
                parent_id_st,
            ),
            min_size=0,
            max_size=20,
            unique_by=lambda x: x[0],
        )
    )

    operations: dict[str, Operation] = {}
    for op_id, op_type, status, pid in ops:
        operations[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=status,
            parent_id=pid,
        )

    return operations


# --- Property 6 Tests ---


@given(operations=operations_with_at_least_one_non_execution())
@settings(max_examples=200)
def test_root_replay_status_is_replay_when_non_execution_ops_exist(operations):
    """Property 6: Root context initial replay status is REPLAY when non-EXECUTION ops exist.

    **Validates: Requirements 5.1, 5.2**

    For any ExecutionState containing at least one non-EXECUTION operation,
    _determine_initial_replay_status returns REPLAY.
    """
    state = _create_mock_state(operations)

    result = _determine_initial_replay_status(state)

    assert result is ReplayStatus.REPLAY, (
        f"Expected REPLAY when non-EXECUTION operations exist, got {result}. "
        f"Operations: {[(op_id, op.operation_type) for op_id, op in operations.items()]}"
    )


@given(operations=operations_only_execution_type())
@settings(max_examples=200)
def test_root_replay_status_is_new_when_only_execution_ops_exist(operations):
    """Property 6: Root context initial replay status is NEW when only EXECUTION ops exist.

    **Validates: Requirements 5.1, 5.2**

    For any ExecutionState containing only EXECUTION-type operations (or no operations),
    _determine_initial_replay_status returns NEW.
    """
    state = _create_mock_state(operations)

    result = _determine_initial_replay_status(state)

    assert result is ReplayStatus.NEW, (
        f"Expected NEW when only EXECUTION operations exist, got {result}. "
        f"Operations: {[(op_id, op.operation_type) for op_id, op in operations.items()]}"
    )


@given(operations=arbitrary_operations())
@settings(max_examples=200)
def test_root_replay_status_iff_non_execution_exists(operations):
    """Property 6 (biconditional): REPLAY iff at least one non-EXECUTION operation exists.

    **Validates: Requirements 5.1, 5.2**

    For any ExecutionState, the result is REPLAY if and only if at least one
    non-EXECUTION operation exists. This tests the full biconditional property.
    """
    state = _create_mock_state(operations)

    result = _determine_initial_replay_status(state)

    has_non_execution = any(
        op.operation_type is not OperationType.EXECUTION
        for op in operations.values()
    )

    if has_non_execution:
        assert result is ReplayStatus.REPLAY, (
            f"Expected REPLAY because non-EXECUTION operations exist, got {result}"
        )
    else:
        assert result is ReplayStatus.NEW, (
            f"Expected NEW because no non-EXECUTION operations exist, got {result}"
        )
