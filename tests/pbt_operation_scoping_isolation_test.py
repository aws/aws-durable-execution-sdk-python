"""Property-based test: Operation scoping isolation.

**Validates: Requirements 3.2, 7.1, 7.2**

Property 4: For any set of operations in ExecutionState with varying `parent_id`
values, a DurableContext's `_get_scoped_completed_operations()` returns only
operations whose `parent_id` matches the context's own `_parent_id`, excluding
operations belonging to child, sibling, or ancestor contexts.
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
from aws_durable_execution_sdk_python.state import ExecutionState


# Strategies for generating test data

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

# Non-EXECUTION operation types (eligible for scoping)
NON_EXECUTION_TYPES = [
    OperationType.STEP,
    OperationType.WAIT,
    OperationType.CALLBACK,
    OperationType.CHAINED_INVOKE,
    OperationType.CONTEXT,
]

ALL_OPERATION_TYPES = NON_EXECUTION_TYPES + [OperationType.EXECUTION]

# Strategy for parent_id values (None represents root context)
parent_id_strategy = st.one_of(st.none(), st.text(min_size=1, max_size=20))

# Strategy for operation IDs
operation_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P")),
    min_size=1,
    max_size=30,
)


@st.composite
def operation_strategy(draw, parent_id=None, status=None, operation_type=None):
    """Generate an Operation with configurable parent_id, status, and type."""
    op_id = draw(operation_id_strategy)
    op_parent_id = parent_id if parent_id is not None else draw(parent_id_strategy)
    op_status = status if status is not None else draw(st.sampled_from(ALL_STATUSES))
    op_type = (
        operation_type
        if operation_type is not None
        else draw(st.sampled_from(ALL_OPERATION_TYPES))
    )

    return Operation(
        operation_id=op_id,
        operation_type=op_type,
        status=op_status,
        parent_id=op_parent_id,
    )


@st.composite
def operations_with_varying_parents(draw):
    """Generate a set of operations with varying parent_ids.

    Returns a tuple of (operations_dict, context_parent_id) where:
    - operations_dict maps operation_id -> Operation
    - context_parent_id is the parent_id for the context under test
    """
    # Choose the context's parent_id
    context_parent_id = draw(parent_id_strategy)

    # Generate a list of distinct parent_ids (including the context's own)
    other_parent_ids = draw(
        st.lists(
            st.text(min_size=1, max_size=20),
            min_size=0,
            max_size=5,
        )
    )
    # Include None as a possible other parent_id (root context operations)
    all_parent_ids = [context_parent_id] + other_parent_ids + [None]

    # Generate operations with various parent_ids
    operations = draw(
        st.lists(
            st.tuples(
                operation_id_strategy,
                st.sampled_from(all_parent_ids),
                st.sampled_from(ALL_STATUSES),
                st.sampled_from(ALL_OPERATION_TYPES),
            ),
            min_size=0,
            max_size=20,
            unique_by=lambda x: x[0],  # Unique operation IDs
        )
    )

    operations_dict = {}
    for op_id, op_parent_id, op_status, op_type in operations:
        operations_dict[op_id] = Operation(
            operation_id=op_id,
            operation_type=op_type,
            status=op_status,
            parent_id=op_parent_id,
        )

    return operations_dict, context_parent_id


def _create_context_with_operations(
    operations_dict: dict[str, Operation], context_parent_id: str | None
) -> DurableContext:
    """Create a DurableContext with a mock ExecutionState containing the given operations."""
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
        parent_id=context_parent_id,
    )


@given(data=operations_with_varying_parents())
@settings(max_examples=200)
def test_scoped_operations_only_include_matching_parent_id(data):
    """Property 4: Operation scoping isolation.

    **Validates: Requirements 3.2, 7.1, 7.2**

    For any set of operations in ExecutionState with varying `parent_id` values,
    `_get_scoped_completed_operations()` returns only operations whose `parent_id`
    matches the context's own `_parent_id`, excluding operations belonging to child,
    sibling, or ancestor contexts.
    """
    operations_dict, context_parent_id = data
    context = _create_context_with_operations(operations_dict, context_parent_id)

    # Call the method under test
    scoped_ops = context._get_scoped_completed_operations()

    # Compute expected result manually
    completed_statuses = {
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.STOPPED,
        OperationStatus.TIMED_OUT,
    }

    expected_ops = set()
    for op_id, op in operations_dict.items():
        if op.operation_type is OperationType.EXECUTION:
            continue
        if op.status not in completed_statuses:
            continue
        if op.parent_id == context_parent_id:
            expected_ops.add(op_id)

    # The scoped operations must exactly match the expected set
    assert scoped_ops == expected_ops


@given(data=operations_with_varying_parents())
@settings(max_examples=200)
def test_scoped_operations_exclude_non_matching_parent_ids(data):
    """Property 4 (exclusion aspect): Operations with different parent_ids are excluded.

    **Validates: Requirements 3.2, 7.1, 7.2**

    Verifies that no operation in the result has a parent_id different from
    the context's own _parent_id.
    """
    operations_dict, context_parent_id = data
    context = _create_context_with_operations(operations_dict, context_parent_id)

    scoped_ops = context._get_scoped_completed_operations()

    # Every operation in the result must have parent_id matching the context's _parent_id
    for op_id in scoped_ops:
        op = operations_dict[op_id]
        assert op.parent_id == context_parent_id, (
            f"Operation {op_id} has parent_id={op.parent_id!r} but context's "
            f"_parent_id={context_parent_id!r}"
        )


@given(data=operations_with_varying_parents())
@settings(max_examples=200)
def test_scoped_operations_exclude_execution_type(data):
    """Property 4 (EXECUTION exclusion): EXECUTION operations are never included.

    **Validates: Requirements 3.2, 7.1, 7.2**

    Even if an EXECUTION operation has a matching parent_id and completed status,
    it must not appear in the scoped operations.
    """
    operations_dict, context_parent_id = data
    context = _create_context_with_operations(operations_dict, context_parent_id)

    scoped_ops = context._get_scoped_completed_operations()

    # No EXECUTION operations should be in the result
    for op_id in scoped_ops:
        op = operations_dict[op_id]
        assert op.operation_type is not OperationType.EXECUTION, (
            f"Operation {op_id} is of type EXECUTION but was included in scoped ops"
        )


@given(data=operations_with_varying_parents())
@settings(max_examples=200)
def test_scoped_operations_only_include_completed_statuses(data):
    """Property 4 (completion filter): Only terminal-status operations are included.

    **Validates: Requirements 3.2, 7.1, 7.2**

    Operations with non-terminal statuses (STARTED, PENDING, READY) must not
    appear in the scoped operations even if they have matching parent_id.
    """
    operations_dict, context_parent_id = data
    context = _create_context_with_operations(operations_dict, context_parent_id)

    scoped_ops = context._get_scoped_completed_operations()

    completed_statuses = {
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.STOPPED,
        OperationStatus.TIMED_OUT,
    }

    # Every operation in the result must have a terminal status
    for op_id in scoped_ops:
        op = operations_dict[op_id]
        assert op.status in completed_statuses, (
            f"Operation {op_id} has status={op.status} which is not a terminal status"
        )
