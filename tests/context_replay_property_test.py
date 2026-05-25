"""Property-based tests for DurableContext per-context replay status."""

from threading import Lock
from unittest.mock import Mock

from hypothesis import given
from hypothesis import strategies as st

from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus


def _create_mock_state(
    operations: dict[str, Operation] | None = None,
) -> Mock:
    """Create a mock ExecutionState with real lock and operations dict."""
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"
    state._operations_lock = Lock()
    state.operations = operations or {}
    return state


def _create_context_in_new_status(
    parent_id: str | None = None,
    operations: dict[str, Operation] | None = None,
) -> DurableContext:
    """Create a DurableContext explicitly in NEW status."""
    state = _create_mock_state(operations)
    execution_context = ExecutionContext(
        durable_execution_arn=state.durable_execution_arn
    )
    return DurableContext(
        state=state,
        execution_context=execution_context,
        parent_id=parent_id,
        replay_status=ReplayStatus.NEW,
    )


# Strategy for generating arbitrary operation IDs (non-empty strings)
operation_id_strategy = st.text(
    alphabet=st.characters(categories=("L", "N", "P", "S")),
    min_size=1,
    max_size=64,
)


class TestNewStatusIsStable:
    """Property 3: NEW status is stable.

    **Validates: Requirements 3.4**

    For any DurableContext in NEW status and for any operation ID,
    calling `track_replay` does not change the context's status —
    `is_replaying()` continues to return False.
    """

    @given(operation_id=operation_id_strategy)
    def test_track_replay_on_new_context_keeps_status_new(
        self, operation_id: str
    ) -> None:
        """Calling track_replay on a NEW context does not change status to REPLAY.

        **Validates: Requirements 3.4**
        """
        context = _create_context_in_new_status()

        # Precondition: context starts in NEW status
        assert not context.is_replaying()

        # Action: call track_replay with an arbitrary operation ID
        context.track_replay(operation_id)

        # Postcondition: context remains in NEW status
        assert not context.is_replaying()

    @given(operation_ids=st.lists(operation_id_strategy, min_size=1, max_size=50))
    def test_track_replay_multiple_calls_on_new_context_keeps_status_new(
        self, operation_ids: list[str]
    ) -> None:
        """Calling track_replay multiple times on a NEW context never changes status.

        **Validates: Requirements 3.4**
        """
        context = _create_context_in_new_status()

        # Precondition: context starts in NEW status
        assert not context.is_replaying()

        # Action: call track_replay with multiple arbitrary operation IDs
        for op_id in operation_ids:
            context.track_replay(op_id)

        # Postcondition: context remains in NEW status after all calls
        assert not context.is_replaying()

    @given(
        operation_id=operation_id_strategy,
        parent_id=st.one_of(st.none(), operation_id_strategy),
    )
    def test_track_replay_on_new_context_with_any_parent_id_keeps_status_new(
        self, operation_id: str, parent_id: str | None
    ) -> None:
        """NEW status is stable regardless of the context's parent_id.

        **Validates: Requirements 3.4**
        """
        context = _create_context_in_new_status(parent_id=parent_id)

        # Precondition: context starts in NEW status
        assert not context.is_replaying()

        # Action: call track_replay
        context.track_replay(operation_id)

        # Postcondition: context remains in NEW status
        assert not context.is_replaying()

    @given(
        operation_id=operation_id_strategy,
        parent_id=st.one_of(st.none(), operation_id_strategy),
    )
    def test_track_replay_on_new_context_with_existing_operations_keeps_status_new(
        self, operation_id: str, parent_id: str | None
    ) -> None:
        """NEW status is stable even when completed operations exist in state.

        Even if there are completed operations scoped to this context in
        ExecutionState, once the context is in NEW status, track_replay
        does not revert it back to REPLAY.

        **Validates: Requirements 3.4**
        """
        # Create operations that are scoped to this context (matching parent_id)
        operations = {
            "existing-op-1": Operation(
                operation_id="existing-op-1",
                operation_type=OperationType.STEP,
                status=OperationStatus.SUCCEEDED,
                parent_id=parent_id,
            ),
            "existing-op-2": Operation(
                operation_id="existing-op-2",
                operation_type=OperationType.STEP,
                status=OperationStatus.FAILED,
                parent_id=parent_id,
            ),
        }

        context = _create_context_in_new_status(
            parent_id=parent_id, operations=operations
        )

        # Precondition: context starts in NEW status
        assert not context.is_replaying()

        # Action: call track_replay with an arbitrary operation ID
        context.track_replay(operation_id)

        # Postcondition: context remains in NEW status
        assert not context.is_replaying()
