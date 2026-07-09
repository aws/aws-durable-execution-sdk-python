"""Unit tests for CheckpointProcessor."""

from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
)
from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.token import CheckpointToken


def test_init():
    """Test CheckpointProcessor initialization."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)

    processor = CheckpointProcessor(store, scheduler)

    # Test that processor was created successfully by calling a public method
    # This indirectly verifies that internal components were initialized
    assert processor is not None

    # Test that we can add observers (verifies notifier is initialized)
    observer = Mock()
    processor.add_execution_observer(observer)  # Should not raise an exception


@patch(
    "aws_durable_execution_sdk_python_testing.checkpoint.processor.ExecutionNotifier"
)
def test_add_execution_observer(mock_notifier_class):
    """Test adding execution observer."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    mock_notifier_instance = Mock()
    mock_notifier_class.return_value = mock_notifier_instance

    processor = CheckpointProcessor(store, scheduler)
    observer = Mock()

    processor.add_execution_observer(observer)

    # Verify observer was added through the notifier's public method
    mock_notifier_instance.add_observer.assert_called_once_with(observer)


def test_process_checkpoint_success():
    """End-to-end successful checkpoint through CheckpointProcessor.

    Uses real Execution + InMemoryExecutionStore; no mocks on internal
    dispatch because flow goes pin -> delta -> advance, which
    is meaningless against Mock state.
    """
    store = InMemoryExecutionStore()
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
    )
    execution = Execution.new(start_input)
    execution.start()
    store.save(execution)

    token = CheckpointToken(
        execution_arn=execution.durable_execution_arn, token_sequence=0
    ).to_str()

    updates = [
        OperationUpdate(
            operation_id="step-A",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            name="step-A",
        )
    ]

    result = processor.process_checkpoint(token, updates, "client-token")

    assert isinstance(result, CheckpointOutput)
    assert isinstance(result.new_execution_state, CheckpointUpdatedExecutionState)
    # The freshly-started STEP op is the delta.
    assert any(
        op.operation_id == "step-A" for op in result.new_execution_state.operations
    )


@patch(
    "aws_durable_execution_sdk_python_testing.checkpoint.processor.CheckpointValidator"
)
def test_process_checkpoint_invalid_token_complete_execution(mock_validator):
    """Test checkpoint processing with complete execution."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution as complete
    execution = Mock(spec=Execution)
    execution.is_complete = True
    execution.token_sequence = 1
    execution.last_checkpoint = None  # no cached replay

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105
    updates = []

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1
        mock_from_str.return_value = mock_token

        with pytest.raises(
            InvalidParameterValueException, match="Invalid checkpoint token"
        ):
            processor.process_checkpoint(checkpoint_token, updates, "client-token")


@patch(
    "aws_durable_execution_sdk_python_testing.checkpoint.processor.CheckpointValidator"
)
def test_process_checkpoint_invalid_token_sequence(mock_validator):
    """Test checkpoint processing with invalid token sequence."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution with different token sequence
    execution = Mock(spec=Execution)
    execution.is_complete = False
    execution.token_sequence = 2
    execution.last_checkpoint = None

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105
    updates = []

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_token.token_sequence = 1  # Different from execution
        mock_from_str.return_value = mock_token

        with pytest.raises(
            InvalidParameterValueException, match="Invalid checkpoint token"
        ):
            processor.process_checkpoint(checkpoint_token, updates, "client-token")


def test_process_checkpoint_updates_execution_state():
    """Test that checkpoint processing applies updates and advances
    token_sequence. Uses real state because mocking the dispatcher
    internals no longer tracks the delta semantics."""
    store = InMemoryExecutionStore()
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
    )
    execution = Execution.new(start_input)
    execution.start()
    store.save(execution)

    token = CheckpointToken(
        execution_arn=execution.durable_execution_arn, token_sequence=0
    ).to_str()

    updates = [
        OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            name="test-id",
        )
    ]

    processor.process_checkpoint(token, updates, "client-token")

    refreshed = store.load(execution.durable_execution_arn)
    assert refreshed.token_sequence == 1
    assert any(op.operation_id == "test-id" for op in refreshed.operations)


def test_get_execution_state():
    """Test getting execution state."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    # Mock execution
    execution = Mock(spec=Execution)
    navigable_ops = [Mock()]
    execution.get_navigable_operations.return_value = navigable_ops

    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = processor.get_execution_state(checkpoint_token, "next-marker", 500)

    # Verify calls
    store.load.assert_called_once_with("arn:test")
    execution.get_navigable_operations.assert_called_once()

    # Verify result
    assert isinstance(result, StateOutput)
    assert result.operations == navigable_ops
    assert result.next_marker is None


def test_get_execution_state_default_max_items():
    """Test getting execution state with default max_items."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    execution = Mock(spec=Execution)
    execution.get_navigable_operations.return_value = []
    store.load.return_value = execution

    checkpoint_token = "test-token"  # noqa: S105

    with patch.object(CheckpointToken, "from_str") as mock_from_str:
        mock_token = Mock()
        mock_token.execution_arn = "arn:test"
        mock_from_str.return_value = mock_token

        result = processor.get_execution_state(checkpoint_token, "next-marker")

    assert isinstance(result, StateOutput)


def test_process_checkpoint_idempotent_replay():
    """Covers the in-process _maybe_replay_cached path. Two calls with
    the same (client_token, inbound_checkpoint_token) return the same
    outbound token and operations list."""
    store = InMemoryExecutionStore()
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="inv-idem",
    )
    execution = Execution.new(start_input)
    execution.start()
    store.save(execution)

    inbound = CheckpointToken(
        execution_arn=execution.durable_execution_arn, token_sequence=0
    ).to_str()
    updates = [
        OperationUpdate(
            operation_id="step-A",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            name="step-A",
        )
    ]

    r1 = processor.process_checkpoint(inbound, updates, "c1")
    r2 = processor.process_checkpoint(inbound, updates, "c1")

    assert r1.checkpoint_token == r2.checkpoint_token
    # token_sequence didn't double-advance: replay returned the
    # cached response without applying updates again.
    assert store.load(execution.durable_execution_arn).token_sequence == 1


def test_checkpoint_from_superseded_invocation_is_rejected():
    """A checkpoint carrying a prior invocation's token is rejected once
    a new invocation has been dispatched.

    Reproduces the case where a handler outlives its deadline: the
    runner dispatches a fresh invocation, and the earlier invocation
    that is still running must not be able to checkpoint against the
    live execution.
    """
    store = InMemoryExecutionStore()
    scheduler = Mock(spec=Scheduler)
    processor = CheckpointProcessor(store, scheduler)

    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
    )
    execution = Execution.new(start_input)
    execution.start()

    # First invocation is dispatched; capture the token handed to it.
    execution.begin_new_invocation()
    store.save(execution)
    stale_token = execution.get_new_checkpoint_token()

    # A new invocation supersedes the first (e.g. after a timeout).
    execution.begin_new_invocation()
    store.save(execution)
    current_token = execution.get_new_checkpoint_token()

    updates = [
        OperationUpdate(
            operation_id="step-A",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
            name="step-A",
        )
    ]

    # The superseded invocation's checkpoint is rejected.
    with pytest.raises(InvalidParameterValueException):
        processor.process_checkpoint(stale_token, updates, "client-token")

    # The current invocation's checkpoint is accepted.
    result = processor.process_checkpoint(current_token, updates, "client-token")
    assert isinstance(result, CheckpointOutput)
