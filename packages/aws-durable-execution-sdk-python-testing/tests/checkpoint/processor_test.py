"""Unit tests for CheckpointProcessor."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
    StateOutput,
    WaitDetails,
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


def _make_processor_with_started_execution() -> tuple[
    CheckpointProcessor, InMemoryExecutionStore, Execution, str
]:
    store = InMemoryExecutionStore()
    processor = CheckpointProcessor(store, Mock(spec=Scheduler))
    execution = Execution.new(
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name="test-execution",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="test-inv-id",
        )
    )
    execution.start()
    store.save(execution)
    token = execution.get_new_checkpoint_token()
    return processor, store, execution, token


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


def test_add_execution_observer():
    """Test adding execution observer."""
    store = Mock(spec=ExecutionStore)
    scheduler = Mock(spec=Scheduler)

    processor = CheckpointProcessor(store, scheduler)
    observer = Mock()

    processor.add_execution_observer(observer)

    assert observer in processor._observers  # noqa: SLF001


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


@patch("aws_durable_execution_sdk_python_testing.checkpoint.core.CheckpointValidator")
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


@patch("aws_durable_execution_sdk_python_testing.checkpoint.core.CheckpointValidator")
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


def test_process_checkpoint_delivers_due_wait_completion() -> None:
    """A wait whose scheduled end has passed is completed by the
    checkpoint and returned in the same response delta."""
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

    past: datetime = datetime.now(UTC) - timedelta(seconds=5)
    execution.operations.append(
        Operation(
            operation_id="wait-1",
            parent_id=None,
            name="due-wait",
            start_timestamp=past,
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=past),
        )
    )
    store.save(execution)

    token: str = CheckpointToken(
        execution_arn=execution.durable_execution_arn, token_sequence=0
    ).to_str()

    result: CheckpointOutput = processor.process_checkpoint(token, [], None)

    returned = {op.operation_id: op for op in result.new_execution_state.operations}
    assert "wait-1" in returned
    assert returned["wait-1"].status is OperationStatus.SUCCEEDED

    persisted = store.load(execution.durable_execution_arn)
    persisted_wait = next(
        op for op in persisted.operations if op.operation_id == "wait-1"
    )
    assert persisted_wait.status is OperationStatus.SUCCEEDED


def test_paused_checkpoint_returns_no_token() -> None:
    processor, store, execution, token = _make_processor_with_started_execution()
    execution.pause()
    store.save(execution)

    response = processor.process_checkpoint(token, [], "c1")

    assert response.checkpoint_token is None


def test_paused_checkpoint_returns_and_persists_step_update() -> None:
    processor, store, execution, token = _make_processor_with_started_execution()
    execution.pause()
    store.save(execution)
    update = OperationUpdate(
        operation_id="step-A",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
        name="step-A",
    )

    response = processor.process_checkpoint(token, [update], "c1")

    assert [op.operation_id for op in response.new_execution_state.operations] == [
        "step-A"
    ]
    persisted = store.load(execution.durable_execution_arn)
    assert [
        op.operation_id
        for op in persisted.get_navigable_operations()
        if op.operation_type is OperationType.STEP
    ] == ["step-A"]


def test_empty_paused_checkpoint_advances_token_sequence_once() -> None:
    processor, store, execution, token = _make_processor_with_started_execution()
    previous_sequence = execution.token_sequence
    
    execution.pause()
    store.save(execution)
    processor.process_checkpoint(token, [], "c1")

    assert (
        store.load(execution.durable_execution_arn).token_sequence
        == previous_sequence + 1
    )


def test_paused_checkpoint_does_not_create_idempotency_record() -> None:
    processor, store, execution, token = _make_processor_with_started_execution()
    assert execution.last_checkpoint is None
    execution.pause()
    store.save(execution)

    processor.process_checkpoint(token, [], "c1")

    assert store.load(execution.durable_execution_arn).last_checkpoint is None


@pytest.mark.parametrize("retry_client_token", ["c1", None, "different-client-token"])
def test_paused_checkpoint_retry_is_rejected_while_paused(
    retry_client_token: str | None,
) -> None:
    processor, store, execution, token = _make_processor_with_started_execution()
    arn = execution.durable_execution_arn

    execution.pause()
    store.save(execution)
    processor.process_checkpoint(token, [], "c1")

    assert store.load(arn).is_paused is True

    before_retry = deepcopy(store.load(arn).to_json_dict())

    with pytest.raises(
        InvalidParameterValueException, match="^Invalid checkpoint token$"
    ):
        processor.process_checkpoint(token, [], retry_client_token)
    assert store.load(arn).to_json_dict() == before_retry


@pytest.mark.parametrize("retry_client_token", ["c1", None, "different-client-token"])
def test_paused_checkpoint_retry_is_rejected_even_after_resume(
    retry_client_token: str | None,
) -> None:
    processor, store, execution, token = _make_processor_with_started_execution()
    arn = execution.durable_execution_arn

    execution.pause()
    store.save(execution)
    processor.process_checkpoint(token, [], "c1")
    execution.resume()
    store.save(execution)

    assert store.load(arn).is_paused is False

    before_retry = deepcopy(store.load(arn).to_json_dict())

    with pytest.raises(
        InvalidParameterValueException, match="^Invalid checkpoint token$"
    ):
        processor.process_checkpoint(token, [], retry_client_token)
    assert store.load(arn).to_json_dict() == before_retry
