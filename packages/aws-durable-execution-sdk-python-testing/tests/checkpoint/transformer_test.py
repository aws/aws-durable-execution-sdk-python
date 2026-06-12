"""Unit tests for OperationTransformer."""

from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
    OperationProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (
    OperationTransformer,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


class MockProcessor(OperationProcessor):
    """Mock processor for testing."""

    def __init__(self, return_value=None):
        self.return_value = return_value
        self.process_calls = []

    def process(self, update, current_op, notifier, execution_arn):
        self.process_calls.append((update, current_op, notifier, execution_arn))
        return self.return_value


def test_init_with_default_processors():
    """Test initialization with default processors."""
    transformer = OperationTransformer()

    assert OperationType.STEP in transformer.processors
    assert OperationType.WAIT in transformer.processors
    assert OperationType.CONTEXT in transformer.processors
    assert OperationType.CALLBACK in transformer.processors
    assert OperationType.EXECUTION in transformer.processors


def test_init_with_custom_processors():
    """Test initialization with custom processors."""
    custom_processors = {OperationType.STEP: MockProcessor()}
    transformer = OperationTransformer(processors=custom_processors)

    assert transformer.processors == custom_processors


def test_process_updates_empty_lists():
    """Test processing with empty updates and operations."""
    transformer = OperationTransformer()
    notifier = Mock()

    operations, updates = transformer.process_updates([], [], notifier, "arn:test")

    assert operations == []
    assert updates == []


def test_process_updates_processor_not_found_raises_error():
    """Test that missing processor raises InvalidParameterValueException."""
    transformer = OperationTransformer(processors={OperationType.STEP: MockProcessor()})
    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.WAIT,
        action=OperationAction.START,
    )
    notifier = Mock()

    with pytest.raises(
        InvalidParameterValueException,
        match="Checkpoint for OperationType.WAIT is not implemented yet.",
    ):
        transformer.process_updates([update], [], notifier, "arn:test")


def test_process_updates_processor_returns_none():
    """Test processing when processor returns None."""
    mock_processor = MockProcessor(return_value=None)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [], notifier, "arn:test"
    )

    assert operations == []
    assert updates == [update]
    assert len(mock_processor.process_calls) == 1


def test_process_updates_new_operation():
    """Test processing creates new operation."""
    new_operation = Mock()
    new_operation.operation_id = "new-id"
    mock_processor = MockProcessor(return_value=new_operation)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="new-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [], notifier, "arn:test"
    )

    assert len(operations) == 1
    assert operations[0] == new_operation
    assert updates == [update]


def test_process_updates_existing_operation():
    """Test processing updates existing operation."""
    existing_operation = Mock()
    existing_operation.operation_id = "existing-id"
    updated_operation = Mock()
    updated_operation.operation_id = "existing-id"

    mock_processor = MockProcessor(return_value=updated_operation)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="existing-id",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [existing_operation], notifier, "arn:test"
    )

    assert len(operations) == 1
    assert operations[0] == updated_operation
    assert updates == [update]


def test_process_updates_multiple_operations_preserve_order():
    """Test processing multiple operations preserves order."""
    op1 = Mock()
    op1.operation_id = "op1"
    op2 = Mock()
    op2.operation_id = "op2"
    op3 = Mock()
    op3.operation_id = "op3"

    updated_op2 = Mock()
    updated_op2.operation_id = "op2"
    new_op4 = Mock()
    new_op4.operation_id = "op4"

    mock_processor = MockProcessor()
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    mock_processor.return_value = updated_op2

    updates = [
        OperationUpdate(
            operation_id="op2",
            operation_type=OperationType.STEP,
            action=OperationAction.SUCCEED,
        ),
    ]
    notifier = Mock()

    operations, result_updates = transformer.process_updates(
        updates, [op1, op2, op3], notifier, "arn:test"
    )

    assert len(operations) == 3
    assert operations[0] == op1
    assert operations[1] == updated_op2
    assert operations[2] == op3
    assert result_updates == updates

    mock_processor.return_value = new_op4
    updates2 = [
        OperationUpdate(
            operation_id="op4",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]

    operations2, result_updates2 = transformer.process_updates(
        updates2, [op1, updated_op2, op3], notifier, "arn:test"
    )

    assert len(operations2) == 4
    assert operations2[0] == op1
    assert operations2[1] == updated_op2
    assert operations2[2] == op3
    assert operations2[3] == new_op4


def test_process_updates_multiple_processors():
    """Test processing with multiple processor types."""
    step_op = Mock()
    step_op.operation_id = "step-id"
    wait_op = Mock()
    wait_op.operation_id = "wait-id"

    step_processor = MockProcessor(return_value=step_op)
    wait_processor = MockProcessor(return_value=wait_op)

    transformer = OperationTransformer(
        processors={
            OperationType.STEP: step_processor,
            OperationType.WAIT: wait_processor,
        }
    )

    updates = [
        OperationUpdate(
            operation_id="step-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        ),
        OperationUpdate(
            operation_id="wait-id",
            operation_type=OperationType.WAIT,
            action=OperationAction.START,
        ),
    ]
    notifier = Mock()

    operations, result_updates = transformer.process_updates(
        updates, [], notifier, "arn:test"
    )

    assert len(operations) == 2
    assert operations[0] == step_op
    assert operations[1] == wait_op
    assert len(step_processor.process_calls) == 1
    assert len(wait_processor.process_calls) == 1


def test_process_updates_passes_correct_parameters():
    """Test that correct parameters are passed to processor."""
    existing_op = Mock()
    existing_op.operation_id = "test-id"
    mock_processor = MockProcessor(return_value=existing_op)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()
    execution_arn = "arn:aws:states:us-east-1:123456789012:execution:test"

    transformer.process_updates([update], [existing_op], notifier, execution_arn)

    call_args = mock_processor.process_calls[0]
    assert call_args[0] == update
    assert call_args[1] == existing_op
    assert call_args[2] == notifier
    assert call_args[3] == execution_arn


def test_process_updates_new_operation_not_in_map():
    """Test processing creates new operation when operation_id not in current operations."""
    new_operation = Mock()
    new_operation.operation_id = "new-id"
    mock_processor = MockProcessor(return_value=new_operation)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    # Existing operations with different IDs
    existing_op = Mock()
    existing_op.operation_id = "existing-id"

    update = OperationUpdate(
        operation_id="new-id",  # Different from existing operation
        operation_type=OperationType.STEP,
        action=OperationAction.START,
    )
    notifier = Mock()

    operations, updates = transformer.process_updates(
        [update], [existing_op], notifier, "arn:test"
    )

    # Should have both existing and new operation
    assert len(operations) == 2
    assert operations[0] == existing_op  # Original operation preserved
    assert operations[1] == new_operation  # New operation appended
    assert updates == [update]


def test_process_updates_in_place_update_with_multiple_operations():
    """Test in-place update when operation exists in middle of operations list."""
    # Create three operations
    op1 = Mock()
    op1.operation_id = "op1"
    op2 = Mock()
    op2.operation_id = "op2"
    op3 = Mock()
    op3.operation_id = "op3"

    # Updated version of op2
    updated_op2 = Mock()
    updated_op2.operation_id = "op2"

    mock_processor = MockProcessor(return_value=updated_op2)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    # Update for op2 (middle operation)
    update = OperationUpdate(
        operation_id="op2",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    # Process update with op2 in the middle of the list
    operations, updates = transformer.process_updates(
        [update], [op1, op2, op3], notifier, "arn:test"
    )

    # Verify in-place update occurred
    assert len(operations) == 3
    assert operations[0] == op1  # First operation unchanged
    assert operations[1] == updated_op2  # Middle operation updated in-place
    assert operations[2] == op3  # Last operation unchanged
    assert updates == [update]


def test_process_updates_in_place_update_break_coverage():
    """Test to ensure break statement in in-place update loop is covered."""
    # Create operations where target is first in list to ensure break is hit
    target_op = Mock()
    target_op.operation_id = "target"
    other_op = Mock()
    other_op.operation_id = "other"

    updated_target = Mock()
    updated_target.operation_id = "target"

    mock_processor = MockProcessor(return_value=updated_target)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="target",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    # Target operation is first - should hit break immediately
    operations, updates = transformer.process_updates(
        [update], [target_op, other_op], notifier, "arn:test"
    )

    assert len(operations) == 2
    assert operations[0] == updated_target


def test_process_updates_empty_operations_list():
    """Test for loop exit when result_operations is empty."""
    updated_op = Mock()
    updated_op.operation_id = "test-id"

    mock_processor = MockProcessor(return_value=updated_op)
    transformer = OperationTransformer(processors={OperationType.STEP: mock_processor})

    update = OperationUpdate(
        operation_id="test-id",
        operation_type=OperationType.STEP,
        action=OperationAction.SUCCEED,
    )
    notifier = Mock()

    # Empty current_operations list - for loop should exit immediately
    operations, updates = transformer.process_updates(
        [update], [], notifier, "arn:test"
    )

    assert len(operations) == 1
    assert operations[0] == updated_op
