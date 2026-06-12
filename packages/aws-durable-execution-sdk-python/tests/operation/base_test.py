"""Unit tests for OperationExecutor base framework."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.exceptions import InvalidStateError
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.state import CheckpointedResult

# Test fixtures and helpers


class ConcreteOperationExecutor(OperationExecutor[str]):
    """Concrete implementation for testing the abstract base class."""

    def __init__(self):
        self.check_result_status_called = 0
        self.execute_called = 0
        self.check_result_to_return = None
        self.execute_result_to_return = "executed_result"

    def check_result_status(self) -> CheckResult[str]:
        """Mock implementation that returns configured result."""
        self.check_result_status_called += 1
        if self.check_result_to_return is None:
            msg = "check_result_to_return not configured"
            raise ValueError(msg)
        return self.check_result_to_return

    def execute(self, checkpointed_result: CheckpointedResult) -> str:
        """Mock implementation that returns configured result."""
        self.execute_called += 1
        return self.execute_result_to_return


def create_mock_checkpoint(status: OperationStatus) -> CheckpointedResult:
    """Create a mock CheckpointedResult with the given status."""
    operation = Operation(
        operation_id="test_op",
        operation_type=OperationType.STEP,
        status=status,
    )
    return CheckpointedResult.create_from_operation(operation)


# Tests for CheckResult factory methods


def test_check_result_create_is_ready_to_execute():
    """Test CheckResult.create_is_ready_to_execute factory method."""
    checkpoint = create_mock_checkpoint(OperationStatus.STARTED)

    result = CheckResult.create_is_ready_to_execute(checkpoint)

    assert result.is_ready_to_execute is True
    assert result.has_checkpointed_result is False
    assert result.checkpointed_result is checkpoint
    assert result.deserialized_result is None


def test_check_result_create_started():
    """Test CheckResult.create_started factory method."""
    result = CheckResult.create_started()

    assert result.is_ready_to_execute is False
    assert result.has_checkpointed_result is False
    assert result.checkpointed_result is None
    assert result.deserialized_result is None


def test_check_result_create_completed():
    """Test CheckResult.create_completed factory method."""
    test_result = "test_completed_result"

    result = CheckResult.create_completed(test_result)

    assert result.is_ready_to_execute is False
    assert result.has_checkpointed_result is True
    assert result.checkpointed_result is None
    assert result.deserialized_result == test_result


def test_check_result_create_completed_with_none():
    """Test CheckResult.create_completed with None result (valid for operations that return None)."""
    result = CheckResult.create_completed(None)

    assert result.is_ready_to_execute is False
    assert result.has_checkpointed_result is True
    assert result.checkpointed_result is None
    assert result.deserialized_result is None


# Tests for OperationExecutor.process() method


def test_process_with_terminal_result_on_first_check():
    """Test process() when check_result_status returns terminal result on first call."""
    executor = ConcreteOperationExecutor()
    executor.check_result_to_return = CheckResult.create_completed("terminal_result")

    result = executor.process()

    assert result == "terminal_result"
    assert executor.check_result_status_called == 1
    assert executor.execute_called == 0


def test_process_with_ready_to_execute_on_first_check():
    """Test process() when check_result_status returns ready_to_execute on first call."""
    executor = ConcreteOperationExecutor()
    checkpoint = create_mock_checkpoint(OperationStatus.STARTED)
    executor.check_result_to_return = CheckResult.create_is_ready_to_execute(checkpoint)
    executor.execute_result_to_return = "execution_result"

    result = executor.process()

    assert result == "execution_result"
    assert executor.check_result_status_called == 1
    assert executor.execute_called == 1


def test_process_with_checkpoint_created_then_terminal():
    """Test process() when checkpoint is created, then terminal result on second check."""
    executor = ConcreteOperationExecutor()

    # First call returns create_started (checkpoint was created)
    # Second call returns terminal result (immediate response)
    call_count = 0

    def check_result_side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return CheckResult.create_started()
        return CheckResult.create_completed("immediate_response")

    executor.check_result_status = check_result_side_effect

    result = executor.process()

    assert result == "immediate_response"
    assert call_count == 2
    assert executor.execute_called == 0


def test_process_with_checkpoint_created_then_ready_to_execute():
    """Test process() when checkpoint is created, then ready_to_execute on second check."""
    executor = ConcreteOperationExecutor()
    checkpoint = create_mock_checkpoint(OperationStatus.STARTED)

    # First call returns create_started (checkpoint was created)
    # Second call returns ready_to_execute (no immediate response, proceed to execute)
    call_count = 0

    def check_result_side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return CheckResult.create_started()
        return CheckResult.create_is_ready_to_execute(checkpoint)

    executor.check_result_status = check_result_side_effect
    executor.execute_result_to_return = "execution_result"

    result = executor.process()

    assert result == "execution_result"
    assert call_count == 2
    assert executor.execute_called == 1


def test_process_with_none_result_terminal():
    """Test process() with terminal result that is None (valid for operations returning None)."""
    executor = ConcreteOperationExecutor()
    executor.check_result_to_return = CheckResult.create_completed(None)

    result = executor.process()

    assert result is None
    assert executor.check_result_status_called == 1
    assert executor.execute_called == 0


def test_process_raises_invalid_state_when_checkpointed_result_missing():
    """Test process() raises InvalidStateError when ready_to_execute but checkpoint is None."""
    executor = ConcreteOperationExecutor()
    # Create invalid state: ready_to_execute but no checkpoint
    executor.check_result_to_return = CheckResult(
        is_ready_to_execute=True,
        has_checkpointed_result=False,
        checkpointed_result=None,
    )

    with pytest.raises(InvalidStateError) as exc_info:
        executor.process()

    assert "checkpointed result is not set" in str(exc_info.value)


def test_process_raises_invalid_state_when_neither_terminal_nor_ready():
    """Test process() raises InvalidStateError when result is neither terminal nor ready."""
    executor = ConcreteOperationExecutor()
    # Create invalid state: neither terminal nor ready (both False)
    executor.check_result_to_return = CheckResult(
        is_ready_to_execute=False,
        has_checkpointed_result=False,
    )

    # Mock to return same invalid state on both calls
    call_count = 0

    def check_result_side_effect():
        nonlocal call_count
        call_count += 1
        return CheckResult(
            is_ready_to_execute=False,
            has_checkpointed_result=False,
        )

    executor.check_result_status = check_result_side_effect

    with pytest.raises(InvalidStateError) as exc_info:
        executor.process()

    assert "neither terminal nor ready to execute" in str(exc_info.value)
    assert call_count == 2  # Should call twice before raising


def test_process_double_check_pattern():
    """Test that process() implements the double-check pattern correctly.

    This verifies the core immediate response handling logic:
    1. Check status once (may find existing checkpoint or create new one)
    2. If checkpoint was just created, check again (catches immediate response)
    3. Only call execute() if ready after both checks
    """
    executor = ConcreteOperationExecutor()
    checkpoint = create_mock_checkpoint(OperationStatus.STARTED)

    check_calls = []

    def track_check_calls():
        call_num = len(check_calls) + 1
        check_calls.append(call_num)

        if call_num == 1:
            # First check: checkpoint doesn't exist, create it
            return CheckResult.create_started()
        # Second check: checkpoint exists, ready to execute
        return CheckResult.create_is_ready_to_execute(checkpoint)

    executor.check_result_status = track_check_calls
    executor.execute_result_to_return = "final_result"

    result = executor.process()

    # Verify the double-check pattern
    assert len(check_calls) == 2, "Should check status exactly twice"
    assert check_calls == [1, 2], "Checks should be in order"
    assert executor.execute_called == 1, "Should execute once after both checks"
    assert result == "final_result"


def test_process_single_check_when_terminal_immediately():
    """Test that process() only checks once when terminal result is found immediately."""
    executor = ConcreteOperationExecutor()

    check_calls = []

    def track_check_calls():
        call_num = len(check_calls) + 1
        check_calls.append(call_num)
        return CheckResult.create_completed("immediate_terminal")

    executor.check_result_status = track_check_calls

    result = executor.process()

    # Should only check once since terminal result was found
    assert len(check_calls) == 1, "Should check status only once for immediate terminal"
    assert executor.execute_called == 0, "Should not execute when terminal result found"
    assert result == "immediate_terminal"


def test_process_single_check_when_ready_immediately():
    """Test that process() only checks once when ready_to_execute is found immediately."""
    executor = ConcreteOperationExecutor()
    checkpoint = create_mock_checkpoint(OperationStatus.STARTED)

    check_calls = []

    def track_check_calls():
        call_num = len(check_calls) + 1
        check_calls.append(call_num)
        return CheckResult.create_is_ready_to_execute(checkpoint)

    executor.check_result_status = track_check_calls
    executor.execute_result_to_return = "execution_result"

    result = executor.process()

    # Should only check once since ready_to_execute was found
    assert len(check_calls) == 1, "Should check status only once when ready immediately"
    assert executor.execute_called == 1, "Should execute once"
    assert result == "execution_result"
