"""Additional concurrent tests for wait and retry operations."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
    WaitDetails,
)

from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.model import StartDurableExecutionInput


def test_concurrent_wait_and_retry_completion():
    """Test concurrent complete_wait and complete_retry operations."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
        input='{"test": "data"}',
    )
    execution = Execution.new(input_data)

    # Add WAIT and STEP operations
    wait_op = Operation(
        operation_id="wait-1",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )

    step_op = Operation(
        operation_id="step-1",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(UTC),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(),
    )

    execution.operations.extend([wait_op, step_op])

    results = []
    results_lock = threading.Lock()

    def complete_wait():
        result = execution.complete_wait("wait-1")
        with results_lock:
            results.append(f"wait-completed-{result.status.value}")

    def complete_retry():
        result = execution.complete_retry("step-1")
        with results_lock:
            results.append(f"retry-completed-{result.status.value}")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        futures.append(executor.submit(complete_wait))
        futures.append(executor.submit(complete_retry))

        for future in as_completed(futures):
            future.result()

    assert len(results) == 2
    assert "wait-completed-SUCCEEDED" in results
    assert "retry-completed-READY" in results

    # Each async completion bumps seq_counter (not token_sequence —
    # that only advances on accepted checkpoint calls).
    assert execution.seq_counter == 2
    assert execution.token_sequence == 0


def _make_execution() -> Execution:
    """Create a bare execution for due-operation tests."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
        input='{"test": "data"}',
    )
    return Execution.new(input_data)


def test_complete_due_operations_completes_due_wait_and_retry() -> None:
    """A due wait transitions to SUCCEEDED and a due step retry to READY."""
    execution = _make_execution()
    now: datetime = datetime.now(UTC)
    past: datetime = now - timedelta(seconds=5)

    execution.operations.append(
        Operation(
            operation_id="wait-1",
            parent_id=None,
            name="test-wait",
            start_timestamp=past,
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=past),
        )
    )
    execution.operations.append(
        Operation(
            operation_id="step-1",
            parent_id=None,
            name="test-step",
            start_timestamp=past,
            operation_type=OperationType.STEP,
            status=OperationStatus.PENDING,
            step_details=StepDetails(next_attempt_timestamp=past),
        )
    )

    completed_any: bool = execution.complete_due_operations(now)

    assert completed_any is True

    wait_op = next(op for op in execution.operations if op.operation_id == "wait-1")
    assert wait_op.status is OperationStatus.SUCCEEDED
    assert wait_op.end_timestamp == now

    step_op = next(op for op in execution.operations if op.operation_id == "step-1")
    assert step_op.status is OperationStatus.READY
    assert step_op.step_details is not None
    assert step_op.step_details.next_attempt_timestamp is None

    # Each completion touches its operation, so the transitions land in
    # the next checkpoint response delta.
    assert "wait-1" in execution.operation_last_touched_seq
    assert "step-1" in execution.operation_last_touched_seq


def test_complete_due_operations_ignores_operations_not_yet_due() -> None:
    """Operations scheduled in the future are left untouched."""
    execution = _make_execution()
    now: datetime = datetime.now(UTC)
    future: datetime = now + timedelta(seconds=60)

    execution.operations.append(
        Operation(
            operation_id="wait-1",
            parent_id=None,
            name="test-wait",
            start_timestamp=now,
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            wait_details=WaitDetails(scheduled_end_timestamp=future),
        )
    )
    execution.operations.append(
        Operation(
            operation_id="step-1",
            parent_id=None,
            name="test-step",
            start_timestamp=now,
            operation_type=OperationType.STEP,
            status=OperationStatus.PENDING,
            step_details=StepDetails(next_attempt_timestamp=future),
        )
    )

    completed_any: bool = execution.complete_due_operations(now)

    assert completed_any is False
    assert execution.operations[0].status is OperationStatus.STARTED
    assert execution.operations[1].status is OperationStatus.PENDING
    assert execution.operation_last_touched_seq == {}


def test_complete_due_operations_ignores_non_pending_statuses() -> None:
    """Terminal and unstarted operations never match the due predicate."""
    execution = _make_execution()
    now: datetime = datetime.now(UTC)
    past: datetime = now - timedelta(seconds=5)

    execution.operations.append(
        Operation(
            operation_id="wait-1",
            parent_id=None,
            name="test-wait",
            start_timestamp=past,
            operation_type=OperationType.WAIT,
            status=OperationStatus.SUCCEEDED,
            wait_details=WaitDetails(scheduled_end_timestamp=past),
        )
    )

    completed_any: bool = execution.complete_due_operations(now)

    assert completed_any is False
    assert execution.operation_last_touched_seq == {}
