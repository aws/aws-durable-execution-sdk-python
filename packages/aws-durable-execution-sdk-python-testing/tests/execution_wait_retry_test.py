"""Additional concurrent tests for wait and retry operations."""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
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

    # Verify token sequence was incremented twice
    assert execution.token_sequence == 2
