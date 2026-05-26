"""Tests for parallel with failure tolerance."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.parallel import parallel_with_failure_tolerance
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_with_failure_tolerance.handler,
    lambda_function_name="Parallel with Failure Tolerance",
)
def test_parallel_with_failure_tolerance(durable_runner):
    """Test parallel with failure tolerance."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Should have 3 successes and 2 failures
    assert result_data["success_count"] == 3
    assert result_data["failure_count"] == 2
    assert set(result_data["succeeded"]) == {"success 1", "success 3", "success 5"}
    assert result_data["completion_reason"] == "ALL_COMPLETED"

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_tolerance")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 5 child operations exist
    assert len(parallel_op.child_operations) == 5

    # Count successes and failures
    succeeded = [
        op
        for op in parallel_op.child_operations
        if op.status is OperationStatus.SUCCEEDED
    ]
    failed = [
        op for op in parallel_op.child_operations if op.status is OperationStatus.FAILED
    ]

    assert len(succeeded) == 3
    assert len(failed) == 2
