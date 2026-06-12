"""Tests for parallel first successful example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.parallel import parallel_first_successful
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_first_successful.handler,
    lambda_function_name="Parallel First Successful",
)
def test_parallel_first_successful(durable_runner):
    """Test parallel with first_successful completion strategy."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    # The handler returns a string like "First successful result: Task 1"
    assert result_data.startswith("First successful result: ")
    # The first successful result should be one of the tasks
    assert result_data in [
        "First successful result: Task 1",
        "First successful result: Task 2",
        "First successful result: Task 3",
    ]

    # Get the parallel operation
    parallel_op = result.get_context("first_successful_parallel")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify child operations exist (3 branches)
    assert len(parallel_op.child_operations) == 3

    # At least one child should have succeeded
    succeeded = [
        op
        for op in parallel_op.child_operations
        if op.status is OperationStatus.SUCCEEDED
    ]
    assert len(succeeded) >= 1
