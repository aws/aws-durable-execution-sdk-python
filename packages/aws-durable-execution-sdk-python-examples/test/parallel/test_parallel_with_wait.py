"""Tests for parallel with wait operations."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)
from src.parallel import parallel_with_wait
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_with_wait.handler,
    lambda_function_name="Parallel with Wait",
)
def test_parallel_with_wait(durable_runner):
    """Test parallel with wait operations."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Completed waits"

    # Get the parallel operation
    parallel_op = result.get_context("parallel_waits")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist
    assert len(parallel_op.child_operations) == 3

    # Each child should have a wait operation
    wait_names = set()
    for child in parallel_op.child_operations:
        # Find wait operations in child
        wait_ops = [
            op
            for op in child.child_operations
            if op.operation_type == OperationType.WAIT
        ]
        assert len(wait_ops) == 1
        wait_names.add(wait_ops[0].name)

    # Verify all expected wait operations exist
    assert wait_names == {"wait_1_second", "wait_2_seconds", "wait_5_seconds"}
