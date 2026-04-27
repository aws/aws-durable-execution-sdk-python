"""Tests for parallel example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus

from src.parallel import parallel
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel.handler,
    lambda_function_name="Parallel Operations",
)
def test_parallel(durable_runner):
    """Test parallel example using context.parallel()."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [
        "task 1 completed",
        "task 2 completed",
        "task 3 completed after wait",
    ]

    # Get the parallel operation (CONTEXT type with PARALLEL subtype)
    parallel_op = result.get_context("parallel_operation")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all three child operations exist
    assert len(parallel_op.child_operations) == 3

    # Verify all children succeeded
    for child in parallel_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED
