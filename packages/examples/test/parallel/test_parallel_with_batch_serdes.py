"""Tests for parallel with batch-level serdes."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.parallel import parallel_with_batch_serdes
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_with_batch_serdes.handler,
    lambda_function_name="Parallel with Batch SerDes",
)
def test_parallel_with_batch_serdes(durable_runner):
    """Test parallel with custom batch-level serialization."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify all branches succeeded
    assert result_data["success_count"] == 3

    # Verify results
    results = result_data["results"]
    assert len(results) == 3
    assert results == [100, 200, 300]

    # Verify total
    assert result_data["total"] == 600

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_batch_serdes")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist and succeeded
    assert len(parallel_op.child_operations) == 3
    for child in parallel_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED
