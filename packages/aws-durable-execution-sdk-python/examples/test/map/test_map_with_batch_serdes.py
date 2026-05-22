"""Tests for map with batch-level serdes."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.map import map_with_batch_serdes
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_batch_serdes.handler,
    lambda_function_name="Map with Batch SerDes",
)
def test_map_with_batch_serdes(durable_runner):
    """Test map with custom batch-level serialization."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify all items were processed
    assert result_data["success_count"] == 4

    # Verify results
    results = result_data["results"]
    assert len(results) == 4
    assert results == [20, 40, 60, 80]  # [10*2, 20*2, 30*2, 40*2]

    # Verify sum
    assert result_data["sum"] == 200

    # Get the map operation
    map_op = result.get_context("map_with_batch_serdes")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 4 child operations exist and succeeded
    assert len(map_op.child_operations) == 4
    for child in map_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED
