"""Tests for map with maxConcurrency."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.map import map_with_max_concurrency
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_max_concurrency.handler,
    lambda_function_name="Map with Max Concurrency",
)
def test_map_with_max_concurrency(durable_runner):
    """Test map with maxConcurrency limit."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    results_list = deserialize_operation_payload(result.result)
    assert len(results_list) == 10
    # Items 1-10 multiplied by 3
    assert results_list == [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]

    # Get the map operation
    map_op = result.get_context("map_with_concurrency")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 10 child operations exist
    assert len(map_op.child_operations) == 10

    # Verify all children succeeded
    for child in map_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED
