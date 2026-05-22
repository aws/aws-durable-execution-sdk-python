"""Tests for map_operations example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)

from src.map import map_operations
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_operations.handler,
    lambda_function_name="map operations",
)
def test_map_operations(durable_runner):
    """Test map_operations example using context.map()."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [2, 4, 6, 8, 10]

    # Get the map operation (CONTEXT type with MAP subtype)
    map_op = result.get_context("map_operation")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all five child operations exist
    assert len(map_op.child_operations) == 5

    # Verify child operation names (SDK uses map-item-* format)
    child_names = {op.name for op in map_op.child_operations}
    expected_names = {f"map-item-{i}" for i in range(5)}
    assert child_names == expected_names

    # Verify all children succeeded
    for child in map_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED
