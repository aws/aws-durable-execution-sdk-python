"""Tests for map_with_item_namer example."""

import pytest
from src.map import map_with_item_namer
from test.conftest import deserialize_operation_payload

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
)


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_item_namer.handler,
    lambda_function_name="map with item namer",
)
def test_map_with_item_namer(durable_runner):
    """Test map example with custom item_namer for iteration naming."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [
        "processed-order-101-$25",
        "processed-order-102-$50",
        "processed-order-103-$75",
    ]

    # Get the map operation
    map_op = result.get_context("process_orders")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify custom iteration names from item_namer
    assert len(map_op.child_operations) == 3
    child_names = {op.name for op in map_op.child_operations}
    expected_names = {"order-order-101", "order-order-102", "order-order-103"}
    assert child_names == expected_names
