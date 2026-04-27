"""Tests for map with custom serdes."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.map import map_with_custom_serdes
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_custom_serdes.handler,
    lambda_function_name="Map with Custom SerDes",
)
def test_map_with_custom_serdes(durable_runner):
    """Test map with custom item serialization."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify all items were processed
    assert result_data["success_count"] == 3

    # Verify results were properly deserialized
    results = result_data["results"]
    assert len(results) == 3

    # Verify the custom serdes worked (data was serialized and deserialized correctly)
    processed_names = result_data["processed_names"]
    assert processed_names == ["item1", "item2", "item3"]

    # Verify processing logic worked correctly
    for i, r in enumerate(results):
        assert r["index"] == i
        assert r["doubled_id"] == (i + 1) * 2  # IDs are 1, 2, 3

    # Get the map operation
    map_op = result.get_context("map_with_custom_serdes")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist and succeeded
    assert len(map_op.child_operations) == 3
    for child in map_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED
