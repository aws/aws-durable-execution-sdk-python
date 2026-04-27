"""Tests for map_large_scale."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.map import map_with_large_scale
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_large_scale.handler,
    lambda_function_name="map large scale",
)
def test_handle_50_items_with_100kb_each_using_map(durable_runner):
    """Test handling 50 items with 100KB each using map."""
    pass
    with durable_runner:
        result = durable_runner.run(input=None, timeout=60)

    result_data = deserialize_operation_payload(result.result)

    # Verify the execution succeeded
    assert result.status is InvocationStatus.SUCCEEDED
    assert result_data["success"] is True

    # Verify the expected number of items were processed (50 items)
    assert result_data["summary"]["itemsProcessed"] == 50
    assert result_data["summary"]["allItemsProcessed"] is True

    # Verify data size expectations (~5MB total from 50 items × 100KB each)
    assert result_data["summary"]["totalDataSizeMB"] > 4  # Should be ~5MB
    assert result_data["summary"]["totalDataSizeMB"] < 6
    assert result_data["summary"]["totalDataSizeBytes"] > 5000000  # ~5MB
    assert result_data["summary"]["averageItemSize"] > 100000  # ~100KB per item
    assert result_data["summary"]["maxConcurrency"] == 10
