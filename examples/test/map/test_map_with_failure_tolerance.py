"""Tests for map with failure tolerance."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.map import map_with_failure_tolerance
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_failure_tolerance.handler,
    lambda_function_name="Map with Failure Tolerance",
)
def test_map_with_failure_tolerance(durable_runner):
    """Test map with failure tolerance."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Should have 7 successes and 3 failures (items 3, 6, 9 fail)
    assert result_data["success_count"] == 7
    assert result_data["failure_count"] == 3
    assert result_data["failed_count"] == 3

    # Verify successful results (items 1,2,4,5,7,8,10 multiplied by 2)
    expected_results = [2, 4, 8, 10, 14, 16, 20]
    assert set(result_data["succeeded"]) == set(expected_results)

    assert result_data["completion_reason"] == "ALL_COMPLETED"

    # Get the map operation
    map_op = result.get_context("map_with_tolerance")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 10 child operations exist
    assert len(map_op.child_operations) == 10

    # Count successes and failures
    succeeded = [
        op for op in map_op.child_operations if op.status is OperationStatus.SUCCEEDED
    ]
    failed = [
        op for op in map_op.child_operations if op.status is OperationStatus.FAILED
    ]

    assert len(succeeded) == 7
    assert len(failed) == 3
