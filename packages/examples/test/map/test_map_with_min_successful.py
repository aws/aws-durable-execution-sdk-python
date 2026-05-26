"""Tests for map with min_successful."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.map import map_with_min_successful
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_min_successful.handler,
    lambda_function_name="Map with Min Successful",
)
def test_map_with_min_successful(durable_runner):
    """Test map with min_successful threshold."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # With min_successful=6, operation completes after reaching 6 successes
    # Due to concurrency (max_concurrency=5), some items may complete before check
    # Items 1-6 succeed, item 10 succeeds, items 7-9 fail
    # Depending on timing, we get 6 or 7 successes
    assert result_data["success_count"] >= 6
    assert result_data["success_count"] <= 7

    # Operation stops once min_successful is reached
    # Items 7-9 (which would fail) are never processed
    assert result_data["failure_count"] == 0
    assert result_data["total_count"] == 10

    # Verify we got the expected successful results
    # Items 1-6 always succeed (2, 4, 6, 8, 10, 12)
    # Item 10 might also succeed (20) depending on timing
    assert len(result_data["results"]) == result_data["success_count"]
    for result_val in result_data["results"]:
        assert result_val % 2 == 0  # All results should be even (item * 2)
        assert result_val >= 2 and result_val <= 20  # Range: items 1-10 * 2
        assert result_val not in [14, 16, 18]  # Items 7-9 should not be present

    # Completion reason should be MIN_SUCCESSFUL_REACHED
    assert result_data["completion_reason"] == "MIN_SUCCESSFUL_REACHED"

    # Get the map operation
    map_op = result.get_context("map_min_successful")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # All 10 operations may be started, but only some complete before min_successful
    assert len(map_op.child_operations) == 10

    # Count operations by status
    succeeded = [
        op for op in map_op.child_operations if op.status is OperationStatus.SUCCEEDED
    ]
    failed = [
        op for op in map_op.child_operations if op.status is OperationStatus.FAILED
    ]
    started = [
        op for op in map_op.child_operations if op.status is OperationStatus.STARTED
    ]

    # Should have 6-7 successes, 0 failures, and remaining in STARTED state
    assert len(succeeded) == result_data["success_count"]
    assert len(failed) == 0
    assert len(started) == 10 - result_data["success_count"]
