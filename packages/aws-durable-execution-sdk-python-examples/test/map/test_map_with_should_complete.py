"""Tests for map with should_complete predicate."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.map import map_with_should_complete
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_with_should_complete.handler,
    lambda_function_name="Map with Should Complete",
)
def test_map_with_should_complete(durable_runner):
    """Test map with custom should_complete predicate that stops at 3 successes."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Predicate completes after 3 successes; with max_concurrency=2,
    # a concurrent sibling may finish before the check fires, so up to
    # max_concurrency extra items can complete.
    assert result_data["success_count"] >= 3
    assert result_data["success_count"] <= 4  # at most 1 extra from concurrency
    assert result_data["failure_count"] == 0
    assert result_data["completion_reason"] == "CUSTOM_COMPLETION_SUCCEEDED"

    # Results are item * 10 for the items processed
    assert len(result_data["results"]) == result_data["success_count"]

    # Get the map operation
    map_op = result.get_context("map_should_complete")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED
