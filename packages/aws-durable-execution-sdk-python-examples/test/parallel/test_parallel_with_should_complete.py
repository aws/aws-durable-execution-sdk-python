"""Tests for parallel with should_complete quorum predicate."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from src.parallel import parallel_with_should_complete
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_with_should_complete.handler,
    lambda_function_name="Parallel with Should Complete",
)
def test_parallel_with_should_complete(durable_runner):
    """Test parallel with quorum predicate: branch A OR (B AND C)."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Quorum met: either branch A succeeded (1 success), or B and C both
    # succeeded (2 successes). With concurrency a raced sibling may also
    # complete before the batch stops.
    assert result_data["success_count"] >= 1
    assert result_data["completion_reason"] == "CUSTOM_COMPLETION_SUCCEEDED"
    assert len(result_data["results"]) >= 1

    # Get the parallel operation
    parallel_op = result.get_context("quorum-branches")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED
