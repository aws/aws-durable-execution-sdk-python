"""Tests for run_in_child_context_large_data."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.run_in_child_context import run_in_child_context_large_data
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=run_in_child_context_large_data.handler,
    lambda_function_name="run in child context large data",
)
def test_handle_large_data_exceeding_256k_limit_using_run_in_child_context(
    durable_runner,
):
    """Test handling large data exceeding 256k limit using runInChildContext."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=30)

    result_data = deserialize_operation_payload(result.result)

    # Verify the execution succeeded
    assert result.status is InvocationStatus.SUCCEEDED
    assert result_data["success"] is True

    # Verify large data was processed
    assert result_data["summary"]["totalDataSize"] > 240  # Should be ~250KB
    assert result_data["summary"]["stepsExecuted"] == 5
    assert result_data["summary"]["childContextUsed"] is True
    assert result_data["summary"]["waitExecuted"] is True
    assert result_data["summary"]["dataPreservedAcrossWait"] is True

    # Verify data integrity across wait
    assert result_data["dataIntegrityCheck"] is True
