"""Tests for map_completion."""

import pytest

from src.map import map_completion
from test.conftest import deserialize_operation_payload
from aws_durable_execution_sdk_python.execution import InvocationStatus


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=map_completion.handler,
    lambda_function_name="Map Completion Config",
)
def test_reproduce_completion_config_behavior_with_detailed_logging(durable_runner):
    """Demonstrates map behavior with minSuccessful and concurrent execution."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # 5 items are processed 2 of them succeeded. We exit early because min_successful is 2.
    # Additionally, failure_count shows 0 because failed items have retry strategies configured and are still retrying
    # when execution completes. Failures aren't finalized until retries complete, so they don't appear in the failure_count.
    assert result_data["totalItems"] == 5
    assert result_data["successfulCount"] == 2
    assert result_data["failedCount"] == 0
    assert result_data["hasFailures"] is False
    assert result_data["batchStatus"] == "BatchItemStatus.SUCCEEDED"
    assert result_data["completionReason"] == "CompletionReason.MIN_SUCCESSFUL_REACHED"
