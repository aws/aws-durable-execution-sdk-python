"""Tests for multiple_waits."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait import multiple_wait
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=multiple_wait.handler,
    lambda_function_name="multiple wait",
)
def test_multiple_sequential_wait_operations(durable_runner):
    """Test multiple sequential wait operations."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=20)

    assert result.status is InvocationStatus.SUCCEEDED

    # Verify the final result
    assert deserialize_operation_payload(result.result) == {
        "completedWaits": 2,
        "finalStep": "done",
    }

    # Verify operations were tracked
    operations = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(operations) == 2

    # Find the wait operations by name
    wait_1_ops = [
        op
        for op in operations
        if op.operation_type.value == "WAIT" and op.name == "wait-1"
    ]
    assert len(wait_1_ops) == 1
    first_wait = wait_1_ops[0]

    wait_2_ops = [
        op
        for op in operations
        if op.operation_type.value == "WAIT" and op.name == "wait-2"
    ]
    assert len(wait_2_ops) == 1
    second_wait = wait_2_ops[0]

    # Verify operation types and status
    assert first_wait.operation_type.value == "WAIT"
    assert first_wait.status.value == "SUCCEEDED"
    assert second_wait.operation_type.value == "WAIT"
    assert second_wait.status.value == "SUCCEEDED"

    # Verify wait details
    assert first_wait.scheduled_end_timestamp is not None
    assert second_wait.scheduled_end_timestamp is not None
