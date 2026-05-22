"""Tests for no_replay_execution."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.no_replay_execution import no_replay_execution
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=no_replay_execution.handler,
    lambda_function_name="No Replay Execution",
)
def test_handle_step_operations_when_no_replay_occurs(durable_runner):
    """Test step operations when no replay occurs."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    # Verify final result
    assert deserialize_operation_payload(result.result) == {"completed": True}

    # Get step operations
    user1_step_ops = [
        op
        for op in result.operations
        if op.operation_type.value == "STEP" and op.name == "fetch-user-1"
    ]
    assert len(user1_step_ops) == 1
    user1_step = user1_step_ops[0]

    user2_step_ops = [
        op
        for op in result.operations
        if op.operation_type.value == "STEP" and op.name == "fetch-user-2"
    ]
    assert len(user2_step_ops) == 1
    user2_step = user2_step_ops[0]

    # Verify first-time execution tracking (no replay)
    assert user1_step.operation_type.value == "STEP"
    assert user1_step.status.value == "SUCCEEDED"
    assert deserialize_operation_payload(user1_step.result) == "user-1"

    assert user2_step.operation_type.value == "STEP"
    assert user2_step.status.value == "SUCCEEDED"
    assert deserialize_operation_payload(user2_step.result) == "user-2"

    # Verify both operations tracked
    assert len(result.operations) == 2
