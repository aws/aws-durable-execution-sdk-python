"""Tests for steps_with_retry."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.step import steps_with_retry
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=steps_with_retry.handler,
    lambda_function_name="steps with retry",
)
def test_steps_with_retry(durable_runner):
    """Test steps_with_retry pattern.

    With counter-based deterministic behavior:
    - Poll 1, Attempt 1: counter = 1 → raises RuntimeError ❌
    - Poll 1, Attempt 2: counter = 2 → returns None
    - Poll 2, Attempt 1: counter = 3 → returns item ✓

    The function finds the item on poll 2 after 1 retry on poll 1.
    """
    with durable_runner:
        result = durable_runner.run(input={"name": "test-item"}, timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    # With counter-based deterministic behavior, finds item on poll 2
    result_data = deserialize_operation_payload(result.result)
    assert isinstance(result_data, dict)
    assert result_data.get("success") is True
    assert result_data.get("pollsRequired") == 2
    assert "item" in result_data
    assert result_data["item"]["id"] == "test-item"

    # Verify step operations exist
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    # Should have exactly 2 step operations (poll 1 and poll 2)
    assert len(step_ops) == 2

    # Poll 1: succeeded after 1 retry (returned None)
    assert step_ops[0].name == "get_item_poll_1"
    assert step_ops[0].result == "null"
    assert step_ops[0].attempt == 2  # 1 retry occurred (1-indexed: 2=first retry)

    # Poll 2: succeeded immediately (returned item)
    assert step_ops[1].name == "get_item_poll_2"
    assert step_ops[1].attempt == 1  # No retries needed (1-indexed: 1=initial)
