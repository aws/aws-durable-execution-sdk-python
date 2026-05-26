"""Tests for wait example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait import wait
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait.handler,
    lambda_function_name="Wait State",
)
def test_wait(durable_runner):
    """Test wait example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Wait completed"

    # Find the wait operation (it should be the only non-execution operation)
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    wait_op = wait_ops[0]
    assert wait_op.scheduled_end_timestamp is not None
