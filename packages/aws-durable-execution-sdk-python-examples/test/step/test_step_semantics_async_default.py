"""Tests for the step_semantics_async_default example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)

from src.step import step_semantics_async_default
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_semantics_async_default.handler,
    lambda_function_name="step semantics async default",
)
def test_step_semantics_async_default(durable_runner):
    """Every step is saved, and the replay after the wait reuses the order ID."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    output = deserialize_operation_payload(result.result)
    order_id = output["payment"]["order_id"]
    assert output == {
        "payment": {"order_id": order_id, "amount": 42, "status": "charged"},
        "receipt": f"receipt sent for {order_id}",
    }

    steps = {
        op.name: op
        for op in result.operations
        if op.operation_type is OperationType.STEP
    }
    assert set(steps) == {"create_order_id", "charge", "send_receipt"}
    assert {op.status for op in steps.values()} == {OperationStatus.SUCCEEDED}
    [wait] = [op for op in result.operations if op.operation_type is OperationType.WAIT]
    assert wait.name == "settle"
    assert wait.status is OperationStatus.SUCCEEDED
