"""Tests for logger_example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType

from src.logger_example import logger_example
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=logger_example.handler,
    lambda_function_name="logger example",
)
def test_logger_example(durable_runner):
    """Test logger example."""
    with durable_runner:
        result = durable_runner.run(input={"id": "test-123"}, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "processed-child-processed"

    # Verify step operations exist (process_data at top level)
    # Note: child_step is nested inside the CONTEXT operation, not at top level
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) >= 1

    # Verify context operation exists (child_workflow)
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
