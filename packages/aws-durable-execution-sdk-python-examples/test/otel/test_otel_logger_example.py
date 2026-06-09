"""Tests for the OTel-enriched logger example."""

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.otel import otel_logger_example
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=otel_logger_example.handler,
    lambda_function_name="Otel Logger Example",
)
def test_otel_logger_example(durable_runner):
    """Verify the OTel logger example runs and produces the expected result."""
    with durable_runner:
        result = durable_runner.run(input="{}", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "hello world | hello nested"

    # The top-level step is named "top-greet".
    top_step = result.get_step("top-greet")
    assert deserialize_operation_payload(top_step.result) == "hello world"

    # The child context wraps a nested step, so a CONTEXT operation exists.
    context_ops = [
        op for op in result.operations if op.operation_type is OperationType.CONTEXT
    ]
    assert len(context_ops) >= 1
