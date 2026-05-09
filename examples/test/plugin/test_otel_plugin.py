"""Tests for step example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.plugin import execution_with_otel
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=execution_with_otel.handler,
    lambda_function_name="Otel Plugin",
)
def test_plugin(durable_runner):
    """Test basic step example."""
    with durable_runner:
        result = durable_runner.run(input="{}", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 12

    step_result = result.get_step("add-result-to-2")
    assert deserialize_operation_payload(step_result.result) == 12
