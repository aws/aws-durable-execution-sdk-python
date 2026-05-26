"""Tests for undefined_results."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.none_results import none_results
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=none_results.handler,
    lambda_function_name="None Results",
)
def test_handle_step_operations_with_undefined_result_after_replay(durable_runner):
    """Test handling of step operations with undefined result after replay."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    # Verify execution completed successfully despite undefined operation results
    assert deserialize_operation_payload(result.result) == "result"

    # Verify all operations were tracked even with undefined results
    operations = result.operations
    assert len(operations) == 3  # step + context + wait

    # Verify step operation with undefined result
    step_ops = [
        op
        for op in operations
        if op.operation_type.value == "STEP" and op.name == "fetch-user"
    ]
    assert len(step_ops) == 1
    step_op = step_ops[0]
    assert deserialize_operation_payload(step_op.result) is None

    # Verify child context operation with undefined result
    context_ops = [
        op
        for op in operations
        if op.operation_type.value == "CONTEXT" and op.name == "parent"
    ]
    assert len(context_ops) == 1
    context_op = context_ops[0]
    assert deserialize_operation_payload(context_op.result) is None

    # Verify wait operation completed normally
    wait_op = operations[2]
    assert wait_op.operation_type.value == "WAIT"
