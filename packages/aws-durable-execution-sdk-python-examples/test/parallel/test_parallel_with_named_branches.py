"""Tests for parallel_with_named_branches example."""

import pytest
from src.parallel import parallel_with_named_branches
from test.conftest import deserialize_operation_payload

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_with_named_branches.handler,
    lambda_function_name="parallel with named branches",
)
def test_parallel_with_named_branches(durable_runner):
    """Test parallel example with all branch patterns."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [
        "user-data-loaded",
        "orders-loaded",
        "prefs-loaded",
        "metrics-loaded",
        "config-loaded",
    ]

    # Get the parallel operation
    parallel_op = result.get_context("load_all_data")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify branch names: named branches have custom names, unnamed use defaults
    assert len(parallel_op.child_operations) == 5

    child_names = {op.name for op in parallel_op.child_operations}

    assert child_names == {
        "fetch-user-data",  # Named ParallelBranch
        "fetch-orders",  # Named decorator
        "parallel-branch-2",  # Unnamed decorator
        "parallel-branch-3",  # Unnamed ParallelBranch
        "parallel-branch-4",  # Raw callable
    }

    # Verify all children succeeded
    for child in parallel_op.child_operations:
        assert child.operation_type == OperationType.CONTEXT
        assert child.status is OperationStatus.SUCCEEDED
