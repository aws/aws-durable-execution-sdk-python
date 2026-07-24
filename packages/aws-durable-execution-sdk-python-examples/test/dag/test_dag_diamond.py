"""Cloud e2e tests for the DAG diamond example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.dag import dag_diamond
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=dag_diamond.handler,
    lambda_function_name="DAG Diamond",
)
def test_dag_diamond(durable_runner):
    """Diamond DAG resolves typed deps and fans in to a merge task."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED
    summary = deserialize_operation_payload(result.result)

    assert summary["completion_reason"] == "ALL_COMPLETED"
    assert summary["counts"] == {
        "success": 4,
        "failure": 0,
        "skipped": 0,
        "total": 4,
    }
    tasks = summary["tasks"]
    assert tasks["fetch"]["result"] == 10
    assert tasks["ta"]["result"] == 11
    assert tasks["tb"]["result"] == 20
    assert tasks["merge"]["result"] == 31
    assert all(t["status"] == "SUCCEEDED" for t in tasks.values())
