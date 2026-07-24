"""Cloud e2e tests for the DAG run_if branching example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.dag import dag_run_if
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=dag_run_if.handler,
    lambda_function_name="DAG Run If",
)
def test_dag_run_if_default_review(durable_runner):
    """Only the branch whose run_if predicate matches runs; others are skipped."""
    with durable_runner:
        result = durable_runner.run(input={"category": "review"}, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED
    summary = deserialize_operation_payload(result.result)

    assert summary["completion_reason"] == "ALL_COMPLETED"
    assert summary["counts"] == {
        "success": 2,
        "failure": 0,
        "skipped": 2,
        "total": 4,
    }
    tasks = summary["tasks"]
    assert tasks["classify"]["result"] == "review"
    assert tasks["review"]["status"] == "SUCCEEDED"
    assert tasks["review"]["result"] == "reviewed"
    assert tasks["publish"]["status"] == "SKIPPED"
    assert tasks["publish"]["skip_reason"] == "RUN_IF_PREDICATE"
    assert tasks["block"]["status"] == "SKIPPED"
    assert tasks["block"]["skip_reason"] == "RUN_IF_PREDICATE"
