"""Cloud e2e tests for the DAG wait/suspend-resume example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.dag import dag_wait_resume
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=dag_wait_resume.handler,
    lambda_function_name="DAG Wait Resume",
)
def test_dag_wait_resume(durable_runner):
    """DAG scheduling survives a real suspend/resume across a wait task."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=90)

    assert result.status is InvocationStatus.SUCCEEDED
    summary = deserialize_operation_payload(result.result)

    assert summary["completion_reason"] == "ALL_COMPLETED"
    assert summary["counts"] == {
        "success": 3,
        "failure": 0,
        "skipped": 0,
        "total": 3,
    }
    tasks = summary["tasks"]
    assert tasks["start"]["status"] == "SUCCEEDED"
    assert tasks["start"]["result"] == "started"
    assert tasks["pause"]["status"] == "SUCCEEDED"
    assert tasks["finish"]["status"] == "SUCCEEDED"
    assert tasks["finish"]["result"] == "resumed"
