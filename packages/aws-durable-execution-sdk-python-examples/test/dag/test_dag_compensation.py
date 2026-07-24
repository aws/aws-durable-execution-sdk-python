"""Cloud e2e tests for the DAG compensation example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.dag import dag_compensation
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=dag_compensation.handler,
    lambda_function_name="DAG Compensation",
)
def test_dag_compensation_charge_fails(durable_runner):
    """Failed charge triggers refund + audit, skips fulfill, drains with failures."""
    with durable_runner:
        result = durable_runner.run(input={"charge_ok": False}, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED
    summary = deserialize_operation_payload(result.result)

    assert summary["completion_reason"] == "COMPLETED_WITH_FAILURES"
    assert summary["counts"] == {
        "success": 2,
        "failure": 1,
        "skipped": 1,
        "total": 4,
    }
    tasks = summary["tasks"]
    assert tasks["charge"]["status"] == "FAILED"
    assert tasks["fulfill"]["status"] == "SKIPPED"
    assert tasks["fulfill"]["skip_reason"] == "TRIGGER_RULE"
    assert tasks["refund"]["status"] == "SUCCEEDED"
    assert tasks["refund"]["result"] == "refunded"
    assert tasks["audit"]["status"] == "SUCCEEDED"
    assert tasks["audit"]["result"] == "audited"
