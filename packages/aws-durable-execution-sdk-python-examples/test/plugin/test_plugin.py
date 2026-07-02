"""Tests for step example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)

from src.plugin import execution_with_plugin, execution_with_wait_plugin
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=execution_with_plugin.handler,
    lambda_function_name="Plugin",
)
def test_plugin(durable_runner):
    """Test basic step example."""
    with durable_runner:
        result = durable_runner.run(input="{}", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 12

    step_result = result.get_step("add-result-to-2")
    assert deserialize_operation_payload(step_result.result) == 12


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=execution_with_wait_plugin.handler,
    lambda_function_name="Plugin Wait",
)
def test_plugin_on_operation_end_called_for_wait_completed_during_suspend(
    durable_runner, monkeypatch
):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")

    with durable_runner:
        result = durable_runner.run(input=None, timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    result_data = deserialize_operation_payload(result.result)
    assert result_data["message"] == "Plugin wait completed"

    wait_op = result.get_wait("plugin-wait")
    assert wait_op.status is OperationStatus.SUCCEEDED

    wait_end_infos = result_data["wait_end_infos"]

    assert len(wait_end_infos) == 1
    assert wait_end_infos[0]["operation_type"] == OperationType.WAIT.value
    assert wait_end_infos[0]["status"] == OperationStatus.SUCCEEDED.value
    assert wait_end_infos[0]["is_replayed"] is False
    assert wait_end_infos[0]["has_end_time"] is True
