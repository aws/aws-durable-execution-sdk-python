"""Tests for the combined custom-serdes round-trip example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.serdes_roundtrip import serdes_roundtrip
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=serdes_roundtrip.handler,
    lambda_function_name="Custom SerDes Round-Trip",
)
def test_serdes_roundtrip_all_operations(durable_runner):
    """Every operation returns the canonical, round-tripped value on first run.

    With a non-identity serdes, each operation (step, wait_for_condition, and
    every run_in_child_context variant - normal, virtual, large-payload) must
    return ``deserialize(serialize(result))`` on the first run, so the returned
    value matches what a replay reconstructs. The handler reports a boolean per
    operation that is True only when the marker added by ``deserialize`` is
    present.
    """
    with durable_runner:
        result = durable_runner.run(input="test", timeout=15)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data == {
        "step_round_tripped": True,
        "child_round_tripped": True,
        "virtual_child_round_tripped": True,
        "large_child_round_tripped": True,
        "wait_for_condition_round_tripped": True,
    }
