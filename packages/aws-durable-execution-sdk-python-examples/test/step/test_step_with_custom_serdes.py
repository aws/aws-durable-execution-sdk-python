"""Tests for the step-with-custom-serdes example."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.step import step_with_custom_serdes
from src.step.step_with_custom_serdes import OrderSerDes
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=step_with_custom_serdes.handler,
    lambda_function_name="Step with Custom SerDes",
)
def test_step_with_custom_serdes(durable_runner):
    """The handler returns the canonical, round-tripped step result.

    Even on the first run the step returns the value produced by
    OrderSerDes.serialize then OrderSerDes.deserialize, so the result reflects
    the canonical form (sorted tags, transient field dropped) rather than the
    raw value the step function produced. This is the same value a replay would
    deserialize from the checkpoint.
    """
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data == {
        "order_id": "ORD-42",
        "tags": ["fragile", "gift", "priority"],  # sorted by the serdes
        "has_transient": False,  # transient field dropped on serialize
    }

    # The step checkpoint stores the canonical payload, and deserializing it
    # yields exactly what the handler returned for the order fields.
    step_result = result.get_step("build_order")
    canonical = deserialize_operation_payload(step_result.result, OrderSerDes())
    assert canonical == {
        "order_id": "ORD-42",
        "tags": ["fragile", "gift", "priority"],
    }
