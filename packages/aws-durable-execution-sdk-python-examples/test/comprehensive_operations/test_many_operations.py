"""Tests for many_operations."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.comprehensive_operations import many_operations
from test.conftest import deserialize_operation_payload


TOP_LEVEL_STEP_COUNT = 12
CHILD_CONTEXT_COUNT = 4
CHILD_STEPS_PER_CONTEXT = 2
TOTAL_STEP_COUNT = TOP_LEVEL_STEP_COUNT + CHILD_CONTEXT_COUNT * CHILD_STEPS_PER_CONTEXT


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=many_operations.handler,
    lambda_function_name="Many Operations",
)
def test_fetches_many_steps_and_child_context_operations(durable_runner):
    """Verify result history includes many steps and child context operations."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)
    assert result_data == {
        "topLevelStepCount": TOP_LEVEL_STEP_COUNT,
        "childContextCount": CHILD_CONTEXT_COUNT,
        "childStepCount": CHILD_CONTEXT_COUNT * CHILD_STEPS_PER_CONTEXT,
        "totalStepCount": TOTAL_STEP_COUNT,
        "firstStep": 0,
        "lastStep": TOP_LEVEL_STEP_COUNT - 1,
        "topLevelStepTotal": sum(range(TOP_LEVEL_STEP_COUNT)),
        "childStepTotal": sum(
            group_index * 100 + step_index
            for group_index in range(CHILD_CONTEXT_COUNT)
            for step_index in range(CHILD_STEPS_PER_CONTEXT)
        ),
    }

    all_ops = result.get_all_operations()
    top_level_step_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "STEP"
        and op.name is not None
        and op.name.startswith("many-step-")
    ]
    assert len(top_level_step_ops) == TOP_LEVEL_STEP_COUNT

    child_context_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "CONTEXT"
        and op.name is not None
        and op.name.startswith("child-context-")
    ]
    assert len(child_context_ops) == CHILD_CONTEXT_COUNT

    child_step_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "STEP"
        and op.name is not None
        and op.name.startswith("child-")
    ]
    assert len(child_step_ops) == CHILD_CONTEXT_COUNT * CHILD_STEPS_PER_CONTEXT
