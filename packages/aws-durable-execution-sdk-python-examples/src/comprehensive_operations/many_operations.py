"""Example with many durable step operations."""

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


TOP_LEVEL_STEP_COUNT = 12
CHILD_CONTEXT_COUNT = 4
CHILD_STEPS_PER_CONTEXT = 2


def make_step(value: int):
    return lambda _: value


@durable_with_child_context
def child_group(
    child_context: DurableContext, group_index: int, steps_per_context: int
) -> list[int]:
    values = []
    for step_index in range(steps_per_context):
        value = group_index * 100 + step_index
        values.append(
            child_context.step(
                make_step(value),
                name=f"child-{group_index:02d}-step-{step_index:02d}",
            )
        )
    return values


@durable_execution
def handler(_event, context: DurableContext) -> dict[str, object]:
    """Run many named step operations across parent and child contexts."""

    step_results = []
    for index in range(TOP_LEVEL_STEP_COUNT):
        step_results.append(
            context.step(
                make_step(index),
                name=f"many-step-{index:03d}",
            )
        )

    child_results = []
    for group_index in range(CHILD_CONTEXT_COUNT):
        child_results.append(
            context.run_in_child_context(
                child_group(group_index, CHILD_STEPS_PER_CONTEXT),
                name=f"child-context-{group_index:02d}",
            )
        )

    child_values = [value for child_result in child_results for value in child_result]

    return {
        "topLevelStepCount": TOP_LEVEL_STEP_COUNT,
        "childContextCount": CHILD_CONTEXT_COUNT,
        "childStepCount": CHILD_CONTEXT_COUNT * CHILD_STEPS_PER_CONTEXT,
        "totalStepCount": TOP_LEVEL_STEP_COUNT
        + CHILD_CONTEXT_COUNT * CHILD_STEPS_PER_CONTEXT,
        "firstStep": step_results[0] if step_results else None,
        "lastStep": step_results[-1] if step_results else None,
        "topLevelStepTotal": sum(step_results),
        "childStepTotal": sum(child_values),
    }
