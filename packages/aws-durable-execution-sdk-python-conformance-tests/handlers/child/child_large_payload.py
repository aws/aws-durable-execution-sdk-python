"""3-11: Child context large payload (ReplayChildren mode)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def generate_large_string(size_in_kb: int) -> str:
    """Generate a string of approximately the specified size in KB."""
    return "A" * 1024 * size_in_kb


@durable_step
def generate_data(_step_context: StepContext) -> str:
    return generate_large_string(50)


@durable_with_child_context
def large_data_processor(ctx: DurableContext, *, input_1: str) -> str:
    print(input_1, flush=True)
    step_result: str = ctx.step(generate_data())
    # Build a large result (>256KB) from the small step result
    large_result = step_result * 6  # ~300KB
    return large_result


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    result: str = context.run_in_child_context(
        large_data_processor(input_1=str(event)), name="large-data-processor"
    )
    context.wait(Duration.from_seconds(2))
    return {
        "success": True,
        "dataSize": len(result),
    }
