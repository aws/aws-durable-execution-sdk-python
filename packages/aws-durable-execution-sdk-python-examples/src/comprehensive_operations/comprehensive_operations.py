"""Complex multi-operation example demonstrating all major operations."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
    """Comprehensive example demonstrating all major durable operations."""
    print(f"Starting comprehensive operations example with event: {event}")

    # Step 1: ctx.step - Simple step that returns a result
    step1_result: str = context.step(
        lambda _: "Step 1 completed successfully",
        name="step1",
    )

    # Step 2: ctx.wait - Wait for 1 second
    context.wait(Duration.from_seconds(1))

    # Step 3: ctx.map - Map with 5 iterations returning numbers 1 to 5
    map_input = [1, 2, 3, 4, 5]

    map_results = context.map(
        inputs=map_input,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: item, name=f"map-step-{index}"
        ),
        name="map-numbers",
    ).to_dict()

    # Step 4: ctx.parallel - 3 branches, each returning a fruit name

    parallel_results = context.parallel(
        functions=[
            lambda ctx: ctx.step(lambda _: "apple", name="fruit-step-1"),
            lambda ctx: ctx.step(lambda _: "banana", name="fruit-step-2"),
            lambda ctx: ctx.step(lambda _: "orange", name="fruit-step-3"),
        ]
    ).to_dict()

    # Final result combining all operations
    return {
        "step1": step1_result,
        "waitCompleted": True,
        "mapResults": map_results,
        "parallelResults": parallel_results,
    }
