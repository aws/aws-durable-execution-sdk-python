"""Test runInChildContext with large data exceeding individual step limits."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


def generate_large_string(size_in_kb: int) -> str:
    """Generate a string of approximately the specified size in KB."""
    return "A" * 1024 * size_in_kb


@durable_with_child_context
def large_data_processor(child_context: DurableContext) -> dict[str, Any]:
    """Process large data in child context."""
    # Generate data using a loop - each step returns ~50KB of data (under the step limit)
    step_results: list[str] = []
    step_sizes: list[int] = []

    for i in range(1, 6):  # 1 to 5
        step_result: str = child_context.step(
            lambda _: generate_large_string(50),  # 50KB
            name=f"generate-data-{i}",
        )

        step_results.append(step_result)
        step_sizes.append(len(step_result))

    # Concatenate all results - total should be ~250KB
    concatenated_result = "".join(step_results)

    return {
        "totalSize": len(concatenated_result),
        "sizeInKB": round(len(concatenated_result) / 1024),
        "data": concatenated_result,
        "stepSizes": step_sizes,
    }


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating runInChildContext with large data."""
    # Use runInChildContext to handle large data that would exceed 256k step limit
    large_data_result: dict[str, Any] = context.run_in_child_context(
        large_data_processor(), name="large-data-processor"
    )

    # Add a wait after runInChildContext to test persistence across invocations
    context.wait(Duration.from_seconds(1), name="post-processing-wait")

    # Verify the data is still intact after the wait
    data_integrity_check = (
        len(large_data_result["data"]) == large_data_result["totalSize"]
        and len(large_data_result["data"]) > 0
    )

    return {
        "success": True,
        "message": "Successfully processed large data exceeding individual step limits using runInChildContext",
        "dataIntegrityCheck": data_integrity_check,
        "summary": {
            "totalDataSize": large_data_result["sizeInKB"],
            "stepsExecuted": 5,
            "childContextUsed": True,
            "waitExecuted": True,
            "dataPreservedAcrossWait": data_integrity_check,
        },
    }
