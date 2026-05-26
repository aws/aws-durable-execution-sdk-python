"""Test map with 50 iterations, each returning 100KB data."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


def generate_large_string(size_in_kb: int) -> str:
    """Generate a string of approximately the specified size in KB."""
    return "A" * 1024 * size_in_kb


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating large scale map with substantial data."""
    # Create array of 50 items (more manageable for testing)
    items = list(range(1, 51))  # 1 to 50

    config = MapConfig(max_concurrency=10)  # Process 10 items concurrently
    data = generate_large_string(100)
    results = context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: {
                "itemId": item,
                "index": index,
                "dataSize": len(data),
                "data": data,
                "processed": True,
            }
        ),
        name="large-scale-map",
        config=config,
    )

    context.wait(Duration.from_seconds(1), name="wait1")

    # Process results immediately after map operation
    # Note: After wait operations, the BatchResult may be summarized
    final_results = results.get_results()
    total_data_size = sum(result["dataSize"] for result in final_results)
    all_items_processed = all(result["processed"] for result in final_results)

    total_size_in_mb = round(total_data_size / (1024 * 1024))

    summary = {
        "itemsProcessed": results.success_count,
        "totalDataSizeMB": total_size_in_mb,
        "totalDataSizeBytes": total_data_size,
        "maxConcurrency": 10,
        "averageItemSize": round(total_data_size / results.success_count),
        "allItemsProcessed": all_items_processed,
    }

    context.wait(Duration.from_seconds(1), name="wait2")

    return {
        "success": True,
        "message": "Successfully processed 50 items with substantial data using map",
        "summary": summary,
    }
