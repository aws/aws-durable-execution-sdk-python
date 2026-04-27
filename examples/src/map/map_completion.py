"""Reproduces issue where map with minSuccessful loses failure count."""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    MapConfig,
    StepConfig,
    Duration,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating map with completion config issue."""
    # Test data: Items 2 and 4 will fail (40% failure rate)
    items = [
        {"id": 1, "shouldFail": False},
        {"id": 2, "shouldFail": True},  # Will fail
        {"id": 3, "shouldFail": False},
        {"id": 4, "shouldFail": True},  # Will fail
        {"id": 5, "shouldFail": False},
    ]

    # Fixed completion config that causes the issue
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_percentage=50,
    )

    context.logger.info(
        f"Starting map with config: min_successful=2, tolerated_failure_percentage=50"
    )
    context.logger.info(
        f"Items pattern: {', '.join(['FAIL' if i['shouldFail'] else 'SUCCESS' for i in items])}"
    )

    def process_item(
        ctx: DurableContext, item: dict[str, Any], index: int, _
    ) -> dict[str, Any]:
        """Process each item in the map."""
        context.logger.info(
            f"Processing item {item['id']} (index {index}), shouldFail: {item['shouldFail']}"
        )

        retry_config = RetryStrategyConfig(
            max_attempts=2,
            initial_delay=Duration.from_seconds(1),
            max_delay=Duration.from_seconds(1),
        )
        step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

        def step_function(_: DurableContext) -> dict[str, Any]:
            """Step that processes or fails based on item."""
            if item["shouldFail"]:
                raise Exception(f"Processing failed for item {item['id']}")
            return {
                "itemId": item["id"],
                "processed": True,
                "result": f"Item {item['id']} processed successfully",
            }

        return ctx.step(
            step_function,
            name=f"process-item-{index}",
            config=step_config,
        )

    config = MapConfig(
        max_concurrency=3,
        completion_config=completion_config,
    )

    results = context.map(
        inputs=items,
        func=process_item,
        name="completion-config-items",
        config=config,
    )

    context.logger.info("Map completed with results:")
    context.logger.info(f"Total items processed: {results.total_count}")
    context.logger.info(f"Successful items: {results.success_count}")
    context.logger.info(f"Failed items: {results.failure_count}")
    context.logger.info(f"Has failures: {results.has_failure}")
    context.logger.info(f"Batch status: {results.status}")
    context.logger.info(f"Completion reason: {results.completion_reason}")

    return {
        "totalItems": results.total_count,
        "successfulCount": results.success_count,
        "failedCount": results.failure_count,
        "hasFailures": results.has_failure,
        "batchStatus": str(results.status),
        "completionReason": str(results.completion_reason),
        "successfulItems": [
            {
                "index": item.index,
                "itemId": items[item.index]["id"],
            }
            for item in results.succeeded()
        ],
        "failedItems": [
            {
                "index": item.index,
                "itemId": items[item.index]["id"],
                "error": str(item.error),
            }
            for item in results.failed()
        ],
    }
