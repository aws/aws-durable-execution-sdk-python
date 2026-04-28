"""Example demonstrating nested child contexts (blocks)."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_with_child_context
def nested_block(ctx: DurableContext) -> str:
    """Nested block with its own child context."""
    # Wait in the nested block
    ctx.wait(Duration.from_seconds(1))
    return "nested block result"


@durable_with_child_context
def parent_block(ctx: DurableContext) -> dict[str, str]:
    """Parent block with nested operations."""
    # Nested step
    nested_result: str = ctx.step(
        lambda _: "nested step result",
        name="nested_step",
    )

    # Nested block with its own child context
    nested_block_result: str = ctx.run_in_child_context(nested_block())

    return {
        "nestedStep": nested_result,
        "nestedBlock": nested_block_result,
    }


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, str]:
    """Handler demonstrating nested child contexts."""
    # Run parent block which contains nested operations
    result: dict[str, str] = context.run_in_child_context(
        parent_block(), name="parent_block"
    )

    return result
