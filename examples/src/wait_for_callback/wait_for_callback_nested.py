"""Demonstrates nested waitForCallback operations across multiple child context levels."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def inner_child_context(inner_child_ctx: DurableContext) -> dict[str, Any]:
    """Inner child context with deep nested callback."""
    inner_child_ctx.wait(Duration.from_seconds(5), name="deep-wait")

    nested_callback_result: str = inner_child_ctx.wait_for_callback(
        lambda _callback_id, _context: None,
        name="nested-callback-op",
    )

    return {
        "nestedCallback": nested_callback_result,
        "deepLevel": "inner-child",
    }


@durable_with_child_context
def outer_child_context(outer_child_ctx: DurableContext) -> dict[str, Any]:
    """Outer child context with inner callback and nested context."""
    inner_result: str = outer_child_ctx.wait_for_callback(
        lambda _callback_id, _context: None,
        name="inner-callback-op",
    )

    # Nested child context with another callback
    deep_nested_result: dict[str, Any] = outer_child_ctx.run_in_child_context(
        inner_child_context(),
        name="inner-child-context",
    )

    return {
        "innerCallback": inner_result,
        "deepNested": deep_nested_result,
        "level": "outer-child",
    }


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating nested waitForCallback operations across multiple levels."""
    outer_result: str = context.wait_for_callback(
        lambda _callback_id, _context: None,
        name="outer-callback-op",
    )

    nested_result: dict[str, Any] = context.run_in_child_context(
        outer_child_context(),
        name="outer-child-context",
    )

    return {
        "outerCallback": outer_result,
        "nestedResults": nested_result,
    }
