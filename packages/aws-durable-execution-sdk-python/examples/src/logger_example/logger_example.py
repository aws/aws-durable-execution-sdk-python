"""Example demonstrating logger usage in DurableContext."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_with_child_context,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def child_workflow(ctx: DurableContext) -> str:
    """Child workflow with its own logging context."""
    # Child context logger has step_id populated with child context ID
    ctx.logger.info("Running in child context")

    # Step in child context has nested step ID
    child_result: str = ctx.step(
        lambda _: "child-processed",
        name="child_step",
    )

    ctx.logger.info("Child workflow completed", extra={"result": child_result})

    return child_result


@durable_step
def my_step(step_context: StepContext, my_arg: int) -> str:
    step_context.logger.info("Hello from my_step")
    step_context.logger.warning("Warning from my_step", extra={"my_arg": my_arg})
    step_context.logger.error(
        "Error from my_step", extra={"my_arg": my_arg, "type": "error"}
    )
    return f"from my_step: {my_arg}"


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    """Handler demonstrating logger usage."""
    # Top-level context logger: no step_id field
    context.logger.info("Starting workflow", extra={"eventId": event.get("id")})

    # Logger in steps - gets enriched with step ID and attempt number
    result1: str = context.step(
        lambda _: "processed",
        name="process_data",
    )

    context.step(my_step(123))

    context.logger.info("Step 1 completed", extra={"result": result1})

    # Child contexts inherit the parent's logger and have their own step ID
    result2: str = context.run_in_child_context(child_workflow(), name="child_workflow")

    context.logger.info(
        "Workflow completed", extra={"result1": result1, "result2": result2}
    )

    return f"{result1}-{result2}"
