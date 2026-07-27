"""3-17: Child context with durable logger only (verify no re-execution on replay)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def print_child(ctx: DurableContext, *, input_1: str) -> str:
    # Log through the child context logger so the record carries the execution
    # ARN. Rebinding the replay source enables replay logging, which is what
    # makes this assertion meaningful: under the default de-duplication an
    # incorrect second child execution would be suppressed and still count 1.
    ctx.logger.with_is_replaying(lambda: False).info(input_1)
    return input_1


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(
        print_child(input_1=str(event)), name="print-child"
    )
    context.wait(Duration.from_seconds(1))
    return result
