"""11-1: Replay rejects an operation-type mismatch."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def unexpected_step(step_context: StepContext) -> str:
    step_context.logger.info("DETERMINISM_STEP_BODY_EXECUTED")
    return "unexpected"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str | None:
    replay_logger = context.logger.with_is_replaying(lambda: False)

    if context.is_replaying():
        replay_logger.info("DETERMINISM_REPLAY_CANARY")
        return context.step(unexpected_step(), name="identity-slot")

    context.wait(Duration.from_seconds(1), name="identity-slot")
    return None
