"""Example demonstrating replay-aware logging across a wait boundary."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def prepare(step_context: StepContext, item: str) -> str:
    """A step that runs before the wait.

    Its log is emitted on the first invocation. On replay this step is not
    re-executed (it returns its checkpointed result), so this log does not
    repeat.
    """
    step_context.logger.info("Preparing item", extra={"item": item})
    return f"prepared:{item}"


@durable_step
def finalize(step_context: StepContext, prepared: str) -> str:
    """A step that runs after the wait (new work on the replay invocation)."""
    step_context.logger.info("Finalizing item", extra={"prepared": prepared})
    return f"done:{prepared}"


@durable_with_child_context
def audit(child_ctx: DurableContext, prepared: str) -> str:
    """Child context with its own logger and its own replay status."""
    child_ctx.logger.info(
        "Auditing in child context (before child wait)",
        extra={"prepared": prepared, "child_is_replaying": child_ctx.is_replaying()},
    )

    # The child's own replay boundary.
    child_ctx.wait(duration=Duration.from_seconds(5), name="audit_cooldown")

    # After the child's wait: emitted as new work on the child's replay.
    child_ctx.logger.info(
        "Resumed in child context (after child wait)",
        extra={"child_is_replaying": child_ctx.is_replaying()},
    )

    return child_ctx.step(lambda _: f"audited:{prepared}", name="record_audit")


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating replay-aware logging across a wait."""
    item: str = event.get("item", "widget") if isinstance(event, dict) else "widget"

    # --- Before the wait ---
    # On the replay invocation these lines are de-duplicated by the replay-aware
    # logger because the context is still replaying when it reaches them.
    context.logger.info(
        "Workflow started (before wait)",
        extra={"item": item, "is_replaying": context.is_replaying()},
    )

    prepared: str = context.step(prepare(item), name="prepare")

    context.logger.info(
        "Prepared, about to wait",
        extra={"prepared": prepared, "is_replaying": context.is_replaying()},
    )

    # --- The replay boundary ---
    # The wait suspends the execution. When it resumes, the handler replays from
    # the top; everything above is de-duplicated, and everything below is new.
    context.wait(duration=Duration.from_seconds(5), name="cooldown")

    # --- After the wait ---
    # These logs are emitted on the replay invocation because the context has
    # crossed its replay boundary and is no longer replaying.
    context.logger.info(
        "Resumed after wait",
        extra={"is_replaying": context.is_replaying()},
    )

    audited: str = context.run_in_child_context(audit(prepared), name="audit")

    result: str = context.step(finalize(audited), name="finalize")

    context.logger.info("Workflow completed", extra={"result": result})

    return {"result": result, "item": item}
