"""Make every step AT_LEAST_ONCE_ASYNC, except the one that makes a key.

Experimental: StepSemantics.AT_LEAST_ONCE_ASYNC may change or be removed in a
future release.

An AT_LEAST_ONCE_ASYNC step returns its result before the result is saved.
Each invocation saves the result before it returns. If an invocation ends
first, the step runs again on replay and can return a different value.

The order ID becomes the idempotency key of the charge. A new ID on replay
would charge twice. So that step sets AT_LEAST_ONCE itself, and waits for its
result to be saved. The wait ends the first invocation, and the second
invocation replays the saved steps.
"""

import uuid
from typing import Any

from aws_durable_execution_sdk_python.config import (
    Duration,
    StepConfig,
    StepSemantics,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def create_order_id(_step_context: StepContext) -> str:
    return str(uuid.uuid4())


@durable_step
def charge(_step_context: StepContext, order_id: str, amount: int) -> dict:
    # Call the payment service here, with order_id as the idempotency key.
    return {"order_id": order_id, "amount": amount, "status": "charged"}


@durable_step
def send_receipt(_step_context: StepContext, order_id: str) -> str:
    return f"receipt sent for {order_id}"


@durable_execution(default_step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC)
def handler(_event: Any, context: DurableContext) -> dict:
    order_id = context.step(
        create_order_id(),
        config=StepConfig(step_semantics=StepSemantics.AT_LEAST_ONCE),
    )
    payment = context.step(charge(order_id, 42))
    context.wait(Duration.from_seconds(1), name="settle")
    receipt = context.step(send_receipt(order_id))
    return {"payment": payment, "receipt": receipt}
