# 4-13: Callback failure caught → Wait → return
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import CallbackError
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(name=event)

    try:
        outcome = callback.result()
    except CallbackError as e:
        outcome = f"caught_failure:{e}"
    except Exception as e:  # pragma: no cover - safety net
        outcome = f"caught_other:{type(e).__name__}:{e}"

    context.wait(Duration.from_seconds(2), name="after-cb")
    return outcome
