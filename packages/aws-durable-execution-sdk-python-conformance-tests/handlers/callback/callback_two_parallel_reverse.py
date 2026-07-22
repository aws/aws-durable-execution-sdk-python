# 4-19: Two callbacks — create both then wait in reverse order (cbA, cbB, wcbB, wcbA)
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, str]:
    name_a: str = event[0]
    name_b: str = event[1]

    callback_a = context.create_callback(name=name_a)
    callback_b = context.create_callback(name=name_b)

    result_b = callback_b.result()
    result_a = callback_a.result()

    return {"a": result_a, "b": result_b}
