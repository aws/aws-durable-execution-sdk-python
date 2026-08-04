"""10-14: Plugin operation-end payload semantics.

Two sequential steps: step A returns the constant "task-a" and succeeds; step B
always throws "boom" with no retries so the execution fails. The plugin emits
from the SDK's real ``on_operation_end`` hook, filtering to step-type
operations, reporting the operation's checkpointed serialized result (the
literal NONE when absent) and the error message from the info's error object
(the literal NONE when absent).
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
)
from aws_durable_execution_sdk_python.retries import RetryPresets


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class TerminalPayloadPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation hooks do not carry the execution ARN, so capture it from the
        # invocation-start hook and reuse it for later operation emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        result = info.result if info.result is not None else "NONE"
        error = info.error.message if (info.error and info.error.message) else "NONE"
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-end",
                "op": info.operation_id,
                "status": info.status.name,
                "result": result,
                "error": error,
            },
            self._execution_arn,
        )


@durable_step
def step_a(_step_context: StepContext) -> str:
    return "task-a"


@durable_step
def step_b(_step_context: StepContext) -> str:
    msg = "boom"
    raise RuntimeError(msg)


@durable_execution(plugins=[TerminalPayloadPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.step(step_a())
    result: str = context.step(
        step_b(),
        config=StepConfig(retry_strategy=RetryPresets.none()),
    )
    return result
