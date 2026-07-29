"""10-18: Plugin hook work is flushed before the invocation returns.

A single step succeeds. The plugin performs ~1 second of deliberately slow work
inside its ``on_operation_end`` hook and only then logs its record. Python
dispatches operation-end synchronously as part of the checkpoint-response merge
and the plugin executor is drained (ThreadPoolExecutor.shutdown(wait=True)) in
the ``PluginExecutor.run`` context manager before the durable handler wrapper
returns the invocation response, so the ~1s hook work MUST complete and its
record MUST be emitted before the Lambda response is returned. This is the real
SDK completion contract, not a mock — the sleep genuinely blocks the hook.
"""

import json
import time
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class SlowHookPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation hooks do not carry the execution ARN, so capture it from the
        # invocation-start hook and reuse it for later operation emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        # Deliberately slow hook work; the SDK must not return the invocation
        # response until this completes (otherwise the environment freeze would
        # drop the record).
        time.sleep(1)
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "slow-operation-end",
                "op": info.operation_id,
                "status": info.status.name,
            },
            self._execution_arn,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "invocation-end",
                "status": status,
            },
            info.execution_arn,
        )


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[SlowHookPlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
