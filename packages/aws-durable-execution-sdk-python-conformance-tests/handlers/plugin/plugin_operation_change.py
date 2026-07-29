"""10-8: Plugin operation-change hook reports updated operations + full map.

The plugin implements the SDK's real ``on_operation_change`` hook. When the
step's terminal checkpoint response is merged the SDK fires this hook with the
step in the updated-operations delta and in the full operation map; the plugin
emits one record per step-type updated operation. The execution ARN is captured
from the invocation-start hook and stamped on every record.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationChangeInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class OperationChangePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # The operation-change info also carries the execution ARN, but per the
        # requirement we capture it from invocation-start (consistent with the
        # other plugin handlers) and reuse it here.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        for op_id, op in info.updated_operations.items():
            if op.operation_type.name != "STEP":
                continue
            _emit(
                {
                    "plugin": "CONFPLUGIN",
                    "hook": "operation-change",
                    "op": op_id,
                    "status": op.status.name,
                    "in_full_map": op_id in info.operations,
                },
                self._execution_arn,
            )


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[OperationChangePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
