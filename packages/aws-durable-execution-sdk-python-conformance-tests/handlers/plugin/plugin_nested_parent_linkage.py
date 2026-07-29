"""10-11: Plugin parent linkage for nested operations.

A child context (``run_in_child_context``) wraps a single step. The plugin emits
from the SDK's real ``on_operation_end`` hook for every operation that reaches a
terminal status, reporting the operation id and its parent id (the literal
string NONE when the info carries no parent). The inner step's parent is the
child context's operation id; the child context itself has no parent.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
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


class ParentLinkagePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation hooks do not carry the execution ARN, so capture it from the
        # invocation-start hook and reuse it for later operation emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_end(self, info: OperationEndInfo) -> None:
        parent = info.parent_id if info.parent_id else "NONE"
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-end",
                "op": info.operation_id,
                "parent": parent,
                "status": info.status.name,
            },
            self._execution_arn,
        )


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_with_child_context
def child_operation(ctx: DurableContext, name: str) -> str:
    return ctx.step(greet(name))


@durable_execution(plugins=[ParentLinkagePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(child_operation(str(event)))
    return result
