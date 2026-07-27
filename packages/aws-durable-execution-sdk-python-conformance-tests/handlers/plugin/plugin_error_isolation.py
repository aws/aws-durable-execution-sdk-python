"""10-4: Plugin exceptions are swallowed and never affect the execution outcome.

Every plugin hook first logs its line and then raises. The SDK is expected to
catch and ignore every plugin exception, so the execution result and history are
identical to running without the plugin. Operation/attempt hooks filter to
STEP-type operations. No mocking: the isolation guarantee under test is the
SDK's own hook-dispatch try/except, which we exercise by genuinely raising.
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
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)


def _is_step(info: Any) -> bool:
    return info.operation_type.name == "STEP"


def _emit(hook: str, execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    record: dict[str, Any] = {"plugin": "CONFPLUGIN-FAULTY", "hook": hook}
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class FaultyPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation/attempt hooks do not carry the execution ARN, so capture it
        # from the invocation-start hook and reuse it for later emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        # Capture BEFORE raising so operation/attempt hooks can still emit the ARN.
        self._execution_arn = info.execution_arn
        _emit("invocation-start", info.execution_arn)
        raise RuntimeError("faulty invocation-start")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        _emit("invocation-end", info.execution_arn)
        raise RuntimeError("faulty invocation-end")

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if not _is_step(info):
            return
        _emit("operation-start", self._execution_arn)
        raise RuntimeError("faulty operation-start")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if not _is_step(info):
            return
        _emit("operation-end", self._execution_arn)
        raise RuntimeError("faulty operation-end")

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if not _is_step(info):
            return
        _emit("attempt-start", self._execution_arn)
        raise RuntimeError("faulty attempt-start")

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if not _is_step(info):
            return
        _emit("attempt-end", self._execution_arn)
        raise RuntimeError("faulty attempt-end")


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[FaultyPlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
