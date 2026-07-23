"""10-4: Plugin exceptions are swallowed and never affect the execution outcome.

Every plugin hook first logs its line and then raises. The SDK is expected to
catch and ignore every plugin exception, so the execution result and history are
identical to running without the plugin. Operation/attempt hooks filter to
STEP-type operations. No mocking: the isolation guarantee under test is the
SDK's own hook-dispatch try/except, which we exercise by genuinely raising.
"""

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


class FaultyPlugin(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        print("CONFPLUGIN faulty invocation-start", flush=True)
        raise RuntimeError("faulty invocation-start")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        print("CONFPLUGIN faulty invocation-end", flush=True)
        raise RuntimeError("faulty invocation-end")

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if not _is_step(info):
            return
        print("CONFPLUGIN faulty operation-start", flush=True)
        raise RuntimeError("faulty operation-start")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if not _is_step(info):
            return
        print("CONFPLUGIN faulty operation-end", flush=True)
        raise RuntimeError("faulty operation-end")

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if not _is_step(info):
            return
        print("CONFPLUGIN faulty attempt-start", flush=True)
        raise RuntimeError("faulty attempt-start")

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if not _is_step(info):
            return
        print("CONFPLUGIN faulty attempt-end", flush=True)
        raise RuntimeError("faulty attempt-end")


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[FaultyPlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
