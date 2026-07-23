"""10-2: Plugin operation lifecycle hooks (step start and terminal end).

The plugin emits its lines from the SDK's real ``on_operation_start`` /
``on_operation_end`` hooks, filtering to STEP-type operations only via the
``OperationType`` enum reported on the hook info.
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
    OperationEndInfo,
    OperationStartInfo,
)


def _is_step(info: OperationStartInfo | OperationEndInfo) -> bool:
    return info.operation_type.name == "STEP"


class OperationLifecyclePlugin(DurableInstrumentationPlugin):
    def on_operation_start(self, info: OperationStartInfo) -> None:
        if not _is_step(info):
            return
        print("CONFPLUGIN operation-start", flush=True)

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if not _is_step(info):
            return
        status = info.status.name if info.status is not None else "NONE"
        print(f"CONFPLUGIN operation-end status={status}", flush=True)


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[OperationLifecyclePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
