"""10-1: Plugin invocation lifecycle hooks (start and end on a single invocation).

Registers an instrumentation plugin through the SDK's real ``plugins=[...]``
parameter on ``durable_execution``. The plugin emits its lines from the SDK's
``on_invocation_start`` / ``on_invocation_end`` hooks; the step body logs its
running line via the SDK-provided step context logger (mirrors handler 1-7).
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
)


class LifecyclePlugin(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        first = str(info.is_first_invocation).lower()
        print(f"CONFPLUGIN invocation-start first={first}", flush=True)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        print(f"CONFPLUGIN invocation-end status={status}", flush=True)


@durable_step
def greet(step_context: StepContext, name: str) -> str:
    step_context.logger.info(f"Greeting step running for: {name}")
    return f"Hello, {name}!"


@durable_execution(plugins=[LifecyclePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
