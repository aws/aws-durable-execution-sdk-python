"""10-5: Multiple registered plugins all receive lifecycle hooks.

Two instrumentation plugins are registered together, in order A then B, through
the SDK's real ``plugins=[...]`` parameter. Each emits its own prefixed lines
from the invocation-start / invocation-end hooks.
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
)


class PluginA(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        print(
            json.dumps({"plugin": "CONFPLUGIN-A", "hook": "invocation-start"}),
            flush=True,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        print(
            json.dumps(
                {"plugin": "CONFPLUGIN-A", "hook": "invocation-end", "status": status}
            ),
            flush=True,
        )


class PluginB(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        print(
            json.dumps({"plugin": "CONFPLUGIN-B", "hook": "invocation-start"}),
            flush=True,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        print(
            json.dumps(
                {"plugin": "CONFPLUGIN-B", "hook": "invocation-end", "status": status}
            ),
            flush=True,
        )


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[PluginA(), PluginB()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
