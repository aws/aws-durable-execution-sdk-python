"""10-5: Multiple registered plugins all receive lifecycle hooks.

Two instrumentation plugins are registered together, in order A then B, through
the SDK's real ``plugins=[...]`` parameter, which takes their factories. Each
emits its own prefixed lines from the invocation-start / invocation-end hooks.
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


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class PluginA(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        _emit(
            {"plugin": "CONFPLUGIN-A", "hook": "invocation-start"},
            info.execution_arn,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        _emit(
            {"plugin": "CONFPLUGIN-A", "hook": "invocation-end", "status": status},
            info.execution_arn,
        )


class PluginAFactory:
    """Builds one :class:`PluginA` for each invocation.

    ``durable_execution(plugins=[...])`` takes factory objects whose
    ``create_plugin`` the SDK calls once per invocation. A bare callable is
    rejected while the handler is being initialized, so registering one would
    stop this handler from importing. This factory exists only to construct the
    plugin.
    """

    def create_plugin(self, info: InvocationStartInfo) -> PluginA:
        """Return this invocation's plugin. ``info`` is unused."""
        return PluginA()


class PluginB(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        _emit(
            {"plugin": "CONFPLUGIN-B", "hook": "invocation-start"},
            info.execution_arn,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        _emit(
            {"plugin": "CONFPLUGIN-B", "hook": "invocation-end", "status": status},
            info.execution_arn,
        )


class PluginBFactory:
    """Builds one :class:`PluginB` for each invocation.

    ``durable_execution(plugins=[...])`` takes factory objects whose
    ``create_plugin`` the SDK calls once per invocation. A bare callable is
    rejected while the handler is being initialized, so registering one would
    stop this handler from importing. This factory exists only to construct the
    plugin.
    """

    def create_plugin(self, info: InvocationStartInfo) -> PluginB:
        """Return this invocation's plugin. ``info`` is unused."""
        return PluginB()


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[PluginAFactory(), PluginBFactory()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
