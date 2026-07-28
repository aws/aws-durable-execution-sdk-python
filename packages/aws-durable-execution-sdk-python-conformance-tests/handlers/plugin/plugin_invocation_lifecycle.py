"""10-1: Plugin invocation lifecycle hooks (start and end on a single invocation).

Registers an instrumentation plugin through the SDK's real ``plugins=[...]``
parameter on ``durable_execution``. The plugin emits its lines from the SDK's
``on_invocation_start`` / ``on_invocation_end`` hooks; the step body logs its
running line via the SDK-provided step context logger (mirrors handler 1-7).
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


class LifecyclePlugin(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "invocation-start",
                "first": info.is_first_invocation,
            },
            info.execution_arn,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        _emit(
            {"plugin": "CONFPLUGIN", "hook": "invocation-end", "status": status},
            info.execution_arn,
        )


@durable_step
def greet(step_context: StepContext, name: str) -> str:
    step_context.logger.info(f"Greeting step running for: {name}")
    return f"Hello, {name}!"


@durable_execution(plugins=[LifecyclePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
