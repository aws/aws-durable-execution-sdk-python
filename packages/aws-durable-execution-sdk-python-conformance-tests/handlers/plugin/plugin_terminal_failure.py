"""10-7: Plugin invocation-end hook receives FAILED status when execution fails.

Uses the SDK's real no-retry strategy (``RetryPresets.none()``, mirrors handler
1-19) on a step that always throws, so the execution terminates as FAILED. The
plugin emits its lines from the invocation-start / invocation-end hooks; the
terminal invocation-end reports the FAILED status.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
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
from aws_durable_execution_sdk_python.retries import RetryPresets


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class TerminalFailurePlugin(DurableInstrumentationPlugin):
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
def failing_step(_step_context: StepContext) -> str:
    msg = "Something went wrong"
    raise RuntimeError(msg)


@durable_execution(plugins=[TerminalFailurePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(
        failing_step(),
        config=StepConfig(retry_strategy=RetryPresets.none()),
    )
    return result
