"""10-16: Plugin invocation-end on suspension.

A single 2-second wait suspends the execution on the first invocation and
completes on replay. The plugin emits from the SDK's real ``on_invocation_start``
/ ``on_invocation_end`` hooks. It classifies each invocation-end as terminal
(SUCCEEDED/FAILED) or not — the suspending invocation reports a non-terminal
status (e.g. PENDING) and the replay reports the terminal SUCCEEDED.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
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


class SuspensionPlugin(DurableInstrumentationPlugin):
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
        terminal = status in ("SUCCEEDED", "FAILED")
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "invocation-end",
                "terminal": terminal,
                "status": status,
            },
            info.execution_arn,
        )


@durable_execution(plugins=[SuspensionPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return "Wait completed"
