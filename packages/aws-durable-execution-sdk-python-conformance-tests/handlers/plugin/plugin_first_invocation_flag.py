"""10-6: Plugin sees is-first-invocation true once, then false on replay.

Uses the SDK's real ``context.wait`` (mirrors handler 2-1) so the execution
suspends on the first invocation and replays after the timer completes. The
plugin emits its lines from the invocation-start / invocation-end hooks; the
terminal invocation-end (SUCCEEDED) fires on the replay invocation.
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


class FirstInvocationPlugin(DurableInstrumentationPlugin):
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


@durable_execution(plugins=[FirstInvocationPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return "Wait completed"
