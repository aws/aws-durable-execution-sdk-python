"""10-10: Plugin operation hooks for a wait operation.

The plugin emits from the SDK's real ``on_operation_start`` / ``on_operation_end``
hooks, filtering to wait-type operations only. ``operation_type.name`` is already
the upper-case token (WAIT). The wait's STARTED checkpoint fires operation-start
on the first invocation; the wait's terminal SUCCEEDED status fires operation-end
on the replay invocation (via the external-update path). operation-start may fire
again as a replayed START, so it is asserted at-least-once.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class WaitOperationPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation hooks do not carry the execution ARN, so capture it from the
        # invocation-start hook and reuse it for later operation emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "WAIT":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-start",
                "op": info.operation_id,
                "type": info.operation_type.name,
            },
            self._execution_arn,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "WAIT":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-end",
                "op": info.operation_id,
                "type": info.operation_type.name,
                "status": info.status.name,
            },
            self._execution_arn,
        )


@durable_execution(plugins=[WaitOperationPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return "Wait completed"
