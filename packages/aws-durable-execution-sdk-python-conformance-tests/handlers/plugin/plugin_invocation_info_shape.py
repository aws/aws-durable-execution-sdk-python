"""10-19: Invocation hook info field shape.

A two-second durable wait forces one suspension and replay. Each hook logs a
canonical camelCase dump built only from that hook's own info object; optional
fields are omitted rather than reconstructed.
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
    if execution_arn is not None:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class InvocationInfoShapePlugin(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "invocation-start",
            "isFirstInvocation": info.is_first_invocation,
            "operationsCount": len(info.operations),
            "updatedOperationsCount": len(info.updated_operations),
        }
        if info.request_id is not None:
            record["requestId"] = info.request_id
        if info.execution_start_time is not None:
            record["executionStartTimestamp"] = info.execution_start_time.isoformat()
        _emit(record, info.execution_arn)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "invocation-end",
            "isFirstInvocation": info.is_first_invocation,
            "operationsCount": len(info.operations),
            "status": status,
            "terminal": status in ("SUCCEEDED", "FAILED"),
        }
        if info.request_id is not None:
            record["requestId"] = info.request_id
        if info.execution_start_time is not None:
            record["executionStartTimestamp"] = info.execution_start_time.isoformat()
        if info.error is not None and info.error.message is not None:
            record["executionError"] = info.error.message
        _emit(record, info.execution_arn)


@durable_execution(plugins=[InvocationInfoShapePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return f"done-{event}"
