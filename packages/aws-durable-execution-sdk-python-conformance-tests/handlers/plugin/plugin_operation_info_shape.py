"""10-20: Operation hook info field shape.

A named step succeeds once. The plugin emits canonical camelCase fields directly
from each operation hook info object and omits only fields that are unset.
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
    InvocationStartInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    if execution_arn is not None:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


def _operation_record(hook: str, info: OperationInfo) -> dict[str, Any]:
    record: dict[str, Any] = {
        "plugin": "CONFPLUGIN",
        "hook": hook,
        "id": info.operation_id,
        "type": info.operation_type.name,
        "status": info.status.name,
        "isReplay": info.is_replayed,
    }
    if info.name is not None:
        record["name"] = info.name
    if info.sub_type is not None:
        record["subType"] = info.sub_type.value
    if info.parent_id is not None:
        record["parentId"] = info.parent_id
    if info.start_time is not None:
        record["startTimestamp"] = info.start_time.isoformat()
    if info.end_time is not None:
        record["endTimestamp"] = info.end_time.isoformat()
    if info.result is not None:
        record["result"] = info.result
    if info.error is not None and info.error.message is not None:
        record["error"] = info.error.message
    if info.attempt is not None:
        record["attempt"] = info.attempt
    return record


class OperationInfoShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(_operation_record("operation-start", info), self._execution_arn)

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(_operation_record("operation-end", info), self._execution_arn)


@durable_step
def greet(_step_context: StepContext) -> str:
    return "task-a"


@durable_execution(plugins=[OperationInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(), name="greet")
    return result
