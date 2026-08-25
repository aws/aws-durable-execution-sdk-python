"""10-22: Operation-change hook info field shape.

A named step succeeds once. For every step in the hook's updated-operation map,
the plugin emits hook-level counts and a canonical dump of that delta item.
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
    OperationChangeInfo,
    OperationInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    if execution_arn is not None:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


def _add_operation_fields(record: dict[str, Any], info: OperationInfo) -> None:
    record.update(
        {
            "id": info.operation_id,
            "type": info.operation_type.name,
            "status": info.status.name,
            "isReplay": info.is_replayed,
        }
    )
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


class OperationChangeShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        for operation_id, operation in info.updated_operations.items():
            if operation.operation_type.name != "STEP":
                continue
            record: dict[str, Any] = {
                "plugin": "CONFPLUGIN",
                "hook": "operation-change",
                "updatedOperationsCount": len(info.updated_operations),
                "operationsCount": len(info.operations),
                "inFullMap": operation_id in info.operations,
            }
            if info.execution_arn is not None:
                record["executionArn"] = info.execution_arn
            _add_operation_fields(record, operation)
            _emit(record, self._execution_arn)


@durable_step
def greet(_step_context: StepContext) -> str:
    return "task-a"


@durable_execution(plugins=[OperationChangeShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(), name="greet")
    return result
