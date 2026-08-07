"""10-22: Operation-change hook info field shape (interface-shape probe).

A single step named "greet" returns the constant "task-a" and succeeds on the
first attempt. The plugin implements the SDK's real ``on_operation_change``
hook and, for each step-type operation in the change info's updated-operations
delta, logs ONE single-line JSON record: a CANONICAL DUMP of that DELTA ITEM's
own field surface (a full ``OperationInfo``) plus the hook-level fields. Every
field the item type exposes is mapped one-to-one to its canonical camelCase
name; unset fields (value None) are OMITTED (a missing key fails its assertion —
the honest parity signal). Hook-level fields: ``executionArn`` (the change
info's own ARN), ``updatedOperationsCount`` / ``operationsCount`` (the two map
sizes), and the derived scalar ``inFullMap`` := the item id also appears in the
info's full operations map.

Python surface note: change-delta items are full ``OperationInfo`` objects
(identity + status + payloads), so the item exposes result, end_time, attempt
and an ``is_replayed`` replay indicator; ``OperationChangeInfo`` itself carries
the execution ARN and both operation maps.
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
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class OperationChangeShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation-change hooks carry their own execution ARN, but the
        # top-level durableExecutionArn stamp is captured at invocation-start.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        for op_id, op in info.updated_operations.items():
            if op.operation_type.name != "STEP":
                continue
            # Hook-level fields dumped from the change info's own surface.
            record: dict[str, Any] = {
                "plugin": "CONFPLUGIN",
                "hook": "operation-change",
                "updatedOperationsCount": len(info.updated_operations),
                "operationsCount": len(info.operations),
                "inFullMap": op_id in info.operations,
            }
            if info.execution_arn is not None:
                record["executionArn"] = info.execution_arn
            # Canonical dump of the delta ITEM's own OperationInfo surface.
            record["id"] = op.operation_id
            record["type"] = op.operation_type.name.upper()
            record["status"] = op.status.name
            record["isReplay"] = op.is_replayed
            if op.name is not None:
                record["name"] = op.name
            if op.sub_type is not None:
                record["subType"] = op.sub_type.name
            if op.parent_id is not None:
                record["parentId"] = op.parent_id
            if op.start_time is not None:
                record["startTimestamp"] = op.start_time.isoformat()
            if op.end_time is not None:
                record["endTimestamp"] = op.end_time.isoformat()
            if op.result is not None:
                record["result"] = op.result
            if op.error is not None and op.error.message is not None:
                record["error"] = op.error.message
            if op.attempt is not None:
                record["attempt"] = op.attempt
            _emit(record, self._execution_arn)


@durable_step
def greet(_step_context: StepContext) -> str:
    return "task-a"


@durable_execution(plugins=[OperationChangeShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(), name="greet")
    return result
