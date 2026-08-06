"""10-20: Operation hook info field shape (interface-shape probe).

A single step named "greet" returns the constant "task-a" and succeeds on the
first attempt. The plugin emits from the SDK's real ``on_operation_start`` /
``on_operation_end`` hooks (filtering to step-type operations) a CANONICAL DUMP
of the CURRENT hook's own info parameter: every field the Python
``OperationInfo`` type exposes is mapped one-to-one to its canonical camelCase
name; unset fields (value None) are OMITTED (a missing key fails its assertion —
the honest parity signal); type tokens are upper-cased, timestamps ISO-8601,
the serialized result the raw serialized string, errors their message string.

Python surface note: ``OperationInfo`` exposes operation_id, operation_type,
sub_type, name, parent_id, start_time, is_replayed, status, end_time, result,
error and attempt, so the full operation-end field set is available.
``status`` / ``startTimestamp`` / ``attempt`` are dumped at operation-start when
populated (STARTED) but NOT asserted — live-first-start population is
legitimately SDK-divergent.
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
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


def _dump_operation(hook: str, info: OperationInfo) -> dict[str, Any]:
    # Canonical dump of an OperationInfo's own field surface. Identity + replay
    # flag + status are always present; optional fields are emitted only when
    # the info populates them (None -> omitted key = honest missing-field red).
    record: dict[str, Any] = {
        "plugin": "CONFPLUGIN",
        "hook": hook,
        "id": info.operation_id,
        "type": info.operation_type.name.upper(),
        "status": info.status.name,
        "isReplay": info.is_replayed,
    }
    if info.name is not None:
        record["name"] = info.name
    if info.sub_type is not None:
        record["subType"] = info.sub_type.name
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
        # Operation hooks do not carry the execution ARN, so capture it from the
        # invocation-start hook and stamp it on later operation emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(_dump_operation("operation-start", info), self._execution_arn)

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(_dump_operation("operation-end", info), self._execution_arn)


@durable_step
def greet(_step_context: StepContext) -> str:
    return "task-a"


@durable_execution(plugins=[OperationInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(), name="greet")
    return result
