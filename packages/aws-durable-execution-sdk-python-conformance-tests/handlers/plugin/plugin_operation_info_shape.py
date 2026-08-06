"""10-20: Operation hook info field shape (interface-shape probe).

A single step named "greet" returns the constant "task-a" and succeeds on the
first attempt. The plugin emits from the SDK's real ``on_operation_start`` /
``on_operation_end`` hooks, filtering to step-type operations. Every logged
field is read from the CURRENT hook's own info parameter — never reconstructed
from another hook or from plugin state. When the Python ``OperationInfo`` type
does not expose a field, the plugin logs the corresponding ``has_*`` flag as
false; that omission is the honest signal of a missing API surface.

Python surface note: ``OperationInfo`` exposes operation_id, operation_type,
name, start_time, is_replayed, status, end_time, result, error and attempt, so
the full operation-end field set is available. ``has_status`` is emitted at
operation-start for observability but not asserted (status population on a live
first start varies by SDK).
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
    OperationStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


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
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-start",
                "op": info.operation_id,
                "name": info.name,
                "type": info.operation_type.name.upper(),
                "replay": info.is_replayed,
                "has_start_time": info.start_time is not None,
                "has_status": info.status is not None,
            },
            self._execution_arn,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        status = info.status.name if info.status is not None else "NONE"
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "operation-end",
            "op": info.operation_id,
            "name": info.name,
            "type": info.operation_type.name.upper(),
            "replay": info.is_replayed,
            "status": status,
            "has_result": info.result is not None,
            "has_error": info.error is not None,
            "attempt": info.attempt,
            "has_end_time": info.end_time is not None,
        }
        # Include the checkpointed serialized result exactly as exposed on the
        # info; omit the key entirely when the info carries no result value.
        if info.result is not None:
            record["result"] = info.result
        _emit(record, self._execution_arn)


@durable_step
def greet(_step_context: StepContext) -> str:
    return "task-a"


@durable_execution(plugins=[OperationInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(), name="greet")
    return result
