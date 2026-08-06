"""10-22: Operation-change hook info field shape (interface-shape probe).

A single step named "greet" returns the constant "task-a" and succeeds on the
first attempt. The plugin implements the SDK's real ``on_operation_change``
hook and, for each step-type operation in the change info's updated-operations
delta, probes the DELTA ITEM's own field surface. Every logged field is read
from the CURRENT hook's own info parameter (the change info and its delta
items) — never reconstructed from another hook or from plugin state. When the
Python type does not expose a field, the plugin logs the corresponding
``has_*`` flag as false; that omission is the honest signal of a missing API
surface.

Python surface note: change-delta items are full ``OperationInfo`` objects
(identity + status + payloads), so the item exposes result, end_time, attempt
and an ``is_replayed`` replay indicator; ``OperationChangeInfo`` itself carries
the execution ARN.
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
        # durableExecutionArn stamping is captured at invocation-start; has_arn
        # below is probed from the change info's OWN execution_arn field.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        for op_id, op in info.updated_operations.items():
            if op.operation_type.name != "STEP":
                continue
            _emit(
                {
                    "plugin": "CONFPLUGIN",
                    "hook": "operation-change",
                    "op": op_id,
                    "status": op.status.name,
                    "in_full_map": op_id in info.operations,
                    # has_arn probes whether the change info itself carries the
                    # execution ARN (its own field, read directly here).
                    "has_arn": info.execution_arn is not None,
                    "item_name": op.name,
                    "item_type": op.operation_type.name.upper(),
                    "item_has_result": op.result is not None,
                    "item_has_end_time": op.end_time is not None,
                    "item_has_attempt": op.attempt is not None,
                    # A replay indicator field exists on the item type itself.
                    "item_has_replay": hasattr(op, "is_replayed"),
                },
                self._execution_arn,
            )


@durable_step
def greet(_step_context: StepContext) -> str:
    return "task-a"


@durable_execution(plugins=[OperationChangeShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(), name="greet")
    return result
