"""10-9: Plugin observes an externally-updated wait on re-invocation.

Python does NOT surface externally-updated operations on the invocation-start
info. Instead, when a suspended execution is re-invoked, the SDK routes an
operation that changed while the execution was suspended (its
UpdatedOperationIds) through ``on_operation_update`` -> ``on_operation_end``
during replay (see ExecutionState.emit_operation_update_hook /
DurableContext replay gating). The requirement explicitly allows emitting the
``updated-on-invoke`` record from whichever hook carries this semantic, so this
handler emits it from ``on_operation_end`` for wait-type operations.

A 2-second ``context.wait`` always suspends after checkpointing WaitStarted and
completes externally (the timer fires between invocations), so the wait's
terminal end only ever fires on the replay invocation via the external-update
path — never on the first invocation. The ``first`` flag is captured from the
invocation-start hook of the same invocation, so it is naturally False on replay.
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
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class ExternalUpdatePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self._execution_arn: str | None = None
        self._is_first: bool = False

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn
        self._is_first = info.is_first_invocation

    def on_operation_end(self, info: OperationEndInfo) -> None:
        # For a 2s wait, on_operation_end fires exclusively via the
        # external-update path on the replay invocation (the wait never
        # completes in-process), so this record is the externally-updated signal.
        if info.operation_type.name != "WAIT":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "updated-on-invoke",
                "op": info.operation_id,
                "status": info.status.name,
                "first": self._is_first,
            },
            self._execution_arn,
        )


@durable_execution(plugins=[ExternalUpdatePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return "Wait completed"
