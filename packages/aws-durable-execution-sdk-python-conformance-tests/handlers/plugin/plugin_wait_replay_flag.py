"""10-18: Plugin replay flag for a non-terminal wait.

The parallel operation "waits" runs two branches concurrently (max-concurrency
2): branch 0 runs a wait named "short" of 2 seconds then returns "short-done";
branch 1 runs a wait named "long" of 8 seconds then returns "long-done". Each
wait is given its stable name via the SDK's real operation naming parameter
(``context.wait(duration, name=...)``), so records can be correlated to a
specific wait even though branch event ids are nondeterministic under
concurrency.

Both waits checkpoint WaitStarted in the first invocation, so the plugin
observes ``operation-start`` with ``replay=false`` once per wait name. When the
2-second wait completes the execution is re-invoked while the 8-second wait is
still NON-terminal, so the plugin observes the named "long" wait via the SDK's
real ``on_operation_start`` hook with ``is_replayed`` (``replay``) true. The
terminal "short" wait must not emit a replayed start. Each wait reaches a
terminal SUCCEEDED end exactly once.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, ParallelConfig
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


class WaitReplayFlagPlugin(DurableInstrumentationPlugin):
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
                "type": info.operation_type.name,
                "name": info.name,
                "replay": info.is_replayed,
                # Non-terminal at hook time, from the hook info's own operation
                # state (no end timestamp yet) — no cross-invocation state.
                "pending": info.end_time is None,
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
                "type": info.operation_type.name,
                "name": info.name,
                "status": info.status.name,
            },
            self._execution_arn,
        )


def wait_short(ctx: DurableContext) -> str:
    ctx.wait(Duration.from_seconds(2), name="short")
    return "short-done"


def wait_long(ctx: DurableContext) -> str:
    ctx.wait(Duration.from_seconds(8), name="long")
    return "long-done"


@durable_execution(plugins=[WaitReplayFlagPlugin()])
def handler(_event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [wait_short, wait_long],
        name="waits",
        config=ParallelConfig(max_concurrency=2),
    )
    return result.get_results()
