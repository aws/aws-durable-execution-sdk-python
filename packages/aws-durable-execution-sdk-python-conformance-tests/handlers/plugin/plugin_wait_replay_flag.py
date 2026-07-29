"""10-19: Plugin replay flag for a non-terminal wait.

The parallel operation "waits" runs two branches concurrently (max-concurrency
2): branch 0 waits 2 seconds then returns "short-done"; branch 1 waits 8 seconds
then returns "long-done". Both waits checkpoint WaitStarted in the first
invocation, so the plugin observes ``operation-start`` with ``replay=false``
twice. When the 2-second wait completes the execution is re-invoked while the
8-second wait is still NON-terminal, so the plugin observes it via the SDK's real
``on_operation_start`` hook with ``is_replayed`` (``replay``) true. Each wait
reaches a terminal SUCCEEDED end exactly once.

The plugin filters to wait-type operations and emits from the SDK's real
``on_operation_start`` / ``on_operation_end`` hooks. Operation ids are
deliberately not logged: branch event ids are nondeterministic under concurrency,
and the wait type + replay flag alone identify the behavior under test.
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
                "replay": info.is_replayed,
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
                "status": info.status.name,
            },
            self._execution_arn,
        )


def wait_short(ctx: DurableContext) -> str:
    ctx.wait(Duration.from_seconds(2))
    return "short-done"


def wait_long(ctx: DurableContext) -> str:
    ctx.wait(Duration.from_seconds(8))
    return "long-done"


@durable_execution(plugins=[WaitReplayFlagPlugin()])
def handler(_event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [wait_short, wait_long],
        name="waits",
        config=ParallelConfig(max_concurrency=2),
    )
    return result.get_results()
