"""10-23: Context-typed hook info field shape.

A serialised two-branch parallel operation suspends inside branch-a, causing its
function to run again and replay its children. The plugin dumps context operation
and user-function start info directly, including the SDK's children-replay flag.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python import BatchResult
from aws_durable_execution_sdk_python.config import (
    Duration,
    ParallelBranch,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationInfo,
    OperationStartInfo,
    UserFunctionStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    if execution_arn is not None:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


def _context_record(hook: str, info: OperationInfo) -> dict[str, Any]:
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
    if info.attempt is not None:
        record["attempt"] = info.attempt
    return record


class ContextInfoShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "CONTEXT":
            return
        _emit(_context_record("operation-start", info), self._execution_arn)

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if info.operation_type.name != "CONTEXT":
            return
        record = _context_record("fn-start", info)
        record["isReplayingChildren"] = info.is_replay_children
        _emit(record, self._execution_arn)


@durable_step
def inner(_step_context: StepContext) -> str:
    return "x"


def branch_a(context: DurableContext) -> str:
    context.step(inner(), name="inner")
    context.wait(Duration.from_seconds(2))
    return "a-done"


def branch_b(_context: DurableContext) -> str:
    return "b-done"


@durable_execution(plugins=[ContextInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> list[str]:
    result: BatchResult[str] = context.parallel(
        [
            ParallelBranch(func=branch_a, name="branch-a"),
            ParallelBranch(func=branch_b, name="branch-b"),
        ],
        name="ctx",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
