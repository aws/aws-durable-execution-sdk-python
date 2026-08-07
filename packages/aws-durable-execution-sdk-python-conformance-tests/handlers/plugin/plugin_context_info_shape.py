"""10-23: Context-typed hook info field shape (interface-shape probe).

A parallel operation named "ctx" with max-concurrency 1 and two branches:
branch A runs a step named "inner" returning "x", then a 2-second wait, and
returns "a-done"; branch B returns "b-done" directly. The plugin filters to
CONTEXT-type operations and, from the SDK's real ``on_operation_start`` /
``on_user_function_start`` hooks, logs ONE single-line JSON record per event: a
CANONICAL camelCase DUMP of that hook's OWN info parameter. Unset / unexposed
fields (value None) are OMITTED (a missing key fails its assertion — the honest
parity signal); sub-type tokens are dumped as the SDK reports them (the
``OperationSubType`` enum ``.value``: "Parallel" for the parent, "ParallelBranch"
for each branch).

The operation-start record carries the context operation's ``isReplay`` flag;
the fn-start record carries ``isReplayingChildren`` from the info's own
``is_replay_children`` indicator — true when the context function re-runs so its
checkpointed child operations replay. Attempt-end hooks are NOT probed (end-hook
semantics for a suspending context run are SDK-divergent and out of scope).
Every record also carries a ``durableExecutionArn`` field (ARN captured at
invocation-start, unasserted) so the runner's CloudWatch filter can scope logs
to the execution.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, ParallelConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationStartInfo,
    UserFunctionStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class ContextInfoShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation / user-function hooks do not carry the execution ARN, so
        # capture it from the invocation-start hook and reuse it for later
        # emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_start(self, info: OperationStartInfo) -> None:
        # Filter to CONTEXT-type operations (the parent parallel + its branches).
        if info.operation_type.name != "CONTEXT":
            return
        # Canonical dump of the OperationStartInfo's own field surface. Identity +
        # replay flag are always present; optional fields are emitted only when
        # populated (None -> omitted key = honest missing-field red).
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "operation-start",
            "id": info.operation_id,
            "type": info.operation_type.name.upper(),
            "isReplay": info.is_replayed,
        }
        if info.name is not None:
            record["name"] = info.name
        if info.sub_type is not None:
            # Raw SDK token: OperationSubType enum .value ("Parallel"/"ParallelBranch").
            record["subType"] = info.sub_type.value
        if info.parent_id is not None:
            record["parentId"] = info.parent_id
        if info.status is not None:
            record["status"] = info.status.name
        if info.start_time is not None:
            record["startTimestamp"] = info.start_time.isoformat()
        if info.end_time is not None:
            record["endTimestamp"] = info.end_time.isoformat()
        _emit(record, self._execution_arn)

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        # Filter to CONTEXT-type operations (the branch functions).
        if info.operation_type.name != "CONTEXT":
            return
        # Canonical dump of the UserFunctionStartInfo's own field surface. The
        # children-replay indicator is the probe under test.
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "fn-start",
            "id": info.operation_id,
            "type": info.operation_type.name.upper(),
            "isReplayingChildren": info.is_replay_children,
        }
        if info.name is not None:
            record["name"] = info.name
        if info.sub_type is not None:
            # Raw SDK token: OperationSubType enum .value ("Parallel"/"ParallelBranch").
            record["subType"] = info.sub_type.value
        if info.parent_id is not None:
            record["parentId"] = info.parent_id
        if info.attempt is not None:
            record["attempt"] = info.attempt
        if info.start_time is not None:
            record["startTimestamp"] = info.start_time.isoformat()
        _emit(record, self._execution_arn)


@durable_step
def inner(_step_context: StepContext) -> str:
    return "x"


def branch_a(ctx: DurableContext) -> str:
    ctx.step(inner(), name="inner")
    ctx.wait(Duration.from_seconds(2))
    return "a-done"


def branch_b(_ctx: DurableContext) -> str:
    return "b-done"


@durable_execution(plugins=[ContextInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch_a, branch_b],
        name="ctx",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
