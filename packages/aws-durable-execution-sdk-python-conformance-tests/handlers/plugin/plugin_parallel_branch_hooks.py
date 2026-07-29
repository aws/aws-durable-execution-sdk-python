"""10-12: Plugin user-function hooks for parallel branches.

Two parallel branches each return a constant string directly (no inner step),
with max-concurrency 1 so they run sequentially in index order. The plugin emits
from the SDK's real ``on_user_function_start`` / ``on_user_function_end`` hooks,
filtering to parallel-branch sub-type operations. Each record carries the branch
operation id and its parent (the parallel operation id). These hooks run on the
same thread as the branch function, so start-before-end per branch is
deterministic.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


def _is_branch(info: UserFunctionStartInfo | UserFunctionEndInfo) -> bool:
    return info.sub_type is not None and info.sub_type.name == "PARALLEL_BRANCH"


class ParallelBranchPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # User-function hooks do not carry the execution ARN, so capture it from
        # the invocation-start hook and reuse it for later emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if not _is_branch(info):
            return
        parent = info.parent_id if info.parent_id else "NONE"
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "fn-start",
                "op": info.operation_id,
                "parent": parent,
            },
            self._execution_arn,
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if not _is_branch(info):
            return
        parent = info.parent_id if info.parent_id else "NONE"
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "fn-end",
                "op": info.operation_id,
                "parent": parent,
                "outcome": info.outcome.name,
            },
            self._execution_arn,
        )


def branch0(_ctx: DurableContext) -> str:
    return "task-1"


def branch1(_ctx: DurableContext) -> str:
    return "task-2"


@durable_execution(plugins=[ParallelBranchPlugin()])
def handler(_event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1],
        name="parallel",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
